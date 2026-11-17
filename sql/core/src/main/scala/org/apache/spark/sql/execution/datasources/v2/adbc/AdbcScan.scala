/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.adbc

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

/**
 * Reads the result of `query`, whose first `readSchema.length` columns must match `readSchema`.
 * With range partitioning, each partition adds one of `rangePredicates` to it as a WHERE clause.
 * Otherwise, with driver partitioning, the driver may split the result into partitions.
 */
case class AdbcScan(
    readSchema: StructType,
    query: String,
    options: AdbcOptions,
    rangePredicates: Seq[String] = Seq.empty)
  extends Scan with Batch {

  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    Scan.ColumnarSupportMode.SUPPORTED

  override def toBatch: Batch = this

  // With driver partitioning, the query is executed when Spark first asks for the partitions,
  // and executors must read the descriptors it returned, so they're kept. Spark keeps the
  // physical plan of a DataFrame, so every action on it reads these same descriptors.
  private lazy val partitions: Array[InputPartition] = if (rangePredicates.nonEmpty) {
    rangePredicates.map(p => AdbcQueryPartition(s"$query WHERE $p"): InputPartition).toArray
  } else {
    // The driver doesn't split a read that is range partitioned, even into a single range.
    val descriptors = if (options.driverPartitioning && options.rangePartitioning.isEmpty) {
      AdbcUtils.withConnection(options)(AdbcUtils.executePartitioned(_, query))
    } else {
      None
    }
    descriptors match {
      case Some(ds) => ds.map(AdbcDescriptorPartition(_): InputPartition).toArray
      case None => Array(AdbcQueryPartition(query))
    }
  }

  override def planInputPartitions(): Array[InputPartition] = partitions

  override def createReaderFactory(): PartitionReaderFactory =
    AdbcPartitionReaderFactory(readSchema, options)

  override def description(): String = {
    val ranges = if (rangePredicates.isEmpty) "" else {
      s" [Range predicates: ${rangePredicates.mkString(", ")}]"
    }
    s"${super.description()} [Query: $query]$ranges"
  }
}

/** A partition of an ADBC scan. */
sealed trait AdbcPartition extends InputPartition

/** A partition that executes `query` on the executor. */
case class AdbcQueryPartition(query: String) extends AdbcPartition

/** A partition that reads a partition of a result returned by `executePartitioned`. */
case class AdbcDescriptorPartition(descriptor: Array[Byte]) extends AdbcPartition
