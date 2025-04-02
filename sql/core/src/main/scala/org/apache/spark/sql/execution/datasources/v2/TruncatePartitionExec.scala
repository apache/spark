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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{SupportsAtomicPartitionManagement, SupportsPartitionManagement}
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * Physical plan node for table partition truncation.
 */
case class TruncatePartitionExec(
    table: SupportsPartitionManagement,
    partSpec: ResolvedPartitionSpec,
    refreshCache: () => Unit) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val isTableAltered = if (table.partitionSchema.length != partSpec.names.length) {
      table match {
        case atomicPartTable: SupportsAtomicPartitionManagement =>
          val partitionIdentifiers = atomicPartTable.listPartitionIdentifiers(
            partSpec.names.toArray, partSpec.ident)
          atomicPartTable.truncatePartitions(partitionIdentifiers)
        case _ =>
          throw QueryExecutionErrors.truncateMultiPartitionUnsupportedError(table.name())
      }
    } else {
      table.truncatePartition(partSpec.ident)
    }
    if (isTableAltered) refreshCache()
    Seq.empty
  }
}
