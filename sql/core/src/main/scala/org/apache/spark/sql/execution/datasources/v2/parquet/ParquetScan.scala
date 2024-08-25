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
package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ParquetScan(
                        sparkSession: SparkSession,
                        hadoopConf: Configuration,
                        fileIndex: PartitioningAwareFileIndex,
                        dataSchema: StructType,
                        readDataSchema: StructType,
                        readPartitionSchema: StructType,
                        pushedFilters: Array[Filter],
                        options: CaseInsensitiveStringMap,
                        pushedAggregate: Option[Aggregation] = None,
                        partitionFilters: Seq[Expression] = Seq.empty,
                        dataFilters: Seq[Expression] = Seq.empty
                      ) extends ParquetScanBase(
  sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
  partitionFilters, dataFilters
) {
  override def equals(obj: Any): Boolean =
    super.equals(obj)
  override def hashCode(): Int =
    super.hashCode()

  /**
   * Returns a factory to create a [[org.apache.spark.sql.connector.read.PartitionReader]]
   * for each [[org.apache.spark.sql.connector.read.InputPartition]].
   */
  override def createReaderFactory(): PartitionReaderFactory =
    (ParquetPartitionReaderFactory.apply _).tupled(createReaderFactoryArgs())
}
