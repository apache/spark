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
package org.apache.spark.sql.execution.datasources.v2.fwf

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.fwf.{FixedWidthOptions, FixedWidthParser}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.fwf.FixedWidthDataSource
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A factory used to create fixed-width readers.
 *
 * @param sqlConf SQL configuration.
 * @param broadcastedConf Broadcasted serializable Hadoop Configuration.
 * @param dataSchema Schema of the fixed-width files.
 * @param readDataSchema Required data schema in the batch scan.
 * @param partitionSchema Schema of partitions.
 * @param colspecs The resolved (never `"infer"`), already column-pruned colspecs to slice each
 *                 line with -- see `FixedWidthDataSource.effectiveColspecs`.
 * @param options Options for parsing fixed-width files.
 * @param filters Pushdown filters, already stripped of any referring to the corrupt-record column.
 */
case class FixedWidthPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    colspecs: Seq[(Int, Int)],
    options: FixedWidthOptions,
    filters: Seq[Filter]) extends FilePartitionReaderFactory {

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    val parser = new FixedWidthParser(dataSchema, readDataSchema, options, filters)
    val iter =
      FixedWidthDataSource.readFile(conf, file, colspecs, parser, options, readDataSchema)
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }
}
