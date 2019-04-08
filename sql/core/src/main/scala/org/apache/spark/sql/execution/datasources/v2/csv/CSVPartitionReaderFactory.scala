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
package org.apache.spark.sql.execution.datasources.v2.csv

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVHeaderChecker, CSVOptions, UnivocityParser}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.reader.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A factory used to create CSV readers.
 *
 * @param sqlConf SQL configuration.
 * @param broadcastedConf Broadcasted serializable Hadoop Configuration.
 * @param dataSchema Schema of CSV files.
 * @param readDataSchema Required data schema in the batch scan.
 * @param partitionSchema Schema of partitions.
 * @param parsedOptions Options for parsing CSV files.
 */
case class CSVPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    parsedOptions: CSVOptions) extends FilePartitionReaderFactory {
  private val columnPruning = sqlConf.csvColumnPruning

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    val parser = new UnivocityParser(
      StructType(dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
      StructType(readDataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)),
      parsedOptions)
    val schema = if (columnPruning) readDataSchema else dataSchema
    val isStartOfFile = file.start == 0
    val headerChecker = new CSVHeaderChecker(
      schema, parsedOptions, source = s"CSV file: ${file.filePath}", isStartOfFile)
    val iter = CSVDataSource(parsedOptions).readFile(
      conf,
      file,
      parser,
      headerChecker,
      readDataSchema)
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }
}
