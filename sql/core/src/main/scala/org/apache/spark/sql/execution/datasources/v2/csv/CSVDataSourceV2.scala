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

import java.util

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.csv.{CSVDataSource, CSVFileFormat}
import org.apache.spark.sql.execution.datasources.v2.{FileDataSourceV2, FileTable}
import org.apache.spark.sql.types.StructType

class CSVDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[CSVFileFormat]

  override def shortName(): String = "CSV"

  override protected def inferDataSchema(
      files: Seq[FileStatus], options: Map[String, String]): Option[StructType] = {
    val parsedOptions = new CSVOptions(
      options,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)

    CSVDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
  }

  override protected def createFileTable(
      paths: Seq[String],
      fileIndexGetter: () => PartitioningAwareFileIndex,
      dataSchema: StructType,
      partitionSchema: StructType,
      tableProps: util.Map[String, String]): FileTable = {
    CSVTable(
      sparkSession,
      paths,
      fileIndexGetter,
      dataSchema,
      partitionSchema,
      tableProps,
      fallbackFileFormat)
  }
}
