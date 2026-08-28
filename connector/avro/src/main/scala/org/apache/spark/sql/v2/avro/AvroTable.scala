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
package org.apache.spark.sql.v2.avro

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.{AvroOptions, AvroUtils}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class AvroTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def newScanBuilder(options: CaseInsensitiveStringMap): AvroScanBuilder =
    AvroScanBuilder(sparkSession, fileIndex, schema, dataSchema, mergedOptions(options))

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    AvroUtils.inferSchema(sparkSession, options.asScala.toMap, files)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder {
      override def build(): Write =
        AvroWrite(paths, formatName, supportsDataType, mergedWriteInfo(info))
    }
  }

  override def supportsDataType(dataType: DataType): Boolean = AvroUtils.supportsDataType(dataType)

  override def formatName: String = "Avro"

  // Avro has no record-level parse verdict: a record is either decodable or the read fails, and
  // there is no mode that drops or rewrites a record based on the columns asked for. The `mode`
  // option in AvroOptions is read by from_avro and schema_of_avro, not by this scan.
  //
  // `positionalFieldMatching` is the exception. AvroPartitionReaderFactory builds the deserializer
  // from the pruned read schema while the Avro side stays the full Avro schema, so under that
  // option catalyst field i of the projection takes Avro field i of that schema, and widening the
  // projection changes the values a column comes back with. Read the option off the map rather than
  // through AvroOptions, whose constructor resolves `avroSchemaUrl` and would do I/O here, and read
  // it leniently so a malformed value still fails where Avro reports it rather than here.
  override protected def supportsScanMerging: Boolean =
    !"true".equalsIgnoreCase(options.get(AvroOptions.POSITIONAL_FIELD_MATCHING))
}
