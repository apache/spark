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
package org.apache.spark.sql.execution.datasources.v2.json

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.json.JsonDataSource
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class JsonTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def newScanBuilder(options: CaseInsensitiveStringMap): JsonScanBuilder =
    new JsonScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = new JSONOptionsInRead(
      options.asScala.toMap,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    JsonDataSource(parsedOptions).inferSchema(
      sparkSession, files, parsedOptions)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new JsonWriteBuilder(paths, formatName, supportsDataType, info)

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }

  override def formatName: String = "JSON"
}
