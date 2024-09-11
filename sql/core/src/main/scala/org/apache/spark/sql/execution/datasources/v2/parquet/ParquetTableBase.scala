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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

abstract class ParquetTableBase(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  /**
   * This function should be used by the derived class to create a required ScanBuilder
   *
   * @return ScanBuilder arguments
   */
  protected def newScanBuilderArgs(options: CaseInsensitiveStringMap): (
    SparkSession, PartitioningAwareFileIndex, StructType, StructType, CaseInsensitiveStringMap
    ) =
    (sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    ParquetUtils.inferSchema(sparkSession, options.asScala.toMap, files)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write = ParquetWrite(paths, formatName, supportsDataType, info)
    }

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _ => false
  }

  override def formatName: String = "Parquet"
}
