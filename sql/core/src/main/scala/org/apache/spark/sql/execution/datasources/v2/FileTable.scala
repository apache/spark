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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.Utils

abstract class FileTable extends Table with SupportsRead with SupportsWrite {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected val sparkSession: SparkSession
  protected val paths: Seq[String]
  val dataSchema: StructType
  val partitionSchema: StructType

  override lazy val schema: StructType = {
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    SchemaUtils.checkColumnNameDuplication(dataSchema.fieldNames,
      "in the data schema", caseSensitive)
    dataSchema.foreach { field =>
      if (!supportsDataType(field.dataType)) {
        throw new AnalysisException(
          s"$formatName data source does not support ${field.dataType.catalogString} data type.")
      }
    }
    SchemaUtils.checkColumnNameDuplication(partitionSchema.fieldNames,
      "in the partition schema", caseSensitive)
    StructType(dataSchema ++ partitionSchema)
  }
  override lazy val name: String = {
    val name = formatName + " " + paths.map(qualifiedPathName).mkString(",")
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, name)
  }

  protected val fileIndexGetter: () => PartitioningAwareFileIndex
  lazy val fileIndex = fileIndexGetter.apply()

  private def qualifiedPathName(path: String): String = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
    hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
  }

  override def partitioning: Array[Transform] = partitionSchema.map(_.name).asTransforms

  override def capabilities: java.util.Set[TableCapability] = FileTable.CAPABILITIES

  /**
   * Returns whether this format supports the given [[DataType]] in read/write path.
   * By default all data types are supported.
   */
  def supportsDataType(dataType: DataType): Boolean = true

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def formatName(): String = "ORC"
   * }}}
   */
  def formatName: String

  /**
   * Returns a V1 [[FileFormat]] class of the same file data source.
   * This is a solution for the following cases:
   * 1. File datasource V2 implementations cause regression. Users can disable the problematic data
   *    source via SQL configuration and fall back to FileFormat.
   * 2. Catalog support is required, which is still under development for data source V2.
   */
  def fallbackFileFormat: Class[_ <: FileFormat]
}

object FileTable {
  private val CAPABILITIES = Set(BATCH_READ, BATCH_WRITE, TRUNCATE).asJava
}
