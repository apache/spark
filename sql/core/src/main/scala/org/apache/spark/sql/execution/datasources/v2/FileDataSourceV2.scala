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

import java.io.FileNotFoundException
import java.util

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkUpgradeException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

/**
 * A base interface for data source v2 implementations of the built-in file-based data sources.
 */
trait FileDataSourceV2 extends TableProvider with DataSourceRegister {
  /**
   * Returns a V1 [[FileFormat]] class of the same file data source.
   * This is a solution for the following cases:
   * 1. File datasource V2 implementations cause regression. Users can disable the problematic data
   *    source via SQL configuration and fall back to FileFormat.
   * 2. Catalog support is required, which is still under development for data source V2.
   */
  def fallbackFileFormat: Class[_ <: FileFormat]

  lazy val sparkSession = SparkSession.active

  protected def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    val paths = Option(map.get("paths")).map { pathStr =>
      FileDataSourceV2.readPathsToSeq(pathStr)
    }.getOrElse(Seq.empty)
    paths ++ Option(map.get("path")).toSeq
  }

  protected def getOptionsWithoutPaths(map: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val withoutPath = map.asCaseSensitiveMap().asScala.filter { case (k, _) =>
      !k.equalsIgnoreCase("path") && !k.equalsIgnoreCase("paths")
    }
    new CaseInsensitiveStringMap(withoutPath.toMap.asJava)
  }

  protected def getTableName(map: CaseInsensitiveStringMap, paths: Seq[String]): String = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(
      map.asCaseSensitiveMap().asScala.toMap)
    val name = shortName() + " " + paths.map(qualifiedPathName(_, hadoopConf)).mkString(",")
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, name)
  }

  private def qualifiedPathName(path: String, hadoopConf: Configuration): String = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
  }

  // TODO: To reduce code diff of SPARK-29665, we create stub implementations for file source v2, so
  //       that we don't need to touch all the file source v2 classes. We should remove the stub
  //       implementation and directly implement the TableProvider APIs.
  protected def getTable(options: CaseInsensitiveStringMap): Table
  protected def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    throw QueryExecutionErrors.unsupportedUserSpecifiedSchemaError()
  }

  override def supportsExternalMetadata(): Boolean = true

  private var t: Table = null

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    if (t == null) t = getTable(options)
    t.columns.asSchema
  }

  // TODO: implement a light-weight partition inference which only looks at the path of one leaf
  //       file and return partition column names. For now the partition inference happens in
  //       `getTable`, because we don't know the user-specified schema here.
  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    Array.empty
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    // If the table is already loaded during schema inference, return it directly.
    if (t != null) {
      t
    } else {
      getTable(new CaseInsensitiveStringMap(properties), schema)
    }
  }
}

object FileDataSourceV2 {
  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private def readPathsToSeq(paths: String): Seq[String] =
    objectMapper.readValue(paths, classOf[Seq[String]])

  def attachFilePath(filePath: => String, ex: Throwable): Throwable = {
    ex match {
      // This is not a file issue, throw it directly and ask users to handle the upgrade issue.
      case sue: SparkUpgradeException =>
        throw sue
      // The error is already FAILED_READ_FILE, throw it directly. To be consistent, schema
      // inference code path throws `FAILED_READ_FILE`, but the file reading code path can reach
      // that code path as well and we should not double-wrap the error.
      case e: SparkException if e.getCondition == "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER" =>
        throw e
      case e: SchemaColumnConvertNotSupportedException =>
        throw QueryExecutionErrors.parquetColumnDataTypeMismatchError(
          filePath, e.getColumn, e.getLogicalType, e.getPhysicalType, e)
      case e: FileNotFoundException =>
        throw QueryExecutionErrors.fileNotExistError(filePath, e)
      case NonFatal(e) =>
        throw QueryExecutionErrors.cannotReadFilesError(e, filePath)
    }
  }
}
