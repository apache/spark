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

import java.util

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A base interface for data source v2 implementations of the built-in file-based data sources.
 */
trait FileDataSourceV2 extends TableProvider with DataSourceRegister {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /**
   * Returns a V1 [[FileFormat]] class of the same file data source.
   * This is a solution for the following cases:
   * 1. File datasource V2 implementations cause regression. Users can disable the problematic data
   *    source via SQL configuration and fall back to FileFormat.
   * 2. Catalog support is required, which is still under development for data source V2.
   */
  def fallbackFileFormat: Class[_ <: FileFormat]

  lazy val sparkSession = SparkSession.active

  private def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    val objectMapper = new ObjectMapper()
    val paths = Option(map.get("paths")).map { pathStr =>
      objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
    }.getOrElse(Seq.empty)
    paths ++ Option(map.get("path")).toSeq
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // If we need to infer schema, this must be the first time to create file index.
    assert(fileIndex.isEmpty)
    val caseSensitiveMap = options.asCaseSensitiveMap().asScala.toMap
    val paths = getPaths(options)
    fileIndex = Some(createFileIndex(None, paths, caseSensitiveMap))

    val scalaInsensitiveMap = CaseInsensitiveMap(caseSensitiveMap)
    val dataSchema = inferDataSchema(fileIndex.get.allFiles(), scalaInsensitiveMap).getOrElse {
      throw new AnalysisException(
        s"Unable to infer schema for $shortName. It must be specified manually.")
    }
    val partitionSchema = fileIndex.get.partitionSchema

    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    val partitionNameSet: Set[String] =
      partitionSchema.fields.map(PartitioningUtils.getColName(_, caseSensitive)).toSet
    // When data and partition schemas have overlapping columns,
    // tableSchema = dataSchema - overlapSchema + partitionSchema
    val fields = dataSchema.fields.filterNot { field =>
      val colName = PartitioningUtils.getColName(field, caseSensitive)
      partitionNameSet.contains(colName)
    } ++ partitionSchema.fields
    StructType(fields)
  }

  override def inferPartitioning(
      schema: StructType,
      options: CaseInsensitiveStringMap): Array[Transform] = {
    if (fileIndex.isEmpty) {
      fileIndex = Some(createFileIndex(
        Some(schema), getPaths(options), options.asCaseSensitiveMap().asScala.toMap))
    }
    fileIndex.get.partitionSchema.map(_.name).asTransforms
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val caseSensitiveMap = properties.asScala.toMap
    val paths = getPaths(options)
    val partitionCols = partitioning.toSeq.asPartitionColumns

    val resolver = sparkSession.sessionState.conf.resolver
    val dataSchema = StructType(
      schema.filterNot(f => partitionCols.exists(col => resolver(col, f.name)))
    ).asNullable
    val partitionSchema = if (fileIndex.isDefined) {
      // The schema/partitioning has been inferred before.
      fileIndex.get.partitionSchema
    } else {
      StructType(partitionCols.map { partCol =>
        schema.find { f => resolver(f.name, partCol) }.getOrElse {
          throw new IllegalArgumentException("invalid partition column: " + partCol)
        }
      })
    }

    createFileTable(
      paths,
      () => fileIndex.getOrElse(createFileIndex(None, paths, caseSensitiveMap)),
      dataSchema,
      partitionSchema,
      properties)
  }

  /**
   * When possible, this method should return the schema of the given `files`.  When the format
   * does not support inference, or no valid files are given should return None.  In these cases
   * Spark will require that user specify the schema manually.
   */
  protected def inferDataSchema(
      files: Seq[FileStatus],
      options: Map[String, String]): Option[StructType]

  protected def createFileTable(
      paths: Seq[String],
      fileIndexGetter: () => PartitioningAwareFileIndex,
      dataSchema: StructType,
      partitionSchema: StructType,
      tableProps: java.util.Map[String, String]): FileTable

  protected var fileIndex: Option[PartitioningAwareFileIndex] = None

  private def createFileIndex(
      userSpecifiedSchema: Option[StructType],
      paths: Seq[String],
      options: Map[String, String]): PartitioningAwareFileIndex = {
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    if (FileStreamSink.hasMetadata(paths, hadoopConf, sparkSession.sessionState.conf)) {
      // We are reading from the results of a streaming query. We will load files from
      // the metadata log instead of listing them using HDFS APIs.
      new MetadataLogFileIndex(sparkSession, new Path(paths.head), options, userSpecifiedSchema)
    } else {
      // This is a non-streaming file based datasource.
      val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(paths, hadoopConf,
        checkEmptyGlobPath = true, checkFilesExist = true)
      val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
      new InMemoryFileIndex(
        sparkSession, rootPathsSpecified, options, userSpecifiedSchema, fileStatusCache)
    }
  }
}
