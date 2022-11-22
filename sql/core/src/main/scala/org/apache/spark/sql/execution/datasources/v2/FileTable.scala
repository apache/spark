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
import java.util.Objects

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Refreshable, SupportsRead, SupportsWrite, TableCapability, V1Table, V2TableWithOptionalV1Fallback}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, SchemaUtils}

abstract class FileTable(
    sparkSession: SparkSession,
    val options: CaseInsensitiveStringMap,
    val paths: Seq[String],
    val userSpecifiedSchema: Option[StructType],
    override val v1Table: Option[CatalogTable] = None)
  extends V2TableWithOptionalV1Fallback with SupportsRead with SupportsWrite with Refreshable {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  lazy val allOptions = options.asScala.toMap ++
    v1Table.map(t => V1Table.toOptions(V1Table.addV2TableProperties(t))).getOrElse(Map.empty)

  lazy val useCatalogFileIndex = sparkSession.sqlContext.conf.manageFilesourcePartitions &&
    v1Table.isDefined && v1Table.get.tracksPartitionsInCatalog &&
    v1Table.get.partitionColumnNames.nonEmpty

  lazy val fileIndex: FileIndex = {
    if (useCatalogFileIndex) {
      val defaultTableSize = sparkSession.sessionState.conf.defaultSizeInBytes
      new CatalogFileIndex(
        sparkSession,
        v1Table.get,
        v1Table.get.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
    } else {
      val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      // Hadoop Configurations are case sensitive.
      val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
      if (FileStreamSink.hasMetadata(paths, hadoopConf, sparkSession.sessionState.conf)) {
        // We are reading from the results of a streaming query. We will load files from
        // the metadata log instead of listing them using HDFS APIs.
        new MetadataLogFileIndex(sparkSession, new Path(paths.head),
          options.asScala.toMap, userSpecifiedSchema)
      } else {
        // This is a non-streaming file based datasource.
        val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(paths, hadoopConf,
          checkEmptyGlobPath = true, checkFilesExist = v1Table.isEmpty, enableGlobbing = globPaths)
        val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
        new InMemoryFileIndex(
          sparkSession, rootPathsSpecified, caseSensitiveMap, userSpecifiedSchema, fileStatusCache)
      }
    }
  }

  lazy val dataSchema: StructType = {
    if (useCatalogFileIndex) {
      v1Table.get.dataSchema
    } else {
      val schema = userSpecifiedSchema.map { schema =>
        val partitionSchema = fileIndex.partitionSchema
        val resolver = sparkSession.sessionState.conf.resolver
        StructType(schema.filterNot(f => partitionSchema.exists(p => resolver(p.name, f.name))))
      }.orElse {
        inferSchema(fileIndex.asInstanceOf[PartitioningAwareFileIndex].allFiles())
      }.getOrElse {
        throw QueryCompilationErrors.dataSchemaNotSpecifiedError(formatName)
      }
      fileIndex match {
        case _: MetadataLogFileIndex => schema
        case _ => schema.asNullable
      }
    }
  }

  override lazy val schema: StructType = {
    if (useCatalogFileIndex) {
      v1Table.get.schema
    } else {
      val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
      SchemaUtils.checkSchemaColumnNameDuplication(dataSchema, caseSensitive)
      dataSchema.foreach { field =>
        if (!supportsDataType(field.dataType)) {
          throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError(formatName, field)
        }
      }
      val partitionSchema = fileIndex.partitionSchema
      SchemaUtils.checkSchemaColumnNameDuplication(partitionSchema, caseSensitive)

      val partitionNameMap =
        partitionSchema.fields.map(f => PartitioningUtils.getColName(f, caseSensitive) -> f).toMap
      val dataNameSet = dataSchema.fields.map(PartitioningUtils.getColName(_, caseSensitive)).toSet
      // When data and partition schemas have overlapping columns,
      // tableSchema = dataSchema + (partitionSchema - overlapSchema)
      val fields = dataSchema.fields.map { field =>
          val colName = PartitioningUtils.getColName(field, caseSensitive)
          partitionNameMap.getOrElse(colName, field)
        } ++ partitionSchema.fields.filterNot { field =>
          val colName = PartitioningUtils.getColName(field, caseSensitive)
          dataNameSet.contains(colName)
        }
      StructType(fields)
    }
  }

  override def partitioning: Array[Transform] = {
    v1Table.map(V1Table.toV2Partitioning)
      .getOrElse(fileIndex.partitionSchema.names.toSeq.asTransforms)
  }

  override def properties: util.Map[String, String] =
    v1Table.map(t => V1Table.addV2TableProperties(t).asJava)
      .getOrElse(options.asCaseSensitiveMap())

  override def capabilities: java.util.Set[TableCapability] = FileTable.CAPABILITIES

  /**
   * When possible, this method should return the schema of the given `files`.  When the format
   * does not support inference, or no valid files are given should return None.  In these cases
   * Spark will require that user specify the schema manually.
   */
  def inferSchema(files: Seq[FileStatus]): Option[StructType]

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

  /**
   * Whether or not paths should be globbed before being used to access files.
   */
  private def globPaths: Boolean = {
    val entry = options.get(DataSource.GLOB_PATHS_KEY)
    Option(entry).map(_ == "true").getOrElse(true)
  }

  override def refresh: Unit = {
    fileIndex.refresh()
  }

  override def equals(obj: Any): Boolean = obj match {
    case f: FileTable =>
      options == f.options && paths == f.paths &&
        userSpecifiedSchema == f.userSpecifiedSchema

    case _ => false
  }

  override def hashCode(): Int = Objects.hash(options, paths, userSpecifiedSchema)
}

object FileTable {
  private val CAPABILITIES = util.EnumSet.of(BATCH_READ, BATCH_WRITE)
}
