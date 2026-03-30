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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column,
  SupportsPartitionManagement, SupportsRead, SupportsWrite,
  Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo,
  LogicalWriteInfoImpl, SupportsDynamicOverwrite,
  SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.runtime.MetadataLogFileIndex
import org.apache.spark.sql.execution.streaming.sinks.FileStreamSink
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._

abstract class FileTable(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType])
  extends Table with SupportsRead with SupportsWrite
    with SupportsPartitionManagement {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  // Partition column names from partitionBy(). Fallback when
  // fileIndex.partitionSchema is empty (new/empty directory).
  private[v2] var userSpecifiedPartitioning: Seq[String] =
    Seq.empty

  // CatalogTable reference set by V2SessionCatalog.loadTable.
  private[sql] var catalogTable: Option[
    org.apache.spark.sql.catalyst.catalog.CatalogTable
  ] = None

  // When true, use CatalogFileIndex to support custom
  // partition locations. Set by V2SessionCatalog.loadTable.
  private[v2] var useCatalogFileIndex: Boolean = false

  lazy val fileIndex: PartitioningAwareFileIndex = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    // When userSpecifiedSchema is provided (e.g., write path via DataFrame API), the path
    // may not exist yet. Skip streaming metadata check and file existence checks.
    val isStreamingMetadata = userSpecifiedSchema.isEmpty &&
      FileStreamSink.hasMetadata(paths, hadoopConf, sparkSession.sessionState.conf)
    if (isStreamingMetadata) {
      new MetadataLogFileIndex(sparkSession, new Path(paths.head),
        options.asScala.toMap, userSpecifiedSchema)
    } else if (useCatalogFileIndex &&
        catalogTable.exists(_.partitionColumnNames.nonEmpty)) {
      val ct = catalogTable.get
      val stats = sparkSession.sessionState.catalog
        .getTableMetadata(ct.identifier).stats
        .map(_.sizeInBytes.toLong).getOrElse(0L)
      new CatalogFileIndex(sparkSession, ct, stats)
        .filterPartitions(Nil)
    } else {
      val checkFilesExist = userSpecifiedSchema.isEmpty
      val rootPathsSpecified =
        DataSource.checkAndGlobPathIfNecessary(
          paths, hadoopConf,
          checkEmptyGlobPath = checkFilesExist,
          checkFilesExist = checkFilesExist,
          enableGlobbing = globPaths)
      val fileStatusCache =
        FileStatusCache.getOrCreate(sparkSession)
      new InMemoryFileIndex(
        sparkSession, rootPathsSpecified,
        caseSensitiveMap, userSpecifiedSchema,
        fileStatusCache)
    }
  }

  lazy val dataSchema: StructType = {
    val schema = userSpecifiedSchema.map { schema =>
      val partitionSchema = fileIndex.partitionSchema
      val resolver = sparkSession.sessionState.conf.resolver
      StructType(schema.filterNot(f => partitionSchema.exists(p => resolver(p.name, f.name))))
    }.orElse {
      inferSchema(fileIndex.allFiles())
    }.getOrElse {
      throw QueryCompilationErrors.dataSchemaNotSpecifiedError(formatName)
    }
    fileIndex match {
      case _: MetadataLogFileIndex => schema
      case _ => schema.asNullable
    }
  }

  override lazy val schema: StructType = {
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    // Check column name duplication for non-catalog tables.
    // Skip for catalog tables where the analyzer handles
    // ambiguity at query time.
    if (catalogTable.isEmpty) {
      SchemaUtils.checkSchemaColumnNameDuplication(
        dataSchema, caseSensitive)
    }
    dataSchema.foreach { field =>
      if (!supportsDataType(field.dataType)) {
        throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError(formatName, field)
      }
    }
    val partitionSchema = fileIndex.partitionSchema
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

  override def columns(): Array[Column] = {
    val baseSchema = schema
    val conf = sparkSession.sessionState.conf
    if (conf.getConf(SQLConf.FILE_SOURCE_INSERT_ENFORCE_NOT_NULL)
        && catalogTable.isDefined) {
      val catFields = catalogTable.get.schema.fields
        .map(f => f.name -> f).toMap
      val restored = StructType(baseSchema.fields.map { f =>
        catFields.get(f.name) match {
          case Some(cf) =>
            f.copy(nullable = cf.nullable,
              dataType = restoreNullability(
                f.dataType, cf.dataType))
          case None => f
        }
      })
      CatalogV2Util.structTypeToV2Columns(restored)
    } else {
      CatalogV2Util.structTypeToV2Columns(baseSchema)
    }
  }

  private def restoreNullability(
      dataType: DataType,
      catalogType: DataType): DataType = {
    import org.apache.spark.sql.types._
    (dataType, catalogType) match {
      case (ArrayType(et1, _), ArrayType(et2, cn)) =>
        ArrayType(restoreNullability(et1, et2), cn)
      case (MapType(kt1, vt1, _), MapType(kt2, vt2, vcn)) =>
        MapType(restoreNullability(kt1, kt2),
          restoreNullability(vt1, vt2), vcn)
      case (StructType(f1), StructType(f2)) =>
        val catMap = f2.map(f => f.name -> f).toMap
        StructType(f1.map { f =>
          catMap.get(f.name) match {
            case Some(cf) =>
              f.copy(nullable = cf.nullable,
                dataType = restoreNullability(
                  f.dataType, cf.dataType))
            case None => f
          }
        })
      case _ => dataType
    }
  }

  override def partitioning: Array[Transform] = {
    val fromIndex =
      fileIndex.partitionSchema.names.toImmutableArraySeq
    if (fromIndex.nonEmpty) {
      fromIndex.asTransforms
    } else if (userSpecifiedPartitioning.nonEmpty) {
      userSpecifiedPartitioning.asTransforms
    } else {
      catalogTable
        .map(_.partitionColumnNames.toArray
          .toImmutableArraySeq.asTransforms)
        .getOrElse(fromIndex.asTransforms)
    }
  }

  override def properties: util.Map[String, String] = options.asCaseSensitiveMap

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

  /**
   * Merge the options of FileTable and the table operation while respecting the
   * keys of the table operation.
   *
   * @param options The options of the table operation.
   * @return
   */
  protected def mergedOptions(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val finalOptions = this.options.asCaseSensitiveMap().asScala ++
      options.asCaseSensitiveMap().asScala
    new CaseInsensitiveStringMap(finalOptions.asJava)
  }

  /**
   * Merge the options of FileTable and the LogicalWriteInfo while respecting the
   * keys of the options carried by LogicalWriteInfo.
   */
  protected def mergedWriteInfo(writeInfo: LogicalWriteInfo): LogicalWriteInfo = {
    LogicalWriteInfoImpl(
      writeInfo.queryId(),
      writeInfo.schema(),
      mergedOptions(writeInfo.options()),
      writeInfo.rowIdSchema(),
      writeInfo.metadataSchema())
  }

  /**
   * Creates a [[WriteBuilder]] that supports truncate and
   * dynamic partition overwrite for file-based tables.
   */
  protected def createFileWriteBuilder(
      info: LogicalWriteInfo)(
      buildWrite: (LogicalWriteInfo, StructType,
        Map[Map[String, String], String],
        Boolean, Boolean) => Write
  ): WriteBuilder = {
    new WriteBuilder with SupportsDynamicOverwrite with SupportsTruncate {
      private var isDynamicOverwrite = false
      private var isTruncate = false

      override def overwriteDynamicPartitions(): WriteBuilder = {
        isDynamicOverwrite = true
        this
      }

      override def truncate(): WriteBuilder = {
        isTruncate = true
        this
      }

      override def build(): Write = {
        val merged = mergedWriteInfo(info)
        val fromIndex = fileIndex.partitionSchema
        val partSchema =
          if (fromIndex.nonEmpty) {
            fromIndex
          } else if (userSpecifiedPartitioning.nonEmpty) {
            val full = merged.schema()
            StructType(userSpecifiedPartitioning.map { c =>
              full.find(_.name == c).getOrElse(
                throw new IllegalArgumentException(
                  s"Partition column '$c' not found"))
            })
          } else {
            fromIndex
          }
        val customLocs = getCustomPartitionLocations(
          partSchema)
        buildWrite(merged, partSchema,
          customLocs, isDynamicOverwrite, isTruncate)
      }
    }
  }

  private def getCustomPartitionLocations(
      partSchema: StructType
  ): Map[Map[String, String], String] = {
    catalogTable match {
      case Some(ct) if ct.partitionColumnNames.nonEmpty =>
        val outputPath = new Path(paths.head)
        val hadoopConf = sparkSession.sessionState
          .newHadoopConfWithOptions(
            options.asCaseSensitiveMap.asScala.toMap)
        val fs = outputPath.getFileSystem(hadoopConf)
        val qualifiedOutputPath = outputPath.makeQualified(
          fs.getUri, fs.getWorkingDirectory)
        val partitions = sparkSession.sessionState.catalog
          .listPartitions(ct.identifier)
        partitions.flatMap { p =>
          val defaultLocation = qualifiedOutputPath.suffix(
            "/" + PartitioningUtils.getPathFragment(
              p.spec, partSchema)).toString
          val catalogLocation = new Path(p.location)
            .makeQualified(
              fs.getUri, fs.getWorkingDirectory).toString
          if (catalogLocation != defaultLocation) {
            Some(p.spec -> catalogLocation)
          } else {
            None
          }
        }.toMap
      case _ => Map.empty
    }
  }

  // ---- SupportsPartitionManagement ----

  override def partitionSchema(): StructType = {
    val fromIndex = fileIndex.partitionSchema
    if (fromIndex.nonEmpty) {
      fromIndex
    } else if (userSpecifiedPartitioning.nonEmpty) {
      val full = schema
      StructType(userSpecifiedPartitioning.flatMap(
        col => full.find(_.name == col)))
    } else {
      fromIndex
    }
  }

  override def createPartition(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = {
    val partPath = partitionPath(ident)
    val hadoopConf = sparkSession.sessionState
      .newHadoopConfWithOptions(
        options.asCaseSensitiveMap.asScala.toMap)
    val fs = partPath.getFileSystem(hadoopConf)
    if (fs.exists(partPath)) {
      throw new org.apache.spark.sql.catalyst
        .analysis.PartitionsAlreadyExistException(
          name(), ident, partitionSchema())
    }
    fs.mkdirs(partPath)
    // Sync to catalog metastore if available.
    catalogTable.foreach { ct =>
      val spec = partitionSpec(ident)
      val loc = Option(properties.get("location"))
        .orElse(Some(partPath.toString))
      val part = org.apache.spark.sql.catalyst.catalog
        .CatalogTablePartition(spec,
          org.apache.spark.sql.catalyst.catalog
            .CatalogStorageFormat.empty
            .copy(locationUri = loc.map(new java.net.URI(_))))
      try {
        sparkSession.sessionState.catalog
          .createPartitions(ct.identifier,
            Seq(part), ignoreIfExists = true)
      } catch { case _: Exception => }
    }
    fileIndex.refresh()
  }

  override def dropPartition(
      ident: InternalRow): Boolean = {
    val partPath = partitionPath(ident)
    val hadoopConf = sparkSession.sessionState
      .newHadoopConfWithOptions(
        options.asCaseSensitiveMap.asScala.toMap)
    val fs = partPath.getFileSystem(hadoopConf)
    if (fs.exists(partPath)) {
      fs.delete(partPath, true)
      // Sync to catalog metastore if available.
      catalogTable.foreach { ct =>
        val spec = partitionSpec(ident)
        try {
          sparkSession.sessionState.catalog
            .dropPartitions(ct.identifier,
              Seq(spec), ignoreIfNotExists = true,
              purge = false, retainData = false)
        } catch { case _: Exception => }
      }
      fileIndex.refresh()
      true
    } else {
      false
    }
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException(
      "File-based tables do not support " +
        "partition metadata")
  }

  override def loadPartitionMetadata(
      ident: InternalRow
  ): util.Map[String, String] = {
    throw new UnsupportedOperationException(
      "File-based tables do not support " +
        "partition metadata")
  }

  override def listPartitionIdentifiers(
      names: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    val schema = partitionSchema()
    if (schema.isEmpty) return Array.empty

    val basePath = new Path(paths.head)
    val hadoopConf = sparkSession.sessionState
      .newHadoopConfWithOptions(
        options.asCaseSensitiveMap.asScala.toMap)
    val fs = basePath.getFileSystem(hadoopConf)

    val allPartitions = if (schema.length == 1) {
      val field = schema.head
      if (!fs.exists(basePath)) {
        Array.empty[InternalRow]
      } else {
        fs.listStatus(basePath)
          .filter(_.isDirectory)
          .map(_.getPath.getName)
          .filter(_.contains("="))
          .map { dirName =>
            val value = dirName.split("=", 2)(1)
            val converted = Cast(
              Literal(value), field.dataType).eval()
            InternalRow(converted)
          }
      }
    } else {
      fileIndex.refresh()
      fileIndex match {
        case idx: PartitioningAwareFileIndex =>
          idx.partitionSpec().partitions
            .map(_.values).toArray
        case _ => Array.empty[InternalRow]
      }
    }

    if (names.isEmpty) {
      allPartitions
    } else {
      val indexes = names.map(schema.fieldIndex)
      val dataTypes = names.map(schema(_).dataType)
      allPartitions.filter { row =>
        var matches = true
        var i = 0
        while (i < names.length && matches) {
          val actual = row.get(indexes(i), dataTypes(i))
          val expected = ident.get(i, dataTypes(i))
          matches = actual == expected
          i += 1
        }
        matches
      }
    }
  }

  private def partitionPath(ident: InternalRow): Path = {
    val schema = partitionSchema()
    val basePath = new Path(paths.head)
    val parts = (0 until schema.length).map { i =>
      val name = schema(i).name
      val value = ident.get(i, schema(i).dataType)
      val valueStr = if (value == null) {
        "__HIVE_DEFAULT_PARTITION__"
      } else {
        value.toString
      }
      s"$name=$valueStr"
    }
    new Path(basePath, parts.mkString("/"))
  }

  private def partitionSpec(
      ident: InternalRow): Map[String, String] = {
    val schema = partitionSchema()
    (0 until schema.length).map { i =>
      val name = schema(i).name
      val value = ident.get(i, schema(i).dataType)
      name -> (if (value == null) null else value.toString)
    }.toMap
  }
}

object FileTable {
  private val CAPABILITIES = util.EnumSet.of(
    BATCH_READ, BATCH_WRITE, TRUNCATE, OVERWRITE_DYNAMIC)
}
