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

package org.apache.spark.sql.hive

import java.util.Locale

import scala.util.control.NonFatal

import com.google.common.util.concurrent.Striped
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{FullQualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.HiveCaseSensitiveInferenceMode._
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.types._

/**
 * Legacy catalog for interacting with the Hive metastore.
 *
 * This is still used for things like creating data source tables, but in the future will be
 * cleaned up to integrate more nicely with [[HiveExternalCatalog]].
 */
private[hive] class HiveMetastoreCatalog(sparkSession: SparkSession) extends Logging {
  // these are def_s and not val/lazy val since the latter would introduce circular references
  private def sessionState = sparkSession.sessionState
  private def catalogProxy = sparkSession.sessionState.catalog
  import HiveMetastoreCatalog._

  /** These locks guard against multiple attempts to instantiate a table, which wastes memory. */
  private val tableCreationLocks = Striped.lazyWeakLock(100)

  /** Acquires a lock on the table cache for the duration of `f`. */
  private def withTableCreationLock[A](tableName: FullQualifiedTableName, f: => A): A = {
    val lock = tableCreationLocks.get(tableName)
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  // For testing only
  private[hive] def getCachedDataSourceTable(table: TableIdentifier): LogicalPlan = {
    val key = FullQualifiedTableName(
      // scalastyle:off caselocale
      table.catalog.getOrElse(CatalogManager.SESSION_CATALOG_NAME).toLowerCase,
      table.database.getOrElse(sessionState.catalog.getCurrentDatabase).toLowerCase,
      table.table.toLowerCase)
      // scalastyle:on caselocale
    catalogProxy.getCachedTable(key)
  }

  private def getCached(
      tableIdentifier: FullQualifiedTableName,
      pathsInMetastore: Seq[Path],
      schemaInMetastore: StructType,
      expectedFileFormat: Class[_ <: FileFormat],
      partitionSchema: Option[StructType]): Option[LogicalRelation] = {

    catalogProxy.getCachedTable(tableIdentifier) match {
      case null => None // Cache miss
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _, _) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.rootPaths.toSet == pathsInMetastore.toSet &&
                DataTypeUtils.sameType(logical.schema, schemaInMetastore) &&
                // We don't support hive bucketed tables. This function `getCached` is only used for
                // converting supported Hive tables to data source tables.
                relation.bucketSpec.isEmpty &&
                relation.partitionSchema == partitionSchema.getOrElse(StructType(Nil))

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              catalogProxy.invalidateCachedTable(tableIdentifier)
              None
            }
          case _ =>
            logWarningUnexpectedFileFormat(tableIdentifier, expectedFileFormat,
              relation.fileFormat.toString)
            catalogProxy.invalidateCachedTable(tableIdentifier)
            None
        }
      case other =>
        logWarningUnexpectedFileFormat(tableIdentifier, expectedFileFormat, other.toString)
        catalogProxy.invalidateCachedTable(tableIdentifier)
        None
    }
  }

  private def logWarningUnexpectedFileFormat(
      tableIdentifier: FullQualifiedTableName,
      expectedFileFormat: Class[_ <: FileFormat],
      actualFileFormat: String): Unit = {
    logWarning(log"Table ${MDC(TABLE_NAME, tableIdentifier)} should be stored as " +
      log"${MDC(FILE_FORMAT, expectedFileFormat)}. However, we are getting a " +
      log"${MDC(FILE_FORMAT2, actualFileFormat)} from the metastore cache. " +
      log"This cached entry will be invalidated.")
  }

  // Return true for Apache ORC and Hive ORC-related configuration names.
  // Note that Spark doesn't support configurations like `hive.merge.orcfile.stripe.level`.
  private def isOrcProperty(key: String) =
    key.startsWith("orc.") || key.contains(".orc.")

  private def isParquetProperty(key: String) =
    key.startsWith("parquet.") || key.contains(".parquet.")

  def convert(relation: HiveTableRelation, isWrite: Boolean): LogicalRelation = {
    val serde = relation.tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)

    // Consider table and storage properties. For properties existing in both sides, storage
    // properties will supersede table properties.
    if (serde.contains("parquet")) {
      val options = relation.tableMeta.properties.filter { case (k, _) => isParquetProperty(k) } ++
        relation.tableMeta.storage.properties + (ParquetOptions.MERGE_SCHEMA ->
        SQLConf.get.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING).toString)
        convertToLogicalRelation(relation, options, classOf[ParquetFileFormat], "parquet", isWrite)
    } else {
      val options = relation.tableMeta.properties.filter { case (k, _) => isOrcProperty(k) } ++
        relation.tableMeta.storage.properties
      if (SQLConf.get.getConf(SQLConf.ORC_IMPLEMENTATION) == "native") {
        convertToLogicalRelation(
          relation,
          options,
          classOf[org.apache.spark.sql.execution.datasources.orc.OrcFileFormat],
          "orc",
          isWrite)
      } else {
        convertToLogicalRelation(
          relation,
          options,
          classOf[org.apache.spark.sql.hive.orc.OrcFileFormat],
          "orc",
          isWrite)
      }
    }
  }

  def convertStorageFormat(storage: CatalogStorageFormat): CatalogStorageFormat = {
    val serde = storage.serde.getOrElse("").toLowerCase(Locale.ROOT)

    if (serde.contains("parquet")) {
      val options = storage.properties + (ParquetOptions.MERGE_SCHEMA ->
        SQLConf.get.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING).toString)
      storage.copy(
        serde = None,
        properties = options
      )
    } else {
      val options = storage.properties
      if (SQLConf.get.getConf(SQLConf.ORC_IMPLEMENTATION) == "native") {
        storage.copy(
          serde = None,
          properties = options
        )
      } else {
        storage.copy(
          serde = None,
          properties = options
        )
      }
    }
  }

  private def convertToLogicalRelation(
      relation: HiveTableRelation,
      options: Map[String, String],
      fileFormatClass: Class[_ <: FileFormat],
      fileType: String,
      isWrite: Boolean): LogicalRelation = {
    val metastoreSchema = relation.tableMeta.schema
    // scalastyle:off caselocale
    val catalog = relation.tableMeta.identifier.catalog.getOrElse(
      CatalogManager.SESSION_CATALOG_NAME).toLowerCase
    // scalastyle:on caselocale
    val tableIdentifier = FullQualifiedTableName(
      catalog, relation.tableMeta.database, relation.tableMeta.identifier.table)

    val lazyPruningEnabled = sparkSession.sessionState.conf.manageFilesourcePartitions
    val tablePath = new Path(relation.tableMeta.location)
    val fileFormat = fileFormatClass.getConstructor().newInstance()
    val bucketSpec = relation.tableMeta.bucketSpec
    val (hiveOptions, hiveBucketSpec) =
      if (isWrite) {
        (options.updated(BucketingUtils.optionForHiveCompatibleBucketWrite, "true"),
          bucketSpec)
      } else {
        (options, None)
      }

    val result = if (relation.isPartitioned) {
      val partitionSchema = relation.tableMeta.partitionSchema
      val rootPaths: Seq[Path] = if (lazyPruningEnabled) {
        Seq(tablePath)
      } else {
        // By convention (for example, see CatalogFileIndex), the definition of a
        // partitioned table's paths depends on whether that table has any actual partitions.
        // Partitioned tables without partitions use the location of the table's base path.
        // Partitioned tables with partitions use the locations of those partitions' data
        // locations,_omitting_ the table's base path.
        val paths = sparkSession.sharedState.externalCatalog
          .listPartitions(tableIdentifier.database, tableIdentifier.name)
          .map(p => new Path(p.storage.locationUri.get))

        if (paths.isEmpty) {
          Seq(tablePath)
        } else {
          paths
        }
      }

      withTableCreationLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          rootPaths,
          metastoreSchema,
          fileFormatClass,
          Some(partitionSchema))

        val logicalRelation = cached.getOrElse {
          val sizeInBytes = relation.stats.sizeInBytes.toLong
          val fileIndex = {
            val index = new CatalogFileIndex(sparkSession, relation.tableMeta, sizeInBytes)
            if (lazyPruningEnabled) {
              index
            } else {
              index.filterPartitions(Nil)  // materialize all the partitions in memory
            }
          }

          val updatedTable = inferIfNeeded(relation, hiveOptions, fileFormat, Option(fileIndex))

          // Spark SQL's data source table now support static and dynamic partition insert. Source
          // table converted from Hive table should always use dynamic.
          val enableDynamicPartition = hiveOptions.updated(DataSourceUtils.PARTITION_OVERWRITE_MODE,
            PartitionOverwriteMode.DYNAMIC.toString)
          val fsRelation = HadoopFsRelation(
            location = fileIndex,
            partitionSchema = partitionSchema,
            dataSchema = updatedTable.dataSchema,
            bucketSpec = hiveBucketSpec,
            fileFormat = fileFormat,
            options = enableDynamicPartition)(sparkSession = sparkSession)
          val created = LogicalRelation(fsRelation, updatedTable)
          catalogProxy.cacheTable(tableIdentifier, created)
          created
        }

        logicalRelation
      })
    } else {
      val rootPath = tablePath
      withTableCreationLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          Seq(rootPath),
          metastoreSchema,
          fileFormatClass,
          None)
        val logicalRelation = cached.getOrElse {
          val updatedTable = inferIfNeeded(relation, hiveOptions, fileFormat)
          val created =
            LogicalRelation(
              DataSource(
                sparkSession = sparkSession,
                paths = rootPath.toString :: Nil,
                userSpecifiedSchema = Option(updatedTable.dataSchema),
                bucketSpec = hiveBucketSpec,
                // Do not interpret the 'path' option at all when tables are read using the Hive
                // source, since the URIs will already have been read from the table's LOCATION.
                options = hiveOptions.filter { case (k, _) => !k.equalsIgnoreCase("path") },
                className = fileType).resolveRelation(),
              table = updatedTable)

          catalogProxy.cacheTable(tableIdentifier, created)
          created
        }

        logicalRelation
      })
    }
    // The inferred schema may have different field names as the table schema, we should respect
    // it, but also respect the exprId in table relation output.
    if (result.output.length != relation.output.length) {
      throw new AnalysisException(
        errorClass = "_LEGACY_ERROR_TEMP_3096",
        messageParameters = Map(
          "resLen" ->  result.output.length.toString,
          "relLen" -> relation.output.length.toString,
          "key" -> HiveUtils.CONVERT_METASTORE_PARQUET.key,
          "ident" -> relation.tableMeta.identifier.toString))
    }
    if (!result.output.zip(relation.output).forall {
          case (a1, a2) => a1.dataType == a2.dataType }) {
      throw new AnalysisException(
        errorClass = "_LEGACY_ERROR_TEMP_3097",
        messageParameters = Map(
          "key" -> HiveUtils.CONVERT_METASTORE_PARQUET.key,
          "ident" -> relation.tableMeta.identifier.toString))
    }
    val newOutput = result.output.zip(relation.output).map {
      case (a1, a2) => a1.withExprId(a2.exprId)
    }
    result.copy(output = newOutput)
  }

  private def inferIfNeeded(
      relation: HiveTableRelation,
      options: Map[String, String],
      fileFormat: FileFormat,
      fileIndexOpt: Option[FileIndex] = None): CatalogTable = {
    val inferenceMode = sparkSession.sessionState.conf.caseSensitiveInferenceMode
    val shouldInfer = (inferenceMode != NEVER_INFER) && !relation.tableMeta.schemaPreservesCase
    val tableName = relation.tableMeta.identifier.unquotedString
    if (shouldInfer) {
      logInfo(log"Inferring case-sensitive schema for table ${MDC(TABLE_NAME, tableName)} " +
        log"(inference mode:  ${MDC(INFERENCE_MODE, inferenceMode)})})")
      val fileIndex = fileIndexOpt.getOrElse {
        val rootPath = new Path(relation.tableMeta.location)
        new InMemoryFileIndex(sparkSession, Seq(rootPath), options, None)
      }

      val inferredSchema = fileFormat
        .inferSchema(
          sparkSession,
          options,
          fileIndex.listFiles(Nil, Nil).flatMap(_.files).map(_.fileStatus))
        .map(mergeWithMetastoreSchema(relation.tableMeta.dataSchema, _))

      inferredSchema match {
        case Some(dataSchema) =>
          if (inferenceMode == INFER_AND_SAVE) {
            updateDataSchema(relation.tableMeta.identifier, dataSchema)
          }
          val newSchema = StructType(dataSchema ++ relation.tableMeta.partitionSchema)
          relation.tableMeta.copy(schema = newSchema)
        case None =>
          logWarning(log"Unable to infer schema for table ${MDC(TABLE_NAME, tableName)} from " +
            log"file format ${MDC(FILE_FORMAT, fileFormat)} (inference mode: " +
            log"${MDC(INFERENCE_MODE, inferenceMode)}). Using metastore schema.")
          relation.tableMeta
      }
    } else {
      relation.tableMeta
    }
  }

  private def updateDataSchema(identifier: TableIdentifier, newDataSchema: StructType): Unit = try {
    logInfo(
      log"Saving case-sensitive schema for table ${MDC(TABLE_NAME, identifier.unquotedString)}")
    sparkSession.sessionState.catalog.alterTableDataSchema(identifier, newDataSchema)
  } catch {
    case NonFatal(ex) =>
      logWarning(log"Unable to save case-sensitive schema for table " +
        log"${MDC(TABLE_NAME, identifier.unquotedString)}", ex)
  }
}


private[hive] object HiveMetastoreCatalog {
  def mergeWithMetastoreSchema(
      metastoreSchema: StructType,
      inferredSchema: StructType): StructType = try {
    // scalastyle:off caselocale
    // Find any nullable fields in metastore schema that are missing from the inferred schema.
    val metastoreFields = metastoreSchema.map(f => f.name.toLowerCase -> f).toMap
    val missingNullables = metastoreFields
      .filter { case (k, _) => !inferredSchema.map(_.name.toLowerCase).contains(k) }
      .values
      .filter(_.nullable)
    // Merge missing nullable fields to inferred schema and build a case-insensitive field map.
    val inferredFields = StructType(inferredSchema ++ missingNullables)
      .map(f => f.name.toLowerCase -> f).toMap
    StructType(metastoreSchema.map(f => f.copy(name = inferredFields(f.name.toLowerCase).name)))
    // scalastyle:on caselocale
  } catch {
    case NonFatal(_) =>
      val msg = s"""Detected conflicting schemas when merging the schema obtained from the Hive
         | Metastore with the one inferred from the file format. Metastore schema:
         |${metastoreSchema.prettyJson}
         |
         |Inferred schema:
         |${inferredSchema.prettyJson}
       """.stripMargin
      throw new SparkException(msg)
  }
}
