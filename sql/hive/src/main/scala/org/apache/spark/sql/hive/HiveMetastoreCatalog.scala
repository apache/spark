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

import com.google.common.util.concurrent.Striped
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.types._

/**
 * Legacy catalog for interacting with the Hive metastore.
 *
 * This is still used for things like creating data source tables, but in the future will be
 * cleaned up to integrate more nicely with [[HiveExternalCatalog]].
 */
private[hive] class HiveMetastoreCatalog(sparkSession: SparkSession) extends Logging {
  private val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
  private lazy val tableRelationCache = sparkSession.sessionState.catalog.tableRelationCache

  private def getCurrentDatabase: String = sessionState.catalog.getCurrentDatabase

  def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(getCurrentDatabase).toLowerCase,
      tableIdent.table.toLowerCase)
  }

  /** These locks guard against multiple attempts to instantiate a table, which wastes memory. */
  private val tableCreationLocks = Striped.lazyWeakLock(100)

  /** Acquires a lock on the table cache for the duration of `f`. */
  private def withTableCreationLock[A](tableName: QualifiedTableName, f: => A): A = {
    val lock = tableCreationLocks.get(tableName)
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  def hiveDefaultTableFilePath(tableIdent: TableIdentifier): String = {
    // Code based on: hiveWarehouse.getTablePath(currentDatabase, tableName)
    val QualifiedTableName(dbName, tblName) = getQualifiedTableName(tableIdent)
    val dbLocation = sparkSession.sharedState.externalCatalog.getDatabase(dbName).locationUri
    new Path(new Path(dbLocation), tblName).toString
  }

  private def getCached(
      tableIdentifier: QualifiedTableName,
      pathsInMetastore: Seq[Path],
      metastoreRelation: MetastoreRelation,
      schemaInMetastore: StructType,
      expectedFileFormat: Class[_ <: FileFormat],
      expectedBucketSpec: Option[BucketSpec],
      partitionSchema: Option[StructType]): Option[LogicalRelation] = {

    tableRelationCache.getIfPresent(tableIdentifier) match {
      case null => None // Cache miss
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.rootPaths.toSet == pathsInMetastore.toSet &&
                logical.schema.sameType(schemaInMetastore) &&
                relation.bucketSpec == expectedBucketSpec &&
                relation.partitionSchema == partitionSchema.getOrElse(StructType(Nil))

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              tableRelationCache.invalidate(tableIdentifier)
              None
            }
          case _ =>
            logWarning(
              s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} " +
                s"should be stored as $expectedFileFormat. However, we are getting " +
                s"a ${relation.fileFormat} from the metastore cache. This cached " +
                s"entry will be invalidated.")
            tableRelationCache.invalidate(tableIdentifier)
            None
        }
      case other =>
        logWarning(
          s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
            s"as $expectedFileFormat. However, we are getting a $other from the metastore cache. " +
            s"This cached entry will be invalidated.")
        tableRelationCache.invalidate(tableIdentifier)
        None
    }
  }

  private def convertToLogicalRelation(
      metastoreRelation: MetastoreRelation,
      options: Map[String, String],
      defaultSource: FileFormat,
      fileFormatClass: Class[_ <: FileFormat],
      fileType: String): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val tableIdentifier =
      QualifiedTableName(metastoreRelation.databaseName, metastoreRelation.tableName)
    val bucketSpec = None  // We don't support hive bucketed tables, only ones we write out.

    val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions
    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)

      val rootPaths: Seq[Path] = if (lazyPruningEnabled) {
        Seq(metastoreRelation.hiveQlTable.getDataLocation)
      } else {
        // By convention (for example, see CatalogFileIndex), the definition of a
        // partitioned table's paths depends on whether that table has any actual partitions.
        // Partitioned tables without partitions use the location of the table's base path.
        // Partitioned tables with partitions use the locations of those partitions' data
        // locations,_omitting_ the table's base path.
        val paths = metastoreRelation.getHiveQlPartitions().map { p =>
          new Path(p.getLocation)
        }
        if (paths.isEmpty) {
          Seq(metastoreRelation.hiveQlTable.getDataLocation)
        } else {
          paths
        }
      }

      withTableCreationLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          rootPaths,
          metastoreRelation,
          metastoreSchema,
          fileFormatClass,
          bucketSpec,
          Some(partitionSchema))

        val logicalRelation = cached.getOrElse {
          val sizeInBytes =
            metastoreRelation.stats(sparkSession.sessionState.conf).sizeInBytes.toLong
          val fileIndex = {
            val index = new CatalogFileIndex(
              sparkSession, metastoreRelation.catalogTable, sizeInBytes)
            if (lazyPruningEnabled) {
              index
            } else {
              index.filterPartitions(Nil)  // materialize all the partitions in memory
            }
          }
          val partitionSchemaColumnNames = partitionSchema.map(_.name.toLowerCase).toSet
          val dataSchema =
            StructType(metastoreSchema
              .filterNot(field => partitionSchemaColumnNames.contains(field.name.toLowerCase)))

          val relation = HadoopFsRelation(
            location = fileIndex,
            partitionSchema = partitionSchema,
            dataSchema = dataSchema,
            bucketSpec = bucketSpec,
            fileFormat = defaultSource,
            options = options)(sparkSession = sparkSession)

          val created = LogicalRelation(relation,
            catalogTable = Some(metastoreRelation.catalogTable))
          tableRelationCache.put(tableIdentifier, created)
          created
        }

        logicalRelation
      })
    } else {
      val rootPath = metastoreRelation.hiveQlTable.getDataLocation
      withTableCreationLock(tableIdentifier, {
        val cached = getCached(tableIdentifier,
          Seq(rootPath),
          metastoreRelation,
          metastoreSchema,
          fileFormatClass,
          bucketSpec,
          None)
        val logicalRelation = cached.getOrElse {
          val created =
            LogicalRelation(
              DataSource(
                sparkSession = sparkSession,
                paths = rootPath.toString :: Nil,
                userSpecifiedSchema = Some(metastoreRelation.schema),
                bucketSpec = bucketSpec,
                options = options,
                className = fileType).resolveRelation(),
              catalogTable = Some(metastoreRelation.catalogTable))

          tableRelationCache.put(tableIdentifier, created)
          created
        }

        logicalRelation
      })
    }
    result.copy(expectedOutputAttributes = Some(metastoreRelation.output))
  }

  /**
   * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
   * data source relations for better performance.
   */
  object ParquetConversions extends Rule[LogicalPlan] {
    private def shouldConvertMetastoreParquet(relation: MetastoreRelation): Boolean = {
      relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") &&
        sessionState.convertMetastoreParquet
    }

    private def convertToParquetRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new ParquetFileFormat()
      val fileFormatClass = classOf[ParquetFileFormat]

      val mergeSchema = sessionState.convertMetastoreParquetWithSchemaMerging
      val options = Map(ParquetOptions.MERGE_SCHEMA -> mergeSchema.toString)

      convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "parquet")
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, query, overwrite, ifNotExists)
            // Inserting into partitioned table is not supported in Parquet data source (yet).
            if query.resolved && !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
          InsertIntoTable(convertToParquetRelation(r), partition, query, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if shouldConvertMetastoreParquet(relation) =>
          val parquetRelation = convertToParquetRelation(relation)
          SubqueryAlias(relation.tableName, parquetRelation, None)
      }
    }
  }

  /**
   * When scanning Metastore ORC tables, convert them to ORC data source relations
   * for better performance.
   */
  object OrcConversions extends Rule[LogicalPlan] {
    private def shouldConvertMetastoreOrc(relation: MetastoreRelation): Boolean = {
      relation.tableDesc.getSerdeClassName.toLowerCase.contains("orc") &&
        sessionState.convertMetastoreOrc
    }

    private def convertToOrcRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new OrcFileFormat()
      val fileFormatClass = classOf[OrcFileFormat]
      val options = Map[String, String]()

      convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "orc")
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, query, overwrite, ifNotExists)
            // Inserting into partitioned table is not supported in Orc data source (yet).
            if query.resolved && !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
          InsertIntoTable(convertToOrcRelation(r), partition, query, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if shouldConvertMetastoreOrc(relation) =>
          val orcRelation = convertToOrcRelation(relation)
          SubqueryAlias(relation.tableName, orcRelation, None)
      }
    }
  }
}
