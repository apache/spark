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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.types.StructType

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = HiveScriptIOSchema(ioschema)
        ScriptTransformation(input, script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists) =>
        InsertIntoHiveTable(table, partition, planLater(child), overwrite, ifNotExists) :: Nil
      case _ => Nil
    }
  }

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionKeys)
        val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
          !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionKeyIds)
        }

        pruneFilterProject(
          projectList,
          otherPredicates,
          identity[Seq[Expression]],
          HiveTableScanExec(_, relation, pruningPredicates)(sparkSession)) :: Nil
      case _ =>
        Nil
    }
  }
}

/**
 * Creates any tables required for query execution.
 * For example, because of a CREATE TABLE X AS statement.
 */
class CreateTables(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Wait until children are resolved.
    case p: LogicalPlan if !p.childrenResolved => p

    case CreateTable(tableDesc, mode, Some(query)) if tableDesc.provider.get == "hive" =>
      val catalog = sparkSession.sessionState.catalog
      val dbName = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase).toLowerCase

      val newTableDesc = if (tableDesc.storage.serde.isEmpty) {
        // add default serde
        tableDesc.withNewStorage(
          serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
      } else {
        tableDesc
      }

      // Currently we will never hit this branch, as SQL string API can only use `Ignore` or
      // `ErrorIfExists` mode, and `DataFrameWriter.saveAsTable` doesn't support hive serde
      // tables yet.
      if (mode == SaveMode.Append || mode == SaveMode.Overwrite) {
        throw new AnalysisException("" +
          "CTAS for hive serde tables does not support append or overwrite semantics.")
      }

      execution.CreateHiveTableAsSelectCommand(
        newTableDesc.copy(identifier = TableIdentifier(tableDesc.identifier.table, Some(dbName))),
        query,
        mode == SaveMode.Ignore)
  }
}


/**
 * When scanning Metastore ORC tables, convert them to ORC data source relations
 * for better performance.
 */
class OrcConversions(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def shouldConvertMetastoreOrc(relation: MetastoreRelation): Boolean = {
    relation.tableDesc.getSerdeClassName.toLowerCase.contains("orc") &&
    sparkSession.sessionState.conf.getConf(HiveUtils.CONVERT_METASTORE_ORC)
  }

  private def convertToOrcRelation(relation: MetastoreRelation): LogicalRelation = {
    val defaultSource = new OrcFileFormat()
    val fileFormatClass = classOf[OrcFileFormat]
    val options = Map[String, String]()

    ConvertMetastoreTablesUtils.convertToLogicalRelation(
      sparkSession, relation, options, defaultSource, fileFormatClass, "orc")
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved || plan.analyzed) {
      return plan
    }

    plan transformUp {
      // Write path
      case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
        // Inserting into partitioned table is not supported in Orc data source (yet).
        if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
        InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

      // Read path
      case relation: MetastoreRelation if shouldConvertMetastoreOrc(relation) =>
        val orcRelation = convertToOrcRelation(relation)
        SubqueryAlias(relation.tableName, orcRelation)
    }
  }
}

/**
 * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
 * data source relations for better performance.
 */
class ParquetConversions(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def shouldConvertMetastoreParquet(relation: MetastoreRelation): Boolean = {
    relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") &&
      sparkSession.sessionState.conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET)
  }

  private def convertToParquetRelation(relation: MetastoreRelation): LogicalRelation = {
    val defaultSource = new ParquetFileFormat()
    val fileFormatClass = classOf[ParquetFileFormat]

    val mergeSchema = sparkSession.sessionState.conf.getConf(
      HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING)
    val options = Map(ParquetOptions.MERGE_SCHEMA -> mergeSchema.toString)

    ConvertMetastoreTablesUtils.convertToLogicalRelation(
      sparkSession, relation, options, defaultSource, fileFormatClass, "parquet")
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved || plan.analyzed) {
      return plan
    }

    plan transformUp {
      // Write path
      case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
        // Inserting into partitioned table is not supported in Parquet data source (yet).
        if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
        InsertIntoTable(convertToParquetRelation(r), partition, child, overwrite, ifNotExists)

      // Read path
      case relation: MetastoreRelation if shouldConvertMetastoreParquet(relation) =>
        val parquetRelation = convertToParquetRelation(relation)
        SubqueryAlias(relation.tableName, parquetRelation)
    }
  }
}

object ConvertMetastoreTablesUtils extends Logging {

  def convertToLogicalRelation(
      sparkSession: SparkSession,
      metastoreRelation: MetastoreRelation,
      options: Map[String, String],
      defaultSource: FileFormat,
      fileFormatClass: Class[_ <: FileFormat],
      fileType: String): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val tableIdentifier =
      TableIdentifier(metastoreRelation.tableName, Some(metastoreRelation.databaseName))
    val bucketSpec = None  // We don't support hive bucketed tables, only ones we write out.

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      // We're converting the entire table into HadoopFsRelation, so predicates to Hive metastore
      // are empty.
      val partitions = metastoreRelation.getHiveQlPartitions().map { p =>
        val location = p.getLocation
        val values = InternalRow.fromSeq(p.getValues.asScala.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        PartitionDirectory(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val partitionPaths = partitions.map(_.path.toString)

      // By convention (for example, see MetaStorePartitionedTableFileCatalog), the definition of a
      // partitioned table's paths depends on whether that table has any actual partitions.
      // Partitioned tables without partitions use the location of the table's base path.
      // Partitioned tables with partitions use the locations of those partitions' data locations,
      // _omitting_ the table's base path.
      val paths = if (partitionPaths.isEmpty) {
        Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)
      } else {
        partitionPaths
      }

      val cached = getCached(
        sparkSession,
        tableIdentifier,
        paths,
        metastoreRelation,
        metastoreSchema,
        fileFormatClass,
        bucketSpec,
        Some(partitionSpec))

      val hadoopFsRelation = cached.getOrElse {
        val fileCatalog = new MetaStorePartitionedTableFileCatalog(
          sparkSession,
          new Path(metastoreRelation.catalogTable.storage.locationUri.get),
          partitionSpec)

        val inferredSchema = if (fileType.equals("parquet")) {
          val inferredSchema =
            defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles())
          inferredSchema.map { inferred =>
            ParquetFileFormat.mergeMetastoreParquetSchema(metastoreSchema, inferred)
          }.getOrElse(metastoreSchema)
        } else {
          defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles()).get
        }

        val relation = HadoopFsRelation(
          sparkSession = sparkSession,
          location = fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = inferredSchema,
          bucketSpec = bucketSpec,
          fileFormat = defaultSource,
          options = options)

        val created = LogicalRelation(
          relation,
          metastoreTableIdentifier = Some(tableIdentifier))
        sparkSession.sessionState.catalog.cacheDataSourceTable(tableIdentifier, created)
        created
      }

      hadoopFsRelation
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      val cached = getCached(
        sparkSession,
        tableIdentifier,
        paths,
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
              paths = paths,
              userSpecifiedSchema = Some(metastoreRelation.schema),
              bucketSpec = bucketSpec,
              options = options,
              className = fileType).resolveRelation(),
            metastoreTableIdentifier = Some(tableIdentifier))
        sparkSession.sessionState.catalog.cacheDataSourceTable(tableIdentifier, created)
        created
      }

      logicalRelation
    }
    result.copy(expectedOutputAttributes = Some(metastoreRelation.output))
  }

  private def getCached(
      sparkSession: SparkSession,
      tableIdentifier: TableIdentifier,
      pathsInMetastore: Seq[String],
      metastoreRelation: MetastoreRelation,
      schemaInMetastore: StructType,
      expectedFileFormat: Class[_ <: FileFormat],
      expectedBucketSpec: Option[BucketSpec],
      partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {

    sparkSession.sessionState.catalog.getCachedDataSourceTableIfPresent(tableIdentifier) match {
      case null => None // Cache miss
      case Some(logical @ LogicalRelation(relation: HadoopFsRelation, _, _)) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.paths.map(_.toString).toSet == pathsInMetastore.toSet &&
                logical.schema.sameType(schemaInMetastore) &&
                relation.bucketSpec == expectedBucketSpec &&
                relation.partitionSpec == partitionSpecInMetastore.getOrElse {
                  PartitionSpec(StructType(Nil), Array.empty[PartitionDirectory])
                }

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
              None
            }
          case _ =>
            logWarning(
              s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} " +
                s"should be stored as $expectedFileFormat. However, we are getting " +
                s"a ${relation.fileFormat} from the metastore cache. This cached " +
                s"entry will be invalidated.")
            sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
            None
        }
      case other =>
        logWarning(
          s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
            s"as $expectedFileFormat. However, we are getting a $other from the metastore cache. " +
            s"This cached entry will be invalidated.")
        sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
        None
    }
  }

  /**
   * An override of the standard HDFS listing based catalog, that overrides the partition spec with
   * the information from the metastore.
   *
   * @param tableBasePath The default base path of the Hive metastore table
   * @param partitionSpec The partition specifications from Hive metastore
   */
  private[hive] class MetaStorePartitionedTableFileCatalog(
      sparkSession: SparkSession,
      tableBasePath: Path,
      override val partitionSpec: PartitionSpec)
    extends ListingFileCatalog(
      sparkSession,
      MetaStorePartitionedTableFileCatalog.getPaths(tableBasePath, partitionSpec),
      Map.empty,
      Some(partitionSpec.partitionColumns)) {
  }

  private[hive] object MetaStorePartitionedTableFileCatalog {
    /** Get the list of paths to list files in the for a metastore table */
    def getPaths(tableBasePath: Path, partitionSpec: PartitionSpec): Seq[Path] = {
      // If there are no partitions currently specified then use base path,
      // otherwise use the paths corresponding to the partitions.
      if (partitionSpec.partitions.isEmpty) {
        Seq(tableBasePath)
      } else {
        partitionSpec.partitions.map(_.path)
      }
    }
  }
}
