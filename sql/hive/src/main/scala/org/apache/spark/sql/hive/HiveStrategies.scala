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

import java.io.IOException
import java.util.Locale

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, InsertIntoDir, InsertIntoStatement, LogicalPlan, ScriptTransformation, Statistics, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{CreateTableCommand, DDLUtils, InsertIntoDataSourceDirCommand}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSourceStrategy, HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation, LogicalRelationWithTable}
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.hive.execution.HiveScriptTransformationExec
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable.BY_CTAS
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}


/**
 * Determine the database, serde/format and schema of the Hive serde table, according to the storage
 * properties.
 */
class ResolveHiveSerdeTable(session: SparkSession) extends Rule[LogicalPlan] {
  private def determineHiveSerde(table: CatalogTable): CatalogTable = {
    if (table.storage.serde.nonEmpty) {
      table
    } else {
      if (table.bucketSpec.isDefined) {
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3082", messageParameters = Map.empty)
      }

      val defaultStorage = HiveSerDe.getDefaultStorage(conf)
      val options = new HiveOptions(table.storage.properties)

      val fileStorage = if (options.fileFormat.isDefined) {
        HiveSerDe.sourceToSerDe(options.fileFormat.get) match {
          case Some(s) =>
            CatalogStorageFormat.empty.copy(
              inputFormat = s.inputFormat,
              outputFormat = s.outputFormat,
              serde = s.serde)
          case None =>
            throw new IllegalArgumentException(s"invalid fileFormat: '${options.fileFormat.get}'")
        }
      } else if (options.hasInputOutputFormat) {
        CatalogStorageFormat.empty.copy(
          inputFormat = options.inputFormat,
          outputFormat = options.outputFormat)
      } else {
        CatalogStorageFormat.empty
      }

      val rowStorage = if (options.serde.isDefined) {
        CatalogStorageFormat.empty.copy(serde = options.serde)
      } else {
        CatalogStorageFormat.empty
      }

      val storage = table.storage.copy(
        inputFormat = fileStorage.inputFormat.orElse(defaultStorage.inputFormat),
        outputFormat = fileStorage.outputFormat.orElse(defaultStorage.outputFormat),
        serde = rowStorage.serde.orElse(fileStorage.serde).orElse(defaultStorage.serde),
        properties = options.serdeProperties)

      table.copy(storage = storage)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case c @ CreateTable(t, _, query) if DDLUtils.isHiveTable(t) =>
      // Finds the database name if the name does not exist.
      val dbName = t.identifier.database.getOrElse(session.catalog.currentDatabase)
      val table = t.copy(identifier = t.identifier.copy(database = Some(dbName)))

      // Determines the serde/format of Hive tables
      val withStorage = determineHiveSerde(table)

      // Infers the schema, if empty, because the schema could be determined by Hive
      // serde.
      val withSchema = if (query.isEmpty) {
        val inferred = HiveUtils.inferSchema(withStorage)
        if (inferred.schema.length <= 0) {
          throw new AnalysisException(
            errorClass = "_LEGACY_ERROR_TEMP_3083",
            messageParameters = Map("tableName" -> inferred.identifier.toString))
        }
        inferred
      } else {
        withStorage
      }

      c.copy(tableDesc = withSchema)
  }
}

class DetermineTableStats(session: SparkSession) extends Rule[LogicalPlan] {

  override def conf: SQLConf = session.sessionState.conf

  private def hiveTableWithStats(relation: HiveTableRelation): HiveTableRelation = {
    val table = relation.tableMeta
    val partitionCols = relation.partitionCols
    // For partitioned tables, the partition directory may be outside of the table directory.
    // Which is expensive to get table size. Please see how we implemented it in the AnalyzeTable.
    val sizeInBytes = if (conf.fallBackToHdfsForStatsEnabled && partitionCols.isEmpty) {
      try {
        val hadoopConf = session.sessionState.newHadoopConf()
        val tablePath = new Path(table.location)
        val fs: FileSystem = tablePath.getFileSystem(hadoopConf)
        fs.getContentSummary(tablePath).getLength
      } catch {
        case e: IOException =>
          logWarning("Failed to get table size from HDFS.", e)
          conf.defaultSizeInBytes
      }
    } else {
      conf.defaultSizeInBytes
    }

    val stats = Some(Statistics(sizeInBytes = BigInt(sizeInBytes)))
    relation.copy(tableStats = stats)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case relation: HiveTableRelation
      if DDLUtils.isHiveTable(relation.tableMeta) && relation.tableMeta.stats.isEmpty =>
      hiveTableWithStats(relation)

    // handles InsertIntoStatement specially as the table in InsertIntoStatement is not added in its
    // children, hence not matched directly by previous HiveTableRelation case.
    case i @ InsertIntoStatement(relation: HiveTableRelation, _, _, _, _, _, _)
      if DDLUtils.isHiveTable(relation.tableMeta) && relation.tableMeta.stats.isEmpty =>
      i.copy(table = hiveTableWithStats(relation))
  }
}

/**
 * Replaces generic operations with specific variants that are designed to work with Hive.
 *
 * Note that, this rule must be run after `PreprocessTableCreation` and
 * `PreprocessTableInsertion`.
 */
object HiveAnalysis extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case InsertIntoStatement(
        r: HiveTableRelation, partSpec, _, query, overwrite, ifPartitionNotExists, _)
        if DDLUtils.isHiveTable(r.tableMeta) && query.resolved =>
      InsertIntoHiveTable(r.tableMeta, partSpec, query, overwrite,
        ifPartitionNotExists, query.output.map(_.name))

    case CreateTable(tableDesc, mode, None) if DDLUtils.isHiveTable(tableDesc) =>
      CreateTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query))
        if DDLUtils.isHiveTable(tableDesc) && query.resolved =>
      CreateHiveTableAsSelectCommand(tableDesc, query, query.output.map(_.name), mode)

    case InsertIntoDir(isLocal, storage, provider, child, overwrite)
        if DDLUtils.isHiveTable(provider) && child.resolved =>
      val outputPath = new Path(storage.locationUri.get)
      if (overwrite) DDLUtils.verifyNotReadPath(child, outputPath)

      InsertIntoHiveDirCommand(isLocal, storage, child, overwrite, child.output.map(_.name))

    case DeleteFromTable(SubqueryAlias(_, HiveTableRelation(table, _, _, _, _)), _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(
        table.identifier,
        "DELETE")
  }
}

/**
 * Relation conversion from metastore relations to data source relations for better performance
 *
 * - When writing to non-partitioned Hive-serde Parquet/Orc tables
 * - When writing to partitioned Hive-serde Parquet/Orc tables when
 *   `spark.sql.hive.convertInsertingPartitionedTable` is true
 * - When writing to unpartitioned Hive-serde Parquet/Orc tables when
 *   `spark.sql.hive.convertInsertingUnpartitionedTable` is true
 * - When writing to directory with Hive-serde
 * - When writing to non-partitioned Hive-serde Parquet/ORC tables using CTAS
 * - When scanning Hive-serde Parquet/ORC tables
 *
 * This rule must be run before all other DDL post-hoc resolution rules, i.e.
 * `PreprocessTableCreation`, `PreprocessTableInsertion`, `DataSourceAnalysis` and `HiveAnalysis`.
 */
case class RelationConversions(
    sessionCatalog: HiveSessionCatalog) extends Rule[LogicalPlan] {
  private def isConvertible(relation: HiveTableRelation): Boolean = {
    isConvertible(relation.tableMeta)
  }

  private def isConvertible(tableMeta: CatalogTable): Boolean = {
    isConvertible(tableMeta.storage)
  }

  private def isConvertible(storage: CatalogStorageFormat): Boolean = {
    val serde = storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    serde.contains("parquet") && conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET) ||
      serde.contains("orc") && conf.getConf(HiveUtils.CONVERT_METASTORE_ORC)
  }

  private def convertProvider(storage: CatalogStorageFormat): String = {
    val serde = storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    if (serde.contains("parquet")) "parquet" else "orc"
  }

  private val metastoreCatalog = sessionCatalog.metastoreCatalog

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      // Write path
      case InsertIntoStatement(
          r: HiveTableRelation, partition, cols, query, overwrite, ifPartitionNotExists, byName)
          if query.resolved && DDLUtils.isHiveTable(r.tableMeta) &&
            ((r.isPartitioned && conf.getConf(HiveUtils.CONVERT_INSERTING_PARTITIONED_TABLE)) ||
              (!r.isPartitioned && conf.getConf(HiveUtils.CONVERT_INSERTING_UNPARTITIONED_TABLE)))
            && isConvertible(r) =>
        InsertIntoStatement(metastoreCatalog.convert(r, isWrite = true), partition, cols,
          query, overwrite, ifPartitionNotExists, byName)

      // Read path
      case relation: HiveTableRelation if doConvertHiveTableRelationForRead(relation) =>
        convertHiveTableRelationForRead(relation)

      // CTAS path
      // This `InsertIntoHiveTable` is derived from `CreateHiveTableAsSelectCommand`,
      // that only matches table insertion inside Hive CTAS.
      // This pattern would not cause conflicts because this rule is always applied before
      // `HiveAnalysis` and both of these rules are running once.
      case i @ InsertIntoHiveTable(
        tableDesc, _, query, overwrite, ifPartitionNotExists, _, _, _, _, _, _)
          if query.resolved && DDLUtils.isHiveTable(tableDesc) &&
            tableDesc.partitionColumnNames.isEmpty && isConvertible(tableDesc) &&
            conf.getConf(HiveUtils.CONVERT_METASTORE_CTAS) && i.getTagValue(BY_CTAS).isDefined =>
        // validation is required to be done here before relation conversion.
        DDLUtils.checkTableColumns(tableDesc.copy(schema = query.schema))
        val hiveTable = DDLUtils.readHiveTable(tableDesc)
        val hadoopRelation = metastoreCatalog.convert(hiveTable, isWrite = true) match {
          case LogicalRelationWithTable(t: HadoopFsRelation, _) => t
          case _ => throw QueryCompilationErrors.tableIdentifierNotConvertedToHadoopFsRelationError(
            tableDesc.identifier)
        }
        InsertIntoHadoopFsRelationCommand(
          hadoopRelation.location.rootPaths.head,
          Map.empty, // We don't support to convert partitioned table.
          ifPartitionNotExists,
          Seq.empty, // We don't support to convert partitioned table.
          hadoopRelation.bucketSpec,
          hadoopRelation.fileFormat,
          hadoopRelation.options,
          query,
          if (overwrite) SaveMode.Overwrite else SaveMode.Append,
          Some(tableDesc),
          Some(hadoopRelation.location),
          query.output.map(_.name))

      // INSERT HIVE DIR
      case InsertIntoDir(_, storage, provider, query, overwrite)
        if query.resolved && DDLUtils.isHiveTable(provider) &&
          isConvertible(storage) && conf.getConf(HiveUtils.CONVERT_METASTORE_INSERT_DIR) =>
        val outputPath = new Path(storage.locationUri.get)
        if (overwrite) DDLUtils.verifyNotReadPath(query, outputPath)

        InsertIntoDataSourceDirCommand(metastoreCatalog.convertStorageFormat(storage),
          convertProvider(storage), query, overwrite)
    }
  }

  private[hive] def doConvertHiveTableRelationForRead(relation: HiveTableRelation): Boolean = {
    DDLUtils.isHiveTable(relation.tableMeta) && isConvertible(relation)
  }

  private[hive] def convertHiveTableRelationForRead(
      relation: HiveTableRelation): LogicalRelation = {
    metastoreCatalog.convert(relation, isWrite = false)
  }
}

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object HiveScripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ScriptTransformation(script, output, child, ioschema) =>
        val hiveIoSchema = ScriptTransformationIOSchema(ioschema)
        HiveScriptTransformationExec(script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, relation: HiveTableRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionCols)
        val normalizedFilters = DataSourceStrategy.normalizeExprs(
          filters.filter(_.deterministic), relation.output)

        val partitionKeyFilters = DataSourceStrategy.getPushedDownFilters(relation.partitionCols,
          normalizedFilters)

        pruneFilterProject(
          projectList,
          filters.filter(f => f.references.isEmpty || !f.references.subsetOf(partitionKeyIds)),
          identity[Seq[Expression]],
          HiveTableScanExec(_, relation, partitionKeyFilters.toSeq)(sparkSession)) :: Nil
      case _ =>
        Nil
    }
  }
}
