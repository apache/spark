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

package org.apache.spark.sql.execution.command

import java.util.{Locale, UUID}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, DataSourceUtils, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2Utils, FileDataSourceV2, WriteToDataSourceV2}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.TableCapability._
import org.apache.spark.sql.sources.v2.writer.SupportsSaveMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A command that represents a call to df.write.save/saveAsTable().
 *
 * @param source Value passed in spark.write.source(value), or the default source.
 * @param mode Value passed in spark.write.mode(value), or the default mode.
 * @param extraOptions Optional values passed in spark.write.options(values)
 * @param partitioningColumns Optional values passed in spark.write.partitionBy(values)
 * @param bucketColumnNames Optional values passed in spark.write.bucketBy(numBuckets, values)
 * @param numBuckets Optional bucket count set by spark.write.bucketBy()
 * @param sortColumnNames Optional values passed in spark.write.sortBy(values)
 * @param tableIdent Optional table specified by saveAsTable(table)
 */
case class DataFrameWriteCommand(
    plan: LogicalPlan,
    source: String,
    mode: SaveMode,
    extraOptions: Map[String, String],
    partitioningColumns: Option[Seq[String]],
    bucketColumnNames: Option[Seq[String]],
    numBuckets: Option[Int],
    sortColumnNames: Option[Seq[String]],
    tableIdent: Option[TableIdentifier]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (tableIdent.isDefined) {
      saveAsTable(sparkSession, tableIdent.get)
    } else {
      save(sparkSession)
    }
    Nil
  }

  private def save(session: SparkSession): Unit = {
    val useV1Sources =
      session.sessionState.conf.useV1SourceWriterList.toLowerCase(Locale.ROOT).split(",")
    val lookupCls = DataSource.lookupDataSource(source, session.sessionState.conf)
    val cls = lookupCls.newInstance() match {
      case f: FileDataSourceV2 if useV1Sources.contains(f.shortName()) ||
        useV1Sources.contains(lookupCls.getCanonicalName.toLowerCase(Locale.ROOT)) =>
        f.fallbackFileFormat
      case _ => lookupCls
    }
    // In Data Source V2 project, partitioning is still under development.
    // Here we fallback to V1 if partitioning columns are specified.
    // TODO(SPARK-26778): use V2 implementations when partitioning feature is supported.
    if (classOf[TableProvider].isAssignableFrom(cls) && partitioningColumns.isEmpty) {
      val provider = cls.getConstructor().newInstance().asInstanceOf[TableProvider]
      val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
        provider, session.sessionState.conf)
      val options = sessionOptions ++ extraOptions
      val dsOptions = new CaseInsensitiveStringMap(options.asJava)

      import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
      provider.getTable(dsOptions) match {
        case table: SupportsWrite if table.supports(BATCH_WRITE) =>
          lazy val relation = DataSourceV2Relation.create(table, dsOptions)
          mode match {
            case SaveMode.Append =>
              runCommand(session, "save") {
                AppendData.byName(relation, plan)
              }

            case SaveMode.Overwrite if table.supportsAny(TRUNCATE, OVERWRITE_BY_FILTER) =>
              // truncate the table
              runCommand(session, "save") {
                OverwriteByExpression.byName(relation, plan, Literal(true))
              }

            case _ =>
              table.newWriteBuilder(dsOptions) match {
                case writeBuilder: SupportsSaveMode =>
                  val write = writeBuilder.mode(mode)
                      .withQueryId(UUID.randomUUID().toString)
                      .withInputDataSchema(plan.schema)
                      .buildForBatch()
                  // It can only return null with `SupportsSaveMode`. We can clean it up after
                  // removing `SupportsSaveMode`.
                  if (write != null) {
                    runCommand(session, "save") {
                      WriteToDataSourceV2(write, plan)
                    }
                  }

                case _ =>
                  throw new AnalysisException(
                    s"data source ${table.name} does not support SaveMode $mode")
              }
          }

        // Streaming also uses the data source V2 API. So it may be that the data source implements
        // v2, but has no v2 implementation for batch writes. In that case, we fall back to saving
        // as though it's a V1 source.
        case _ => saveToV1Source(session)
      }
    } else {
      saveToV1Source(session)
    }
  }

  private def saveAsTable(session: SparkSession, tableIdent: TableIdentifier): Unit = {
    val catalog = session.sessionState.catalog
    val tableExists = catalog.tableExists(tableIdent)
    val db = tableIdent.database.getOrElse(catalog.getCurrentDatabase)
    val tableIdentWithDB = tableIdent.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    (tableExists, mode) match {
      case (true, SaveMode.Ignore) =>
        // Do nothing

      case (true, SaveMode.ErrorIfExists) =>
        throw new AnalysisException(s"Table $tableIdent already exists.")

      case (true, SaveMode.Overwrite) =>
        // Get all input data source or hive relations of the query.
        val srcRelations = plan.collect {
          case LogicalRelation(src: BaseRelation, _, _, _) => src
          case relation: HiveTableRelation => relation.tableMeta.identifier
        }

        val tableRelation = session.table(tableIdentWithDB).queryExecution.analyzed
        EliminateSubqueryAliases(tableRelation) match {
          // check if the table is a data source table (the relation is a BaseRelation).
          case LogicalRelation(dest: BaseRelation, _, _, _) if srcRelations.contains(dest) =>
            throw new AnalysisException(
              s"Cannot overwrite table $tableName that is also being read from")
          // check hive table relation when overwrite mode
          case relation: HiveTableRelation
              if srcRelations.contains(relation.tableMeta.identifier) =>
            throw new AnalysisException(
              s"Cannot overwrite table $tableName that is also being read from")
          case _ => // OK
        }

        // Drop the existing table
        catalog.dropTable(tableIdentWithDB, ignoreIfNotExists = true, purge = false)
        createTable(session, tableIdentWithDB)
        // Refresh the cache of the table in the catalog.
        catalog.refreshTable(tableIdentWithDB)

      case _ => createTable(session, tableIdent)
    }
  }

  private def createTable(session: SparkSession, tableIdent: TableIdentifier): Unit = {
    val storage = DataSource.buildStorageFormatFromOptions(extraOptions.toMap)
    val tableType = if (storage.locationUri.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    val tableDesc = CatalogTable(
      identifier = tableIdent,
      tableType = tableType,
      storage = storage,
      schema = new StructType,
      provider = Some(source),
      partitionColumnNames = partitioningColumns.getOrElse(Nil),
      bucketSpec = getBucketSpec)

    runCommand(session, "saveAsTable")(CreateTable(tableDesc, mode, Some(plan)))
  }

  private def saveToV1Source(session: SparkSession): Unit = {
    var options = extraOptions

    if (session.sessionState.conf.getConf(
      SQLConf.LEGACY_PASS_PARTITION_BY_AS_OPTIONS)) {
      partitioningColumns.foreach { columns =>
        options += (DataSourceUtils.PARTITIONING_COLUMNS_KEY ->
          DataSourceUtils.encodePartitioningColumns(columns))
      }
    }

    // Code path for data source v1.
    runCommand(session, "save") {
      DataSource(
        sparkSession = session,
        className = source,
        partitionColumns = partitioningColumns.getOrElse(Nil),
        options = options).planForWriting(mode, plan)
    }
  }

  private def getBucketSpec: Option[BucketSpec] = {
    if (sortColumnNames.isDefined && numBuckets.isEmpty) {
      throw new AnalysisException("sortBy must be used together with bucketBy")
    }

    numBuckets.map { n =>
      BucketSpec(n, bucketColumnNames.get, sortColumnNames.getOrElse(Nil))
    }
  }

  /**
   * Wrap a DataFrameWriter action to track the QueryExecution and time cost, then report to the
   * user-registered callback functions.
   */
  private def runCommand(session: SparkSession, name: String)(command: LogicalPlan): Unit = {
    val qe = session.sessionState.executePlan(command)
    // call `QueryExecution.toRDD` to trigger the execution of commands.
    SQLExecution.withNewExecutionId(session, qe, Some(name))(qe.toRdd)
  }
}
