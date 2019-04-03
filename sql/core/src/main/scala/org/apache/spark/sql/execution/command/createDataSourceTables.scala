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

import java.net.URI
import java.util.UUID

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, DataSourceV2Utils, OverwriteByExpressionExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.v2.TableProvider
import org.apache.spark.sql.sources.v2.writer.SupportsSaveMode
import org.apache.spark.sql.types.StructType

/**
 * A command used to create a data source table.
 *
 * Note: This is different from [[CreateTableCommand]]. Please check the syntax for difference.
 * This is not intended for temporary tables.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   [(col1 data_type [COMMENT col_comment], ...)]
 *   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
 * }}}
 */
case class CreateDataSourceTableCommand(table: CatalogTable, ignoreIfExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    if (sessionState.catalog.tableExists(table.identifier)) {
      if (ignoreIfExists) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table ${table.identifier.unquotedString} already exists.")
      }
    }
    val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
    val shouldReadInV2 = DataSourceV2Utils.
      shouldReadWithV2(sparkSession, Option(table.schema), table.provider.get, pathOption.toMap)
    if (shouldReadInV2) {
      processV2Source(sparkSession, pathOption)
    } else {
      processV1Source(sparkSession, pathOption)
    }

    Seq.empty[Row]
  }

  def processV1Source(
      sparkSession: SparkSession,
      pathOption: Option[(String, String)]): Unit = {
    val sessionState = sparkSession.sessionState
    // Create the relation to validate the arguments before writing the metadata to the metastore,
    // and infer the table schema and partition if users didn't specify schema in CREATE TABLE.

    // Fill in some default table options from the session conf
    val tableWithDefaultOptions = table.copy(
      identifier = table.identifier.copy(
        database = Some(
          table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase))),
      tracksPartitionsInCatalog = sessionState.conf.manageFilesourcePartitions)

    val dataSource: DataSource = DataSource(
      sparkSession = sparkSession,
      userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
      partitionColumns = table.partitionColumnNames,
      className = table.provider.get,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      // As discussed in SPARK-19583, we don't check if the location is existed
      catalogTable = Some(tableWithDefaultOptions))

    val baseRelation: BaseRelation = dataSource.resolveRelation(checkFilesExist = false)

    val partitionColumnNames = if (table.schema.nonEmpty) {
      table.partitionColumnNames
    } else {
      // This is guaranteed in `PreprocessDDL`.
      assert(table.partitionColumnNames.isEmpty)
      baseRelation match {
        case r: HadoopFsRelation => r.partitionSchema.fieldNames.toSeq
        case _ => Nil
      }
    }

    val newTable = baseRelation match {
      // Since Spark 2.1, we store the inferred schema of data source in metastore, to avoid
      // inferring the schema again at read path. However if the data source has overlapped columns
      // between data and partition schema, we can't store it in metastore as it breaks the
      // assumption of table schema. Here we fallback to the behavior of Spark prior to 2.1, store
      // empty schema in metastore and infer it at runtime. Note that this also means the new
      // scalable partitioning handling feature(introduced at Spark 2.1) is disabled in this case.
      case r: HadoopFsRelation if r.overlappedPartCols.nonEmpty =>
        logWarning("It is not recommended to create a table with overlapped data and partition " +
          "columns, as Spark cannot store a valid table schema and has to infer it at runtime, " +
          "which hurts performance. Please check your data files and remove the partition " +
          "columns in it.")
        table.copy(schema = new StructType(), partitionColumnNames = Nil)

      case _ =>
        table.copy(
          schema = baseRelation.schema,
          partitionColumnNames = partitionColumnNames,
          // If metastore partition management for file source tables is enabled, we start off with
          // partition provider hive, but no partitions in the metastore. The user has to call
          // `msck repair table` to populate the table partitions.
          tracksPartitionsInCatalog = partitionColumnNames.nonEmpty &&
            sessionState.conf.manageFilesourcePartitions)
    }

    // We will return Nil or throw exception at the beginning if the table already exists, so when
    // we reach here, the table should not exist and we should set `ignoreIfExists` to false.
    sessionState.catalog.createTable(newTable, ignoreIfExists = false)
  }

  def processV2Source(
      sparkSession: SparkSession,
      pathOption: Option[(String, String)]): Unit = {
    // treat it as V2 data source
    val newTable = if (table.schema.nonEmpty) {
      table
    } else {
      val cls = DataSourceV2Utils.isV2Source(sparkSession, table.provider.get)
      val provider = cls.get.getConstructor().newInstance().asInstanceOf[TableProvider]
      val dsOptions = DataSourceV2Utils.
        extractSessionConfigs(provider, sparkSession.sessionState.conf, pathOption.toMap)
      val readTable = DataSourceV2Utils.
        getBatchReadTable(sparkSession, None, cls.get, dsOptions)
      table.copy(schema = readTable.get.schema())
    }

    // We will return Nil or throw exception at the beginning if the table already exists, so when
    // we reach here, the table should not exist and we should set `ignoreIfExists` to false.
    sparkSession.sessionState.catalog.createTable(newTable, ignoreIfExists = false)
  }
}

/**
 * A command used to create a data source table using the result of a query.
 *
 * Note: This is different from `CreateHiveTableAsSelectCommand`. Please check the syntax for
 * difference. This is not intended for temporary tables.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
 *   AS SELECT ...
 * }}}
 */
case class CreateDataSourceTableAsSelectCommand(
    table: CatalogTable,
    mode: SaveMode,
    query: LogicalPlan,
    outputColumnNames: Seq[String])
  extends DataWritingCommand {

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableName has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"Table $tableName already exists. You need to drop it first.")
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      saveDataIntoTable(
        sparkSession, table, table.storage.locationUri, child, SaveMode.Append, tableExists = true)
    } else {
      assert(table.schema.isEmpty)
      sparkSession.sessionState.catalog.validateTableLocation(table)
      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
        Some(sessionState.catalog.defaultTablePath(table.identifier))
      } else {
        table.storage.locationUri
      }
      saveDataIntoTable(
        sparkSession, table, tableLocation, child, SaveMode.Overwrite, tableExists = false)

    }

    Seq.empty[Row]
  }

  private def saveDataIntoTable(
      session: SparkSession,
      table: CatalogTable,
      tableLocation: Option[URI],
      physicalPlan: SparkPlan,
      mode: SaveMode,
      tableExists: Boolean): Unit = {
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val shouldReadInV2 = DataSourceV2Utils.
      shouldWriteWithV2(session, Option(table.schema), table.provider.get, pathOption.toMap)
    if (shouldReadInV2) {
      saveToV2Source(session, table, tableLocation, physicalPlan, mode, tableExists)
    } else {
      saveToV1Source(session, table, tableLocation, physicalPlan, mode, tableExists)
    }
  }

  private def saveToV1Source(
      session: SparkSession,
      table: CatalogTable,
      tableLocation: Option[URI],
      physicalPlan: SparkPlan,
      mode: SaveMode,
      tableExists: Boolean): Unit = {
    // Create the relation based on the input logical plan: `query`.
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val sessionState = session.sessionState
    val dataSource = DataSource(
      session,
      className = table.provider.get,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = if (tableExists) Some(table) else None)

    try {
      val result = dataSource.writeAndRead(mode, query, outputColumnNames, physicalPlan)
      if (!tableExists) {
        val newTable = table.copy(
          storage = table.storage.copy(locationUri = tableLocation),
          // We will use the schema of resolved.relation as the schema of the table (instead of
          // the schema of df). It is important since the nullability may be changed by the relation
          // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
          schema = result.schema)
        // Table location is already validated. No need to check it again during table creation.
        sessionState.catalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)

        result match {
          case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
            session.sqlContext.conf.manageFilesourcePartitions =>
            // Need to recover partitions into the metastore so our saved data is visible.
            sessionState.executePlan(AlterTableRecoverPartitionsCommand(table.identifier)).toRdd
          case _ =>
        }
      }
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table ${table.identifier.unquotedString}", ex)
        throw ex
    }
  }

  private def saveToV2Source(
      session: SparkSession,
      table: CatalogTable,
      tableLocation: Option[URI],
      physicalPlan: SparkPlan,
      mode: SaveMode,
      tableExists: Boolean): Unit = {
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val cls = DataSourceV2Utils.isV2Source(session, table.provider.get)
    val tableProvider = cls.get.getConstructor().newInstance().asInstanceOf[TableProvider]
    val dsOptions = DataSourceV2Utils.
      extractSessionConfigs(tableProvider, session.sessionState.conf, pathOption.toMap)
    val writeTable = DataSourceV2Utils.
      getBatchWriteTable(session, Option(physicalPlan.schema), cls.get, dsOptions).get
    val writeExec = mode match {
      case SaveMode.Append => AppendDataExec(writeTable, dsOptions, physicalPlan)
      case SaveMode.Overwrite =>
        OverwriteByExpressionExec(writeTable, Array.empty, dsOptions, physicalPlan)
      case SaveMode.ErrorIfExists =>
        writeTable.newWriteBuilder(dsOptions) match {
          case writeBuilder: SupportsSaveMode =>
            val write = writeBuilder.mode(mode)
              .withQueryId(UUID.randomUUID().toString)
              .withInputDataSchema(physicalPlan.schema)
              .buildForBatch()
            // It can only return null with `SupportsSaveMode`. We can clean it up after
            // removing `SupportsSaveMode`.
            if (write != null) {
              WriteToDataSourceV2Exec(write, physicalPlan)
            } else {
              null
            }

          case _ =>
            throw new AnalysisException(
              s"data source ${writeTable.name} does not support SaveMode $mode")
        }
      case _ =>
        throw new AnalysisException(
          s"${mode} is not supported in CreateDataSourceTableAsSelectCommand")
    }

    Option(writeExec).foreach(_.execute().count())
    if (!tableExists) {
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        schema = writeTable.schema())
      // Table location is already validated. No need to check it again during table creation.
      session.sessionState.catalog.createTable(
        newTable, ignoreIfExists = false, validateLocation = false)
    }
  }
}
