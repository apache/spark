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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.BaseRelation

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

    // Create the relation to validate the arguments before writing the metadata to the metastore,
    // and infer the table schema and partition if users didn't specify schema in CREATE TABLE.
    val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
    // Fill in some default table options from the session conf
    val tableWithDefaultOptions = table.copy(
      identifier = table.identifier.copy(
        database = Some(
          table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase))),
      tracksPartitionsInCatalog = sessionState.conf.manageFilesourcePartitions)
    val dataSource: BaseRelation =
      DataSource(
        sparkSession = sparkSession,
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        partitionColumns = table.partitionColumnNames,
        className = table.provider.get,
        bucketSpec = table.bucketSpec,
        options = table.storage.properties ++ pathOption,
        // As discussed in SPARK-19583, we don't check if the location is existed
        catalogTable = Some(tableWithDefaultOptions)).resolveRelation(checkFilesExist = false)

    val partitionColumnNames = if (table.schema.nonEmpty) {
      table.partitionColumnNames
    } else {
      // This is guaranteed in `PreprocessDDL`.
      assert(table.partitionColumnNames.isEmpty)
      dataSource match {
        case r: HadoopFsRelation => r.partitionSchema.fieldNames.toSeq
        case _ => Nil
      }
    }

    val newTable = table.copy(
      schema = dataSource.schema,
      partitionColumnNames = partitionColumnNames,
      // If metastore partition management for file source tables is enabled, we start off with
      // partition provider hive, but no partitions in the metastore. The user has to call
      // `msck repair table` to populate the table partitions.
      tracksPartitionsInCatalog = partitionColumnNames.nonEmpty &&
        sessionState.conf.manageFilesourcePartitions)
    // We will return Nil or throw exception at the beginning if the table already exists, so when
    // we reach here, the table should not exist and we should set `ignoreIfExists` to false.
    sessionState.catalog.createTable(newTable, ignoreIfExists = false)

    Seq.empty[Row]
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
    query: LogicalPlan)
  extends RunnableCommand {

  override protected def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
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
        sparkSession, table, table.storage.locationUri, query, SaveMode.Append, tableExists = true)
    } else {
      assert(table.schema.isEmpty)

      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
        Some(sessionState.catalog.defaultTablePath(table.identifier))
      } else {
        table.storage.locationUri
      }
      val result = saveDataIntoTable(
        sparkSession, table, tableLocation, query, SaveMode.Overwrite, tableExists = false)
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        // We will use the schema of resolved.relation as the schema of the table (instead of
        // the schema of df). It is important since the nullability may be changed by the relation
        // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
        schema = result.schema)
      sessionState.catalog.createTable(newTable, ignoreIfExists = false)

      result match {
        case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
            sparkSession.sqlContext.conf.manageFilesourcePartitions =>
          // Need to recover partitions into the metastore so our saved data is visible.
          sessionState.executePlan(AlterTableRecoverPartitionsCommand(table.identifier)).toRdd
        case _ =>
      }
    }

    Seq.empty[Row]
  }

  private def saveDataIntoTable(
      session: SparkSession,
      table: CatalogTable,
      tableLocation: Option[URI],
      data: LogicalPlan,
      mode: SaveMode,
      tableExists: Boolean): BaseRelation = {
    // Create the relation based on the input logical plan: `data`.
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val dataSource = DataSource(
      session,
      className = table.provider.get,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = if (tableExists) Some(table) else None)

    try {
      dataSource.writeAndRead(mode, Dataset.ofRows(session, query))
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table ${table.identifier.unquotedString}", ex)
        throw ex
    }
  }
}
