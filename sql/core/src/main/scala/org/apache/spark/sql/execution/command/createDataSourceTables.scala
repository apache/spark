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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types._

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
    val pathOption = table.storage.locationUri.map("path" -> _)
    val dataSource: BaseRelation =
      DataSource(
        sparkSession = sparkSession,
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        className = table.provider.get,
        bucketSpec = table.bucketSpec,
        options = table.storage.properties ++ pathOption).resolveRelation()

    dataSource match {
      case fs: HadoopFsRelation =>
        if (table.tableType == CatalogTableType.EXTERNAL && fs.location.rootPaths.isEmpty) {
          throw new AnalysisException(
            "Cannot create a file-based external data source table without path")
        }
      case _ =>
    }

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
        sparkSession.sessionState.conf.manageFilesourcePartitions)
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
    assert(table.schema.isEmpty)

    val provider = table.provider.get
    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    var createMetastoreTable = false
    var existingSchema = Option.empty[StructType]
    if (sparkSession.sessionState.catalog.tableExists(tableIdentWithDB)) {
      // Check if we need to throw an exception or just return.
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableName already exists. " +
            s"If you are using saveAsTable, you can set SaveMode to SaveMode.Append to " +
            s"insert data into the table or set SaveMode to SaveMode.Overwrite to overwrite" +
            s"the existing data. " +
            s"Or, if you are using SQL CREATE TABLE, you need to drop $tableName first.")
        case SaveMode.Ignore =>
          // Since the table already exists and the save mode is Ignore, we will just return.
          return Seq.empty[Row]
        case SaveMode.Append =>
          // Check if the specified data source match the data source of the existing table.
          val existingProvider = DataSource.lookupDataSource(provider)
          // TODO: Check that options from the resolved relation match the relation that we are
          // inserting into (i.e. using the same compression).

          // Pass a table identifier with database part, so that `lookupRelation` won't get temp
          // views unexpectedly.
          EliminateSubqueryAliases(sessionState.catalog.lookupRelation(tableIdentWithDB)) match {
            case l @ LogicalRelation(_: InsertableRelation | _: HadoopFsRelation, _, _) =>
              // check if the file formats match
              l.relation match {
                case r: HadoopFsRelation if r.fileFormat.getClass != existingProvider =>
                  throw new AnalysisException(
                    s"The file format of the existing table $tableName is " +
                      s"`${r.fileFormat.getClass.getName}`. It doesn't match the specified " +
                      s"format `$provider`")
                case _ =>
              }
              if (query.schema.size != l.schema.size) {
                throw new AnalysisException(
                  s"The column number of the existing schema[${l.schema}] " +
                    s"doesn't match the data schema[${query.schema}]'s")
              }
              existingSchema = Some(l.schema)
            case s: SimpleCatalogRelation if DDLUtils.isDatasourceTable(s.metadata) =>
              existingSchema = Some(s.metadata.schema)
            case c: CatalogRelation if c.catalogTable.provider == Some(DDLUtils.HIVE_PROVIDER) =>
              throw new AnalysisException("Saving data in the Hive serde table " +
                s"${c.catalogTable.identifier} is not supported yet. Please use the " +
                "insertInto() API as an alternative..")
            case o =>
              throw new AnalysisException(s"Saving data in ${o.toString} is not supported.")
          }
        case SaveMode.Overwrite =>
          sessionState.catalog.dropTable(tableIdentWithDB, ignoreIfNotExists = true, purge = false)
          // Need to create the table again.
          createMetastoreTable = true
      }
    } else {
      // The table does not exist. We need to create it in metastore.
      createMetastoreTable = true
    }

    val data = Dataset.ofRows(sparkSession, query)
    val df = existingSchema match {
      // If we are inserting into an existing table, just use the existing schema.
      case Some(s) => data.selectExpr(s.fieldNames: _*)
      case None => data
    }

    val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
      Some(sessionState.catalog.defaultTablePath(table.identifier))
    } else {
      table.storage.locationUri
    }

    // Create the relation based on the data of df.
    val pathOption = tableLocation.map("path" -> _)
    val dataSource = DataSource(
      sparkSession,
      className = provider,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = Some(table))

    val result = try {
      dataSource.write(mode, df)
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table $tableName in $mode mode", ex)
        throw ex
    }
    if (createMetastoreTable) {
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        // We will use the schema of resolved.relation as the schema of the table (instead of
        // the schema of df). It is important since the nullability may be changed by the relation
        // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
        schema = result.schema)
      sessionState.catalog.createTable(newTable, ignoreIfExists = false)
    }

    result match {
      case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
          sparkSession.sqlContext.conf.manageFilesourcePartitions =>
        // Need to recover partitions into the metastore so our saved data is visible.
        sparkSession.sessionState.executePlan(
          AlterTableRecoverPartitionsCommand(table.identifier)).toRdd
      case _ =>
    }

    // Refresh the cache of the table in the catalog.
    sessionState.catalog.refreshTable(tableIdentWithDB)
    Seq.empty[Row]
  }
}
