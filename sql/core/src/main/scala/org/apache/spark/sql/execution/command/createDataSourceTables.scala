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
import org.apache.spark.sql.catalyst.TableIdentifier
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
case class CreateDataSourceTableCommand(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    options: Map[String, String],
    userSpecifiedPartitionColumns: Array[String],
    bucketSpec: Option[BucketSpec],
    ignoreIfExists: Boolean,
    managedIfNoPath: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = tableIdent.unquotedString
    val sessionState = sparkSession.sessionState

    if (sessionState.catalog.tableExists(tableIdent)) {
      if (ignoreIfExists) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table $tableName already exists.")
      }
    }

    var isExternal = true
    val optionsWithPath =
      if (!new CaseInsensitiveMap(options).contains("path") && managedIfNoPath) {
        isExternal = false
        options + ("path" -> sessionState.catalog.defaultTablePath(tableIdent))
      } else {
        options
      }

    // Create the relation to validate the arguments before writing the metadata to the metastore.
    val dataSource: BaseRelation =
      DataSource(
        sparkSession = sparkSession,
        userSpecifiedSchema = userSpecifiedSchema,
        className = provider,
        bucketSpec = None,
        options = optionsWithPath).resolveRelation(checkPathExist = false)

    val partitionColumns = if (userSpecifiedSchema.nonEmpty) {
      userSpecifiedPartitionColumns
    } else {
      // This is guaranteed in `PreprocessDDL`.
      assert(userSpecifiedPartitionColumns.isEmpty)
      dataSource match {
        case r: HadoopFsRelation => r.partitionSchema.fieldNames
        case _ => Array.empty[String]
      }
    }

    val table = CatalogTable(
      identifier = tableIdent,
      tableType = if (isExternal) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(properties = optionsWithPath),
      schema = dataSource.schema,
      provider = Some(provider),
      partitionColumnNames = partitionColumns,
      bucketSpec = bucketSpec
    )

    // We will return Nil or throw exception at the beginning if the table already exists, so when
    // we reach here, the table should not exist and we should set `ignoreIfExists` to false.
    sessionState.catalog.createTable(table, ignoreIfExists = false)
    Seq.empty[Row]
  }
}

/**
 * A command used to create a data source table using the result of a query.
 *
 * Note: This is different from [[CreateTableAsSelectLogicalPlan]]. Please check the syntax for
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
    tableIdent: TableIdentifier,
    provider: String,
    partitionColumns: Array[String],
    bucketSpec: Option[BucketSpec],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan)
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = tableIdent.unquotedString
    val sessionState = sparkSession.sessionState
    var createMetastoreTable = false
    var isExternal = true
    val optionsWithPath =
      if (!new CaseInsensitiveMap(options).contains("path")) {
        isExternal = false
        options + ("path" -> sessionState.catalog.defaultTablePath(tableIdent))
      } else {
        options
      }

    var existingSchema = Option.empty[StructType]
    if (sparkSession.sessionState.catalog.tableExists(tableIdent)) {
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
          val dataSource = DataSource(
            sparkSession = sparkSession,
            userSpecifiedSchema = Some(query.schema.asNullable),
            partitionColumns = partitionColumns,
            bucketSpec = bucketSpec,
            className = provider,
            options = optionsWithPath)
          // TODO: Check that options from the resolved relation match the relation that we are
          // inserting into (i.e. using the same compression).

          EliminateSubqueryAliases(
            sessionState.catalog.lookupRelation(tableIdent)) match {
            case l @ LogicalRelation(_: InsertableRelation | _: HadoopFsRelation, _, _) =>
              // check if the file formats match
              l.relation match {
                case r: HadoopFsRelation if r.fileFormat.getClass != dataSource.providingClass =>
                  throw new AnalysisException(
                    s"The file format of the existing table $tableIdent is " +
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
            case o =>
              throw new AnalysisException(s"Saving data in ${o.toString} is not supported.")
          }
        case SaveMode.Overwrite =>
          sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
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

    // Create the relation based on the data of df.
    val dataSource = DataSource(
      sparkSession,
      className = provider,
      partitionColumns = partitionColumns,
      bucketSpec = bucketSpec,
      options = optionsWithPath)

    val result = try {
      dataSource.write(mode, df)
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table ${tableIdent.identifier} in $mode mode", ex)
        throw ex
    }
    if (createMetastoreTable) {
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      val schema = result.schema
      val table = CatalogTable(
        identifier = tableIdent,
        tableType = if (isExternal) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED,
        storage = CatalogStorageFormat.empty.copy(properties = optionsWithPath),
        schema = schema,
        provider = Some(provider),
        partitionColumnNames = partitionColumns,
        bucketSpec = bucketSpec
      )
      sessionState.catalog.createTable(table, ignoreIfExists = false)
    }

    // Refresh the cache of the table in the catalog.
    sessionState.catalog.refreshTable(tableIdent)
    Seq.empty[Row]
  }
}
