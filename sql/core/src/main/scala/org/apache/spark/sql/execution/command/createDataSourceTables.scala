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
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
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
    val pathOption = table.storage.locationUri.map("path" -> _)
    // Fill in some default table options from the session conf
    val tableWithDefaultOptions = table.copy(
      identifier = table.identifier.copy(
        database = Some(
          table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase))),
      tracksPartitionsInCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions)
    val dataSource: BaseRelation =
      DataSource(
        sparkSession = sparkSession,
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        partitionColumns = table.partitionColumnNames,
        className = table.provider.get,
        bucketSpec = table.bucketSpec,
        options = table.storage.properties ++ pathOption,
        catalogTable = Some(tableWithDefaultOptions)).resolveRelation()

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
    // We may need to reorder the columns of the query to match the existing table.
    var reorderedColumns = Option.empty[Seq[NamedExpression]]
    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
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
          val existingTable = sessionState.catalog.getTableMetadata(tableIdentWithDB)

          if (existingTable.provider.get == DDLUtils.HIVE_PROVIDER) {
            throw new AnalysisException(s"Saving data in the Hive serde table $tableName is " +
              "not supported yet. Please use the insertInto() API as an alternative.")
          }

          // Check if the specified data source match the data source of the existing table.
          val existingProvider = DataSource.lookupDataSource(existingTable.provider.get)
          val specifiedProvider = DataSource.lookupDataSource(table.provider.get)
          // TODO: Check that options from the resolved relation match the relation that we are
          // inserting into (i.e. using the same compression).
          if (existingProvider != specifiedProvider) {
            throw new AnalysisException(s"The format of the existing table $tableName is " +
              s"`${existingProvider.getSimpleName}`. It doesn't match the specified format " +
              s"`${specifiedProvider.getSimpleName}`.")
          }

          if (query.schema.length != existingTable.schema.length) {
            throw new AnalysisException(
              s"The column number of the existing table $tableName" +
                s"(${existingTable.schema.catalogString}) doesn't match the data schema" +
                s"(${query.schema.catalogString})")
          }

          val resolver = sessionState.conf.resolver
          val tableCols = existingTable.schema.map(_.name)

          reorderedColumns = Some(existingTable.schema.map { f =>
            query.resolve(Seq(f.name), resolver).getOrElse {
              val inputColumns = query.schema.map(_.name).mkString(", ")
              throw new AnalysisException(
                s"cannot resolve '${f.name}' given input columns: [$inputColumns]")
            }
          })

          // In `AnalyzeCreateTable`, we verified the consistency between the user-specified table
          // definition(partition columns, bucketing) and the SELECT query, here we also need to
          // verify the the consistency between the user-specified table definition and the existing
          // table definition.

          // Check if the specified partition columns match the existing table.
          val specifiedPartCols = CatalogUtils.normalizePartCols(
            tableName, tableCols, table.partitionColumnNames, resolver)
          if (specifiedPartCols != existingTable.partitionColumnNames) {
            throw new AnalysisException(
              s"""
                |Specified partitioning does not match that of the existing table $tableName.
                |Specified partition columns: [${specifiedPartCols.mkString(", ")}]
                |Existing partition columns: [${existingTable.partitionColumnNames.mkString(", ")}]
              """.stripMargin)
          }

          // Check if the specified bucketing match the existing table.
          val specifiedBucketSpec = table.bucketSpec.map { bucketSpec =>
            CatalogUtils.normalizeBucketSpec(tableName, tableCols, bucketSpec, resolver)
          }
          if (specifiedBucketSpec != existingTable.bucketSpec) {
            val specifiedBucketString =
              specifiedBucketSpec.map(_.toString).getOrElse("not bucketed")
            val existingBucketString =
              existingTable.bucketSpec.map(_.toString).getOrElse("not bucketed")
            throw new AnalysisException(
              s"""
                |Specified bucketing does not match that of the existing table $tableName.
                |Specified bucketing: $specifiedBucketString
                |Existing bucketing: $existingBucketString
              """.stripMargin)
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
    val df = reorderedColumns match {
      // Reorder the columns of the query to match the existing table.
      case Some(cols) => data.select(cols.map(Column(_)): _*)
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
      dataSource.writeAndRead(mode, df)
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
