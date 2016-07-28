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

import java.util.regex.Pattern

import org.apache.spark.internal.Logging
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
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    if (!CreateDataSourceTableUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined &&
      !CreateDataSourceTableUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

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
      val res = dataSource match {
        case r: HadoopFsRelation => r.partitionSchema.fieldNames
        case _ => Array.empty[String]
      }
      if (userSpecifiedPartitionColumns.length > 0) {
        // The table does not have a specified schema, which means that the schema will be inferred
        // when we load the table. So, we are not expecting partition columns and we will discover
        // partitions when we load the table. However, if there are specified partition columns,
        // we simply ignore them and provide a warning message.
        logWarning(
          s"Specified partition columns (${userSpecifiedPartitionColumns.mkString(",")}) will be " +
            s"ignored. The schema and partition columns of table $tableIdent are inferred. " +
            s"Schema: ${dataSource.schema.simpleString}; " +
            s"Partition columns: ${res.mkString("(", ", ", ")")}")
      }
      res
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

    sessionState.catalog.createTable(table, ignoreIfExists)
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
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    if (!CreateDataSourceTableUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined &&
      !CreateDataSourceTableUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

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


object CreateDataSourceTableUtils extends Logging {

  val DATASOURCE_PREFIX = "spark.sql.sources."
  val DATASOURCE_PROVIDER = DATASOURCE_PREFIX + "provider"
  val DATASOURCE_WRITEJOBUUID = DATASOURCE_PREFIX + "writeJobUUID"
  val DATASOURCE_OUTPUTPATH = DATASOURCE_PREFIX + "output.path"
  val DATASOURCE_SCHEMA = DATASOURCE_PREFIX + "schema"
  val DATASOURCE_SCHEMA_PREFIX = DATASOURCE_SCHEMA + "."
  val DATASOURCE_SCHEMA_NUMPARTS = DATASOURCE_SCHEMA_PREFIX + "numParts"
  val DATASOURCE_SCHEMA_NUMPARTCOLS = DATASOURCE_SCHEMA_PREFIX + "numPartCols"
  val DATASOURCE_SCHEMA_NUMSORTCOLS = DATASOURCE_SCHEMA_PREFIX + "numSortCols"
  val DATASOURCE_SCHEMA_NUMBUCKETS = DATASOURCE_SCHEMA_PREFIX + "numBuckets"
  val DATASOURCE_SCHEMA_NUMBUCKETCOLS = DATASOURCE_SCHEMA_PREFIX + "numBucketCols"
  val DATASOURCE_SCHEMA_PART_PREFIX = DATASOURCE_SCHEMA_PREFIX + "part."
  val DATASOURCE_SCHEMA_PARTCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "partCol."
  val DATASOURCE_SCHEMA_BUCKETCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "bucketCol."
  val DATASOURCE_SCHEMA_SORTCOL_PREFIX = DATASOURCE_SCHEMA_PREFIX + "sortCol."

  /**
   * Checks if the given name conforms the Hive standard ("[a-zA-z_0-9]+"),
   * i.e. if this name only contains characters, numbers, and _.
   *
   * This method is intended to have the same behavior of
   * org.apache.hadoop.hive.metastore.MetaStoreUtils.validateName.
   */
  def validateName(name: String): Boolean = {
    val tpat = Pattern.compile("[\\w_]+")
    val matcher = tpat.matcher(name)

    matcher.matches()
  }
}
