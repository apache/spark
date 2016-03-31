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

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.hive.metastore.MetaStoreUtils

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{SqlParser, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.datasources.{LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Analyzes the given table in the current database to generate statistics, which will be
 * used in query optimizations.
 *
 * Right now, it only supports Hive tables and it only updates the size of a Hive table
 * in the Hive metastore.
 */
private[hive]
case class AnalyzeTable(tableName: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.asInstanceOf[HiveContext].analyze(tableName)
    Seq.empty[Row]
  }
}

/**
 * Drops a table from the metastore and removes it if it is cached.
 */
private[hive]
case class DropTable(
    tableName: String,
    ifExists: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    val ifExistsClause = if (ifExists) "IF EXISTS " else ""
    try {
      hiveContext.cacheManager.tryUncacheQuery(hiveContext.table(tableName))
    } catch {
      // This table's metadata is not in Hive metastore (e.g. the table does not exist).
      case _: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
      case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
      // Other Throwables can be caused by users providing wrong parameters in OPTIONS
      // (e.g. invalid paths). We catch it and log a warning message.
      // Users should be able to drop such kinds of tables regardless if there is an error.
      case e: Throwable => log.warn(s"${e.getMessage}", e)
    }
    hiveContext.invalidateTable(tableName)
    val tableNameForHive = {
      // Hive's parser will unquote an identifier (see the rule of QuotedIdentifier in
      // HiveLexer.g of Hive 1.2.1). For the DROP TABLE command that we pass in Hive, we
      // will use the quoted form (db.tableName) if the table name starts with a _.
      // Otherwise, we keep the unquoted form (`db`.`tableName`), which is the same as tableName
      // passed into this DropTable class. Please note that although QuotedIdentifier rule
      // allows backticks appearing in an identifier, Hive does not actually allow such
      // an identifier be a table name. So, we do not check if a table name part has
      // any backtick or not.
      //
      // This change is at here because this patch is just for 1.6 branch and we try to
      // avoid of affecting normal cases (tables do not use _ as the first character of
      // their name).
      val identifier = SqlParser.parseTableIdentifier(tableName)
      if (identifier.table.startsWith("_")) identifier.quotedString else identifier.unquotedString
    }
    hiveContext.runSqlHive(s"DROP TABLE $ifExistsClause$tableNameForHive")
    hiveContext.catalog.unregisterTable(TableIdentifier(tableName))
    Seq.empty[Row]
  }
}

private[hive]
case class AddJar(path: String) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("result", IntegerType, false) :: Nil)
    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.addJar(path)

    Seq(Row(0))
  }
}

private[hive]
case class AddFile(path: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    hiveContext.runSqlHive(s"ADD FILE $path")
    hiveContext.sparkContext.addFile(path)
    Seq.empty[Row]
  }
}

private[hive]
case class CreateMetastoreDataSource(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    options: Map[String, String],
    allowExisting: Boolean,
    managedIfNoPath: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    if (!MetaStoreUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined && !MetaStoreUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

    val tableName = tableIdent.unquotedString
    val hiveContext = sqlContext.asInstanceOf[HiveContext]

    if (hiveContext.catalog.tableExists(tableIdent)) {
      if (allowExisting) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table $tableName already exists.")
      }
    }

    var isExternal = true
    val optionsWithPath =
      if (!options.contains("path") && managedIfNoPath) {
        isExternal = false
        options + ("path" -> hiveContext.catalog.hiveDefaultTableFilePath(tableIdent))
      } else {
        options
      }

    hiveContext.catalog.createDataSourceTable(
      tableIdent,
      userSpecifiedSchema,
      Array.empty[String],
      provider,
      optionsWithPath,
      isExternal)

    Seq.empty[Row]
  }
}

private[hive]
case class CreateMetastoreDataSourceAsSelect(
    tableIdent: TableIdentifier,
    provider: String,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    if (!MetaStoreUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined && !MetaStoreUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

    val tableName = tableIdent.unquotedString
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    var createMetastoreTable = false
    var isExternal = true
    val optionsWithPath =
      if (!options.contains("path")) {
        isExternal = false
        options + ("path" -> hiveContext.catalog.hiveDefaultTableFilePath(tableIdent))
      } else {
        options
      }

    var existingSchema = None: Option[StructType]
    if (sqlContext.catalog.tableExists(tableIdent)) {
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
          val resolved = ResolvedDataSource(
            sqlContext, Some(query.schema.asNullable), partitionColumns, provider, optionsWithPath)
          val createdRelation = LogicalRelation(resolved.relation)
          EliminateSubQueries(sqlContext.catalog.lookupRelation(tableIdent)) match {
            case l @ LogicalRelation(_: InsertableRelation | _: HadoopFsRelation, _) =>
              if (l.relation != createdRelation.relation) {
                val errorDescription =
                  s"Cannot append to table $tableName because the resolved relation does not " +
                  s"match the existing relation of $tableName. " +
                  s"You can use insertInto($tableName, false) to append this DataFrame to the " +
                  s"table $tableName and using its data source and options."
                val errorMessage =
                  s"""
                     |$errorDescription
                     |== Relations ==
                     |${sideBySide(
                        s"== Expected Relation ==" :: l.toString :: Nil,
                        s"== Actual Relation ==" :: createdRelation.toString :: Nil
                      ).mkString("\n")}
                   """.stripMargin
                throw new AnalysisException(errorMessage)
              }
              existingSchema = Some(l.schema)
            case o =>
              throw new AnalysisException(s"Saving data in ${o.toString} is not supported.")
          }
        case SaveMode.Overwrite =>
          hiveContext.sql(s"DROP TABLE IF EXISTS $tableName")
          // Need to create the table again.
          createMetastoreTable = true
      }
    } else {
      // The table does not exist. We need to create it in metastore.
      createMetastoreTable = true
    }

    val data = DataFrame(hiveContext, query)
    val df = existingSchema match {
      // If we are inserting into an existing table, just use the existing schema.
      case Some(schema) => sqlContext.internalCreateDataFrame(data.queryExecution.toRdd, schema)
      case None => data
    }

    // Create the relation based on the data of df.
    val resolved =
      ResolvedDataSource(sqlContext, provider, partitionColumns, mode, optionsWithPath, df)

    if (createMetastoreTable) {
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      hiveContext.catalog.createDataSourceTable(
        tableIdent,
        Some(resolved.relation.schema),
        partitionColumns,
        provider,
        optionsWithPath,
        isExternal)
    }

    // Refresh the cache of the table in the catalog.
    hiveContext.catalog.refreshTable(tableIdent)
    Seq.empty[Row]
  }
}
