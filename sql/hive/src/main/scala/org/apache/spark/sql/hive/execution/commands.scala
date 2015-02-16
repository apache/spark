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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType

/**
 * :: DeveloperApi ::
 * Analyzes the given table in the current database to generate statistics, which will be
 * used in query optimizations.
 *
 * Right now, it only supports Hive tables and it only updates the size of a Hive table
 * in the Hive metastore.
 */
@DeveloperApi
case class AnalyzeTable(tableName: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    sqlContext.asInstanceOf[HiveContext].analyze(tableName)
    Seq.empty[Row]
  }
}

/**
 * :: DeveloperApi ::
 * Drops a table from the metastore and removes it if it is cached.
 */
@DeveloperApi
case class DropTable(
    tableName: String,
    ifExists: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    val ifExistsClause = if (ifExists) "IF EXISTS " else ""
    try {
      hiveContext.cacheManager.tryUncacheQuery(hiveContext.table(tableName))
    } catch {
      // This table's metadata is not in
      case _: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
      // Other exceptions can be caused by users providing wrong parameters in OPTIONS
      // (e.g. invalid paths). We catch it and log a warning message.
      // Users should be able to drop such kinds of tables regardless if there is an exception.
      case e: Exception => log.warn(s"${e.getMessage}")
    }
    hiveContext.invalidateTable(tableName)
    hiveContext.runSqlHive(s"DROP TABLE $ifExistsClause$tableName")
    hiveContext.catalog.unregisterTable(Seq(tableName))
    Seq.empty[Row]
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class AddJar(path: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    hiveContext.runSqlHive(s"ADD JAR $path")
    hiveContext.sparkContext.addJar(path)
    Seq.empty[Row]
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class AddFile(path: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    hiveContext.runSqlHive(s"ADD FILE $path")
    hiveContext.sparkContext.addFile(path)
    Seq.empty[Row]
  }
}

case class CreateMetastoreDataSource(
    tableName: String,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    options: Map[String, String],
    allowExisting: Boolean,
    managedIfNoPath: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]

    if (hiveContext.catalog.tableExists(tableName :: Nil)) {
      if (allowExisting) {
        return Seq.empty[Row]
      } else {
        sys.error(s"Table $tableName already exists.")
      }
    }

    var isExternal = true
    val optionsWithPath =
      if (!options.contains("path") && managedIfNoPath) {
        isExternal = false
        options + ("path" -> hiveContext.catalog.hiveDefaultTableFilePath(tableName))
      } else {
        options
      }

    hiveContext.catalog.createDataSourceTable(
      tableName,
      userSpecifiedSchema,
      provider,
      optionsWithPath,
      isExternal)

    Seq.empty[Row]
  }
}

case class CreateMetastoreDataSourceAsSelect(
    tableName: String,
    provider: String,
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    var createMetastoreTable = false
    var isExternal = true
    val optionsWithPath =
      if (!options.contains("path")) {
        isExternal = false
        options + ("path" -> hiveContext.catalog.hiveDefaultTableFilePath(tableName))
      } else {
        options
      }

    if (sqlContext.catalog.tableExists(Seq(tableName))) {
      // Check if we need to throw an exception or just return.
      mode match {
        case SaveMode.ErrorIfExists =>
          sys.error(s"Table $tableName already exists. " +
            s"If you want to append into it, please set mode to SaveMode.Append. " +
            s"Or, if you want to overwrite it, please set mode to SaveMode.Overwrite.")
        case SaveMode.Ignore =>
          // Since the table already exists and the save mode is Ignore, we will just return.
          return Seq.empty[Row]
        case SaveMode.Append =>
          // Check if the specified data source match the data source of the existing table.
          val resolved =
            ResolvedDataSource(sqlContext, Some(query.schema), provider, optionsWithPath)
          val createdRelation = LogicalRelation(resolved.relation)
          EliminateSubQueries(sqlContext.table(tableName).logicalPlan) match {
            case l @ LogicalRelation(i: InsertableRelation) =>
              if (l.schema != createdRelation.schema) {
                val errorDescription =
                  s"Cannot append to table $tableName because the schema of this " +
                    s"DataFrame does not match the schema of table $tableName."
                val errorMessage =
                  s"""
                |$errorDescription
                |== Schemas ==
                |${sideBySide(
                s"== Expected Schema ==" +:
                  l.schema.treeString.split("\\\n"),
                s"== Actual Schema ==" +:
                  createdRelation.schema.treeString.split("\\\n")).mkString("\n")}
              """.stripMargin
                sys.error(errorMessage)
              } else if (i != createdRelation.relation) {
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
                s"== Expected Relation ==" ::
                  l.toString :: Nil,
                s"== Actual Relation ==" ::
                  createdRelation.toString :: Nil).mkString("\n")}
              """.stripMargin
                sys.error(errorMessage)
              }
            case o =>
              sys.error(s"Saving data in ${o.toString} is not supported.")
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

    val df = DataFrame(hiveContext, query)

    // Create the relation based on the data of df.
    ResolvedDataSource(sqlContext, provider, mode, optionsWithPath, df)

    if (createMetastoreTable) {
      hiveContext.catalog.createDataSourceTable(
        tableName,
        Some(df.schema),
        provider,
        optionsWithPath,
        isExternal)
    }

    Seq.empty[Row]
  }
}
