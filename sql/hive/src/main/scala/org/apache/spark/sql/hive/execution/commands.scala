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
import org.apache.spark.sql.sources.ResolvedDataSource
import org.apache.spark.sql.{DataFrame, SQLContext}
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
    allowExisting: Boolean) extends RunnableCommand {

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
      if (!options.contains("path")) {
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
    options: Map[String, String],
    allowExisting: Boolean,
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]

    if (hiveContext.catalog.tableExists(tableName :: Nil)) {
      if (allowExisting) {
        return Seq.empty[Row]
      } else {
        sys.error(s"Table $tableName already exists.")
      }
    }

    val df = DataFrame(hiveContext, query)
    var isExternal = true
    val optionsWithPath =
      if (!options.contains("path")) {
        isExternal = false
        options + ("path" -> hiveContext.catalog.hiveDefaultTableFilePath(tableName))
      } else {
        options
      }

    // Create the relation based on the data of df.
    ResolvedDataSource(sqlContext, provider, optionsWithPath, df)

    hiveContext.catalog.createDataSourceTable(
      tableName,
      None,
      provider,
      optionsWithPath,
      isExternal)

    Seq.empty[Row]
  }
}
