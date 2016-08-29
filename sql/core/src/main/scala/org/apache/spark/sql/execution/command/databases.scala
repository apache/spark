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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType


/**
 * A command for users to list the databases/schemas.
 * If a databasePattern is supplied then the databases that only matches the
 * pattern would be listed.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW (DATABASES|SCHEMAS) [LIKE 'identifier_with_wildcards'];
 * }}}
 */
case class ShowDatabasesCommand(databasePattern: Option[String]) extends RunnableCommand {

  // The result of SHOW DATABASES has one column called 'databaseName'
  override val output: Seq[Attribute] = {
    AttributeReference("databaseName", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val databases =
      databasePattern.map(catalog.listDatabases).getOrElse(catalog.listDatabases())
    databases.map { d => Row(d) }
  }
}


/**
 * Command for setting the current database.
 * {{{
 *   USE database_name;
 * }}}
 */
case class SetDatabaseCommand(databaseName: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.catalog.setCurrentDatabase(databaseName)
    Seq.empty[Row]
  }

  override val output: Seq[Attribute] = Seq.empty
}
