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
package org.apache.spark.sql.execution

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.misc.Interval

import org.apache.spark.sql.catalyst.parser.ng.{AbstractSqlParser, AstBuilder}
import org.apache.spark.sql.catalyst.parser.ng.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.execution.datasources.RefreshTable

/**
 * Concrete parser for Spark SQL statements.
 */
object SparkSqlParser extends AbstractSqlParser{
  val astBuilder = new SparkSqlAstBuilder
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 *
 * TODO:
 * DESC TABLE
 * CREATE TABLE USING
 */
class SparkSqlAstBuilder extends AstBuilder {
  import AstBuilder._

  /**
   * Create a [[SetCommand]] logical plan.
   *
   * Note that we assume that everything after the SET keyword is assumed to be a part of the
   * key-value pair. The split between key and value is made by searching for the first `=`
   * character in the raw string.
   */
  override def visitSetConfiguration(ctx: SetConfigurationContext): LogicalPlan = withOrigin(ctx) {
    // Get the remaining text from the stream.
    val stream = ctx.getStop.getInputStream
    val interval = Interval.of(ctx.getStart.getStopIndex + 1, stream.size())
    val remainder = stream.getText(interval)

    // Construct the command.
    val keyValueSeparatorIndex = remainder.indexOf('=')
    if (keyValueSeparatorIndex >= 0) {
      val key = remainder.substring(0, keyValueSeparatorIndex).trim
      val value = remainder.substring(keyValueSeparatorIndex + 1).trim
      SetCommand(Some(key -> Option(value)))
    } else if (remainder.nonEmpty) {
      SetCommand(Some(remainder -> None))
    } else {
      SetCommand(None)
    }
  }

  /**
   * Create a [[SetDatabaseCommand]] logical plan.
   */
  override def visitUse(ctx: UseContext): LogicalPlan = withOrigin(ctx) {
    SetDatabaseCommand(ctx.db.getText)
  }

  /**
   * Create a [[ShowTablesCommand]] logical plan.
   */
  override def visitShowTables(ctx: ShowTablesContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.LIKE != null) {
      logWarning("SHOW TABLES ... LIKE ... is not supported, ignoring the LIKE pattern.")
    }
    ShowTablesCommand(Option(ctx.db).map(_.getText))
  }

  /**
   * Create a [[RefreshTable]] logical plan.
   */
  override def visitRefreshTable(ctx: RefreshTableContext): LogicalPlan = withOrigin(ctx) {
    RefreshTable(visitTableIdentifier(ctx.tableIdentifier))
  }

  /**
   * Create a [[CacheTableCommand]] logical plan.
   */
  override def visitCacheTable(ctx: CacheTableContext): LogicalPlan = withOrigin(ctx) {
    val id = visitTableIdentifier(ctx.tableIdentifier)
    val query = Option(ctx.query).map(plan)
    CacheTableCommand(id.table, query, ctx.LAZY != null)
  }

  /**
   * Create an [[UncacheTableCommand]] logical plan.
   */
  override def visitUncacheTable(ctx: UncacheTableContext): LogicalPlan = withOrigin(ctx) {
    val id = visitTableIdentifier(ctx.tableIdentifier)
    UncacheTableCommand(id.table)
  }

  /**
   * Create a [[ClearCacheCommand]] logical plan.
   */
  override def visitClearCache(ctx: ClearCacheContext): LogicalPlan = withOrigin(ctx) {
    ClearCacheCommand
  }

  /**
   * Create an [[ExplainCommand]] logical plan.
   */
  override def visitExplain(ctx: ExplainContext): LogicalPlan = withOrigin(ctx) {
    val options = ctx.explainOption.asScala
    if (options.exists(_.FORMATTED != null)) {
      logWarning("EXPLAIN ... FORMATTED ... is not supported, ignoring FORMATTED option.")
    }
    if (options.exists(_.LOGICAL != null)) {
      logWarning("EXPLAIN ... LOGICAL ... is not supported, ignoring LOGICAL option.")
    }

    // Create the explain comment.
    val statement = plan(ctx.statement)
    if (isExplainStatement(statement)) {
      ExplainCommand(statement, extended = options.exists(_.EXTENDED != null))
    } else {
      ExplainCommand(OneRowRelation)
    }
  }

  /**
   * Determine if a plan should be explained at all.
   */
  protected def isExplainStatement(plan: LogicalPlan): Boolean = plan match {
    case _: DescribeCommand => false
    case _ => true
  }
}
