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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.ng.{AbstractSqlParser, AstBuilder}
import org.apache.spark.sql.catalyst.parser.ng.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._

/**
 * Concrete parser for Spark SQL statements.
 */
object SparkSqlParser extends AbstractSqlParser{
  val astBuilder = new SparkSqlAstBuilder
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 */
class SparkSqlAstBuilder extends AstBuilder {
  import AstBuilder._
  import org.apache.spark.sql.catalyst.parser.ParseUtils._

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
      SetCommand(Some(remainder.trim -> None))
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
      logWarning("SHOW TABLES LIKE option is ignored.")
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
    val query = Option(ctx.query).map(plan)
    CacheTableCommand(ctx.identifier.getText, query, ctx.LAZY != null)
  }

  /**
   * Create an [[UncacheTableCommand]] logical plan.
   */
  override def visitUncacheTable(ctx: UncacheTableContext): LogicalPlan = withOrigin(ctx) {
    UncacheTableCommand(ctx.identifier.getText)
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
      logWarning("EXPLAIN FORMATTED option is ignored.")
    }
    if (options.exists(_.LOGICAL != null)) {
      logWarning("EXPLAIN LOGICAL option is ignored.")
    }

    // Create the explain comment.
    val statement = plan(ctx.statement)
    if (isExplainableStatement(statement)) {
      ExplainCommand(statement, extended = options.exists(_.EXTENDED != null))
    } else {
      ExplainCommand(OneRowRelation)
    }
  }

  /**
   * Determine if a plan should be explained at all.
   */
  protected def isExplainableStatement(plan: LogicalPlan): Boolean = plan match {
    case _: datasources.DescribeCommand => false
    case _ => true
  }

  /**
   * Create a [[DescribeCommand]] logical plan.
   */
  override def visitDescribeTable(ctx: DescribeTableContext): LogicalPlan = withOrigin(ctx) {
    // FORMATTED and columns are not supported. Return null and let the parser decide what to do
    // with this (create an exception or pass it on to a different system).
    if (ctx.describeColName != null || ctx.FORMATTED != null) {
      null
    } else {
      // Partitioning clause is ignored.
      if (ctx.partitionSpec != null) {
        logWarning("DESCRIBE PARTITIONING option is ignored.")
      }
      datasources.DescribeCommand(
        UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier), None),
        ctx.EXTENDED != null)
    }
  }

  /**
   * Validate a create table statement and return the [[TableIdentifier]].
   */
  override def visitCreateTable(
      ctx: CreateTableContext): (TableIdentifier, Boolean, Boolean) = withOrigin(ctx) {
    val temporary = ctx.TEMPORARY != null
    val ifNotExists = ctx.EXISTS != null
    assert(!temporary || !ifNotExists,
      "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.",
      ctx)
    (visitTableIdentifier(ctx.tableIdentifier), temporary, ifNotExists)
  }

  /**
   * Create a [[CreateTableUsing]] logical plan.
   */
  override def visitCreateTableUsing(ctx: CreateTableUsingContext): LogicalPlan = withOrigin(ctx) {
    val (table, temporary, ifNotExists) = visitCreateTable(ctx.createTable)
    val options = Option(ctx.tableProperties)
      .map(visitTableProperties)
      .getOrElse(Map.empty)
    CreateTableUsing(
      table,
      Option(ctx.colTypeList).map(visitColTypeList),
      ctx.tableProvider.qualifiedName.getText,
      temporary,
      options,
      ifNotExists,
      managedIfNoPath = false)
  }

  /**
   * Create a [[CreateTableUsingAsSelect]] logical plan.
   *
   * TODO add bucketing and partitioning.
   */
  override def visitCreateTableUsingAsSelect(
      ctx: CreateTableUsingAsSelectContext): LogicalPlan = withOrigin(ctx) {
    // Get basic configuration.
    val (table, temporary, ifNotExists) = visitCreateTable(ctx.createTable)
    val options = Option(ctx.tableProperties)
      .map(visitTableProperties)
      .getOrElse(Map.empty)

    // Get the backing query.
    val query = plan(ctx.query)

    // Determine the storage mode.
    val mode = if (ifNotExists) {
      SaveMode.Ignore
    } else if (temporary) {
      SaveMode.Overwrite
    } else {
      SaveMode.ErrorIfExists
    }

    CreateTableUsingAsSelect(
      table,
      ctx.tableProvider.qualifiedName.getText,
      temporary,
      Array.empty,
      None,
      mode,
      options,
      query
    )
  }

  /**
   * Convert TableProperties into a key-value map.
   */
  override def visitTableProperties(
      ctx: TablePropertiesContext): Map[String, String] = withOrigin(ctx) {
    ctx.tableProperty.asScala.map { property =>
      // A key can either be a String or a collection of dot separated elements. We need to treat
      // these differently.
      val key = if (property.key.STRING != null) {
        unescapeSQLString(property.key.STRING.getText)
      } else {
        property.key.getText
      }
      val value = Option(property.value).map(t => unescapeSQLString(t.getText)).getOrElse("")
      key -> value
    }.toMap
  }
}
