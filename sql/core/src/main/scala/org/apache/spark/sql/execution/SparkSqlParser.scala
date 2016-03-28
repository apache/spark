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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ng.{AbstractSqlParser, AstBuilder}
import org.apache.spark.sql.catalyst.parser.ng.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.execution.command.{DescribeCommand => _, _}
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
  import org.apache.spark.sql.catalyst.parser.ng.ParserUtils._

  /**
   * Create a [[SetCommand]] logical plan.
   *
   * Note that we assume that everything after the SET keyword is assumed to be a part of the
   * key-value pair. The split between key and value is made by searching for the first `=`
   * character in the raw string.
   */
  override def visitSetConfiguration(ctx: SetConfigurationContext): LogicalPlan = withOrigin(ctx) {
    // Construct the command.
    val raw = remainder(ctx.SET.getSymbol)
    val keyValueSeparatorIndex = raw.indexOf('=')
    if (keyValueSeparatorIndex >= 0) {
      val key = raw.substring(0, keyValueSeparatorIndex).trim
      val value = raw.substring(keyValueSeparatorIndex + 1).trim
      SetCommand(Some(key -> Option(value)))
    } else if (raw.nonEmpty) {
      SetCommand(Some(raw.trim -> None))
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
    if (ctx.describeColName != null || ctx.FORMATTED != null || ctx.partitionSpec != null) {
      null
    } else {
      datasources.DescribeCommand(
        visitTableIdentifier(ctx.tableIdentifier),
        ctx.EXTENDED != null)
    }
  }

  /** Type to keep track of a table header. */
  type TableHeader = (TableIdentifier, Boolean, Boolean, Boolean)

  /**
   * Validate a create table statement and return the [[TableIdentifier]].
   */
  override def visitCreateTableHeader(
      ctx: CreateTableHeaderContext): TableHeader = withOrigin(ctx) {
    val temporary = ctx.TEMPORARY != null
    val ifNotExists = ctx.EXISTS != null
    assert(!temporary || !ifNotExists,
      "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.",
      ctx)
    (visitTableIdentifier(ctx.tableIdentifier), temporary, ifNotExists, ctx.EXTERNAL != null)
  }

  /**
   * Create a [[CreateTableUsing]] or a [[CreateTableUsingAsSelect]] logical plan.
   *
   * TODO add bucketing and partitioning.
   */
  override def visitCreateTableUsing(ctx: CreateTableUsingContext): LogicalPlan = withOrigin(ctx) {
    val (table, temp, ifNotExists, external) = visitCreateTableHeader(ctx.createTableHeader)
    if (external) {
      logWarning("EXTERNAL option is not supported.")
    }
    val options = Option(ctx.tablePropertyList).map(visitTablePropertyList).getOrElse(Map.empty)
    val provider = ctx.tableProvider.qualifiedName.getText

    if (ctx.query != null) {
      // Get the backing query.
      val query = plan(ctx.query)

      // Determine the storage mode.
      val mode = if (ifNotExists) {
        SaveMode.Ignore
      } else if (temp) {
        SaveMode.Overwrite
      } else {
        SaveMode.ErrorIfExists
      }
      CreateTableUsingAsSelect(table, provider, temp, Array.empty, None, mode, options, query)
    } else {
      val struct = Option(ctx.colTypeList).map(createStructType)
      CreateTableUsing(table, struct, provider, temp, options, ifNotExists, managedIfNoPath = false)
    }
  }

  /**
    * Convert a table property list into a key-value map.
    */
  override def visitTablePropertyList(
      ctx: TablePropertyListContext): Map[String, String] = withOrigin(ctx) {
    ctx.tableProperty.asScala.map { property =>
      // A key can either be a String or a collection of dot separated elements. We need to treat
      // these differently.
      val key = if (property.key.STRING != null) {
        string(property.key.STRING)
      } else {
        property.key.getText
      }
      val value = Option(property.value).map(string).orNull
      key -> value
    }.toMap
  }
}
