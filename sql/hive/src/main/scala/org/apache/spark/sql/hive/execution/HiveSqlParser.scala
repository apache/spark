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

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.apache.hadoop.hive.ql.exec.FunctionRegistry

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ng._
import org.apache.spark.sql.catalyst.parser.ng.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ScriptInputOutputSchema}
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.hive.HiveGenericUDTF
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper

/**
 * Concrete parser for HiveQl statements.
 */
object HiveSqlParser extends AbstractSqlParser{
  val astBuilder = new SparkSqlAstBuilder

  override protected def nativeCommand(sqlText: String): LogicalPlan = {
    HiveNativeCommand(sqlText)
  }
}

/**
 * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
 *
 * TODO:
 * - HiveScriptIOSchema
 * - ALTER VIEW
 * - CREATE VIEW
 * - CREATE TABLE
 * - GenericFileFormat! Together with SparkSqlParser!
 */
class HiveSqlAstBuilder extends SparkSqlAstBuilder {
  import AstBuilder._

  /**
   * Create a [[HiveNativeCommand]].
   */
  private def nativeCommand(ctx: ParserRuleContext): LogicalPlan = withOrigin(ctx) {
    HiveNativeCommand(source(ctx))
  }

  /**
   * Create a [[Generator]]. Override this method in order to support custom Generators.
   */
  override protected def withGenerator(
      name: String,
      expressions: Seq[Expression],
      ctx: LateralViewContext): Generator = {
    val info = Option(FunctionRegistry.getFunctionInfo(name.toLowerCase)).getOrElse {
      throw new ParseException(s"Couldn't find Generator function '$name'", ctx)
    }
    HiveGenericUDTF(name, new HiveFunctionWrapper(info.getFunctionClass.getName), expressions)
  }

  /**
   * Create a (Hive based) [[ScriptInputOutputSchema]].
   */
  override protected def withScriptIOSchema(
      inRowFormat: RowFormatContext,
      outRowFormat: RowFormatContext,
      outRecordReader: Token,
      schemaLess: Boolean): ScriptInputOutputSchema = {
    null
  }

  override def visitRowFormatSerde(ctx: RowFormatSerdeContext): LogicalPlan = withOrigin(ctx) {
    null
  }

  override def visitRowFormatDelimited(
      ctx: RowFormatDelimitedContext): LogicalPlan = withOrigin(ctx) {
    null
  }

  /**
   * Create an [[AddJar]] or [[AddFile]] command depending on the requested resource.
   */
  override def visitAddResource(ctx: AddResourceContext): LogicalPlan = withOrigin(ctx) {
    ctx.identifier.getText.toLowerCase match {
      case "file" => AddFile(remainder(ctx).trim)
      case "jar" => AddJar(remainder(ctx).trim)
      case other => throw new ParseException(s"Unsupported resource type '$other'.", ctx)
    }
  }

  /**
   * Create a [[DropTable]] command.
   */
  override def visitDropTable(ctx: DropTableContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.PURGE != null) {
      logWarning("PURGE option is ignored.")
    }
    if (ctx.REPLICATION != null) {
      logWarning("REPLICATION clause is ignored.")
    }
    DropTable(visitTableIdentifier(ctx.tableIdentifier).toString, ctx.EXISTS != null)
  }

  /**
   * Create an [[AnalyzeTable]] command. This currently only implements the NOSCAN option (other
   * options are passed on to Hive) e.g.:
   * {{{
   *   ANALYZE TABLE table COMPUTE STATISTICS NOSCAN;
   * }}}
   */
  override def visitAnalyze(ctx: AnalyzeContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.partitionSpec == null &&
      ctx.identifier != null &&
      ctx.identifier.getText.toLowerCase == "noscan") {
      AnalyzeTable(visitTableIdentifier(ctx.tableIdentifier).toString)
    } else {
      nativeCommand(ctx)
    }
  }

  /**
   * Pass a DFS command to Hive.
   */
  override def visitDfs(ctx: DfsContext): LogicalPlan = nativeCommand(ctx)

  /**
   * Pass a TRUNCATE command to Hive.
   */
  override def visitTruncate(ctx: TruncateContext): LogicalPlan = nativeCommand(ctx)
}
