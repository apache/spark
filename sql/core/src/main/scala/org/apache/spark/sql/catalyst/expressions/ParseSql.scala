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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, FunctionRegistryBase, TypeCheckResult}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.parser.{ParseSqlMode, ParseSqlResult}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Parses a SQL batch string and returns a compact JSON array describing its
 * statements (source position, identifier/code, lineage references,
 * select-list names, parameters). `PARSE` mode is unresolved. `ANALYZE`
 * mode also types the select list and fully qualifies resolved relations
 * and routines. A statement that does not parse is represented by a
 * STANDARD-format error object at its position in the array.
 *
 * Behind [[SQLConf.PARSE_SQL_ENABLED]] while the JSON contract is still
 * evolving. Designed for batch evaluation over DataFrames of SQL text.
 * User-facing parse / analyze errors become JSON; unexpected internal
 * failures propagate. `ANALYZE` needs an active Spark session (catalog).
 */
// scalastyle:off line.size.limit
// scalastyle:off nonascii
@ExpressionDescription(
  usage = """_FUNC_(sqlStmt[, mode]) - Splits `sqlStmt` into SQL statements, parses each with
    the stock Spark SQL parser, and returns a JSON array describing them (1-based
    UTF-16 code-unit `start` and UTF-16 code-unit `length`, parse success, Table 39
    statement identifier/code, target and source table references for lineage,
    select-list column names, and parameter markers).
    `mode` is `PARSE` (default) or `ANALYZE` (case-insensitive). `PARSE` is unresolved.
    `ANALYZE` also types selected columns and fully qualifies resolved relations and
    routines, and reports analysis failures as JSON without aborting the batch.
    Statement length excludes surrounding whitespace and the terminating semicolon.
    Session parser extensions are not applied.
    Requires spark.sql.function.parseSql.enabled=true. On syntax / parse error returns JSON
    for that statement with `parse_success` false, source location, and a nested STANDARD
    error object instead of throwing or stopping the remaining statements. Nested error
    locations are statement-relative. An empty or closed-comment-only batch returns `[]`.""",
  arguments = """
    Arguments:
      * sqlStmt - A SQL batch string to split and parse.
        An expression that evaluates to a string.
      * mode - Optional `PARSE` or `ANALYZE` (case-insensitive). Defaults to `PARSE`.
        An expression that evaluates to a string.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('SELECT 1;SELECT 2');
       [{"start":1,"length":8,"parse_success":true,"statement_identifier":"SELECT","statement_code":21,"select_list":[{"name":[]}]},{"start":10,"length":8,"parse_success":true,"statement_identifier":"SELECT","statement_code":21,"select_list":[{"name":[]}]}]
      > SELECT get_json_object(_FUNC_('SELECT ''😀'';SELECT 2'), '$[1].start');
       13
      > SELECT get_json_object(_FUNC_('SELEC'), '$[0].error.errorClass');
       PARSE_SYNTAX_ERROR
      > SELECT get_json_object(_FUNC_('SELECT 1 AS a', 'ANALYZE'), '$[0].select_list[0].type');
       INT
  """,
  group = "misc_funcs",
  since = "4.4.0")
// scalastyle:on nonascii
// scalastyle:on line.size.limit
case class ParseSql(sqlStmt: Expression, mode: Expression)
  extends BinaryExpression
  with ImplicitCastInputTypes
  with CodegenFallback {

  def this(sqlStmt: Expression) = this(sqlStmt, Literal("PARSE"))

  override def prettyName: String = "parse_sql"

  override def left: Expression = sqlStmt

  override def right: Expression = mode

  override def nullable: Boolean = true

  override def nullIntolerant: Boolean = true

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] =
    Seq.fill(2)(StringTypeWithCollation(supportsTrimCollation = true))

  // ANALYZE reads the session catalog, so results are not context-independent.
  override def contextIndependentFoldable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!SQLConf.get.parseSqlEnabled) {
      throw new AnalysisException(
        errorClass = "FEATURE_NOT_ENABLED",
        messageParameters = Map(
          "featureName" -> "parse_sql",
          "configKey" -> SQLConf.PARSE_SQL_ENABLED.key,
          "configValue" -> "true"))
    }
    val typeCheck = super.checkInputDataTypes()
    if (!typeCheck.isSuccess) {
      typeCheck
    } else {
      if (mode.foldable) {
        val value = mode.eval()
        if (value != null) {
          val modeStr = value.asInstanceOf[UTF8String].toString
          if (ParseSqlMode.parse(modeStr).isEmpty) {
            throw QueryCompilationErrors.invalidParseSqlMode(mode)
          }
        }
      }
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def nullSafeEval(sqlInput: Any, modeInput: Any): Any = {
    val sqlText = sqlInput.asInstanceOf[UTF8String].toString
    val modeStr = modeInput.asInstanceOf[UTF8String].toString
    val parsedMode = ParseSqlMode.parse(modeStr).getOrElse {
      throw QueryExecutionErrors.invalidParseSqlMode(modeStr)
    }
    UTF8String.fromString(ParseSqlResult.fromSql(sqlText, parsedMode))
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ParseSql =
    copy(sqlStmt = newLeft, mode = newRight)
}

object ParseSql {
  def apply(sqlStmt: Expression): ParseSql = new ParseSql(sqlStmt, Literal("PARSE"))

  /** Register the builtin with a session function registry and the global builtin set. */
  def register(registry: FunctionRegistry): Unit = {
    val (info, builder) = FunctionRegistryBase.build[ParseSql]("parse_sql", Some("4.4.0"))
    // Keep the session registry in sync for the first session (cloned before this runs).
    registry.registerFunction(
      FunctionRegistry.builtinFunctionIdentifier("parse_sql"),
      info,
      builder)
    // Also publish into FunctionRegistry.builtin / functionSet so SHOW USER/SYSTEM
    // FUNCTIONS classify parse_sql as SYSTEM rather than a user/temp function.
    FunctionRegistry.registerExtraBuiltin("parse_sql", info, builder)
  }
}
