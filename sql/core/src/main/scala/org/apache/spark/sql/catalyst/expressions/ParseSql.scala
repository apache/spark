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
import org.apache.spark.sql.catalyst.parser.ParseSqlResult
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Parses a SQL statement string and returns a compact JSON description of the
 * unresolved statement (identifier/code, lineage references, select-list names,
 * parameters), or a STANDARD-format error object when the statement does not
 * parse.
 *
 * Behind [[SQLConf.PARSE_SQL_ENABLED]] while the JSON contract is still
 * evolving. Designed for batch evaluation over DataFrames of SQL text.
 * User-facing parse errors become JSON; unexpected internal failures propagate.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_(sqlStmt) - Parses `sqlStmt` with the stock Spark SQL parser and
    returns a JSON string describing the statement (parse success, Table 39 statement
    identifier/code, target and source table references for lineage, select-list column
    names, and parameter markers). Session parser extensions are not applied.
    Requires spark.sql.function.parseSql.enabled=true. On syntax / parse error returns JSON
    with `parse_success` false, source location, and a nested STANDARD error object
    instead of throwing.""",
  arguments = """
    Arguments:
      * sqlStmt - A SQL statement string to parse.
        An expression that evaluates to a string.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('SELECT a, b FROM t');
       {"parse_success":true,"statement_identifier":"SELECT","statement_code":21,"source_table_references":[["t"]],"select_list":[{"name":["a"]},{"name":["b"]}]}
      > SELECT get_json_object(_FUNC_('SELEC'), '$.error.errorClass');
       PARSE_SYNTAX_ERROR
  """,
  group = "misc_funcs",
  since = "4.4.0")
// scalastyle:on line.size.limit
case class ParseSql(child: Expression)
  extends UnaryExpression
  with ImplicitCastInputTypes
  with CodegenFallback {

  override def prettyName: String = "parse_sql"

  override def nullable: Boolean = true

  override def nullIntolerant: Boolean = true

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true))

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!SQLConf.get.parseSqlEnabled) {
      throw new AnalysisException(
        errorClass = "FEATURE_NOT_ENABLED",
        messageParameters = Map(
          "featureName" -> "parse_sql",
          "configKey" -> SQLConf.PARSE_SQL_ENABLED.key,
          "configValue" -> "true"))
    }
    super.checkInputDataTypes()
  }

  override def nullSafeEval(input: Any): Any = {
    val sql = input.asInstanceOf[UTF8String].toString
    UTF8String.fromString(ParseSqlResult.fromSql(sql))
  }

  override protected def withNewChildInternal(newChild: Expression): ParseSql =
    copy(child = newChild)
}

object ParseSql {
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
