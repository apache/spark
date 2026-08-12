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

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.parser.ParseCommandResult
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Parses a SQL statement string and returns a compact JSON description of the
 * unresolved statement (identifier/code, references, select list, parameters),
 * or a STANDARD-format error object when the statement does not parse.
 *
 * Designed for batch evaluation over DataFrames of SQL text; never throws on
 * syntax errors so a single bad row does not fail the query.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_(sqlStmt) - Parses `sqlStmt` and returns a JSON string describing the
    statement (parse success, Table 39 statement identifier/code, table and function
    references, select-list columns, and parameter markers). On syntax error returns
    JSON with `parse_success` false, source location, and a nested STANDARD error object
    instead of throwing.""",
  arguments = """
    Arguments:
      * sqlStmt - A SQL statement string to parse.
        An expression that evaluates to a string.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('SELECT a, b FROM t');
       {"parse_success":true,"statement_identifier":"SELECT","statement_code":21,...}
      > SELECT _FUNC_('SELEC');
       {"parse_success":false,"error":{"errorClass":"PARSE_SYNTAX_ERROR",...}}
  """,
  group = "misc_funcs",
  since = "4.3.0")
// scalastyle:on line.size.limit
case class ParseCommand(child: Expression)
  extends UnaryExpression
  with ImplicitCastInputTypes
  with CodegenFallback {

  override def prettyName: String = "parse_command"

  override def nullable: Boolean = true

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation(supportsTrimCollation = true))

  override def nullSafeEval(input: Any): Any = {
    val sql = input.asInstanceOf[UTF8String].toString
    UTF8String.fromString(ParseCommandResult.fromSql(sql))
  }

  override protected def withNewChildInternal(newChild: Expression): ParseCommand =
    copy(child = newChild)
}
