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

package org.apache.spark.sql.connector.util

import java.util.Locale
import java.util.regex.Pattern

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Base class for parser extensions that add custom infix operators to SQL.
 * Subclasses define operator names and their rewrite rules. Operators are
 * rewritten to function-call syntax before passing to the underlying parser,
 * allowing them to flow through the standard custom predicate pushdown path
 * (Layer 2: SupportsCustomPredicates).
 *
 * Example usage with SparkSessionExtensions:
 * {{{
 * extensions.injectParser { (session, parser) =>
 *   new CustomOperatorParserExtension(parser) {
 *     override def customOperators: Map[String, String] = Map(
 *       "INDEXQUERY" -> "indexquery"
 *     )
 *   }
 * }
 * }}}
 *
 * This rewrites `col INDEXQUERY 'param'` to `indexquery(col, 'param')`
 * before the SQL is parsed by the delegate parser.
 *
 * Limitations: The rewriting handles common cases where operands are
 * simple identifiers, literals, or backtick-quoted names. For complex
 * operand expressions (e.g., `a + b INDEXQUERY func('x')`), users
 * should use function-call syntax directly.
 *
 * @since 4.1.0
 */
@DeveloperApi
abstract class CustomOperatorParserExtension(delegate: ParserInterface)
    extends ParserInterface {

  /**
   * Define custom infix operators. Each entry maps an operator keyword
   * (case-insensitive) to a function name used in the rewritten query.
   *
   * Example: `Map("INDEXQUERY" -> "indexquery")` rewrites
   * `col INDEXQUERY 'param'` to `indexquery(col, 'param')`
   */
  def customOperators: Map[String, String]

  override def parsePlan(sqlText: String): LogicalPlan =
    delegate.parsePlan(rewriteCustomOperators(sqlText))

  override def parseQuery(sqlText: String): LogicalPlan =
    delegate.parseQuery(rewriteCustomOperators(sqlText))

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)

  override def parseRoutineParam(sqlText: String): StructType =
    delegate.parseRoutineParam(sqlText)

  private def rewriteCustomOperators(sql: String): String = {
    if (customOperators.isEmpty) return sql

    // Extract string literals so we don't rewrite inside them
    val literals = new ArrayBuffer[String]()
    val masked = maskStringLiterals(sql, literals)

    // Apply each operator rewrite
    val rewritten = customOperators.foldLeft(masked) { case (s, (op, func)) =>
      rewriteInfixToFunction(s, op, func)
    }

    // Restore string literals
    restoreLiterals(rewritten, literals)
  }

  /**
   * Replace string literals with placeholders to avoid rewriting inside them.
   * Handles single-quoted strings with SQL-style '' escaping.
   */
  private def maskStringLiterals(
      sql: String, literals: ArrayBuffer[String]): String = {
    val sb = new StringBuilder
    var i = 0
    while (i < sql.length) {
      if (sql.charAt(i) == '\'') {
        val start = i
        i += 1
        var closed = false
        while (i < sql.length && !closed) {
          if (sql.charAt(i) == '\'') {
            i += 1
            if (i < sql.length && sql.charAt(i) == '\'') {
              // SQL escape: '' means literal single quote, continue
              i += 1
            } else {
              closed = true
            }
          } else {
            i += 1
          }
        }
        val lit = sql.substring(start, i)
        sb.append(s"__COPLITERAL_${literals.length}__")
        literals += lit
      } else {
        sb.append(sql.charAt(i))
        i += 1
      }
    }
    sb.toString()
  }

  private def restoreLiterals(
      sql: String, literals: ArrayBuffer[String]): String = {
    var result = sql
    for (idx <- literals.indices) {
      result = result.replace(s"__COPLITERAL_${idx}__", literals(idx))
    }
    result
  }

  /**
   * Rewrite `expr OP expr` to `func(expr, expr)` in SQL text (with
   * string literals already masked).
   *
   * Left operand: identifier (`\w+`), dotted identifier (`a.b`),
   *   or backtick-quoted (`` `name` ``).
   * Right operand: identifier, number, placeholder (masked literal),
   *   or backtick-quoted name.
   */
  private def rewriteInfixToFunction(
      sql: String, op: String, func: String): String = {
    val quotedOp = Pattern.quote(op.toUpperCase(Locale.ROOT))
    // Build pattern for case-insensitive operator match
    // Left: identifier (with optional dots), backtick-quoted
    // Right: identifier, number, backtick-quoted, or literal placeholder
    val pattern = Pattern.compile(
      """(\w+(?:\.\w+)*|`[^`]+`)""" +
      """\s+""" +
      s"(?i)$quotedOp" +
      """\s+""" +
      """(\w+(?:\.\w+)*|`[^`]+`|[0-9]+(?:\.[0-9]+)?|__COPLITERAL_\d+__)"""
    )
    val matcher = pattern.matcher(sql)
    val sb = new StringBuffer()
    while (matcher.find()) {
      matcher.appendReplacement(sb,
        java.util.regex.Matcher.quoteReplacement(
          s"$func(${matcher.group(1)}, ${matcher.group(2)})"))
    }
    matcher.appendTail(sb)
    sb.toString
  }
}
