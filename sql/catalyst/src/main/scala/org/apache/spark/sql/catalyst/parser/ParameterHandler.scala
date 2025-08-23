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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Handler for parameter substitution across different Spark SQL contexts.
 *
 * This class consolidates the common parameter handling logic used by SparkSqlParser,
 * SparkConnectPlanner, and ExecuteImmediate. It provides a single, consistent API
 * for all parameter substitution operations in Spark SQL.
 *
 * Key features:
 * - Automatic parameter type detection (named vs positional)
 * - Automatic substitution rule selection (Statement vs CompoundOrSingleStatement)
 * - Consistent error handling and validation
 * - Support for complex data types (arrays, maps, nested structures)
 * - Thread-safe operations
 *
 * The handler uses the Strategy pattern internally to delegate to appropriate
 * parameter substitution strategies based on the parameter types detected in the SQL.
 *
  * @example Basic usage:
 * {{{
 * val handler = new ParameterHandler()
 * val context = NamedParameterContext(Map("param1" -> Literal(42)))
 * val result = handler.substituteParameters("SELECT :param1", context)
 * // result: "SELECT 42"
 * }}}
 *
 * @example Automatic rule detection:
 * {{{
 * val handler = new ParameterHandler()
 * val context = NamedParameterContext(Map("param1" -> Literal(42)))
 *
 * // Regular statement
 * val result1 = handler.substituteParametersWithAutoRule("SELECT :param1", context)
 *
 * // SQL scripting block - automatically uses CompoundOrSingleStatement rule
 * val result2 = handler.substituteParametersWithAutoRule("BEGIN SELECT :param1; END", context)
 * }}}
 *
 * @see [[ParameterSubstitutionStrategy]] for the underlying strategy pattern
 * @see [[SubstituteParamsParser]] for the low-level parameter substitution
 * @since 4.0.0
 */
class ParameterHandler {

  /**
   * Substitute parameters in SQL text based on the parameter context.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param context The parameter context (named or positional)
   * @param rule The substitution rule to use (default: Statement)
   * @return The SQL text with parameters substituted
   */
  def substituteParameters(
      sqlText: String,
      context: ParameterContext,
      rule: SubstitutionRule = SubstitutionRule.Statement): String = {

    val strategy = ParameterSubstitutionStrategy.fromContext(context)
    val substitutor = new SubstituteParamsParser()

    strategy.substitute(sqlText, rule, substitutor)
  }

  /**
   * Substitute parameters in SQL text with optional context.
   * If no context is provided, returns the original SQL text.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param contextOpt Optional parameter context
   * @param rule The substitution rule to use (default: Statement)
   * @return The SQL text with parameters substituted
   */
  def substituteParametersIfNeeded(
      sqlText: String,
      contextOpt: Option[ParameterContext],
      rule: SubstitutionRule = SubstitutionRule.Statement): String = {

    contextOpt match {
      case Some(context) => substituteParameters(sqlText, context, rule)
      case None => sqlText
    }
  }

  /**
   * Substitute parameters using named parameter map.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param paramMap Map of parameter names to expressions
   * @param rule The substitution rule to use (default: Statement)
   * @return The SQL text with parameters substituted
   */
  def substituteNamedParameters(
      sqlText: String,
      paramMap: Map[String, Expression],
      rule: SubstitutionRule = SubstitutionRule.Statement): String = {

    val strategy = ParameterSubstitutionStrategy.named(paramMap)
    val substitutor = new SubstituteParamsParser()

    strategy.substitute(sqlText, rule, substitutor)
  }

  /**
   * Substitute parameters using positional parameter list.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param paramList Sequence of parameter expressions
   * @param rule The substitution rule to use (default: Statement)
   * @return The SQL text with parameters substituted
   */
  def substitutePositionalParameters(
      sqlText: String,
      paramList: Seq[Expression],
      rule: SubstitutionRule = SubstitutionRule.Statement): String = {

    val strategy = ParameterSubstitutionStrategy.positional(paramList)
    val substitutor = new SubstituteParamsParser()

    strategy.substitute(sqlText, rule, substitutor)
  }

  /**
   * Determine the appropriate substitution rule based on SQL content.
   *
   * @param sqlText The SQL text to analyze
   * @return The appropriate substitution rule
   */
  def determineSubstitutionRule(sqlText: String): SubstitutionRule = {
    // scalastyle:off caselocale
    if (sqlText.trim.toUpperCase.startsWith("BEGIN")) {
      SubstitutionRule.CompoundOrSingleStatement
    } else {
      SubstitutionRule.Statement
    }
    // scalastyle:on caselocale
  }

  /**
   * Substitute parameters with automatic rule detection.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param context The parameter context
   * @return The SQL text with parameters substituted
   */
  def substituteParametersWithAutoRule(
      sqlText: String,
      context: ParameterContext): String = {

    val rule = determineSubstitutionRule(sqlText)
    substituteParameters(sqlText, context, rule)
  }

  /**
   * Check if SQL text contains parameter markers.
   *
   * @param sqlText The SQL text to check
   * @param rule The substitution rule to use (default: Statement)
   * @return Tuple of (hasPositional, hasNamed) parameters
   */
  def detectParameters(
      sqlText: String,
      rule: SubstitutionRule = SubstitutionRule.Statement): (Boolean, Boolean) = {

    // Quick pre-check: if there are no parameter markers in the text, skip parsing entirely
    if (!sqlText.contains("?") && !sqlText.contains(":")) {
      return (false, false)
    }

    val substitutor = new SubstituteParamsParser()
    try {
      substitutor.detectParameters(sqlText, rule)
    } catch {
      case _: ParseException => (false, false)
    }
  }

  /**
   * Validate that parameter types are consistent (not mixed).
   *
   * @param sqlText The SQL text to validate
   * @param rule The substitution rule to use (default: Statement)
   * @throws QueryCompilationErrors.invalidQueryMixedQueryParameters if mixed parameters found
   */
  def validateParameterConsistency(
      sqlText: String,
      rule: SubstitutionRule = SubstitutionRule.Statement): Unit = {

    val (hasPositional, hasNamed) = detectParameters(sqlText, rule)
    if (hasPositional && hasNamed) {
      throw org.apache.spark.sql.errors.QueryCompilationErrors.invalidQueryMixedQueryParameters()
    }
  }
}
