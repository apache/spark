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
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.catalyst.util.ExpressionToSqlConverter

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
 * @see [[SubstituteParamsParser]] for the underlying parameter substitution logic
 * @since 4.0.0
 */
class ParameterHandler {

  // Thread-local storage for position mapper to ensure thread safety
  private val positionMapperStorage = new ThreadLocal[Option[PositionMapper]]() {
    override def initialValue(): Option[PositionMapper] = None
  }

  /**
   * Get the current position mapper for translating error positions.
   * This mapper can translate positions from substituted text back to original text.
   *
   * @return The current position mapper, if available
   */
  def getPositionMapper: Option[PositionMapper] = positionMapperStorage.get()

  /**
   * Helper method to perform parameter substitution and store position mapper.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param rule The substitution rule to use
   * @param namedParams Optional named parameters map
   * @param positionalParams Optional positional parameters list
   * @return The SQL text with parameters substituted
   */
  private def performSubstitution(
      sqlText: String,
      rule: SubstitutionRule,
      namedParams: Map[String, String] = Map.empty,
      positionalParams: List[String] = List.empty): String = {

    // Quick pre-check: if there are no parameter markers in the text, skip parsing entirely
    if (!sqlText.contains("?") && !sqlText.contains(":")) {
      positionMapperStorage.set(Some(PositionMapper.identity(sqlText)))
      return sqlText
    }

    val substitutor = new SubstituteParamsParser()
    val (substituted, _, positionMapper) = substitutor.substitute(sqlText, rule,
      namedParams = namedParams, positionalParams = positionalParams)
    positionMapperStorage.set(Some(positionMapper))
    substituted
  }

  /**
   * Translate error context from substituted text positions to original text positions.
   *
   * @param context The error context with positions in substituted text
   * @return The error context with positions mapped to original text
   */
  def translateErrorContext(context: SQLQueryContext): SQLQueryContext = {
    positionMapperStorage.get() match {
      case Some(mapper) =>
        val translatedStartIndex = context.originStartIndex.map(mapper.mapToOriginal)
        val translatedStopIndex = context.originStopIndex.map(mapper.mapToOriginal)

        context.copy(
          originStartIndex = translatedStartIndex,
          originStopIndex = translatedStopIndex,
          sqlText = Some(mapper.originalText) // Use original text instead of substituted
        )
      case None =>
        context // No position mapper available, return as-is
    }
  }

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

    context match {
      case NamedParameterContext(params) =>
        performSubstitution(sqlText, rule, namedParams = params.map { case (name, expr) =>
          (name, ExpressionToSqlConverter.convert(expr))
        })

      case PositionalParameterContext(params) =>
        performSubstitution(sqlText, rule,
          positionalParams = params.map(ExpressionToSqlConverter.convert).toList)
    }
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

    if (paramMap.isEmpty) return sqlText

    val paramValues = paramMap.map { case (name, expr) =>
      (name, ExpressionToSqlConverter.convert(expr))
    }
    performSubstitution(sqlText, rule, namedParams = paramValues)
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

    if (paramList.isEmpty) return sqlText

    val paramValues = paramList.map(ExpressionToSqlConverter.convert).toList
    performSubstitution(sqlText, rule, positionalParams = paramValues)
  }

  /**
   * Determine the appropriate substitution rule based on SQL content.
   *
   * @param sqlText The SQL text to analyze
   * @return The appropriate substitution rule
   */
  def determineSubstitutionRule(sqlText: String): SubstitutionRule = {
    val trimmed = sqlText.trim
    if (trimmed.length >= 5 && trimmed.regionMatches(true, 0, "BEGIN", 0, 5)) {
      SubstitutionRule.CompoundOrSingleStatement
    } else {
      SubstitutionRule.Statement
    }
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
