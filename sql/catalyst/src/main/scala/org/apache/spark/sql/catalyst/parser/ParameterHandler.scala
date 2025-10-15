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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.parser.SubstituteParamsParser
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.LiteralToSqlConverter

/**
 * Handler for parameter substitution across different Spark SQL contexts.
 *
 * This object consolidates the common parameter handling logic used by SparkSqlParser,
 * SparkConnectPlanner, and ExecuteImmediate. It provides a single, consistent API
 * for all parameter substitution operations in Spark SQL.
 *
 * Key features:
 * - Automatic parameter type detection (named vs positional)
 * - Uses CompoundOrSingleStatement parsing for all SQL constructs
 * - Consistent error handling and validation
 * - Support for complex data types (arrays, maps, nested structures)
 * - Thread-safe operations with position-aware error context
 *
 * The handler integrates with the parser through callback mechanisms stored in
 * CurrentOrigin to ensure error positions are correctly mapped back to the original SQL text.
 *
 * @example Basic usage:
 * {{{
 * val context = NamedParameterContext(Map("param1" -> Literal(42)))
 * val result = ParameterHandler.substituteParameters("SELECT :param1", context)
 * // result: "SELECT 42"
 * }}}
 *
 * @see [[SubstituteParamsParser]] for the underlying parameter substitution logic
 */
object ParameterHandler {

  // Compiled regex pattern for efficient parameter marker detection.
  private val parameterMarkerPattern = java.util.regex.Pattern.compile("[?:]")

  /**
   * Performs parameter substitution and stores the position mapper in CurrentOrigin.
   * The position mapper enables accurate error reporting by mapping positions in the
   * substituted SQL back to the original SQL text.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param namedParams Optional named parameters map
   * @param positionalParams Optional positional parameters list
   * @return The SQL text with parameters substituted
   */
  private def performSubstitution(
      sqlText: String,
      namedParams: Map[String, String] = Map.empty,
      positionalParams: List[String] = List.empty): String = {

    // Optimize: Skip parsing if no parameter markers are present in the SQL text.
    if (!parameterMarkerPattern.matcher(sqlText).find()) {
      return sqlText
    }

    val substitutor = new SubstituteParamsParser()
    val (substituted, _, positionMapper) = substitutor.substitute(sqlText,
      namedParams = namedParams, positionalParams = positionalParams)

    // Store the position mapper to enable accurate error position reporting in the original SQL.
    val currentOrigin = CurrentOrigin.get
    CurrentOrigin.set(currentOrigin.copy(positionMapper = Some(positionMapper)))

    substituted
  }

  /**
   * Converts a value to an Expression, handling different input types safely.
   * Uses pattern matching for type safety.
   *
   * @param value The value to convert (can be Literal, Expression, or raw value)
   * @return An Expression representing the value
   */
  private def convertToExpression(value: Any): Expression = value match {
    case literal: Literal =>
      literal
    case expr: Expression =>
      expr
    case null =>
      Literal(null)
    case other =>
      try {
        Literal(other)
      } catch {
        case _: RuntimeException =>
          throw new IllegalArgumentException(
            s"Cannot convert value of type ${other.getClass.getSimpleName} to Expression: $other")
      }
  }

  /**
   * Converts an Expression to its SQL string representation.
   *
   * @param expr The expression to convert
   * @return SQL string representation
   */
  private def convertToSql(expr: Expression): String = {
    LiteralToSqlConverter.convert(expr)
  }

  /**
   * Substitutes parameters in SQL text based on the parameter context.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param context The parameter context (named or positional)
   * @return The SQL text with parameters substituted
   * @throws IllegalArgumentException if context is null
   */
  def substituteParameters(
      sqlText: String,
      context: ParameterContext): String = {
    require(context != null, "Parameter context cannot be null")

    context match {
      case NamedParameterContext(params) =>
        performSubstitution(sqlText, namedParams = params.map { case (name, expr) =>
          (name, convertToSql(expr))
        })

      case PositionalParameterContext(params) =>
        performSubstitution(sqlText,
          positionalParams = params.map(expr =>
            convertToSql(expr)).toList)

      case HybridParameterContext(args, paramNames) =>
        handleHybridParameters(sqlText, args.toSeq, paramNames.toSeq)
    }
  }

  /**
   * Handles hybrid parameter context which can contain both named and positional parameters.
   * Validates parameter consistency and routes to appropriate substitution method.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param args The parameter values
   * @param paramNames The parameter names (empty string for positional parameters)
   * @return The SQL text with parameters substituted
   */
  private def handleHybridParameters(
      sqlText: String,
      args: Seq[Any],
      paramNames: Seq[String]): String = {

    // Optimize: Skip parsing if no parameter markers are present in the SQL text.
    if (!parameterMarkerPattern.matcher(sqlText).find()) {
      return sqlText
    }

    // Prepare parameters for both types.
    val positionalParams = args.map(convertToExpression).map(convertToSql).toList
    val namedParams = paramNames.zip(args).collect {
      case (name, value) if name.nonEmpty =>
        name -> convertToSql(convertToExpression(value))
    }.toMap

    // Check if all parameters are named (no positional parameters).
    val hasOnlyNamedParams = !paramNames.exists(_.isEmpty)

    // Perform substitution; the substitutor validates parameter consistency internally.
    val substitutor = new SubstituteParamsParser()
    val (substitutedSql, _, positionMapper) = substitutor.substitute(
      sqlText, namedParams, positionalParams, ParameterExpectation.Unknown,
      hasOnlyNamedParams, args, paramNames)

    // Store the position mapper to enable accurate error position reporting in the original SQL.
    val currentOrigin = CurrentOrigin.get
    CurrentOrigin.set(currentOrigin.copy(positionMapper = Some(positionMapper)))

    substitutedSql
  }

  /**
   * Substitutes parameters using named parameter map.
   * This is a convenience method that wraps the main substituteParameters method.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param paramMap Map of parameter names to expressions
   * @return The SQL text with parameters substituted
   */
  def substituteNamedParameters(
      sqlText: String,
      paramMap: Map[String, Expression]): String = {
    if (paramMap.isEmpty) return sqlText

    val namedParams = paramMap.map { case (name, expr) => (name, convertToSql(expr)) }
    val substitutor = new SubstituteParamsParser()
    val (substitutedSql, _, positionMapper) = substitutor.substitute(
      sqlText, namedParams, List.empty, ParameterExpectation.Named)

    // Store the position mapper to enable accurate error position reporting in the original SQL.
    val currentOrigin = CurrentOrigin.get
    CurrentOrigin.set(currentOrigin.copy(positionMapper = Some(positionMapper)))

    substitutedSql
  }

  /**
   * Substitutes parameters using positional parameter list.
   * This is a convenience method that wraps the main substituteParameters method.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param paramList Sequence of parameter expressions
   * @return The SQL text with parameters substituted
   */
  def substitutePositionalParameters(
      sqlText: String,
      paramList: Seq[Expression]): String = {
    if (paramList.isEmpty) return sqlText

    val positionalParams = paramList.map(convertToSql).toList
    val substitutor = new SubstituteParamsParser()
    val (substitutedSql, _, positionMapper) = substitutor.substitute(
      sqlText, Map.empty, positionalParams, ParameterExpectation.Positional)

    // Store the position mapper to enable accurate error position reporting in the original SQL.
    val currentOrigin = CurrentOrigin.get
    CurrentOrigin.set(currentOrigin.copy(positionMapper = Some(positionMapper)))

    substitutedSql
  }
}
