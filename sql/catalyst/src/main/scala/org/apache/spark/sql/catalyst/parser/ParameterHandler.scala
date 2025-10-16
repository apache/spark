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
   * Performs parameter substitution and returns both the substituted SQL and the position mapper.
   * The position mapper enables accurate error reporting by mapping positions in the
   * substituted SQL back to the original SQL text.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param namedParams Optional named parameters map
   * @param positionalParams Optional positional parameters list
   * @return A tuple of (substituted SQL, position mapper)
   */
  private def performSubstitution(
      sqlText: String,
      namedParams: Map[String, String] = Map.empty,
      positionalParams: List[String] = List.empty): (String, PositionMapper) = {

    // Optimize: Skip parsing if no parameter markers are present in the SQL text.
    if (!parameterMarkerPattern.matcher(sqlText).find()) {
      return (sqlText, PositionMapper.identity(sqlText))
    }

    val substitutor = new SubstituteParamsParser()
    val (substituted, _, positionMapper) = substitutor.substitute(sqlText,
      namedParams = namedParams, positionalParams = positionalParams)

    (substituted, positionMapper)
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
   * All expressions must be resolved Literals at this point.
   *
   * @param expr The expression to convert (must be a Literal)
   * @return SQL string representation
   */
  private def convertToSql(expr: Expression): String = expr match {
    case lit: Literal => lit.sql
    case other =>
      throw new IllegalArgumentException(
        s"ParameterHandler only accepts resolved Literal expressions. " +
        s"Received: ${other.getClass.getSimpleName}. " +
        s"All parameters must be resolved using SparkSession.resolveAndValidateParameters " +
        s"before being passed to the pre-parser.")
  }

  /**
   * Substitutes parameters in SQL text based on the parameter context.
   * Returns both the substituted SQL and the position mapper for error reporting.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param context The parameter context (named or positional)
   * @return A tuple of (substituted SQL, position mapper)
   * @throws IllegalArgumentException if context is null
   */
  def substituteParameters(
      sqlText: String,
      context: ParameterContext): (String, PositionMapper) = {
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
   * @return A tuple of (substituted SQL, optional position mapper)
   */
  private def handleHybridParameters(
      sqlText: String,
      args: Seq[Any],
      paramNames: Seq[String]): (String, PositionMapper) = {

    // Optimize: Skip parsing if no parameter markers are present in the SQL text.
    if (!parameterMarkerPattern.matcher(sqlText).find()) {
      return (sqlText, PositionMapper.identity(sqlText))
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

    (substitutedSql, positionMapper)
  }

  /**
   * Substitutes parameters using named parameter map.
   * This is a convenience method that wraps the main substituteParameters method.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param paramMap Map of parameter names to expressions
   * @return A tuple of (substituted SQL, position mapper)
   */
  def substituteNamedParameters(
      sqlText: String,
      paramMap: Map[String, Expression]): (String, PositionMapper) = {
    if (paramMap.isEmpty) return (sqlText, PositionMapper.identity(sqlText))

    val namedParams = paramMap.map { case (name, expr) => (name, convertToSql(expr)) }
    val substitutor = new SubstituteParamsParser()
    val (substitutedSql, _, positionMapper) = substitutor.substitute(
      sqlText, namedParams, List.empty, ParameterExpectation.Named)

    (substitutedSql, positionMapper)
  }

  /**
   * Substitutes parameters using positional parameter list.
   * This is a convenience method that wraps the main substituteParameters method.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param paramList Sequence of parameter expressions
   * @return A tuple of (substituted SQL, position mapper)
   */
  def substitutePositionalParameters(
      sqlText: String,
      paramList: Seq[Expression]): (String, PositionMapper) = {
    if (paramList.isEmpty) return (sqlText, PositionMapper.identity(sqlText))

    val positionalParams = paramList.map(convertToSql).toList
    val substitutor = new SubstituteParamsParser()
    val (substitutedSql, _, positionMapper) = substitutor.substitute(
      sqlText, Map.empty, positionalParams, ParameterExpectation.Positional)

    (substitutedSql, positionMapper)
  }
}
