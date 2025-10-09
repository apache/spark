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

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.ParameterSubstitutionInfo
import org.apache.spark.sql.catalyst.util.LiteralToSqlConverter
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Handler for parameter substitution across different Spark SQL contexts.
 *
 * This class consolidates the common parameter handling logic used by SparkSqlParser,
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
 * val handler = new ParameterHandler()
 * val context = NamedParameterContext(Map("param1" -> Literal(42)))
 * val result = handler.substituteParameters("SELECT :param1", context)
 * // result: "SELECT 42"
 * }}}
 *
 * @example Optional context:
 * {{{
 * val handler = new ParameterHandler()
 * val context = Some(NamedParameterContext(Map("param1" -> Literal(42))))
 * val result = handler.substituteParametersIfNeeded("SELECT :param1", context)
 * // result: "SELECT 42"
 * }}}
 *
 * @see [[SubstituteParamsParser]] for the underlying parameter substitution logic
 */
class ParameterHandler {

  // Compiled regex pattern for efficient parameter marker detection.
  private val parameterMarkerPattern = java.util.regex.Pattern.compile("[?:]")


  /**
   * Helper method to perform parameter substitution and store position mapper.
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

    // Quick pre-check: if there are no parameter markers in the text, skip parsing entirely.
    if (!parameterMarkerPattern.matcher(sqlText).find()) {
      val identityMapper = PositionMapper.identity(sqlText)
      setupSubstitutionContext(sqlText, sqlText, identityMapper, isIdentity = true)
      return sqlText
    }

    val substitutor = new SubstituteParamsParser()
    val (substituted, _, positionMapper) = substitutor.substitute(sqlText,
      namedParams = namedParams, positionalParams = positionalParams)

    setupSubstitutionContext(sqlText, substituted, positionMapper, isIdentity = false)
    substituted
  }

  /**
   * Set up parameter substitution context in CurrentOrigin for error reporting.
   * This creates a ParameterSubstitutionInfo object with position mapping data.
   *
   * @param originalSql The original SQL text before substitution
   * @param substitutedSql The SQL text after substitution
   * @param positionMapper The mapper for translating positions
   * @param isIdentity Whether this is an identity mapping (no substitution occurred)
   */
  private[sql] def setupSubstitutionContext(
      originalSql: String,
      substitutedSql: String,
      positionMapper: PositionMapper,
      isIdentity: Boolean): Unit = {

    // Create parameter substitution info with the position mapper.
    val substitutionInfo = ParameterSubstitutionInfo(
      originalSql = originalSql,
      isIdentity = isIdentity,
      positionMapper = if (isIdentity) None else Some(positionMapper)
    )

    // Store the info in CurrentOrigin for position mapping during error reporting.
    val currentOrigin = org.apache.spark.sql.catalyst.trees.CurrentOrigin.get
    val updatedOrigin = currentOrigin.copy(parameterSubstitutionInfo = Some(substitutionInfo))
    org.apache.spark.sql.catalyst.trees.CurrentOrigin.set(updatedOrigin)
  }

  /**
   * Convert a value to an Expression, handling different input types safely.
   * Uses pattern matching for type safety instead of unsafe casting.
   *
   * @param value The value to convert (can be Literal, Expression, or raw value)
   * @return An Expression representing the value
   */
  private def convertToExpression(value: Any): Expression = value match {
    case literal: Literal =>
      // Already a Literal from ResolveExecuteImmediate - use it directly.
      literal
    case expr: Expression =>
      // Expression from Column object - use it directly.
      expr
    case null =>
      // Handle null values explicitly.
      Literal(null)
    case other =>
      // Raw value - create new Literal with proper error handling.
      try {
        Literal(other)
      } catch {
        case _: RuntimeException =>
          // Fallback for unsupported types.
          throw new IllegalArgumentException(
            s"Cannot convert value of type ${other.getClass.getSimpleName} to Expression: $other")
      }
  }

  /**
   * Convert an Expression to SQL string representation.
   *
   * @param expr The expression to convert
   * @return SQL string representation
   */
  private def convertToSql(expr: Expression): String = {
    LiteralToSqlConverter.convert(expr)
  }

  /**
   * Substitute parameters in SQL text based on the parameter context.
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
   * Handle hybrid parameter context which can contain both named and positional parameters.
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
    // Detect which parameter types are actually used in the query.
    val (hasPositional, hasNamed) = detectParameters(sqlText)

    // Validate that the query doesn't mix parameter types.
    if (hasPositional && hasNamed) {
      throw QueryCompilationErrors.invalidQueryMixedQueryParameters()
    }

    // Validate ALL_PARAMETERS_MUST_BE_NAMED: if query uses named parameters,
    // all USING expressions must have names.
    if (hasNamed && !hasPositional) {
      validateAllParametersNamed(args, paramNames)
    }

    if (hasPositional && !hasNamed) {
      // Query uses only positional parameters.
      val positionalParams = args.map(convertToExpression)
      performSubstitution(sqlText,
        positionalParams = positionalParams.map(convertToSql).toList)
    } else if (hasNamed && !hasPositional) {
      // Query uses only named parameters.
      val namedParams = paramNames.zip(args).collect {
        case (name, value) if name.nonEmpty =>
          name -> convertToExpression(value)
      }.toMap
      performSubstitution(sqlText,
        namedParams = namedParams.map { case (name, expr) =>
          (name, convertToSql(expr))
        })
    } else {
      // No parameters in query - return as-is.
      performSubstitution(sqlText)
    }
  }

  /**
   * Validate that all parameters have names when named parameters are used.
   *
   * @param args The parameter values
   * @param paramNames The parameter names
   * @throws QueryCompilationErrors.invalidQueryAllParametersMustBeNamed if unnamed parameters found
   */
  private def validateAllParametersNamed(args: Seq[Any], paramNames: Seq[String]): Unit = {
    val unnamedExprs = paramNames.zip(args).collect {
      case ("", arg) => convertToExpression(arg) // Empty strings are unnamed.
    }.toList
    if (unnamedExprs.nonEmpty) {
      throw QueryCompilationErrors.invalidQueryAllParametersMustBeNamed(unnamedExprs)
    }
  }

  /**
   * Substitute parameters using named parameter map.
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
    substituteParameters(sqlText, NamedParameterContext(paramMap))
  }

  /**
   * Substitute parameters using positional parameter list.
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
    substituteParameters(sqlText, PositionalParameterContext(paramList))
  }



  /**
   * Check if SQL text contains parameter markers.
   * Always uses compoundOrSingleStatement parsing which can handle all SQL constructs.
   *
   * @param sqlText The SQL text to check
   * @return Tuple of (hasPositional, hasNamed) parameters
   */
  def detectParameters(
      sqlText: String): (Boolean, Boolean) = {

    // Quick pre-check: if there are no parameter markers in the text, skip parsing entirely.
    if (!parameterMarkerPattern.matcher(sqlText).find()) {
      return (false, false)
    }

    val substitutor = new SubstituteParamsParser()
    Try(substitutor.detectParameters(sqlText)) match {
      case Success(result) => result
      case Failure(_: ParseException) => (false, false)
      case Failure(ex) =>
        // Re-throw unexpected exceptions for better debugging.
        throw new RuntimeException(
          s"Unexpected error during parameter detection: ${ex.getMessage}", ex)
    }
  }

  /**
   * Validate that parameter types are consistent (not mixed).
   *
   * @param sqlText The SQL text to validate
   * @throws QueryCompilationErrors.invalidQueryMixedQueryParameters if mixed parameters found
   */
  def validateParameterConsistency(
      sqlText: String): Unit = {

    val (hasPositional, hasNamed) = detectParameters(sqlText)
    if (hasPositional && hasNamed) {
      throw QueryCompilationErrors.invalidQueryMixedQueryParameters()
    }
  }
}
