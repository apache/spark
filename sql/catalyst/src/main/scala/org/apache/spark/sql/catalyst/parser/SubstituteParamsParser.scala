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

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream, ParserRuleContext}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Parameter expectation types.
 */
object ParameterExpectation extends Enumeration {
  val Named, Positional, Unknown = Value
}

/**
 * A parameter substitution parser that replaces parameter markers in SQL text with their values.
 * This parser finds parameter markers and substitutes them with provided values to produce
 * a modified SQL string ready for execution.
 */
class SubstituteParamsParser extends Logging {

  /**
   * Substitute parameter markers in SQL text with provided values.
   * Always uses compoundOrSingleStatement parsing which can handle all SQL constructs.
   *
   * @param sqlText          The original SQL text containing parameter markers
   * @param namedParams      Map of named parameter values (paramName -> value)
   * @param positionalParams List of positional parameter values in order
   * @return A tuple of (modified SQL string with parameters substituted,
   *                   number of consumed positional parameters)
   */
  def substitute(
      sqlText: String,
      namedParams: Map[String, String] = Map.empty,
      positionalParams: List[String] = List.empty,
      expectationType: ParameterExpectation.Value = ParameterExpectation.Unknown,
      allParametersAreNamed: Boolean = true,
      originalArgs: Seq[Any] = Seq.empty,
      originalParamNames: Seq[String] = Seq.empty): (String, Int, PositionMapper) = {

    // Quick pre-check: if there are no parameter markers in the text, skip parsing entirely
    if (!sqlText.contains("?") && !sqlText.contains(":")) {
      return (sqlText, 0, PositionMapper.identity(sqlText))
    }

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)

    // Use shared parser configuration to ensure consistency with main parser.
    AbstractParser.configureParser(parser, sqlText, tokenStream, SQLConf.get)

    // Use shared two-stage parsing strategy for consistent error handling.
    def parseParameterLocations(): (ParserRuleContext, ParameterLocationInfo) = {
      val ctx = AbstractParser.executeWithTwoStageStrategy(parser, tokenStream,
        _.compoundOrSingleStatement())
      (ctx, new SubstituteParmsAstBuilder().extractParameterLocations(ctx))
    }
    val (ctx, parameterLocations) = parseParameterLocations() match {
      // A rule that accepts any tokens, such as `SET .*?`, parses a statement such as
      // `SET VAR v = :p?`, which is not valid without optional references. Only the parser that
      // builds the plan can tell, so parse a statement with a `?` that is not a parameter marker
      // again with optional references.
      case (_, locations)
          if AbstractParser.questions(tokenStream).length >
            locations.positionalParameterLocations.length &&
            AbstractParser.restartWithOptionalReferences(parser, tokenStream) =>
        parseParameterLocations()
      case parsed =>
        parsed
    }
    // In a statement with optional references, a `?` after an identifier or a non-reserved keyword
    // is an optional reference where the syntax allows a reference. The remaining markers must
    // not take the values of such a `?` silently, so a value without a marker is an error.
    val numPositionalMarkers = parameterLocations.positionalParameterLocations.length
    if (parameterLocations.hasOptionalReferences &&
        positionalParams.length > numPositionalMarkers) {
      throw QueryCompilationErrors.unusedSqlParameterValuesError(
        numPositionalMarkers, positionalParams.length)
    }
    // Optional named parameter markers are substituted before the AstBuilder sees them.
    if (parameterLocations.namedParameterLocations.values.exists(_.exists(_.optional))) {
      OptionalReferenceValidation.validateNamedParameterMarkers(ctx, sqlText)
    }

    // Simple mixed parameter detection based on expectation type
    val hasNamed = parameterLocations.namedParameterLocations.nonEmpty
    val hasPositional = parameterLocations.positionalParameterLocations.nonEmpty

    // Check for mixed parameters based on expectation
    if (expectationType == ParameterExpectation.Positional && hasNamed) {
      throw QueryCompilationErrors.invalidQueryMixedQueryParameters()
    }
    if (expectationType == ParameterExpectation.Named && hasPositional) {
      throw QueryCompilationErrors.invalidQueryMixedQueryParameters()
    }
    if (expectationType == ParameterExpectation.Unknown && hasNamed && hasPositional) {
      throw QueryCompilationErrors.invalidQueryMixedQueryParameters()
    }

    // If SQL uses named parameters but not all USING params are named, validate
    if (hasNamed && !hasPositional && !allParametersAreNamed && originalArgs.nonEmpty) {
      val unnamedExprs = originalParamNames.zip(originalArgs).collect {
        case ("", arg) => Literal(arg) // Empty strings are unnamed.
      }.toList
      if (unnamedExprs.nonEmpty) {
        throw QueryCompilationErrors.invalidQueryAllParametersMustBeNamed(unnamedExprs)
      }
    }

    // Substitute parameters in the original text.
    val (substitutedSql, appliedSubstitutions) = substituteAtLocations(sqlText, parameterLocations,
      namedParams, positionalParams)
    val consumedPositionalParams = parameterLocations.positionalParameterLocations.length

    // Create position mapper for error context translation.
    val positionMapper = PositionMapper(sqlText, substitutedSql, appliedSubstitutions)

    (substitutedSql, consumedPositionalParams, positionMapper)
  }

  /**
   * Apply substitutions to the original SQL text at specified locations.
   * Returns both the substituted text and the list of substitutions applied.
   */
  private def substituteAtLocations(
      sqlText: String,
      locations: ParameterLocationInfo,
      namedParams: Map[String, String],
      positionalParams: List[String]): (String, List[Substitution]) = {

    val substitutions = scala.collection.mutable.ListBuffer[Substitution]()

    // Handle named parameters
    locations.namedParameterLocations.foreach { case (name, locationList) =>
      namedParams.get(name) match {
        case Some(value) =>
          // Substitute all occurrences of this parameter
          locationList.foreach { location =>
            substitutions += Substitution(location, namedReplacement(sqlText, location, value))
          }
        case None if locationList.forall(_.optional) =>
          // An optional named parameter marker (`:name?`) without a value is NULL.
          locationList.foreach { location =>
            substitutions += Substitution(location, namedReplacement(sqlText, location, "NULL"))
          }
        case None =>
          // Use the first location that is not optional for error reporting
          val location = locationList.find(!_.optional).getOrElse(locationList.head)
          val queryContext = SQLQueryContext(
            line = None,
            startPosition = None,
            originStartIndex = Some(location.start),
            originStopIndex = Some(location.end - 1), // end is exclusive, so subtract 1
            sqlText = Some(sqlText),
            originObjectType = None,
            originObjectName = None)
          throw new AnalysisException(
            errorClass = "UNBOUND_SQL_PARAMETER",
            messageParameters = Map("name" -> name),
            context = Array(queryContext),
            cause = None)
      }
    }

    // Handle positional parameters
    if (locations.positionalParameterLocations.length > positionalParams.length) {
      // Find the first unbound positional parameter
      val unboundIndex = positionalParams.length
      val unboundLocation = locations.positionalParameterLocations(unboundIndex)
      val queryContext = SQLQueryContext(
        line = None,
        startPosition = None,
        originStartIndex = Some(unboundLocation.start),
        originStopIndex = Some(unboundLocation.end - 1),
        sqlText = Some(sqlText),
        originObjectType = None,
        originObjectName = None)
      throw new AnalysisException(
        errorClass = "UNBOUND_SQL_PARAMETER",
        messageParameters = Map("name" -> s"_${unboundLocation.start}"),
        context = Array(queryContext),
        cause = None)
    }

    locations.positionalParameterLocations.zip(positionalParams).foreach {
      case (location, value) =>
        substitutions += Substitution(location, value)
    }

    val substitutedText = applySubstitutions(sqlText, substitutions.toList)
    (substitutedText, substitutions.toList)
  }

  /**
   * Returns the text that replaces the named parameter marker at `location` with `value`. Unlike
   * a parameter name, the `?` of an optional marker can be directly followed by a keyword or an
   * identifier, as in `:x?AND`, which must not become a part of the value.
   */
  private def namedReplacement(
      sqlText: String,
      location: ParameterLocation,
      value: String): String = {
    val isFollowedByWord = location.optional && {
      val nextIndex = sqlText.offsetByCodePoints(0, location.end)
      nextIndex < sqlText.length && {
        val nextCodePoint = sqlText.codePointAt(nextIndex)
        Character.isLetterOrDigit(nextCodePoint) || nextCodePoint == '_'
      }
    }
    if (isFollowedByWord) s"$value " else value
  }

  /**
   * Apply a list of substitutions to the SQL text.
   * Inserts a space separator when a parameter is immediately preceded by a quote
   * to avoid back-to-back quotes after substitution.
   *
   * ANTLR's CodePointCharStream reports token positions in Unicode code points, but
   * Java/Scala String indices are in UTF-16 code units. Supplementary characters
   * (e.g. emojis) occupy 1 code point but 2 code units, so we must convert.
   */
  private def applySubstitutions(sqlText: String, substitutions: List[Substitution]): String = {
    val sortedSubstitutions = substitutions.sortBy(-_.start)

    var result = sqlText
    sortedSubstitutions.foreach { substitution =>
      val startCU = result.offsetByCodePoints(0, substitution.start)
      val endCU = result.offsetByCodePoints(0, substitution.end)
      val prefix = result.substring(0, startCU)
      val replacement = substitution.replacement
      val suffix = result.substring(endCU)

      val needsSpace = startCU > 0 &&
        (result.charAt(startCU - 1) == '\'' || result.charAt(startCU - 1) == '"') &&
        replacement.nonEmpty && !replacement.charAt(0).isWhitespace

      val space = if (needsSpace) " " else ""
      result = s"$prefix$space$replacement$suffix"
    }
    result
  }
}

object SubstituteParamsParser {
  /** Singleton instance for convenience */
  private val instance = new SubstituteParamsParser()

  def substitute(
      sqlText: String,
      namedParams: Map[String, String] = Map.empty,
      positionalParams: List[String] = List.empty): (String, Int, PositionMapper) =
    instance.substitute(sqlText, namedParams, positionalParams)
}
