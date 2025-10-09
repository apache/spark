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

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.internal.SQLConf


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
   *         number of consumed positional parameters)
   */
  def substitute(
                  sqlText: String,
                  namedParams: Map[String, String] = Map.empty,
                  positionalParams: List[String] = List.empty): (String, Int, PositionMapper) = {

    // Quick pre-check: if there are no parameter markers in the text, skip parsing entirely
    if (!sqlText.contains("?") && !sqlText.contains(":")) {
      return (sqlText, 0, PositionMapper.identity(sqlText))
    }

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    // Match main parser configuration for consistent error messages
    parser.addParseListener(PostProcessor)
    parser.addParseListener(UnclosedCommentProcessor(sqlText, tokenStream))
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser.legacy_setops_precedence_enabled = SQLConf.get.setOpsPrecedenceEnforced
    parser.legacy_exponent_literal_as_decimal_enabled = SQLConf.get.exponentLiteralAsDecimalEnabled
    parser.SQL_standard_keyword_behavior = SQLConf.get.enforceReservedKeywords
    parser.double_quoted_identifiers = SQLConf.get.doubleQuotedIdentifiers
    parser.parameter_substitution_enabled = !SQLConf.get.legacyParameterSubstitutionConstantsOnly

    val astBuilder = new SubstituteParmsAstBuilder()

    // Use the same two-stage parsing strategy as the main parser for consistent error messages
    val ctx = try {
      // First attempt: SLL mode with bail error strategy
      parser.setErrorHandler(new SparkParserBailErrorStrategy())
      parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
      parser.compoundOrSingleStatement()
    } catch {
      case e: ParseCancellationException =>
        // Second attempt: LL mode with full error strategy
        tokenStream.seek(0) // rewind input stream
        parser.reset()
        parser.setErrorHandler(new SparkParserErrorStrategy())
        parser.getInterpreter.setPredictionMode(PredictionMode.LL)
        parser.compoundOrSingleStatement()
    }
    val parameterLocations = astBuilder.extractParameterLocations(ctx)

    // Substitute parameters in the original text
    val (substitutedSql, appliedSubstitutions) = substituteAtLocations(sqlText, parameterLocations,
      namedParams, positionalParams)
    val consumedPositionalParams = parameterLocations.positionalParameterLocations.length

    // Create position mapper for error context translation
    val positionMapper = PositionMapper(sqlText, substitutedSql, appliedSubstitutions)

    (substitutedSql, consumedPositionalParams, positionMapper)
  }

  /**
   * Detects parameter markers in SQL text without performing substitution.
   * Always uses compoundOrSingleStatement parsing which can handle all SQL constructs.
   *
   * @param sqlText The original SQL text to analyze
   * @return A tuple of (hasPositionalParameters, hasNamedParameters)
   */
  def detectParameters(sqlText: String): (Boolean, Boolean) = {
    // Quick pre-check: if there are no parameter markers in the text, skip parsing entirely
    if (!sqlText.contains("?") && !sqlText.contains(":")) {
      return (false, false)
    }

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    // Match main parser configuration for consistent error messages
    parser.addParseListener(PostProcessor)
    parser.addParseListener(UnclosedCommentProcessor(sqlText, tokenStream))
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser.legacy_setops_precedence_enabled = SQLConf.get.setOpsPrecedenceEnforced
    parser.legacy_exponent_literal_as_decimal_enabled = SQLConf.get.exponentLiteralAsDecimalEnabled
    parser.SQL_standard_keyword_behavior = SQLConf.get.enforceReservedKeywords
    parser.double_quoted_identifiers = SQLConf.get.doubleQuotedIdentifiers
    parser.parameter_substitution_enabled = !SQLConf.get.legacyParameterSubstitutionConstantsOnly

    val astBuilder = new SubstituteParmsAstBuilder()

    // Use the same two-stage parsing strategy as the main parser for consistent error messages
    val ctx = try {
      // First attempt: SLL mode with bail error strategy
      parser.setErrorHandler(new SparkParserBailErrorStrategy())
      parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
      parser.compoundOrSingleStatement()
    } catch {
      case e: ParseCancellationException =>
        // Second attempt: LL mode with full error strategy
        tokenStream.seek(0) // rewind input stream
        parser.reset()
        parser.setErrorHandler(new SparkParserErrorStrategy())
        parser.getInterpreter.setPredictionMode(PredictionMode.LL)
        parser.compoundOrSingleStatement()
    }
    val parameterLocations = astBuilder.extractParameterLocations(ctx)

    val hasPositionalParams = parameterLocations.positionalParameterLocations.nonEmpty
    val hasNamedParams = parameterLocations.namedParameterLocations.nonEmpty

    (hasPositionalParams, hasNamedParams)
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
            substitutions += Substitution(location.start, location.end, value)
          }
        case None =>
          // Use the first location for error reporting
          val location = locationList.head
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
        substitutions += Substitution(location.start, location.end, value)
    }

    val substitutedText = applySubstitutions(sqlText, substitutions.toList)
    (substitutedText, substitutions.toList)
  }

  /**
   * Apply a list of substitutions to the SQL text.
   */
  private def applySubstitutions(sqlText: String, substitutions: List[Substitution]): String = {
    // Sort substitutions by start position in reverse order to avoid offset issues
    val sortedSubstitutions = substitutions.sortBy(-_.start)

    var result = sqlText
    sortedSubstitutions.foreach { substitution =>
      result = result.substring(0, substitution.start) +
        substitution.replacement +
        result.substring(substitution.end)
    }
    result
  }
}

/**
 * Case class representing a text substitution.
 */
case class Substitution(start: Int, end: Int, replacement: String)

object SubstituteParamsParser {
  /** Singleton instance for convenience */
  private val instance = new SubstituteParamsParser()

  def substitute(
      sqlText: String,
      namedParams: Map[String, String] = Map.empty,
      positionalParams: List[String] = List.empty): (String, Int, PositionMapper) =
    instance.substitute(sqlText, namedParams, positionalParams)
}

