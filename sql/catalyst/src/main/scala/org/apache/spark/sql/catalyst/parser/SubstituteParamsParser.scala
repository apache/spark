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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException

/**
 * Enumeration for the types of SQL rules that can be parsed for parameter substitution.
 */
sealed trait SubstitutionRule

object SubstitutionRule {
  /** Rule for parsing complete SQL queries */
  case object Query extends SubstitutionRule

  /** Rule for parsing SQL statements (includes queries, DDL, DML, etc.) */
  case object Statement extends SubstitutionRule

  /** Rule for parsing SQL expressions */
  case object Expression extends SubstitutionRule

  /** Rule for parsing Column definition lists */
  case object ColDefinitionList extends SubstitutionRule
}

/**
 * A parameter substitution parser that replaces parameter markers in SQL text with their values.
 * This parser finds parameter markers and substitutes them with provided values to produce
 * a modified SQL string ready for execution.
 */
class SubstituteParamsParser extends Logging {

  /**
   * Substitute parameter markers in SQL text with provided values.
   *
   * @param sqlText          The original SQL text containing parameter markers
   * @param rule             The type of SQL rule to parse (Query or Expression)
   * @param namedParams      Map of named parameter values (paramName -> value)
   * @param positionalParams List of positional parameter values in order
   * @return A tuple of (modified SQL string with parameters substituted,
   *         number of consumed positional parameters)
   */
  def substitute(
                  sqlText: String,
                  rule: SubstitutionRule,
                  namedParams: Map[String, String] = Map.empty,
                  positionalParams: List[String] = List.empty): (String, Int) = {
    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    val astBuilder = new SubstituteParmsAstBuilder()

    // Parse as a single statement to get parameter locations
    val ctx = rule match {
      case SubstitutionRule.Query => parser.query()
      case SubstitutionRule.Statement => parser.singleStatement()
      case SubstitutionRule.Expression => parser.expression()
      case SubstitutionRule.ColDefinitionList => parser.colDefinitionList()
    }
    val parameterLocations = astBuilder.extractParameterLocations(ctx)

    // Substitute parameters in the original text
    val substitutedSql = substituteAtLocations(sqlText, parameterLocations, namedParams,
      positionalParams)
    val consumedPositionalParams = parameterLocations.positionalParameterLocations.length

    (substitutedSql, consumedPositionalParams)
  }

  /**
   * Detects parameter markers in SQL text without performing substitution.
   *
   * @param sqlText The original SQL text to analyze
   * @param rule    The type of SQL rule to parse (Query or Expression)
   * @return A tuple of (hasPositionalParameters, hasNamedParameters)
   */
  def detectParameters(sqlText: String, rule: SubstitutionRule): (Boolean, Boolean) = {
    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    val astBuilder = new SubstituteParmsAstBuilder()

    // Parse as a single statement to get parameter locations
    val ctx = rule match {
      case SubstitutionRule.Query => parser.query()
      case SubstitutionRule.Statement => parser.singleStatement()
      case SubstitutionRule.Expression => parser.expression()
      case SubstitutionRule.ColDefinitionList => parser.colDefinitionList()
    }
    val parameterLocations = astBuilder.extractParameterLocations(ctx)

    val hasPositionalParams = parameterLocations.positionalParameterLocations.nonEmpty
    val hasNamedParams = parameterLocations.namedParameterLocations.nonEmpty

    (hasPositionalParams, hasNamedParams)
  }

  /**
   * Apply substitutions to the original SQL text at specified locations.
   */
  private def substituteAtLocations(
                                     sqlText: String,
                                     locations: ParameterLocationInfo,
                                     namedParams: Map[String, String],
                                     positionalParams: List[String]): String = {

    val substitutions = scala.collection.mutable.ListBuffer[Substitution]()

    // Handle named parameters
    locations.namedParameterLocations.foreach { case (name, location) =>
      namedParams.get(name) match {
        case Some(value) =>
          substitutions += Substitution(location.start, location.end, value)
        case None =>
          throw new IllegalArgumentException(s"Missing value for named parameter: $name")
      }
    }

    // Handle positional parameters
    if (locations.positionalParameterLocations.length > positionalParams.length) {
      throw new AnalysisException(
        errorClass = "UNBOUND_SQL_PARAMETER",
        messageParameters = Map("name" -> "?"))
    }

    locations.positionalParameterLocations.zip(positionalParams).foreach {
      case (location, value) =>
        substitutions += Substitution(location.start, location.end, value)
    }

    applySubstitutions(sqlText, substitutions.toList)
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
      rule: SubstitutionRule,
      namedParams: Map[String, String] = Map.empty,
      positionalParams: List[String] = List.empty): (String, Int) =
    instance.substitute(sqlText, rule, namedParams, positionalParams)
}

