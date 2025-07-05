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

/**
 * A shallow parser that only extracts parameter markers from SQL text.
 * This parser is lightweight and doesn't perform full SQL parsing,
 * making it suitable for parameter substitution preprocessing.
 */
class SubstituteParamsParser {

  /**
   * Extract all parameter markers from the given SQL text.
   *
   * @param sqlText The SQL text to parse
   * @return A ParameterInfo object containing all found parameters
   */
  def extractParameters(sqlText: String): ParameterInfo = {

    try {
      val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sqlText)))
      lexer.removeErrorListeners()
      lexer.addErrorListener(ParseErrorListener)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new SqlBaseParser(tokenStream)
      parser.removeErrorListeners()
      parser.addErrorListener(ParseErrorListener)

      val astBuilder = new SubstituteParmsAstBuilder()

      // Try to parse as a single statement to extract parameters
      val ctx = parser.singleStatement()
      astBuilder.extractFromContext(ctx)

    } catch {
      case _: Exception =>
        // If parsing fails, fall back to token-based extraction
        extractParametersFromTokens(sqlText)
    }
  }

  /**
   * Fallback method to extract parameters using token-level parsing.
   * This is used when full parsing fails but we still need to find parameter markers.
   */
  private def extractParametersFromTokens(sqlText: String): ParameterInfo = {
    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()

    val tokenStream = new CommonTokenStream(lexer)
    tokenStream.fill()

    val namedParams = scala.collection.mutable.Set[String]()
    val positionalParams = scala.collection.mutable.ListBuffer[Int]()

    for (i <- 0 until tokenStream.size()) {
      val token = tokenStream.get(i)
      token.getType match {
        case SqlBaseLexer.COLON =>
          // Check if next token is an identifier (named parameter)
          if (i + 1 < tokenStream.size()) {
            val nextToken = tokenStream.get(i + 1)
            if (nextToken.getType == SqlBaseLexer.IDENTIFIER) {
              namedParams += nextToken.getText
            }
          }
        case SqlBaseLexer.QUESTION =>
          // Positional parameter
          positionalParams += token.getStartIndex
        case _ =>
          // Ignore other tokens
      }
    }

    ParameterInfo(namedParams.toSet, positionalParams.toList)
  }

  /**
   * Check if the given SQL text contains any parameter markers.
   */
  def hasParameters(sqlText: String): Boolean = {
    val params = extractParameters(sqlText)
    params.namedParameters.nonEmpty || params.positionalParameters.nonEmpty
  }

  /**
   * Count the total number of parameter markers in the SQL text.
   */
  def countParameters(sqlText: String): Int = {
    val params = extractParameters(sqlText)
    params.namedParameters.size + params.positionalParameters.size
  }
}

// ParameterInfo is defined in SubstituteParmsAstBuilder.scala

object SubstituteParamsParser {
  /** Singleton instance for convenience */
  private val instance = new SubstituteParamsParser()

  def extractParameters(sqlText: String): ParameterInfo = instance.extractParameters(sqlText)
  def hasParameters(sqlText: String): Boolean = instance.hasParameters(sqlText)
  def countParameters(sqlText: String): Int = instance.countParameters(sqlText)
}

