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

import java.util
import java.util.Locale

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNodeImpl}

import org.apache.spark.sql.catalyst.util.SparkParserUtils
import org.apache.spark.sql.errors.QueryParsingErrors

/**
 * A collection of utility methods for use during the parsing process.
 */
object ParserUtils extends SparkParserUtils {
  def operationNotAllowed(message: String, ctx: ParserRuleContext): Nothing = {
    throw QueryParsingErrors.operationNotAllowedError(message, ctx)
  }

  def invalidStatement(statement: String, ctx: ParserRuleContext): Nothing = {
    throw QueryParsingErrors.invalidStatementError(statement, ctx)
  }

  def checkDuplicateClauses[T](
      nodes: util.List[T], clauseName: String, ctx: ParserRuleContext): Unit = {
    if (nodes.size() > 1) {
      throw QueryParsingErrors.duplicateClausesError(clauseName, ctx)
    }
  }

  /** Check if duplicate keys exist in a set of key-value pairs. */
  def checkDuplicateKeys[T](keyPairs: Seq[(String, T)], ctx: ParserRuleContext): Unit = {
    keyPairs.groupBy(_._1).filter(_._2.size > 1).foreach { case (key, _) =>
      throw QueryParsingErrors.duplicateKeysError(key, ctx)
    }
  }

  /** Get the code that creates the given node. */
  def source(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(ctx.getStart.getStartIndex, ctx.getStop.getStopIndex))
  }

  /** Get all the text which comes after the given rule. */
  def remainder(ctx: ParserRuleContext): String = remainder(ctx.getStop)

  /** Get all the text which comes after the given token. */
  def remainder(token: Token): String = {
    val stream = token.getInputStream
    val interval = Interval.of(token.getStopIndex + 1, stream.size() - 1)
    stream.getText(interval)
  }

  /**
   * Get all the text which between the given start and end tokens.
   * When we need to extract everything between two tokens including all spaces we should use
   * this method instead of defined a named Antlr4 rule for .*?,
   * which somehow parse "a b" -> "ab" in some cases
   */
  def interval(start: Token, end: Token): String = {
    val interval = Interval.of(start.getStopIndex + 1, end.getStartIndex - 1)
    start.getInputStream.getText(interval)
  }

  /** Convert a string node into a string without unescaping. */
  def stringWithoutUnescape(node: Token): String = {
    // STRING parser rule forces that the input always has quotes at the starting and ending.
    node.getText.slice(1, node.getText.length - 1)
  }

  /** Collect the entries if any. */
  def entry(key: String, value: Token): Seq[(String, String)] = {
    Option(value).toSeq.map(x => key -> string(x))
  }

  /** Validate the condition. If it doesn't throw a parse exception. */
  def validate(f: => Boolean, message: String, ctx: ParserRuleContext): Unit = {
    if (!f) {
      throw new ParseException(
        errorClass = "_LEGACY_ERROR_TEMP_0064",
        messageParameters = Map("msg" -> message),
        ctx)
    }
  }

  /** the column name pattern in quoted regex without qualifier */
  val escapedIdentifier = "`((?s).+)`".r

  /** the column name pattern in quoted regex with qualifier */
  val qualifiedEscapedIdentifier = ("((?s).+)" + """.""" + "`((?s).+)`").r

  /**
   * Normalizes the expression parser tree to a SQL string which will be used to generate
   * the expression alias. In particular, it concatenates terminal nodes of the tree and
   * upper casts keywords and numeric literals.
   */
  def toExprAlias(ctx: ParseTree): String = {
    val sb = new StringBuilder()
    def concatTerms(ctx: ParseTree): Unit = {
      for (i <- 0 until ctx.getChildCount) {
        ctx.getChild(i) match {
          case term: TerminalNodeImpl =>
            val termText = term.getText
            val tt = term.getSymbol.getType
            val current = if ((SqlBaseParser.ADD <= tt && tt <= SqlBaseParser.ZONE) ||
              (SqlBaseParser.BIGINT_LITERAL <= tt && tt <= SqlBaseParser.BIGDECIMAL_LITERAL)) {
              termText.toUpperCase(Locale.ROOT)
            } else {
              termText
            }
            sb.append(current)
          case child => concatTerms(child)
        }
      }
    }
    concatTerms(ctx)
    sb.toString()
  }
}
