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

import java.lang.{Long => JLong}
import java.nio.CharBuffer
import java.util

import scala.collection.mutable.StringBuilder

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.errors.QueryParsingErrors

/**
 * A collection of utility methods for use during the parsing process.
 */
object ParserUtils {

  val U16_CHAR_PATTERN = """\\u([a-fA-F0-9]{4})(?s).*""".r
  val U32_CHAR_PATTERN = """\\U([a-fA-F0-9]{8})(?s).*""".r
  val OCTAL_CHAR_PATTERN = """\\([01][0-7]{2})(?s).*""".r
  val ESCAPED_CHAR_PATTERN = """\\((?s).)(?s).*""".r

  /** Get the command which created the token. */
  def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size() - 1))
  }

  def operationNotAllowed(message: String, ctx: ParserRuleContext): Nothing = {
    throw QueryParsingErrors.operationNotAllowedError(message, ctx)
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

  /** Convert a string token into a string. */
  def string(token: Token): String = unescapeSQLString(token.getText)

  /** Convert a string node into a string. */
  def string(node: TerminalNode): String = unescapeSQLString(node.getText)

  /** Convert a string node into a string without unescaping. */
  def stringWithoutUnescape(node: Token): String = {
    // STRING parser rule forces that the input always has quotes at the starting and ending.
    node.getText.slice(1, node.getText.size - 1)
  }

  /** Collect the entries if any. */
  def entry(key: String, value: Token): Seq[(String, String)] = {
    Option(value).toSeq.map(x => key -> string(x))
  }

  /** Get the origin (line and position) of the token. */
  def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }

  def positionAndText(
      startToken: Token,
      stopToken: Token,
      sqlText: String,
      objectType: Option[String],
      objectName: Option[String]): Origin = {
    val startOpt = Option(startToken)
    val stopOpt = Option(stopToken)
    Origin(
      line = startOpt.map(_.getLine),
      startPosition = startOpt.map(_.getCharPositionInLine),
      startIndex = startOpt.map(_.getStartIndex),
      stopIndex = stopOpt.map(_.getStopIndex),
      sqlText = Some(sqlText),
      objectType = objectType,
      objectName = objectName)
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

  /**
   * Register the origin of the context. Any TreeNode created in the closure will be assigned the
   * registered origin. This method restores the previously set origin after completion of the
   * closure.
   */
  def withOrigin[T](ctx: ParserRuleContext, sqlText: Option[String] = None)(f: => T): T = {
    val current = CurrentOrigin.get
    val text = sqlText.orElse(current.sqlText)
    if (text.isEmpty) {
      CurrentOrigin.set(position(ctx.getStart))
    } else {
      CurrentOrigin.set(positionAndText(ctx.getStart, ctx.getStop, text.get,
        current.objectType, current.objectName))
    }
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  /** Unescape backslash-escaped string enclosed by quotes. */
  def unescapeSQLString(b: String): String = {
    val sb = new StringBuilder(b.length())

    def appendEscapedChar(n: Char): Unit = {
      n match {
        case '0' => sb.append('\u0000')
        case '\'' => sb.append('\'')
        case '"' => sb.append('\"')
        case 'b' => sb.append('\b')
        case 'n' => sb.append('\n')
        case 'r' => sb.append('\r')
        case 't' => sb.append('\t')
        case 'Z' => sb.append('\u001A')
        case '\\' => sb.append('\\')
        // The following 2 lines are exactly what MySQL does TODO: why do we do this?
        case '%' => sb.append("\\%")
        case '_' => sb.append("\\_")
        case _ => sb.append(n)
      }
    }

    if (b.startsWith("r") || b.startsWith("R")) {
      b.substring(2, b.length - 1)
    } else {
      // Skip the first and last quotations enclosing the string literal.
      val charBuffer = CharBuffer.wrap(b, 1, b.length - 1)

      while (charBuffer.remaining() > 0) {
        charBuffer match {
          case U16_CHAR_PATTERN(cp) =>
            // \u0000 style 16-bit unicode character literals.
            sb.append(Integer.parseInt(cp, 16).toChar)
            charBuffer.position(charBuffer.position() + 6)
          case U32_CHAR_PATTERN(cp) =>
            // \U00000000 style 32-bit unicode character literals.
            // Use Long to treat codePoint as unsigned in the range of 32-bit.
            val codePoint = JLong.parseLong(cp, 16)
            if (codePoint < 0x10000) {
              sb.append((codePoint & 0xFFFF).toChar)
            } else {
              val highSurrogate = (codePoint - 0x10000) / 0x400 + 0xD800
              val lowSurrogate = (codePoint - 0x10000) % 0x400 + 0xDC00
              sb.append(highSurrogate.toChar)
              sb.append(lowSurrogate.toChar)
            }
            charBuffer.position(charBuffer.position() + 10)
          case OCTAL_CHAR_PATTERN(cp) =>
            // \000 style character literals.
            sb.append(Integer.parseInt(cp, 8).toChar)
            charBuffer.position(charBuffer.position() + 4)
          case ESCAPED_CHAR_PATTERN(c) =>
            // escaped character literals.
            appendEscapedChar(c.charAt(0))
            charBuffer.position(charBuffer.position() + 2)
          case _ =>
            // non-escaped character literals.
            sb.append(charBuffer.get())
        }
      }
      sb.toString()
    }
  }

  /** the column name pattern in quoted regex without qualifier */
  val escapedIdentifier = "`((?s).+)`".r

  /** the column name pattern in quoted regex with qualifier */
  val qualifiedEscapedIdentifier = ("((?s).+)" + """.""" + "`((?s).+)`").r

  /** Some syntactic sugar which makes it easier to work with optional clauses for LogicalPlans. */
  implicit class EnhancedLogicalPlan(val plan: LogicalPlan) extends AnyVal {
    /**
     * Create a plan using the block of code when the given context exists. Otherwise return the
     * original plan.
     */
    def optional(ctx: AnyRef)(f: => LogicalPlan): LogicalPlan = {
      if (ctx != null) {
        f
      } else {
        plan
      }
    }

    /**
     * Map a [[LogicalPlan]] to another [[LogicalPlan]] if the passed context exists using the
     * passed function. The original plan is returned when the context does not exist.
     */
    def optionalMap[C](ctx: C)(f: (C, LogicalPlan) => LogicalPlan): LogicalPlan = {
      if (ctx != null) {
        f(ctx, plan)
      } else {
        plan
      }
    }
  }
}
