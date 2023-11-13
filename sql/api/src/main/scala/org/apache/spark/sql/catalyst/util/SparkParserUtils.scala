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
package org.apache.spark.sql.catalyst.util

import java.lang.{Long => JLong}
import java.nio.CharBuffer

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

trait SparkParserUtils {
  val U16_CHAR_PATTERN = """\\u([a-fA-F0-9]{4})(?s).*""".r
  val U32_CHAR_PATTERN = """\\U([a-fA-F0-9]{8})(?s).*""".r
  val OCTAL_CHAR_PATTERN = """\\([01][0-7]{2})(?s).*""".r
  val ESCAPED_CHAR_PATTERN = """\\((?s).)(?s).*""".r

  /** Unescape backslash-escaped string enclosed by quotes. */
  def unescapeSQLString(b: String): String = {
    val sb = new StringBuilder(b.length())

    def appendEscapedChar(n: Char): Unit = {
      n match {
        case '0' => sb.append('\u0000')
        case 'b' => sb.append('\b')
        case 'n' => sb.append('\n')
        case 'r' => sb.append('\r')
        case 't' => sb.append('\t')
        case 'Z' => sb.append('\u001A')
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

  /** Convert a string token into a string. */
  def string(token: Token): String = unescapeSQLString(token.getText)

  /** Convert a string node into a string. */
  def string(node: TerminalNode): String = unescapeSQLString(node.getText)

  /** Get the origin (line and position) of the token. */
  def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
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

  /** Get the command which created the token. */
  def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size() - 1))
  }
}

object SparkParserUtils extends SparkParserUtils
