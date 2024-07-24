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

import java.lang.{Long => JLong, StringBuilder => JStringBuilder}

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

trait SparkParserUtils {

  /** Unescape backslash-escaped string enclosed by quotes. */
  def unescapeSQLString(b: String): String = {
    def appendEscapedChar(n: Char, sb: JStringBuilder): Unit = {
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

    def allCharsAreHex(s: String, start: Int, length: Int): Boolean = {
      val end = start + length
      var i = start
      while (i < end) {
        val c = s.charAt(i)
        val cIsHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
        if (!cIsHex) {
          return false
        }
        i += 1
      }
      true
    }

    def isThreeDigitOctalEscape(s: String, start: Int): Boolean = {
      val firstChar = s.charAt(start)
      val secondChar = s.charAt(start + 1)
      val thirdChar = s.charAt(start + 2)
      (firstChar == '0' || firstChar == '1') &&
        (secondChar >= '0' && secondChar <= '7') &&
        (thirdChar >= '0' && thirdChar <= '7')
    }

    val isRawString = {
      val firstChar = b.charAt(0)
      firstChar == 'r' || firstChar == 'R'
    }

    if (isRawString) {
      // Skip the 'r' or 'R' and the first and last quotations enclosing the string literal.
      b.substring(2, b.length - 1)
    } else if (b.indexOf('\\') == -1) {
      // Fast path for the common case where the string has no escaped characters,
      // in which case we just skip the first and last quotations enclosing the string literal.
      b.substring(1, b.length - 1)
    } else {
      val sb = new JStringBuilder(b.length())
      // Skip the first and last quotations enclosing the string literal.
      var i = 1
      val length = b.length - 1
      while (i < length) {
        val c = b.charAt(i)
        if (c != '\\' || i + 1 == length) {
          // Either a regular character or a backslash at the end of the string:
          sb.append(c)
          i += 1
        } else {
          // A backslash followed by at least one character:
          i += 1
          val cAfterBackslash = b.charAt(i)
          if (cAfterBackslash == 'u' && i + 1 + 4 <= length && allCharsAreHex(b, i + 1, 4)) {
            // \u0000 style 16-bit unicode character literals.
            sb.append(Integer.parseInt(b, i + 1, i + 1 + 4, 16).toChar)
            i += 1 + 4
          } else if (cAfterBackslash == 'U' && i + 1 + 8 <= length && allCharsAreHex(b, i + 1, 8)) {
            // \U00000000 style 32-bit unicode character literals.
            // Use Long to treat codePoint as unsigned in the range of 32-bit.
            val codePoint = JLong.parseLong(b, i + 1, i + 1 + 8, 16)
            if (codePoint < 0x10000) {
              sb.append((codePoint & 0xFFFF).toChar)
            } else {
              val highSurrogate = (codePoint - 0x10000) / 0x400 + 0xD800
              val lowSurrogate = (codePoint - 0x10000) % 0x400 + 0xDC00
              sb.append(highSurrogate.toChar)
              sb.append(lowSurrogate.toChar)
            }
            i += 1 + 8
          } else if (i + 3 <= length && isThreeDigitOctalEscape(b, i)) {
            // \000 style character literals.
            sb.append(Integer.parseInt(b, i, i + 3, 8).toChar)
            i += 3
          } else {
            appendEscapedChar(cAfterBackslash, sb)
            i += 1
          }
        }
      }
      sb.toString
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
