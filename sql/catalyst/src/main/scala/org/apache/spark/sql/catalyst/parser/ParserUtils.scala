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

import scala.collection.mutable.StringBuilder

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

/**
 * A collection of utility methods for use during the parsing process.
 */
object ParserUtils {
  /** Get the command which created the token. */
  def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size()))
  }

  def operationNotAllowed(message: String, ctx: ParserRuleContext): Nothing = {
    throw new ParseException(s"Operation not allowed: $message", ctx)
  }

  /** Check if duplicate keys exist in a set of key-value pairs. */
  def checkDuplicateKeys[T](keyPairs: Seq[(String, T)], ctx: ParserRuleContext): Unit = {
    keyPairs.groupBy(_._1).filter(_._2.size > 1).foreach { case (key, _) =>
      throw new ParseException(s"Found duplicate keys '$key'.", ctx)
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
    val interval = Interval.of(token.getStopIndex + 1, stream.size())
    stream.getText(interval)
  }

  /** Convert a string token into a string. */
  def string(token: Token): String = unescapeSQLString(token.getText)

  /** Convert a string node into a string. */
  def string(node: TerminalNode): String = unescapeSQLString(node.getText)

  /** Convert a string node into a string without unescaping. */
  def stringWithoutUnescape(node: TerminalNode): String = {
    // STRING parser rule forces that the input always has quotes at the starting and ending.
    node.getText.slice(1, node.getText.size - 1)
  }

  /** Get the origin (line and position) of the token. */
  def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }

  /** Validate the condition. If it doesn't throw a parse exception. */
  def validate(f: => Boolean, message: String, ctx: ParserRuleContext): Unit = {
    if (!f) {
      throw new ParseException(message, ctx)
    }
  }

  /**
   * Register the origin of the context. Any TreeNode created in the closure will be assigned the
   * registered origin. This method restores the previously set origin after completion of the
   * closure.
   */
  def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  /** Unescape baskslash-escaped string enclosed by quotes. */
  def unescapeSQLString(b: String): String = {
    var enclosure: Character = null
    val sb = new StringBuilder(b.length())

    def appendEscapedChar(n: Char) {
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

    var i = 0
    val strLength = b.length
    while (i < strLength) {
      val currentChar = b.charAt(i)
      if (enclosure == null) {
        if (currentChar == '\'' || currentChar == '\"') {
          enclosure = currentChar
        }
      } else if (enclosure == currentChar) {
        enclosure = null
      } else if (currentChar == '\\') {

        if ((i + 6 < strLength) && b.charAt(i + 1) == 'u') {
          // \u0000 style character literals.

          val base = i + 2
          val code = (0 until 4).foldLeft(0) { (mid, j) =>
            val digit = Character.digit(b.charAt(j + base), 16)
            (mid << 4) + digit
          }
          sb.append(code.asInstanceOf[Char])
          i += 5
        } else if (i + 4 < strLength) {
          // \000 style character literals.

          val i1 = b.charAt(i + 1)
          val i2 = b.charAt(i + 2)
          val i3 = b.charAt(i + 3)

          if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7') && (i3 >= '0' && i3 <= '7')) {
            val tmp = ((i3 - '0') + ((i2 - '0') << 3) + ((i1 - '0') << 6)).asInstanceOf[Char]
            sb.append(tmp)
            i += 3
          } else {
            appendEscapedChar(i1)
            i += 1
          }
        } else if (i + 2 < strLength) {
          // escaped character literals.
          val n = b.charAt(i + 1)
          appendEscapedChar(n)
          i += 1
        }
      } else {
        // non-escaped character literals.
        sb.append(currentChar)
      }
      i += 1
    }
    sb.toString()
  }

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
