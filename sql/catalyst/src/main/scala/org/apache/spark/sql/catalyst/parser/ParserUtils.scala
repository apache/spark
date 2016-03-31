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

import org.antlr.v4.runtime.{CharStream, ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.catalyst.parser.ParseUtils.unescapeSQLString
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

/**
 * A collection of utility methods for use during the parsing process.
 */
object ParserUtils {
  /** Get the command which created the token. */
  def command(ctx: ParserRuleContext): String = {
    command(ctx.getStart.getInputStream)
  }

  /** Get the command which created the token. */
  def command(stream: CharStream): String = {
    stream.getText(Interval.of(0, stream.size()))
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

  /** Get the origin (line and position) of the token. */
  def position(token: Token): Origin = {
    Origin(Option(token.getLine), Option(token.getCharPositionInLine))
  }

  /** Assert if a condition holds. If it doesn't throw a parse exception. */
  def assert(f: => Boolean, message: String, ctx: ParserRuleContext): Unit = {
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
    def optionalMap[C <: ParserRuleContext](
        ctx: C)(
        f: (C, LogicalPlan) => LogicalPlan): LogicalPlan = {
      if (ctx != null) {
        f(ctx, plan)
      } else {
        plan
      }
    }
  }
}
