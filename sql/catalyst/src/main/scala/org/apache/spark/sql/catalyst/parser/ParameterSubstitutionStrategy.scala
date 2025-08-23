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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ExpressionToSqlConverter

/**
 * Strategy pattern for parameter substitution in SQL text.
 *
 * This trait defines a unified interface for handling different parameter types and contexts
 * in Spark SQL. It enables polymorphic behavior for named parameters (:param), positional
 * parameters (?), and SQL text with no parameters.
 *
 * The strategy pattern allows for:
 * - Type-safe parameter handling
 * - Extensible parameter types
 * - Testable components in isolation
 * - Consistent behavior across different SQL contexts
 *
 * @example
 * {{{
 * val strategy = ParameterSubstitutionStrategy.named(Map("param1" -> Literal(42)))
 * val result = strategy.substitute("SELECT :param1", SubstitutionRule.Statement, substitutor)
 * // result: "SELECT 42"
 * }}}
 *
 * @see [[ParameterHandler]] for the main entry point
 * @see [[NamedParameterStrategy]] for named parameter handling
 * @see [[PositionalParameterStrategy]] for positional parameter handling
 * @since 4.0.0
 */
sealed trait ParameterSubstitutionStrategy {

  /**
   * Substitute parameters in SQL text with provided values.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param rule The substitution rule to use for parsing
   * @param substitutor The parameter substitution parser instance
   * @return The SQL text with parameters substituted
   */
  def substitute(
      sqlText: String,
      rule: SubstitutionRule,
      substitutor: SubstituteParamsParser): String

  /**
   * Detect if the SQL text contains parameters that this strategy can handle.
   *
   * @param sqlText The SQL text to check
   * @param rule The substitution rule to use for parsing
   * @param substitutor The parameter substitution parser instance
   * @return True if this strategy can handle parameters in the SQL text
   */
  def canHandle(
      sqlText: String,
      rule: SubstitutionRule,
      substitutor: SubstituteParamsParser): Boolean
}

/**
 * Strategy for handling named parameters (e.g., :paramName).
 */
case class NamedParameterStrategy(paramMap: Map[String, Expression])
    extends ParameterSubstitutionStrategy {

  override def substitute(
      sqlText: String,
      rule: SubstitutionRule,
      substitutor: SubstituteParamsParser): String = {

    val paramValues = paramMap.map { case (name, expr) =>
      (name, ExpressionToSqlConverter.convert(expr))
    }

    try {
      val (substituted, _) = substitutor.substitute(
        sqlText, rule, namedParams = paramValues)
      substituted
    } catch {
      case _: ParseException =>
        // If parameter substitution fails due to syntax errors, return original query
        // and let the main parser handle the error
        sqlText
    }
  }

  override def canHandle(
      sqlText: String,
      rule: SubstitutionRule,
      substitutor: SubstituteParamsParser): Boolean = {
    try {
      val (_, hasNamed) = substitutor.detectParameters(sqlText, rule)
      hasNamed && paramMap.nonEmpty
    } catch {
      case _: ParseException => false
    }
  }
}

/**
 * Strategy for handling positional parameters (e.g., ?).
 */
case class PositionalParameterStrategy(paramList: Seq[Expression])
    extends ParameterSubstitutionStrategy {

  override def substitute(
      sqlText: String,
      rule: SubstitutionRule,
      substitutor: SubstituteParamsParser): String = {

    val paramValues = paramList.map(ExpressionToSqlConverter.convert).toList

    try {
      val (substituted, _) = substitutor.substitute(
        sqlText, rule, positionalParams = paramValues)
      substituted
    } catch {
      case _: ParseException =>
        // If parameter substitution fails due to syntax errors, return original query
        // and let the main parser handle the error
        sqlText
    }
  }

  override def canHandle(
      sqlText: String,
      rule: SubstitutionRule,
      substitutor: SubstituteParamsParser): Boolean = {
    try {
      val (hasPositional, _) = substitutor.detectParameters(sqlText, rule)
      hasPositional && paramList.nonEmpty
    } catch {
      case _: ParseException => false
    }
  }
}

/**
 * Strategy for handling cases where no parameters are present.
 */
object NoParameterStrategy extends ParameterSubstitutionStrategy {

  override def substitute(
      sqlText: String,
      rule: SubstitutionRule,
      substitutor: SubstituteParamsParser): String = {
    sqlText // No substitution needed
  }

  override def canHandle(
      sqlText: String,
      rule: SubstitutionRule,
      substitutor: SubstituteParamsParser): Boolean = {
    try {
      val (hasPositional, hasNamed) = substitutor.detectParameters(sqlText, rule)
      !hasPositional && !hasNamed
    } catch {
      case _: ParseException => true // If parsing fails, assume no parameters
    }
  }
}

/**
 * Factory for creating parameter substitution strategies.
 */
object ParameterSubstitutionStrategy {

  /**
   * Create a strategy based on the parameter context.
   *
   * @param context The parameter context containing parameter values
   * @return The appropriate strategy for the given context
   */
  def fromContext(context: ParameterContext): ParameterSubstitutionStrategy = {
    context match {
      case NamedParameterContext(params) => NamedParameterStrategy(params)
      case PositionalParameterContext(params) => PositionalParameterStrategy(params)
    }
  }

  /**
   * Create a strategy for named parameters.
   *
   * @param paramMap Map of parameter names to expressions
   * @return Named parameter strategy
   */
  def named(paramMap: Map[String, Expression]): ParameterSubstitutionStrategy = {
    if (paramMap.nonEmpty) NamedParameterStrategy(paramMap) else NoParameterStrategy
  }

  /**
   * Create a strategy for positional parameters.
   *
   * @param paramList Sequence of parameter expressions
   * @return Positional parameter strategy
   */
  def positional(paramList: Seq[Expression]): ParameterSubstitutionStrategy = {
    if (paramList.nonEmpty) PositionalParameterStrategy(paramList) else NoParameterStrategy
  }

  /**
   * Substitute parameters in SQL text using the most appropriate strategy.
   *
   * @param sqlText The SQL text containing parameter markers
   * @param rule The substitution rule to use for parsing
   * @param strategies List of strategies to try in order
   * @return The SQL text with parameters substituted
   */
  def substituteWithStrategies(
      sqlText: String,
      rule: SubstitutionRule,
      strategies: Seq[ParameterSubstitutionStrategy]): String = {

    val substitutor = new SubstituteParamsParser()

    // Find the first strategy that can handle the SQL text
    strategies.find(_.canHandle(sqlText, rule, substitutor)) match {
      case Some(strategy) => strategy.substitute(sqlText, rule, substitutor)
      case None => sqlText // No strategy can handle, return original
    }
  }
}
