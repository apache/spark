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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}

/**
 * Context for passing parameter values to the SQL parser.
 * This allows parameter substitution to happen during the parsing phase
 * rather than during analysis.
 */
sealed trait ParameterContext

/**
 * Context for named parameters (e.g., :paramName).
 *
 * @param params Map of parameter names to their expression values
 */
case class NamedParameterContext(params: Map[String, Expression]) extends ParameterContext

/**
 * Context for positional parameters (e.g., ?).
 *
 * @param params Sequence of expression values in order
 */
case class PositionalParameterContext(params: Seq[Expression]) extends ParameterContext

/**
 * Thread-local storage for parameter context to pass parameters
 * from SparkSession to the parser without changing method signatures.
 */
object ThreadLocalParameterContext {
  private val context = new ThreadLocal[Option[ParameterContext]]()
  
  /**
   * Set the parameter context for the current thread.
   */
  def set(ctx: ParameterContext): Unit = context.set(Some(ctx))
  
  /**
   * Get the parameter context for the current thread.
   */
  def get(): Option[ParameterContext] = Option(context.get()).flatten
  
  /**
   * Clear the parameter context for the current thread.
   */
  def clear(): Unit = context.remove()
  
  /**
   * Execute a block of code with the given parameter context,
   * ensuring cleanup happens even if an exception occurs.
   */
  def withContext[T](ctx: ParameterContext)(block: => T): T = {
    set(ctx)
    try {
      block
    } finally {
      clear()
    }
  }
}
