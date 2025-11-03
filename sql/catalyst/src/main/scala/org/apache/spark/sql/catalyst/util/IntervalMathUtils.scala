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

import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * Helper functions for interval arithmetic operations with overflow.
 */
object IntervalMathUtils {

  def addExact(a: Int, b: Int): Int = withOverflow(Math.addExact(a, b), "try_add")

  def addExact(a: Long, b: Long): Long = withOverflow(Math.addExact(a, b), "try_add")

  def subtractExact(a: Int, b: Int): Int = withOverflow(Math.subtractExact(a, b), "try_subtract")

  def subtractExact(a: Long, b: Long): Long = withOverflow(Math.subtractExact(a, b), "try_subtract")

  def negateExact(a: Int): Int = withOverflow(Math.negateExact(a))

  def negateExact(a: Long): Long = withOverflow(Math.negateExact(a))

  private def withOverflow[A](f: => A, suggestedFunc: String = ""): A = {
    try {
      f
    } catch {
      case _: ArithmeticException if suggestedFunc.isEmpty =>
        throw QueryExecutionErrors.withoutSuggestionIntervalArithmeticOverflowError(context = null)
      case _: ArithmeticException =>
        throw QueryExecutionErrors.withSuggestionIntervalArithmeticOverflowError(
          suggestedFunc, context = null)
    }
  }
}
