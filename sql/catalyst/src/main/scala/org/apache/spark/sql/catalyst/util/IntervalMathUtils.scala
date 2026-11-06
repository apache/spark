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

import java.math.RoundingMode

import com.google.common.math.{DoubleMath, LongMath}

import org.apache.spark.QueryContext
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.Decimal

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

  // The divisions of a year-month interval, in months, and of a day-time interval, in micros, by
  // a number, rounded half up. The interpreted and the generated code of `DivideYMInterval` and
  // `DivideDTInterval` both call them, so that the checks and the error are in one place. A zero
  // divisor raises `INTERVAL_DIVIDED_BY_ZERO`, and a quotient that does not fit the interval
  // type raises the interval overflow error with `try_divide` suggested: the minimum interval
  // divided by -1, or a decimal or fractional quotient out of the range once rounded, an infinite
  // one included. A NaN quotient, from a NaN divisor, is not an overflow and is left to the
  // rounding, which raises for it.

  def divide(months: Int, num: Long, context: QueryContext): Int = {
    checkDivisor(num == 0, context)
    if (months == Int.MinValue && num == -1) {
      throw QueryExecutionErrors.withSuggestionIntervalArithmeticOverflowError(
        "try_divide", context)
    }
    // The quotient of an Int by a Long fits an Int.
    LongMath.divide(months, num, RoundingMode.HALF_UP).toInt
  }

  def divide(months: Int, num: Decimal, context: QueryContext): Int = {
    checkDivisor(num.isZero, context)
    withOverflow(
      ((new Decimal).set(months) / num).toJavaBigDecimal
        .setScale(0, RoundingMode.HALF_UP).intValueExact(),
      "try_divide", context)
  }

  def divide(months: Int, num: Double, context: QueryContext): Int = {
    checkDivisor(num == 0, context)
    val quotient = months / num
    if (quotient.isNaN) {
      DoubleMath.roundToInt(quotient, RoundingMode.HALF_UP)
    } else {
      withOverflow(DoubleMath.roundToInt(quotient, RoundingMode.HALF_UP), "try_divide", context)
    }
  }

  def divide(micros: Long, num: Long, context: QueryContext): Long = {
    checkDivisor(num == 0, context)
    if (micros == Long.MinValue && num == -1) {
      throw QueryExecutionErrors.withSuggestionIntervalArithmeticOverflowError(
        "try_divide", context)
    }
    LongMath.divide(micros, num, RoundingMode.HALF_UP)
  }

  def divide(micros: Long, num: Decimal, context: QueryContext): Long = {
    checkDivisor(num.isZero, context)
    withOverflow(
      ((new Decimal).set(micros) / num).toJavaBigDecimal
        .setScale(0, RoundingMode.HALF_UP).longValueExact(),
      "try_divide", context)
  }

  def divide(micros: Long, num: Double, context: QueryContext): Long = {
    checkDivisor(num == 0, context)
    val quotient = micros / num
    if (quotient.isNaN) {
      DoubleMath.roundToLong(quotient, RoundingMode.HALF_UP)
    } else {
      withOverflow(DoubleMath.roundToLong(quotient, RoundingMode.HALF_UP), "try_divide", context)
    }
  }

  private def checkDivisor(isZero: Boolean, context: QueryContext): Unit = {
    if (isZero) throw QueryExecutionErrors.intervalDividedByZeroError(context)
  }

  private def withOverflow[A](
      f: => A,
      suggestedFunc: String = "",
      context: QueryContext = null): A = {
    try {
      f
    } catch {
      case _: ArithmeticException if suggestedFunc.isEmpty =>
        throw QueryExecutionErrors.withoutSuggestionIntervalArithmeticOverflowError(context)
      case _: ArithmeticException =>
        throw QueryExecutionErrors.withSuggestionIntervalArithmeticOverflowError(
          suggestedFunc, context)
    }
  }
}
