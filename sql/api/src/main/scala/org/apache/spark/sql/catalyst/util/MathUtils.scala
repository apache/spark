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

import org.apache.spark.QueryContext
import org.apache.spark.sql.errors.ExecutionErrors

/**
 * Helper functions for arithmetic operations with overflow.
 */
object MathUtils {

  def addExact(a: Int, b: Int): Int = withOverflow(Math.addExact(a, b))

  def addExact(a: Int, b: Int, context: QueryContext): Int = {
    withOverflow(Math.addExact(a, b), hint = "try_add", context)
  }

  def addExact(a: Long, b: Long): Long = withOverflow(Math.addExact(a, b))

  def addExact(a: Long, b: Long, context: QueryContext): Long = {
    withOverflow(Math.addExact(a, b), hint = "try_add", context)
  }

  def subtractExact(a: Int, b: Int): Int = withOverflow(Math.subtractExact(a, b))

  def subtractExact(a: Int, b: Int, context: QueryContext): Int = {
    withOverflow(Math.subtractExact(a, b), hint = "try_subtract", context)
  }

  def subtractExact(a: Long, b: Long): Long = withOverflow(Math.subtractExact(a, b))

  def subtractExact(a: Long, b: Long, context: QueryContext): Long = {
    withOverflow(Math.subtractExact(a, b), hint = "try_subtract", context)
  }

  def multiplyExact(a: Int, b: Int): Int = withOverflow(Math.multiplyExact(a, b))

  def multiplyExact(a: Int, b: Int, context: QueryContext): Int = {
    withOverflow(Math.multiplyExact(a, b), hint = "try_multiply", context)
  }

  def multiplyExact(a: Long, b: Long): Long = withOverflow(Math.multiplyExact(a, b))

  def multiplyExact(a: Long, b: Long, context: QueryContext): Long = {
    withOverflow(Math.multiplyExact(a, b), hint = "try_multiply", context)
  }

  def negateExact(a: Byte): Byte = {
    if (a == Byte.MinValue) { // if and only if x is Byte.MinValue, overflow can happen
      throw ExecutionErrors.arithmeticOverflowError("byte overflow")
    }
    (-a).toByte
  }

  def negateExact(a: Short): Short = {
    if (a == Short.MinValue) { // if and only if x is Short.MinValue, overflow can happen
      throw ExecutionErrors.arithmeticOverflowError("short overflow")
    }
    (-a).toShort
  }

  def negateExact(a: Int): Int = withOverflow(Math.negateExact(a))

  def negateExact(a: Long): Long = withOverflow(Math.negateExact(a))

  def toIntExact(a: Long): Int = withOverflow(Math.toIntExact(a))

  def floorDiv(a: Int, b: Int): Int = withOverflow(Math.floorDiv(a, b), hint = "try_divide")

  def floorDiv(a: Long, b: Long): Long = withOverflow(Math.floorDiv(a, b), hint = "try_divide")

  def floorMod(a: Int, b: Int): Int = withOverflow(Math.floorMod(a, b))

  def floorMod(a: Long, b: Long): Long = withOverflow(Math.floorMod(a, b))

  // Positive modulo (`pmod`). For a divisor `n > 0` the result is in `[0, n)` (for the
  // float/double overloads, when both inputs are finite). For `n < 0` it shares the sign of the
  // dividend `a`, except that for `Int`/`Long` the retained `(r + n) % n` below silently wraps
  // when `r + n` overflows (`n < -2^30` / `n < -2^62`), e.g. `pmod(-1, Int.MinValue)` is
  // `Int.MaxValue`; that pre-existing behavior is kept as-is. Unlike `floorMod`, whose result
  // takes the sign of `n`, this matches the `pmod` SQL function / `HashPartitioning` semantics.
  // Shared by `Pmod`'s eval and codegen paths so the two never diverge.
  //
  // For `n > 0` the integral result is exactly `Math.floorMod(a, n)`: a negative `r = a % n` only
  // needs `n` added, so the second `% n` is skipped. `n > 0` is tested first because it is
  // predictable (and constant for `HashPartitioning`'s literal divisor), and the fix-up
  // `r + (n & (r >> 31))` is branchless, so hash-like dividends do not mispredict on the sign of
  // `r`. For `n < 0` the `% n` is retained to preserve the original result. `Byte`/`Short`
  // delegate to the `Int` overload; their values are too small for `r + n` to overflow. The
  // float/double overloads always keep the `% n` because `r + n` can round up to exactly `n`.

  def pmod(a: Int, n: Int): Int = {
    val r = a % n
    if (n > 0) r + (n & (r >> 31)) else if (r >= 0) r else (r + n) % n
  }

  def pmod(a: Long, n: Long): Long = {
    val r = a % n
    if (n > 0) r + (n & (r >> 63)) else if (r >= 0) r else (r + n) % n
  }

  def pmod(a: Byte, n: Byte): Byte = pmod(a.toInt, n.toInt).toByte

  def pmod(a: Short, n: Short): Short = pmod(a.toInt, n.toInt).toShort

  def pmod(a: Float, n: Float): Float = {
    val r = a % n
    if (r < 0) (r + n) % n else r
  }

  def pmod(a: Double, n: Double): Double = {
    val r = a % n
    if (r < 0) (r + n) % n else r
  }

  // Casts a rounded double (the result of Math.ceil/Math.floor) to long, throwing an arithmetic
  // overflow error when the value cannot be represented as a long. NaN is passed through to the
  // JVM cast (which yields 0), matching the previous behavior. Shared by the eval and codegen
  // paths of `Ceil`/`Floor` so the two never diverge.
  def doubleToLong(value: Double, context: QueryContext): Long = {
    if (!value.isNaN &&
      (value < Long.MinValue.toDouble || value >= Long.MaxValue.toDouble)) {
      throw ExecutionErrors.arithmeticOverflowError("long overflow", context = context)
    }
    value.toLong
  }

  def withOverflow[A](f: => A, hint: String = "", context: QueryContext = null): A = {
    try {
      f
    } catch {
      case e: ArithmeticException =>
        throw ExecutionErrors.arithmeticOverflowError(e.getMessage, hint, context)
    }
  }

  def withOverflowCode(evalCode: String, context: String): String = {
    s"""
       |try {
       |  $evalCode
       |} catch (ArithmeticException e) {
       |  throw QueryExecutionErrors.arithmeticOverflowError(e.getMessage(), "", $context);
       |}
       |""".stripMargin
  }
}
