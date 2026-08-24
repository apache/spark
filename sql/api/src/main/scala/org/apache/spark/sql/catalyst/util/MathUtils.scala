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

import java.{lang => jl}

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

  // Positive modulo (`pmod`): the remainder `a % n` adjusted to share the sign of `n`.
  // Unlike `floorMod`, this matches the `pmod` SQL function / `HashPartitioning` semantics.
  // Shared by `Pmod`'s eval and codegen paths so the two never diverge.

  def pmod(a: Int, n: Int): Int = {
    val r = a % n
    if (r < 0) (r + n) % n else r
  }

  def pmod(a: Long, n: Long): Long = {
    val r = a % n
    if (r < 0) (r + n) % n else r
  }

  def pmod(a: Byte, n: Byte): Byte = {
    val r = a % n
    if (r < 0) ((r + n) % n).toByte else r.toByte
  }

  def pmod(a: Short, n: Short): Short = {
    val r = a % n
    if (r < 0) ((r + n) % n).toShort else r.toShort
  }

  def pmod(a: Float, n: Float): Float = {
    val r = a % n
    if (r < 0) (r + n) % n else r
  }

  def pmod(a: Double, n: Double): Double = {
    val r = a % n
    if (r < 0) (r + n) % n else r
  }

  // Greatest common divisor of two longs, computed with the Euclidean algorithm. The result is
  // always non-negative, and `gcd(0, 0)` is 0. The only unrepresentable result is `-Long.MinValue`,
  // reached by `(0, x)`, `(x, 0)` and `(x, x)` for `x == Long.MinValue`; as elsewhere in Spark that
  // overflow raises under ANSI mode and yields null otherwise. Shared by `Gcd`'s eval and codegen
  // paths so the two never diverge.
  def gcd(a: Long, b: Long, ansiEnabled: Boolean, context: QueryContext): jl.Long = {
    var x = a
    var y = b
    while (y != 0) {
      val remainder = x % y
      x = y
      y = remainder
    }
    // `x` carries the sign of the inputs, so take the absolute value to normalize the result.
    if (x == Long.MinValue) {
      overflowOrNull(ansiEnabled, context)
    } else {
      Math.abs(x)
    }
  }

  // Least common multiple of two longs. Dividing by the greatest common divisor before multiplying
  // keeps the intermediate product as small as possible, so only genuinely unrepresentable results
  // overflow. The result is always non-negative, and is 0 when either input is 0. Shared by `Lcm`'s
  // eval and codegen paths so the two never diverge.
  def lcm(a: Long, b: Long, ansiEnabled: Boolean, context: QueryContext): jl.Long = {
    if (a == 0 || b == 0) {
      jl.Long.valueOf(0L)
    } else {
      val divisor = gcd(a, b, ansiEnabled, context)
      if (divisor == null) {
        null
      } else {
        try {
          Math.multiplyExact(Math.absExact(a / divisor.longValue()), Math.absExact(b))
        } catch {
          case _: ArithmeticException => overflowOrNull(ansiEnabled, context)
        }
      }
    }
  }

  private def overflowOrNull(ansiEnabled: Boolean, context: QueryContext): jl.Long = {
    if (ansiEnabled) {
      throw ExecutionErrors.arithmeticOverflowError("long overflow", context = context)
    } else {
      null
    }
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
