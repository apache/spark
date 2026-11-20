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
import org.apache.spark.sql.types.Decimal

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

  /**
   * `Math.rint` with a canonical NaN result. The bits of a NaN that `Math.rint` returns depend on
   * the JIT tier: the interpreter and C1 run `StrictMath.rint`, which clears the sign, while C2's
   * intrinsic keeps it. Every NaN result becomes `Double.NaN`, which also drops a NaN payload;
   * every other result is `Math.rint`'s, bit for bit.
   */
  def rint(a: Double): Double = {
    val r = Math.rint(a)
    if (java.lang.Double.isNaN(r)) Double.NaN else r
  }

  // Positive modulo (`pmod`): for `n > 0` the result lies in `[0, n)` (for the float/double
  // overloads, when both inputs are finite); for integral `n < 0` it takes the sign of the
  // dividend `a` (unlike `floorMod`, which takes the sign of `n`). Matches the `pmod` SQL
  // function and `HashPartitioning`, and is shared by `Pmod`'s eval and codegen so the two never
  // diverge.
  //
  // Integral `n > 0` computes `Math.floorMod(a, n)` with a branchless sign fix-up instead of
  // calling it, because `Math.floorMod` adjusts with a data-dependent conditional (SPARK-59696).
  // The `n < 0` path can silently wrap for `Int`/`Long` when `r + n` overflows (large-magnitude
  // negative `n`), e.g. `pmod(-1, Int.MinValue)` returns `Int.MaxValue`; tracked by SPARK-59934.
  // `Byte`/`Short` delegate to `Int` (too narrow for `r + n` to overflow); the float/double
  // overloads keep `% n` because `r + n` can round up to exactly `n`.

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

  // Python-style modulo, used by `pandas_mod` for pandas API on Spark: the result takes the sign
  // of the divisor `n`, as Python's and NumPy's `%` do, and a floating zero result is a zero with
  // the sign of `n`. This is CPython's algorithm: add `n` to a nonzero remainder whose sign
  // differs from `n`'s. That cannot overflow, because the remainder is smaller than `n` in
  // magnitude. `n` must not be zero. Shared by `PandasMod`'s eval and codegen.

  def pythonMod(a: Int, n: Int): Int = {
    val r = a % n
    if (r != 0 && (r ^ n) < 0) r + n else r
  }

  def pythonMod(a: Long, n: Long): Long = {
    val r = a % n
    if (r != 0 && (r ^ n) < 0) r + n else r
  }

  def pythonMod(a: Byte, n: Byte): Byte = pythonMod(a.toInt, n.toInt).toByte

  def pythonMod(a: Short, n: Short): Short = pythonMod(a.toInt, n.toInt).toShort

  def pythonMod(a: Float, n: Float): Float = {
    val r = a % n
    if (r == 0) Math.copySign(0.0f, n) else if ((r < 0) != (n < 0)) r + n else r
  }

  def pythonMod(a: Double, n: Double): Double = {
    val r = a % n
    if (r == 0) Math.copySign(0.0, n) else if ((r < 0) != (n < 0)) r + n else r
  }

  def pythonMod(a: Decimal, n: Decimal): Decimal = {
    val r = a % n
    if (!r.isZero && (r.compare(Decimal.ZERO) < 0) != (n.compare(Decimal.ZERO) < 0)) r + n else r
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
