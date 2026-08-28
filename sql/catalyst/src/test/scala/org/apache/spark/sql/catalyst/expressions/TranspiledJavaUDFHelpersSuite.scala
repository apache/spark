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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.{SparkArithmeticException, SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.expressions.{TranspiledJavaUDFHelpers => H}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Pins the operators the `java` transpiler emits, one assertion per point where Python, Java
 * and ANSI SQL disagree. Everything here is also reachable through the Python suites, but a
 * failure there says "a query returned the wrong number" while a failure here says which
 * operator, which is the difference between a minute and an afternoon.
 *
 * The expected values are CPython's, checked against a real interpreter rather than reasoned
 * out.
 */
class TranspiledJavaUDFHelpersSuite extends SparkFunSuite {

  private def utf8(s: String): UTF8String = UTF8String.fromString(s)

  test("// and % follow Python's signs, not Java's") {
    // Python: -7 // 3 == -3 and 7 // -3 == -3. Java's `/` gives -2 for both.
    assert(H.floorDivideLong(-7L, 3L) === -3L)
    assert(H.floorDivideLong(7L, -3L) === -3L)
    assert(H.floorDivideLong(7L, 3L) === 2L)
    // Python: -7 % 3 == 2 and 7 % -3 == -2. Java's `%` gives -1 and 1.
    assert(H.modLong(-7L, 3L) === 2L)
    assert(H.modLong(7L, -3L) === -2L)
    assert(H.modLong(7L, 3L) === 1L)
  }

  test("float % follows the divisor's sign") {
    // Python: -7.0 % 3.0 == 2.0, where Java's `%` gives -1.0.
    assert(H.modDouble(-7.0d, 3.0d) === 2.0d)
    assert(H.modDouble(7.0d, -3.0d) === -2.0d)
    assert(H.floorDivideDouble(7.0d, 2.0d) === 3.0d)
    assert(H.floorDivideDouble(-7.0d, 2.0d) === -4.0d)
  }

  test("float % and // are exact, not just correctly signed") {
    // Getting the sign right is the easy half. `a - b * Math.floor(a / b)` rounds three times and
    // is wrong for almost every inexact pair -- it returns 0.0 for the first case below. Every
    // expected value here is CPython's.
    assert(H.modDouble(5.5d, 1.1d) === 1.0999999999999996d)
    assert(H.modDouble(100.0d, 3.3d) === 1.0000000000000053d)
    assert(H.modDouble(0.7d, 0.1d) === 0.09999999999999992d)
    assert(H.modDouble(1.0e16d, 3.0d) === 1.0d)
    // Past 2^53 the quotient is inexact, so flooring it is off by one.
    assert(H.floorDivideDouble(9007199254740994.0d, 3.0d) === 3002399751580330.0d)
    assert(H.modDouble(9007199254740994.0d, 3.0d) === 1.0d)
  }

  test("an infinite operand behaves as it does in Python") {
    // A DoubleType column can hold Infinity -- it is what 1e308 * 10 produces -- and
    // `Math.floor(a / b)` gets every one of these wrong.
    assert(H.modDouble(1.0d, Double.PositiveInfinity) === 1.0d)
    assert(H.floorDivideDouble(1.0d, Double.PositiveInfinity) === 0.0d)
    assert(H.modDouble(-1.0d, Double.PositiveInfinity) === Double.PositiveInfinity)
    assert(H.floorDivideDouble(-1.0d, Double.PositiveInfinity) === -1.0d)
    assert(H.modDouble(1.0d, Double.NegativeInfinity) === Double.NegativeInfinity)
    assert(H.floorDivideDouble(1.0d, Double.NegativeInfinity) === -1.0d)
    assert(H.floorDivideDouble(Double.PositiveInfinity, 2.0d).isNaN)
  }

  test("/ on two ints is float division") {
    // Python: 5 / 2 == 2.5, not 2.
    assert(H.divideLong(5L, 2L) === 2.5d)
    assert(H.divideLong(-5L, 2L) === -2.5d)
  }

  test("/ on large ints rounds once, as CPython's exact-quotient division does") {
    // Widening both operands to double first rounds twice and lands a whole ULP away. Expected
    // values are CPython's `a / b` on ints, which is exact-rational then correctly rounded.
    assert(H.divideLong(9007199254740993L, 3L) === 3002399751580331.0d)
    assert(H.divideLong(-228370678136506164L, 2709003016019L) === -84300.63635444267d)
    // Still exact where both operands are representable, which is the fast path.
    assert(H.divideLong(1L << 53, 2L) === 4503599627370496.0d)
    // A quotient sitting exactly on a tie, which needs a dyadic value and so a terminating
    // expansion: rounding to 34 digits and then to a double rounds twice and lands 1 ULP out, so
    // the exact divide has to be tried first. Compared as bits, because the two candidates are
    // adjacent doubles and `===` on the wrong one would still read as "close enough". CPython's
    // answer for this pair, and for 22k others swept alongside it.
    assert(java.lang.Double.doubleToRawLongBits(
      H.divideLong(14410791856275153L, -4611686018427387904L)) === 0xbf699944f8c33f68L)
  }

  test("division by zero raises for both categories") {
    // Java gives Infinity for the double case; Python raises for both.
    Seq[() => Any](
      () => H.divideLong(1L, 0L),
      () => H.modLong(1L, 0L),
      () => H.floorDivideLong(1L, 0L),
      () => H.divideDouble(1.0d, 0.0d),
      () => H.modDouble(1.0d, 0.0d),
      () => H.floorDivideDouble(1.0d, 0.0d)
    ).foreach { op =>
      intercept[ArithmeticException](op())
    }
  }

  test("integral overflow raises rather than wrapping") {
    intercept[ArithmeticException](H.addLong(Long.MaxValue, 1L))
    intercept[ArithmeticException](H.subtractLong(Long.MinValue, 1L))
    intercept[ArithmeticException](H.multiplyLong(Long.MaxValue, 2L))
    intercept[ArithmeticException](H.negateLong(Long.MinValue))
    // The one floorDiv overflow, which Math.floorDiv wraps through silently.
    intercept[ArithmeticException](H.floorDivideLong(Long.MinValue, -1L))
    // Guarded before MathUtils.floorDiv, which would report a zero divisor as an overflow.
    assert(intercept[SparkArithmeticException](H.floorDivideLong(1L, 0L)).getCondition
      === "DIVIDE_BY_ZERO")
  }

  test("float arithmetic overflows to infinity, as Python's does") {
    assert(H.multiplyDouble(1.0e308d, 10.0d) === Double.PositiveInfinity)
  }

  test("null propagates through arithmetic") {
    assert(H.addLong(null, 1L) === null)
    assert(H.addLong(1L, null) === null)
    assert(H.multiplyDouble(null, 1.0d) === null)
    assert(H.concat(null, utf8("a")) === null)
    assert(H.repeat(utf8("a"), null) === null)
    assert(H.negateLong(null) === null)
    assert(H.toDouble(null) === null)
  }

  test("equality is null-safe, because Python's is") {
    // `None == None` is True and `None == 1` is False in Python -- definite booleans, never None.
    // SQL's `=` would answer NULL to both, which comes back out of the UDF as `None` where Python
    // produced a bool: `x != 1` over a NULL column would be NULL instead of True. These are SQL's
    // `<=>`, which coincides with Python exactly, and the Catalyst target hand-rolls the same four
    // cases in `_lower_eq`.
    assert(H.equalsLong(null, null) === true)
    assert(H.equalsLong(null, 1L) === false)
    assert(H.equalsLong(1L, null) === false)
    assert(H.equalsLong(1L, 1L) === true)
    assert(H.equalsDouble(null, null) === true)
    assert(H.equalsDouble(null, 1.0d) === false)
    assert(H.equalsString(null, null) === true)
    assert(H.equalsString(null, utf8("a")) === false)
    assert(H.equalsBoolean(null, null) === true)
    assert(H.equalsBoolean(null, true) === false)
    // And so `!=`, which the transpiler lowers as `not(equals)`, is never NULL either.
    assert(H.not(H.equalsLong(null, 1L)) === true)
    assert(H.not(H.equalsLong(null, null)) === false)
  }

  test("repeat's overflow carries an error class like every other failure here") {
    assert(intercept[SparkArithmeticException](
      H.repeat(utf8("ab"), Int.MaxValue.toLong + 1L)).getCondition === "ARITHMETIC_OVERFLOW")
    // The PRODUCT, not just the count: 4 bytes x 6e8 overflows an int inside UTF8String.repeat,
    // which throws a bare ArithmeticException with no error class. A count under Int.MaxValue is
    // not on its own safe.
    assert(intercept[SparkArithmeticException](
      H.repeat(utf8("abcd"), 600000000L)).getCondition === "ARITHMETIC_OVERFLOW")
  }

  test("null in an ordering comparison raises rather than propagating") {
    // Returning NULL would make `if x > 0` take its false branch and hand back a confident wrong
    // answer. Python raises TypeError; the Catalyst target raises via `raise_error`, i.e.
    // USER_RAISED_EXCEPTION, so this matches its error class as well as its behaviour.
    Seq[() => Any](
      () => H.lessThanLong(null, 1L),
      () => H.lessThanOrEqualLong(1L, null),
      () => H.greaterThanLong(null, 1L),
      () => H.greaterThanOrEqualLong(1L, null),
      () => H.lessThanDouble(null, 1.0d),
      () => H.greaterThanDouble(1.0d, null),
      () => H.lessThanString(null, utf8("a")),
      () => H.greaterThanString(utf8("a"), null)
    ).foreach { op =>
      val e = intercept[SparkRuntimeException](op())
      assert(e.getCondition === "USER_RAISED_EXCEPTION")
      assert(e.getMessage.contains("cannot compare NULL"))
    }
  }

  test("the ordering helpers each compare their own operands, in order") {
    // `>` is its own helper rather than `<` with the operands swapped: swapping is correct for the
    // result but reverses which side Java evaluates first, so the wrong operand's exception wins.
    assert(H.greaterThanLong(2L, 1L) === true)
    assert(H.greaterThanLong(1L, 2L) === false)
    assert(H.greaterThanOrEqualLong(2L, 2L) === true)
    assert(H.greaterThanDouble(2.0d, 1.5d) === true)
    assert(H.greaterThanOrEqualDouble(1.5d, 1.5d) === true)
    assert(H.greaterThanString(utf8("b"), utf8("a")) === true)
    assert(H.greaterThanOrEqualString(utf8("a"), utf8("a")) === true)
  }

  test("arithmetic failures carry Spark's error classes, not bare ArithmeticException") {
    // The same overflow through the Catalyst target is [ARITHMETIC_OVERFLOW]; a bare
    // java.lang.ArithmeticException here would make the error class depend on which target lowered
    // the UDF.
    assert(intercept[SparkArithmeticException](H.addLong(Long.MaxValue, 1L)).getCondition
      === "ARITHMETIC_OVERFLOW")
    assert(intercept[SparkArithmeticException](H.divideLong(1L, 0L)).getCondition
      === "DIVIDE_BY_ZERO")
    assert(intercept[SparkArithmeticException](H.modLong(1L, 0L)).getCondition
      === "REMAINDER_BY_ZERO")
    assert(intercept[SparkArithmeticException](H.floorDivideLong(1L, 0L)).getCondition
      === "DIVIDE_BY_ZERO")
    assert(intercept[SparkArithmeticException](H.divideDouble(1.0d, 0.0d)).getCondition
      === "DIVIDE_BY_ZERO")
    assert(intercept[SparkArithmeticException](H.modDouble(1.0d, 0.0d)).getCondition
      === "REMAINDER_BY_ZERO")
  }

  test("text concat and repeat match Python") {
    assert(H.concat(utf8("ab"), utf8("cd")) === utf8("abcd"))
    assert(H.repeat(utf8("ab"), 3L) === utf8("ababab"))
    // Python: "ab" * 0 and "ab" * -1 are both "".
    assert(H.repeat(utf8("ab"), 0L) === utf8(""))
    assert(H.repeat(utf8("ab"), -1L) === utf8(""))
    // A count no Java array could hold raises rather than silently truncating to an int; the
    // error class it carries is pinned separately.
    intercept[ArithmeticException](H.repeat(utf8("ab"), Int.MaxValue.toLong + 1L))
  }

  test("comparison helpers order by value and by codepoint") {
    assert(H.lessThanLong(1L, 2L) === true)
    assert(H.lessThanOrEqualLong(2L, 2L) === true)
    assert(H.equalsLong(2L, 2L) === true)
    assert(H.lessThanDouble(1.5d, 2.0d) === true)
    assert(H.equalsDouble(2.0d, 2.0d) === true)
    assert(H.lessThanString(utf8("a"), utf8("b")) === true)
    // Codepoint order, which is Python's: uppercase sorts before lowercase.
    assert(H.lessThanString(utf8("Z"), utf8("a")) === true)
    assert(H.equalsString(utf8("abc"), utf8("ABC")) === false)
    assert(H.equalsBoolean(true, true) === true)
  }

  test("not keeps null, and is the only boolean connective here") {
    // `and` / `or` were removed: the transpiler refuses `ast.BoolOp`, so nothing called them, and
    // their SQL three-valued semantics are not what a short-circuiting lowering will want.
    assert(H.not(null) === null)
    assert(H.not(true) === false)
    assert(H.not(false) === true)
  }

  test("a null condition does not take its branch") {
    // Matching Catalyst's `If` with an unknown predicate.
    assert(!H.isTrue(null))
    assert(!H.isTrue(false))
    assert(H.isTrue(true))
  }
}
