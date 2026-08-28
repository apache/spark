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

package org.apache.spark.sql.catalyst.expressions;

import java.math.BigDecimal;
import java.math.MathContext;

import org.apache.spark.sql.catalyst.util.MathUtils;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * The operators the `java` transpiler (`pyspark.sql.transpile_java`) emits calls to.
 *
 * Every place Python, Java and ANSI SQL disagree about an operator is handled here and nowhere
 * else, so the divergences are in one file a reviewer can read rather than spread through generated
 * source. Generated code calls these by their fully-qualified name; they are not on the codegen
 * default-import list, since this feature is off by default and every generated class in Spark
 * would otherwise carry the import.
 *
 * Three rules hold throughout:
 *
 * <ul>
 *   <li><b>Null propagates through arithmetic, and raises in ordering.</b> Any null operand makes
 *   an arithmetic result null, which is SQL's rule and the Catalyst target's; Python would raise
 *   `TypeError` on `None + 1`, so both transpiled targets diverge from the interpreted UDF there,
 *   and they diverge the same way. Ordering is the opposite: `<`, `<=`, `>`, `>=` RAISE on a null
 *   operand, because returning null would make `if x > 0` quietly take its false branch, and
 *   because the Catalyst target raises there too.
 *
 *   <p>These two targets are not interchangeable in every case, and the transpiler does not claim
 *   they are. `NaN` is the standing example: Spark normalizes it so `NaN = NaN` is true and NaN
 *   sorts greatest, while these helpers use raw IEEE semantics, which is what CPython does. So a
 *   body comparing doubles that can be NaN gives different answers under `catalyst` and under
 *   `java` -- with `java` the closer of the two to the interpreted UDF.</li>
 *   <li><b>Overflow raises.</b> The transpiler targets ANSI mode, so the integral operators use
 *   `Math.*Exact` rather than Java's silently wrapping ones. Python `int` is arbitrary precision
 *   and `long` is not; that remains the documented divergence it already is for the Catalyst
 *   target.</li>
 *   <li><b>Division by zero raises.</b> Python raises `ZeroDivisionError` for both `int` and
 *   `float`. Java agrees for `long` but yields `Infinity`/`NaN` for `double`, so the fractional
 *   operators check explicitly.</li>
 * </ul>
 */
public final class TranspiledJavaUDFHelpers {

  private TranspiledJavaUDFHelpers() {}

  // Beyond this a long is no longer exactly representable as a double, so a conversion to double
  // stops being lossless and `divideLong` has to take its slow path.
  private static final long EXACT_DOUBLE_BOUND = 1L << 53;

  // ---------------------------------------------------------------------------------------------
  // Integral arithmetic (Python `int`, lowered to `Long`)
  // ---------------------------------------------------------------------------------------------

  // Through MathUtils rather than Math.*Exact directly, so an overflow here carries the same
  // [ARITHMETIC_OVERFLOW] error class and SQLSTATE the Catalyst target's lowering of the same
  // expression produces. A bare java.lang.ArithmeticException would make the failure message
  // depend on which target happened to lower the UDF.
  public static Long addLong(Long a, Long b) {
    if (a == null || b == null) return null;
    return MathUtils.addExact(a.longValue(), b.longValue());
  }

  public static Long subtractLong(Long a, Long b) {
    if (a == null || b == null) return null;
    return MathUtils.subtractExact(a.longValue(), b.longValue());
  }

  public static Long multiplyLong(Long a, Long b) {
    if (a == null || b == null) return null;
    return MathUtils.multiplyExact(a.longValue(), b.longValue());
  }

  public static Long negateLong(Long a) {
    if (a == null) return null;
    // `-Long.MIN_VALUE` has no `long` representation and plain negation returns MIN_VALUE itself.
    return MathUtils.negateExact(a.longValue());
  }

  /**
   * Python's `//`. `Math.floorDiv` matches it exactly, including for mixed signs -- verified
   * against CPython: `-7 // 3 == -3` and `7 // -3 == -3`, where Java's own `/` gives -2.
   */
  public static Long floorDivideLong(Long a, Long b) {
    if (a == null || b == null) return null;
    // Checked before MathUtils.floorDiv: it maps every ArithmeticException from Math.floorDiv to
    // [ARITHMETIC_OVERFLOW], which would mislabel a zero divisor.
    if (b == 0L) throw TranspiledJavaUDFErrors.divideByZero();
    // `a // -1` is exactly `-a`, and routing it that way is not a shortcut but the fix for the one
    // input where floorDiv is wrong: `Math.floorDiv(Long.MIN_VALUE, -1)` returns MIN_VALUE itself
    // rather than throwing (verified), so MathUtils has no exception to translate and the overflow
    // would pass through silently. Python gives 2**63 here, which no long holds.
    if (b == -1L) return MathUtils.negateExact(a.longValue());
    return MathUtils.floorDiv(a.longValue(), b.longValue());
  }

  /**
   * Python's `%`. `Math.floorMod` matches it exactly -- verified against CPython: `-7 % 3 == 2`
   * and `7 % -3 == -2`, where Java's own `%` gives -1 and 1. This is the operator the Catalyst
   * target has to reach with `sign(b) * pmod(sign(b) * a, abs(b))`.
   */
  public static Long modLong(Long a, Long b) {
    if (a == null || b == null) return null;
    if (b == 0L) throw TranspiledJavaUDFErrors.remainderByZero();
    return MathUtils.floorMod(a.longValue(), b.longValue());
  }

  /**
   * Python's `/` on two ints, which yields a float: `5 / 2 == 2.5`, not 2.
   *
   * CPython divides the exact integer quotient and rounds once. Converting each operand to double
   * first would round twice, and the two disagree: `9007199254740993 / 3` is 3002399751580331.0 in
   * Python and 3002399751580330.5 through doubles. So the double path is taken only when both
   * operands are exactly representable, where IEEE division is itself correctly rounded and the two
   * agree by construction; beyond that the quotient is computed in BigDecimal.
   */
  public static Double divideLong(Long a, Long b) {
    if (a == null || b == null) return null;
    if (b == 0L) throw TranspiledJavaUDFErrors.divideByZero();
    long x = a, y = b;
    // `Math.abs(Long.MIN_VALUE)` is MIN_VALUE, i.e. negative, so MIN_VALUE passes this guard by
    // accident rather than by test. It is nonetheless in the right branch: -2^63 is a power of two
    // and so exactly representable as a double, which is the property the branch actually needs.
    // Spelled out because the coincidence is load-bearing -- a swept 465k pairs including
    // MIN_VALUE on both sides matched CPython exactly, and nothing here should be "fixed" without
    // knowing why it works.
    if (Math.abs(x) <= EXACT_DOUBLE_BOUND && Math.abs(y) <= EXACT_DOUBLE_BOUND) {
      return ((double) x) / ((double) y);
    }
    // Exact first, and only then a bounded precision. `doubleValue()` rounds, so handing it an
    // already-rounded quotient rounds twice, and the two roundings disagree exactly when the true
    // quotient sits on a tie -- 14410791856275153 / -4611686018427387904 came out 1 ULP from
    // CPython that way, and over half of the 54-significant-bit-numerator / power-of-two-divisor
    // pairs did.
    //
    // Trying UNLIMITED first is not a heuristic: a tie needs the quotient to be exactly
    // representable in 54 bits, so it is dyadic, so its decimal expansion terminates and the exact
    // divide succeeds. Whatever reaches the fallback therefore cannot be a tie, and DECIMAL128's
    // 34 digits round it correctly.
    try {
      return new BigDecimal(x).divide(new BigDecimal(y)).doubleValue();
    } catch (ArithmeticException nonTerminating) {
      return new BigDecimal(x).divide(new BigDecimal(y), MathContext.DECIMAL128).doubleValue();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Fractional arithmetic (Python `float`, lowered to `Double`)
  // ---------------------------------------------------------------------------------------------

  // No `*Exact` counterparts: Python floats are C doubles and overflow to `inf` there too, so the
  // plain operators already agree.

  public static Double addDouble(Double a, Double b) {
    if (a == null || b == null) return null;
    return a + b;
  }

  public static Double subtractDouble(Double a, Double b) {
    if (a == null || b == null) return null;
    return a - b;
  }

  public static Double multiplyDouble(Double a, Double b) {
    if (a == null || b == null) return null;
    return a * b;
  }

  public static Double negateDouble(Double a) {
    if (a == null) return null;
    return -a;
  }

  public static Double divideDouble(Double a, Double b) {
    if (a == null || b == null) return null;
    // Java would return Infinity or NaN; Python raises ZeroDivisionError.
    if (b == 0.0d) throw TranspiledJavaUDFErrors.divideByZero();
    return a / b;
  }

  /**
   * Python's `//` on floats, which stays a float: `7.0 // 2.0 == 3.0`.
   *
   * `Math.floor(a / b)` is NOT this. It rounds the quotient before flooring, so it is off by one
   * once the quotient is inexact (`9007199254740994.0 // 3.0`) and wrong for an infinite operand
   * (`1.0 // inf` is -0.0 through floor but 0.0 in Python). See [[divmod]].
   */
  public static Double floorDivideDouble(Double a, Double b) {
    if (a == null || b == null) return null;
    if (b == 0.0d) throw TranspiledJavaUDFErrors.divideByZero();
    return divmod(a, b)[0];
  }

  /**
   * Python's `%` on floats, which takes the divisor's sign: `-7.0 % 3.0 == 2.0`, where Java's `%`
   * gives -1.0.
   *
   * Correcting the sign is necessary but not sufficient. `a - b * Math.floor(a / b)` gets the sign
   * right and the value wrong, because it rounds three times: `5.5 % 1.1` is 1.0999999999999996 in
   * Python and 0.0 that way. A 200k-pair sweep disagreed on 97% of inputs. See [[divmod]].
   */
  public static Double modDouble(Double a, Double b) {
    if (a == null || b == null) return null;
    if (b == 0.0d) throw TranspiledJavaUDFErrors.remainderByZero();
    return divmod(a, b)[1];
  }

  /**
   * `{floordiv, mod}` for two finite-or-infinite doubles, following CPython's `float_divmod`
   * (Objects/floatobject.c) statement for statement.
   *
   * The starting point is `%`, which in Java IS C's `fmod` and so is exact -- no rounding to undo.
   * The quotient is then recovered from the exact remainder rather than computed independently,
   * which is what keeps the two consistent with each other and with Python. Verified against
   * CPython over the sign cases, the inexact cases, the values past 2^53, and both infinities.
   */
  private static double[] divmod(double vx, double wx) {
    double mod = vx % wx;
    double div = (vx - mod) / wx;
    if (mod != 0.0d) {
      if ((wx < 0.0d) != (mod < 0.0d)) {
        mod += wx;
        div -= 1.0d;
      }
    } else {
      // Python hands back a zero carrying the divisor's sign, so `-1.0 % -1.0` is -0.0.
      mod = Math.copySign(0.0d, wx);
    }
    double floordiv;
    if (div != 0.0d) {
      floordiv = Math.floor(div);
      if (div - floordiv > 0.5d) {
        floordiv += 1.0d;
      }
    } else {
      floordiv = Math.copySign(0.0d, vx / wx);
    }
    return new double[] {floordiv, mod};
  }

  /** Python's int-to-float promotion, for an expression mixing the two categories. */
  public static Double toDouble(Long a) {
    if (a == null) return null;
    return (double) a;
  }

  // ---------------------------------------------------------------------------------------------
  // Text (Python `str`, lowered to `UTF8String`)
  // ---------------------------------------------------------------------------------------------

  /** Python's `+` on text. `UTF8String.concat` already returns null for a null input. */
  public static UTF8String concat(UTF8String a, UTF8String b) {
    if (a == null || b == null) return null;
    return UTF8String.concat(a, b);
  }

  /**
   * Python's `*` on text. A count of zero or less gives an empty string, which is what
   * `UTF8String.repeat` does and what Python does.
   *
   * The count is a `Long` because the transpiler only lowers this when the count is statically
   * integral -- which is also how it avoids the Catalyst target's documented divergence, where a
   * fractional count arriving from a column gets truncated by a cast rather than raising the way
   * Python does.
   */
  public static UTF8String repeat(UTF8String a, Long times) {
    if (a == null || times == null) return null;
    if (times <= 0L) return UTF8String.EMPTY_UTF8;
    // The PRODUCT, not just the count. `UTF8String.repeat` sizes its array with
    // `Math.multiplyExact(numBytes, times)`, which throws a bare java.lang.ArithmeticException --
    // no error class, no SQLSTATE -- so bounding only `times` let `"abcd" * 600000000` past the
    // guard and failed with the one shape of error this file exists to avoid.
    if ((long) a.numBytes() * times > Integer.MAX_VALUE) {
      throw TranspiledJavaUDFErrors.arithmeticOverflow("integer overflow");
    }
    return a.repeat(times.intValue());
  }

  // ---------------------------------------------------------------------------------------------
  // Comparison -- null-propagating, so the result is a nullable Boolean
  // ---------------------------------------------------------------------------------------------

  /**
   * Equality is null-SAFE, not null-propagating, because that is what Python does.
   *
   * `None == None` is True and `None == 1` is False -- definite booleans, never `None`. SQL's `=`
   * would give NULL for both, which round-trips out of the UDF as `None` where Python produced a
   * bool: `x != 1` over a NULL column returns NULL instead of True, and `if x != 1` then takes the
   * wrong branch. So these implement SQL's `<=>` (`IS NOT DISTINCT FROM`), which coincides with
   * Python's `==` exactly. The Catalyst target reaches the same semantics by hand, with four `when`
   * branches in `_lower_eq`.
   *
   * Because the result is never null, `!=` can be the plain negation of `==`.
   */
  public static Boolean equalsLong(Long a, Long b) {
    if (a == null || b == null) return a == null && b == null;
    return a.longValue() == b.longValue();
  }

  /**
   * Ordering raises on a null operand rather than returning null.
   *
   * Python raises `TypeError` for `None > 0`, and the Catalyst target reproduces that by wrapping
   * every ordering comparison in `when(isNull, raise_error(...))` (see
   * `CatalystTranspiler._lower_value_compare`). Returning SQL's NULL here instead would make
   * `if x > 0` quietly take its false branch and hand back a definite, plausible, wrong value --
   * and it would make the two targets disagree, which is the one thing this file is trying to
   * avoid. An `is not None` guard around the comparison keeps it from being reached, exactly as on
   * the Catalyst target.
   */
  private static void requireNonNullForOrdering(Object a, Object b, String op) {
    if (a == null || b == null) {
      throw TranspiledJavaUDFErrors.nullComparison(op);
    }
  }

  public static Boolean lessThanLong(Long a, Long b) {
    requireNonNullForOrdering(a, b, "<");
    return a < b;
  }

  public static Boolean lessThanOrEqualLong(Long a, Long b) {
    requireNonNullForOrdering(a, b, "<=");
    return a <= b;
  }

  public static Boolean greaterThanLong(Long a, Long b) {
    requireNonNullForOrdering(a, b, ">");
    return a > b;
  }

  public static Boolean greaterThanOrEqualLong(Long a, Long b) {
    requireNonNullForOrdering(a, b, ">=");
    return a >= b;
  }

  public static Boolean equalsDouble(Double a, Double b) {
    if (a == null || b == null) return a == null && b == null;
    return a.doubleValue() == b.doubleValue();
  }

  public static Boolean lessThanDouble(Double a, Double b) {
    requireNonNullForOrdering(a, b, "<");
    return a < b;
  }

  public static Boolean lessThanOrEqualDouble(Double a, Double b) {
    requireNonNullForOrdering(a, b, "<=");
    return a <= b;
  }

  public static Boolean greaterThanDouble(Double a, Double b) {
    requireNonNullForOrdering(a, b, ">");
    return a > b;
  }

  public static Boolean greaterThanOrEqualDouble(Double a, Double b) {
    requireNonNullForOrdering(a, b, ">=");
    return a >= b;
  }

  /**
   * Byte order, which for UTF-8 is codepoint order and so is Python's. `binaryCompare` rather
   * than `compareTo`, which Spark disables outright, and rather than `semanticCompare`, which
   * would apply collation rules: the transpiler only lowers text under UTF8_BINARY, where
   * Python's codepoint comparison and Spark's agree, and declines any other collation.
   */
  public static Boolean equalsString(UTF8String a, UTF8String b) {
    if (a == null || b == null) return a == null && b == null;
    return a.equals(b);
  }

  public static Boolean lessThanString(UTF8String a, UTF8String b) {
    requireNonNullForOrdering(a, b, "<");
    return a.binaryCompare(b) < 0;
  }

  public static Boolean lessThanOrEqualString(UTF8String a, UTF8String b) {
    requireNonNullForOrdering(a, b, "<=");
    return a.binaryCompare(b) <= 0;
  }

  public static Boolean greaterThanString(UTF8String a, UTF8String b) {
    requireNonNullForOrdering(a, b, ">");
    return a.binaryCompare(b) > 0;
  }

  public static Boolean greaterThanOrEqualString(UTF8String a, UTF8String b) {
    requireNonNullForOrdering(a, b, ">=");
    return a.binaryCompare(b) >= 0;
  }

  public static Boolean equalsBoolean(Boolean a, Boolean b) {
    if (a == null || b == null) return a == null && b == null;
    return a.booleanValue() == b.booleanValue();
  }

  // ---------------------------------------------------------------------------------------------
  // Three-valued logic
  // ---------------------------------------------------------------------------------------------

  // No `and` / `or` here on purpose. The transpiler refuses `ast.BoolOp` (see
  // `transpile_java.py`), so nothing would call them -- and SQL's three-valued `AND`/`OR` are not
  // what the eventual lowering needs anyway: Python short-circuits and yields an OPERAND, so
  // `False and (a // 0)` is False without evaluating the right side, which a helper taking both
  // arguments cannot express. Whoever lowers `and`/`or` should hoist the right operand behind an
  // `if` rather than reach for a helper.

  /** SQL `NOT`: null stays null. Used for `!=`, which negates the null-safe `==`. */
  public static Boolean not(Boolean a) {
    if (a == null) return null;
    return !a;
  }

  /**
   * Whether a condition should take its branch. A null condition does not, which is what Catalyst's
   * `If` does with an unknown predicate.
   */
  public static boolean isTrue(Boolean a) {
    return Boolean.TRUE.equals(a);
  }
}
