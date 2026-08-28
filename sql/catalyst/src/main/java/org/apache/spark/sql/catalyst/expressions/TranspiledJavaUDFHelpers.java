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
 * Operators the `java` transpiler emits. Every Python/Java/ANSI disagreement
 * lives here; generated code calls these by FQCN.
 *
 * <ul>
 *   <li>Null propagates through arithmetic (SQL, same as Catalyst) and raises
 *   in ordering (so `if x > 0` cannot quietly take the false branch).</li>
 *   <li>Overflow raises (ANSI / `Math.*Exact`). Python `int` is arbitrary
 *   precision; that is the same documented divergence Catalyst has.</li>
 *   <li>Division by zero raises for both int and float. Java would give
 *   Infinity/NaN for double.</li>
 *   <li>`NaN` is raw IEEE (CPython), not Spark's normalised `NaN == NaN`.</li>
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

  // MathUtils, not Math.*Exact: same [ARITHMETIC_OVERFLOW] as the Catalyst target.
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
    return MathUtils.negateExact(a.longValue());
  }

  /**
   * Python's `//`. `Math.floorDiv` matches it, including mixed signs
   * (`-7 // 3 == -3`; Java `/` gives -2).
   */
  public static Long floorDivideLong(Long a, Long b) {
    if (a == null || b == null) return null;
    // Before MathUtils.floorDiv, which reports every ArithmeticException as overflow.
    if (b == 0L) throw TranspiledJavaUDFErrors.divideByZero();
    // `Math.floorDiv(Long.MIN_VALUE, -1)` returns MIN_VALUE rather than throwing.
    if (b == -1L) return MathUtils.negateExact(a.longValue());
    return MathUtils.floorDiv(a.longValue(), b.longValue());
  }

  /**
   * Python's `%`. `Math.floorMod` matches it (`-7 % 3 == 2`; Java `%` gives -1).
   * Catalyst reaches this with `sign(b) * pmod(sign(b) * a, abs(b))`.
   */
  public static Long modLong(Long a, Long b) {
    if (a == null || b == null) return null;
    if (b == 0L) throw TranspiledJavaUDFErrors.remainderByZero();
    return MathUtils.floorMod(a.longValue(), b.longValue());
  }

  /**
   * Python's `/` on two ints: `5 / 2 == 2.5`. CPython divides the exact
   * quotient and rounds once; widening to double first rounds twice
   * (`9007199254740993 / 3` is 3002399751580331.0 vs 3002399751580330.5).
   */
  public static Double divideLong(Long a, Long b) {
    if (a == null || b == null) return null;
    if (b == 0L) throw TranspiledJavaUDFErrors.divideByZero();
    long x = a, y = b;
    // `Math.abs(Long.MIN_VALUE)` is negative, so MIN_VALUE passes this
    // guard by accident. It is still the right branch: -2^63 is a power of
    // two and exactly representable as a double. Sweep of 465k pairs
    // including MIN_VALUE on both sides matched CPython; do not "fix".
    if (Math.abs(x) <= EXACT_DOUBLE_BOUND && Math.abs(y) <= EXACT_DOUBLE_BOUND) {
      return ((double) x) / ((double) y);
    }
    // Exact first. A tie is dyadic so the exact divide succeeds; whatever
    // reaches DECIMAL128 cannot be a tie. Rounding an already-rounded
    // quotient is 1 ULP out on
    // 14410791856275153 / -4611686018427387904.
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
   * Python's `//` on floats (`7.0 // 2.0 == 3.0`). Not `Math.floor(a / b)`:
   * that is off by one on inexact quotients and wrong for inf. See [[divmod]].
   */
  public static Double floorDivideDouble(Double a, Double b) {
    if (a == null || b == null) return null;
    if (b == 0.0d) throw TranspiledJavaUDFErrors.divideByZero();
    return divmod(a, b)[0];
  }

  /**
   * Python's `%` on floats (divisor's sign: `-7.0 % 3.0 == 2.0`). Sign-fixing
   * `a - b * floor(a / b)` is 0.0 for `5.5 % 1.1`. See [[divmod]].
   */
  public static Double modDouble(Double a, Double b) {
    if (a == null || b == null) return null;
    if (b == 0.0d) throw TranspiledJavaUDFErrors.remainderByZero();
    return divmod(a, b)[1];
  }

  /**
   * `{floordiv, mod}` following CPython's `float_divmod` (Objects/floatobject.c).
   * Java `%` is C `fmod` (exact); the quotient is recovered from that remainder.
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
   * Python's `*` on text. Count is `Long` because a fractional one is refused
   * rather than truncated (Catalyst's documented divergence).
   */
  public static UTF8String repeat(UTF8String a, Long times) {
    if (a == null || times == null) return null;
    if (times <= 0L) return UTF8String.EMPTY_UTF8;
    // The PRODUCT: bounding only `times` let `"abcd" * 600000000` throw a
    // bare ArithmeticException inside `UTF8String.repeat`.
    if ((long) a.numBytes() * times > Integer.MAX_VALUE) {
      throw TranspiledJavaUDFErrors.arithmeticOverflow("integer overflow");
    }
    return a.repeat(times.intValue());
  }

  // ---------------------------------------------------------------------------------------------
  // Comparison -- null-propagating, so the result is a nullable Boolean
  // ---------------------------------------------------------------------------------------------

  /**
   * Null-safe equality: Python's `==`, SQL's `<=>`. `None == None` is True;
   * SQL `=` would be NULL and `if x != 1` would take the wrong branch.
   */
  public static Boolean equalsLong(Long a, Long b) {
    if (a == null || b == null) return a == null && b == null;
    return a.longValue() == b.longValue();
  }

  /** Raise on null: returning NULL would make `if x > 0` take the false branch. */
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

  /** Byte / codepoint order. `binaryCompare`, not `compareTo` (disabled) or collation. */
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

  // No `and` / `or`: a helper cannot short-circuit. Hoist the right operand
  // behind an `if` when that lands (SPARK-55209 follow-up).

  /** SQL `NOT`: null stays null. Used for `!=`. */
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
