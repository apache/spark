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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class TryEvalSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("try_add") {
    Seq(
      (1, 1, 2),
      (Int.MaxValue, 1, null),
      (Int.MinValue, -1, null)
    ).foreach { case (a, b, expected) =>
      val left = Literal(a)
      val right = Literal(b)
      val input = Add(left, right, EvalMode.TRY)
      checkEvaluation(input, expected)
    }
  }

  test("try_divide") {
    Seq(
      (3.0, 2.0, 1.5),
      (1.0, 0.0, null),
      (-1.0, 0.0, null)
    ).foreach { case (a, b, expected) =>
      val left = Literal(a)
      val right = Literal(b)
      val input = Divide(left, right, EvalMode.TRY)
      checkEvaluation(input, expected)
    }
  }

  test("try_mod") {
    Seq(
      (3.0, 2.0, 1.0),
      (1.0, 0.0, null),
      (-1.0, 0.0, null)
    ).foreach { case (a, b, expected) =>
      val left = Literal(a)
      val right = Literal(b)
      val input = Remainder(left, right, EvalMode.TRY)
      checkEvaluation(input, expected)
    }
  }

  test("try_subtract") {
    Seq(
      (1, 1, 0),
      (Int.MaxValue, -1, null),
      (Int.MinValue, 1, null)
    ).foreach { case (a, b, expected) =>
      val left = Literal(a)
      val right = Literal(b)
      val input = Subtract(left, right, EvalMode.TRY)
      checkEvaluation(input, expected)
    }
  }

  test("try_multiply") {
    Seq(
      (2, 3, 6),
      (Int.MaxValue, -10, null),
      (Int.MinValue, 10, null)
    ).foreach { case (a, b, expected) =>
      val left = Literal(a)
      val right = Literal(b)
      val input = Multiply(left, right, EvalMode.TRY)
      checkEvaluation(input, expected)
    }
  }

  test("SPARK-62149: try_abs and try_negative over integral values") {
    // (minimum, maximum) of each integral type, with their literals.
    Seq[(Any, Any, Any => Literal)](
      (Byte.MinValue, Byte.MaxValue, v => Literal(v.asInstanceOf[Byte])),
      (Short.MinValue, Short.MaxValue, v => Literal(v.asInstanceOf[Short])),
      (Int.MinValue, Int.MaxValue, v => Literal(v.asInstanceOf[Int])),
      (Long.MinValue, Long.MaxValue, v => Literal(v.asInstanceOf[Long]))
    ).foreach { case (min, max, lit) =>
      val dataType = lit(min).dataType
      def value(v: Long): Any = Cast(Literal(v), dataType).eval()
      val minPlusOne = value(min.asInstanceOf[Number].longValue + 1)
      Seq(
        (min, null, null),
        (minPlusOne, max, max),
        (max, max, value(-max.asInstanceOf[Number].longValue)),
        (value(-1), value(1), value(1)),
        (value(0), value(0), value(0)),
        (value(1), value(1), value(-1))
      ).foreach { case (input, abs, negated) =>
        checkEvaluation(IntegralAbsOrNull(lit(input)), abs)
        checkEvaluation(IntegralNegateOrNull(lit(input)), negated)
      }
      checkEvaluation(IntegralAbsOrNull(Literal.create(null, dataType)), null)
      checkEvaluation(IntegralNegateOrNull(Literal.create(null, dataType)), null)
      assert(IntegralAbsOrNull(lit(max)).nullable)
      assert(IntegralNegateOrNull(lit(max)).nullable)

      // Under ANSI mode the functions return NULL on overflow, and otherwise they behave as
      // `abs` and `negative` do, which wrap around.
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        checkEvaluation(new TryAbs(lit(min)).replacement, null)
        checkEvaluation(new TryNegative(lit(min)).replacement, null)
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkEvaluation(new TryAbs(lit(min)).replacement, min)
        checkEvaluation(new TryNegative(lit(min)).replacement, min)
      }
    }
  }

  test("SPARK-62149: try_abs and try_negative over non-integral values") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      Seq(
        (Literal(-1.5d), 1.5d, 1.5d),
        (Literal(-1.5f), 1.5f, 1.5f),
        (Literal(Decimal("-12.34")), Decimal("12.34"), Decimal("12.34")),
        (Literal.create(null, DoubleType), null, null)
      ).foreach { case (input, abs, negated) =>
        checkEvaluation(new TryAbs(input).replacement, abs)
        checkEvaluation(new TryNegative(input).replacement, negated)
      }
      // An ANSI interval overflows in every mode, and the functions raise as `abs` and
      // `negative` do.
      val ymMin = Literal.create(Int.MinValue, YearMonthIntervalType())
      checkExceptionInExpression[ArithmeticException](
        new TryAbs(ymMin).replacement, "ARITHMETIC_OVERFLOW")
      checkExceptionInExpression[ArithmeticException](
        new TryNegative(ymMin).replacement, "ARITHMETIC_OVERFLOW")
    }
  }

  test("Throw exceptions from children") {
    val failingChild = Divide(Literal(1.0), Literal(0.0), EvalMode.ANSI)
    Seq(
      Add(failingChild, Literal(1.0), EvalMode.TRY),
      Add(Literal(1.0), failingChild, EvalMode.TRY),
      Subtract(failingChild, Literal(1.0), EvalMode.TRY),
      Subtract(Literal(1.0), failingChild, EvalMode.TRY),
      Multiply(failingChild, Literal(1.0), EvalMode.TRY),
      Multiply(Literal(1.0), failingChild, EvalMode.TRY),
      Divide(failingChild, Literal(1.0), EvalMode.TRY),
      Divide(Literal(1.0), failingChild, EvalMode.TRY)
    ).foreach { expr =>
      checkExceptionInExpression[ArithmeticException](expr, "DIVIDE_BY_ZERO")
    }
    // An error of the argument of try_abs and try_negative is not caught.
    val failingIntegral = Add(Literal(Int.MaxValue), Literal(1), EvalMode.ANSI)
    Seq(IntegralAbsOrNull(failingIntegral), IntegralNegateOrNull(failingIntegral)).foreach {
      expr => checkExceptionInExpression[ArithmeticException](expr, "ARITHMETIC_OVERFLOW")
    }
  }
}
