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

import java.nio.charset.StandardCharsets

import com.google.common.math.LongMath

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.implicitCast
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.types._

class MathExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  import IntegralLiteralTestUtils._

  /**
   * Used for testing leaf math expressions.
   *
   * @param e expression
   * @param c The constants in scala.math
   * @tparam T Generic type for primitives
   */
  private def testLeaf[T](
      e: () => Expression,
      c: T): Unit = {
    checkEvaluation(e(), c, EmptyRow)
    checkEvaluation(e(), c, create_row(null))
  }

  /**
   * Used for testing unary math expressions.
   *
   * @param c expression
   * @param f The functions in scala.math or elsewhere used to generate expected results
   * @param domain The set of values to run the function with
   * @param expectNull Whether the given values should return null or not
   * @param expectNaN Whether the given values should eval to NaN or not
   * @tparam T Generic type for primitives
   * @tparam U Generic type for the output of the given function `f`
   */
  private def testUnary[T, U](
      c: Expression => Expression,
      f: T => U,
      domain: Iterable[T] = (-20 to 20).map(_ * 0.1),
      expectNull: Boolean = false,
      expectNaN: Boolean = false,
      evalType: DataType = DoubleType): Unit = {
    if (expectNull) {
      domain.foreach { value =>
        checkEvaluation(c(Literal(value)), null, EmptyRow)
      }
    } else if (expectNaN) {
      domain.foreach { value =>
        checkNaN(c(Literal(value)), EmptyRow)
      }
    } else {
      domain.foreach { value =>
        checkEvaluation(c(Literal(value)), f(value), EmptyRow)
      }
    }
    checkEvaluation(c(Literal.create(null, evalType)), null, create_row(null))
  }

  /**
   * Used for testing binary math expressions.
   *
   * @param c The DataFrame function
   * @param f The functions in scala.math
   * @param domain The set of values to run the function with
   * @param expectNull Whether the given values should return null or not
   * @param expectNaN Whether the given values should eval to NaN or not
   */
  private def testBinary(
      c: (Expression, Expression) => Expression,
      f: (Double, Double) => Double,
      domain: Iterable[(Double, Double)] = (-20 to 20).map(v => (v * 0.1, v * -0.1)),
      expectNull: Boolean = false, expectNaN: Boolean = false): Unit = {
    if (expectNull) {
      domain.foreach { case (v1, v2) =>
        checkEvaluation(c(Literal(v1), Literal(v2)), null, create_row(null))
      }
    } else if (expectNaN) {
      domain.foreach { case (v1, v2) =>
        checkNaN(c(Literal(v1), Literal(v2)), EmptyRow)
      }
    } else {
      domain.foreach { case (v1, v2) =>
        checkEvaluation(c(Literal(v1), Literal(v2)), f(v1 + 0.0, v2 + 0.0), EmptyRow)
        checkEvaluation(c(Literal(v2), Literal(v1)), f(v2 + 0.0, v1 + 0.0), EmptyRow)
      }
    }
    checkEvaluation(c(Literal.create(null, DoubleType), Literal(1.0)), null, create_row(null))
    checkEvaluation(c(Literal(1.0), Literal.create(null, DoubleType)), null, create_row(null))
  }

  private def checkNaN(
    expression: Expression, inputRow: InternalRow = EmptyRow): Unit = {
    checkNaNWithoutCodegen(expression, inputRow)
    checkNaNWithGeneratedProjection(expression, inputRow)
    checkNaNWithOptimization(expression, inputRow)
  }

  private def checkNaNWithoutCodegen(
    expression: Expression,
    inputRow: InternalRow = EmptyRow): Unit = {
    val actual = try evaluateWithoutCodegen(expression, inputRow) catch {
      case e: Exception => fail(s"Exception evaluating $expression", e)
    }
    if (!actual.asInstanceOf[Double].isNaN) {
      fail(s"Incorrect evaluation (codegen off): $expression, " +
        s"actual: $actual, " +
        s"expected: NaN")
    }
  }

  private def checkNaNWithGeneratedProjection(
    expression: Expression,
    inputRow: InternalRow = EmptyRow): Unit = {

    val plan =
      GenerateMutableProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil)

    val actual = plan(inputRow).get(0, expression.dataType)
    if (!actual.asInstanceOf[Double].isNaN) {
      fail(s"Incorrect Evaluation: $expression, actual: $actual, expected: NaN")
    }
  }

  private def checkNaNWithOptimization(
    expression: Expression,
    inputRow: InternalRow = EmptyRow): Unit = {
    val plan = Project(Alias(expression, s"Optimized($expression)")() :: Nil, OneRowRelation())
    val optimizedPlan = SimpleTestOptimizer.execute(plan)
    checkNaNWithoutCodegen(optimizedPlan.expressions.head, inputRow)
  }

  test("conv") {
    checkEvaluation(Conv(Literal("3"), Literal(10), Literal(2)), "11")
    checkEvaluation(Conv(Literal("-15"), Literal(10), Literal(-16)), "-F")
    checkEvaluation(Conv(Literal("-15"), Literal(10), Literal(16)), "FFFFFFFFFFFFFFF1")
    checkEvaluation(Conv(Literal("big"), Literal(36), Literal(16)), "3A48")
    checkEvaluation(Conv(Literal.create(null, StringType), Literal(36), Literal(16)), null)
    checkEvaluation(Conv(Literal("3"), Literal.create(null, IntegerType), Literal(16)), null)
    checkEvaluation(Conv(Literal("3"), Literal(16), Literal.create(null, IntegerType)), null)
    checkEvaluation(
      Conv(Literal("1234"), Literal(10), Literal(37)), null)
    checkEvaluation(
      Conv(Literal(""), Literal(10), Literal(16)), null)
    checkEvaluation(
      Conv(Literal("9223372036854775807"), Literal(36), Literal(16)), "FFFFFFFFFFFFFFFF")
    // If there is an invalid digit in the number, the longest valid prefix should be converted.
    checkEvaluation(
      Conv(Literal("11abc"), Literal(10), Literal(16)), "B")
  }

  test("e") {
    testLeaf(EulerNumber, math.E)
  }

  test("pi") {
    testLeaf(Pi, math.Pi)
  }

  test("sin") {
    testUnary(Sin, math.sin)
    checkConsistencyBetweenInterpretedAndCodegen(Sin, DoubleType)
  }

  test("csc") {
    def f: (Double) => Double = (x: Double) => 1 / math.sin(x)
    testUnary(Csc, f)
    checkConsistencyBetweenInterpretedAndCodegen(Csc, DoubleType)
    val nullLit = Literal.create(null, NullType)
    val intNullLit = Literal.create(null, IntegerType)
    val intLit = Literal.create(1, IntegerType)
    checkEvaluation(checkDataTypeAndCast(Csc(nullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Csc(intNullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Csc(intLit)), 1 / math.sin(1), EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Csc(-intLit)), 1 / math.sin(-1), EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Csc(0)), 1 / math.sin(0), EmptyRow)
  }

  test("asin") {
    testUnary(Asin, math.asin, (-10 to 10).map(_ * 0.1))
    testUnary(Asin, math.asin, (11 to 20).map(_ * 0.1), expectNaN = true)
    checkConsistencyBetweenInterpretedAndCodegen(Asin, DoubleType)
  }

  test("sinh") {
    testUnary(Sinh, math.sinh)
    checkConsistencyBetweenInterpretedAndCodegen(Sinh, DoubleType)
  }

  test("asinh") {
    testUnary(Asinh, (x: Double) => StrictMath.log(x + math.sqrt(x * x + 1.0)))
    checkConsistencyBetweenInterpretedAndCodegen(Asinh, DoubleType)

    checkEvaluation(Asinh(Double.NegativeInfinity), Double.NegativeInfinity)

    val nullLit = Literal.create(null, NullType)
    val doubleNullLit = Literal.create(null, DoubleType)
    checkEvaluation(checkDataTypeAndCast(Asinh(nullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Asinh(doubleNullLit)), null, EmptyRow)
  }

  test("cos") {
    testUnary(Cos, math.cos)
    checkConsistencyBetweenInterpretedAndCodegen(Cos, DoubleType)
  }

  test("sec") {
    def f: (Double) => Double = (x: Double) => 1 / math.cos(x)
    testUnary(Sec, f)
    checkConsistencyBetweenInterpretedAndCodegen(Sec, DoubleType)
    val nullLit = Literal.create(null, NullType)
    val intNullLit = Literal.create(null, IntegerType)
    val intLit = Literal.create(1, IntegerType)
    checkEvaluation(checkDataTypeAndCast(Sec(nullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Sec(intNullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Sec(intLit)), 1 / math.cos(1), EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Sec(-intLit)), 1 / math.cos(-1), EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Sec(0)), 1 / math.cos(0), EmptyRow)
  }

  test("acos") {
    testUnary(Acos, math.acos, (-10 to 10).map(_ * 0.1))
    testUnary(Acos, math.acos, (11 to 20).map(_ * 0.1), expectNaN = true)
    checkConsistencyBetweenInterpretedAndCodegen(Acos, DoubleType)
  }

  test("cosh") {
    testUnary(Cosh, math.cosh)
    checkConsistencyBetweenInterpretedAndCodegen(Cosh, DoubleType)
  }

  test("acosh") {
    testUnary(Acosh, (x: Double) => StrictMath.log(x + math.sqrt(x * x - 1.0)))
    checkConsistencyBetweenInterpretedAndCodegen(Cosh, DoubleType)

    val nullLit = Literal.create(null, NullType)
    val doubleNullLit = Literal.create(null, DoubleType)
    checkEvaluation(checkDataTypeAndCast(Acosh(nullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Acosh(doubleNullLit)), null, EmptyRow)
  }

  test("tan") {
    testUnary(Tan, math.tan)
    checkConsistencyBetweenInterpretedAndCodegen(Tan, DoubleType)
  }

  test("cot") {
    def f: (Double) => Double = (x: Double) => 1 / math.tan(x)
    testUnary(Cot, f)
    checkConsistencyBetweenInterpretedAndCodegen(Cot, DoubleType)
    val nullLit = Literal.create(null, NullType)
    val intNullLit = Literal.create(null, IntegerType)
    val intLit = Literal.create(1, IntegerType)
    checkEvaluation(checkDataTypeAndCast(Cot(nullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Cot(intNullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Cot(intLit)), 1 / math.tan(1), EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Cot(-intLit)), 1 / math.tan(-1), EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Cot(0)), 1 / math.tan(0), EmptyRow)
  }

  test("atan") {
    testUnary(Atan, math.atan)
    checkConsistencyBetweenInterpretedAndCodegen(Atan, DoubleType)
  }

  test("tanh") {
    testUnary(Tanh, math.tanh)
    checkConsistencyBetweenInterpretedAndCodegen(Tanh, DoubleType)
  }

  test("atanh") {
    // SPARK-28519: more accurate express for 1/2 * ln((1 + x) / (1 - x))
    testUnary(Atanh, (x: Double) => 0.5 * (StrictMath.log1p(x) - StrictMath.log1p(-x)))
    checkConsistencyBetweenInterpretedAndCodegen(Atanh, DoubleType)

    val nullLit = Literal.create(null, NullType)
    val doubleNullLit = Literal.create(null, DoubleType)
    checkEvaluation(checkDataTypeAndCast(Atanh(nullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Atanh(doubleNullLit)), null, EmptyRow)
  }

  test("toDegrees") {
    testUnary(ToDegrees, math.toDegrees)
    checkConsistencyBetweenInterpretedAndCodegen(ToDegrees, DoubleType)
  }

  test("toRadians") {
    testUnary(ToRadians, math.toRadians)
    checkConsistencyBetweenInterpretedAndCodegen(ToRadians, DoubleType)
  }

  test("cbrt") {
    testUnary(Cbrt, math.cbrt)
    checkConsistencyBetweenInterpretedAndCodegen(Cbrt, DoubleType)
  }

  def checkDataTypeAndCast(expression: UnaryMathExpression): Expression = {
    val expNew = implicitCast(expression.child, expression.inputTypes(0)).getOrElse(expression)
    expression.withNewChildren(Seq(expNew))
  }

  test("ceil") {
    testUnary(Ceil, (d: Double) => math.ceil(d).toLong)
    checkConsistencyBetweenInterpretedAndCodegen(Ceil, DoubleType)

    testUnary(Ceil, (d: Decimal) => d.ceil, (-20 to 20).map(x => Decimal(x * 0.1)))
    checkConsistencyBetweenInterpretedAndCodegen(Ceil, DecimalType(25, 3))
    checkConsistencyBetweenInterpretedAndCodegen(Ceil, DecimalType(25, 0))
    checkConsistencyBetweenInterpretedAndCodegen(Ceil, DecimalType(5, 0))

    val doublePi: Double = 3.1415
    val floatPi: Float = 3.1415f
    val longLit: Long = 12345678901234567L
    val nullLit = Literal.create(null, NullType)
    val floatNullLit = Literal.create(null, FloatType)
    checkEvaluation(checkDataTypeAndCast(Ceil(doublePi)), 4L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(floatPi)), 4L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(longLit)), longLit, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(-doublePi)), -3L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(-floatPi)), -3L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(-longLit)), -longLit, EmptyRow)

    checkEvaluation(checkDataTypeAndCast(Ceil(nullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(floatNullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(0)), 0L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(1)), 1L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(1234567890123456L)), 1234567890123456L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(0.01)), 1L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Ceil(-0.10)), 0L, EmptyRow)
  }

  test("floor") {
    testUnary(Floor, (d: Double) => math.floor(d).toLong)
    checkConsistencyBetweenInterpretedAndCodegen(Floor, DoubleType)

    testUnary(Floor, (d: Decimal) => d.floor, (-20 to 20).map(x => Decimal(x * 0.1)))
    checkConsistencyBetweenInterpretedAndCodegen(Floor, DecimalType(25, 3))
    checkConsistencyBetweenInterpretedAndCodegen(Floor, DecimalType(25, 0))
    checkConsistencyBetweenInterpretedAndCodegen(Floor, DecimalType(5, 0))

    val doublePi: Double = 3.1415
    val floatPi: Float = 3.1415f
    val longLit: Long = 12345678901234567L
    val nullLit = Literal.create(null, NullType)
    val floatNullLit = Literal.create(null, FloatType)
    checkEvaluation(checkDataTypeAndCast(Floor(doublePi)), 3L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(floatPi)), 3L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(longLit)), longLit, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(-doublePi)), -4L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(-floatPi)), -4L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(-longLit)), -longLit, EmptyRow)

    checkEvaluation(checkDataTypeAndCast(Floor(nullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(floatNullLit)), null, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(0)), 0L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(1)), 1L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(1234567890123456L)), 1234567890123456L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(0.01)), 0L, EmptyRow)
    checkEvaluation(checkDataTypeAndCast(Floor(-0.10)), -1L, EmptyRow)
  }

  test("factorial") {
    (0 to 20).foreach { value =>
      checkEvaluation(Factorial(Literal(value)), LongMath.factorial(value), EmptyRow)
    }
    checkEvaluation(Literal.create(null, IntegerType), null, create_row(null))
    checkEvaluation(Factorial(Literal(20)), 2432902008176640000L, EmptyRow)
    checkEvaluation(Factorial(Literal(21)), null, EmptyRow)
    checkConsistencyBetweenInterpretedAndCodegen(Factorial.apply _, IntegerType)
  }

  test("rint") {
    testUnary(Rint, math.rint)
    checkConsistencyBetweenInterpretedAndCodegen(Rint, DoubleType)
  }

  test("exp") {
    testUnary(Exp, StrictMath.exp)
    checkConsistencyBetweenInterpretedAndCodegen(Exp, DoubleType)
  }

  test("expm1") {
    testUnary(Expm1, StrictMath.expm1)
    checkConsistencyBetweenInterpretedAndCodegen(Expm1, DoubleType)
  }

  test("signum") {
    testUnary[Double, Double](Signum, math.signum)
    checkConsistencyBetweenInterpretedAndCodegen(Signum, DoubleType)
  }

  test("log") {
    testUnary(Log, StrictMath.log, (1 to 20).map(_ * 0.1))
    testUnary(Log, StrictMath.log, (-5 to 0).map(_ * 0.1), expectNull = true)
    checkConsistencyBetweenInterpretedAndCodegen(Log, DoubleType)
  }

  test("log10") {
    testUnary(Log10, StrictMath.log10, (1 to 20).map(_ * 0.1))
    testUnary(Log10, StrictMath.log10, (-5 to 0).map(_ * 0.1), expectNull = true)
    checkConsistencyBetweenInterpretedAndCodegen(Log10, DoubleType)
  }

  test("log1p") {
    testUnary(Log1p, StrictMath.log1p, (0 to 20).map(_ * 0.1))
    testUnary(Log1p, StrictMath.log1p, (-10 to -1).map(_ * 1.0), expectNull = true)
    checkConsistencyBetweenInterpretedAndCodegen(Log1p, DoubleType)
  }

  test("bin") {
    testUnary(Bin, java.lang.Long.toBinaryString, (-20 to 20).map(_.toLong), evalType = LongType)

    val row = create_row(null, 12L, 123L, 1234L, -123L)
    val l1 = 'a.long.at(0)
    val l2 = 'a.long.at(1)
    val l3 = 'a.long.at(2)
    val l4 = 'a.long.at(3)
    val l5 = 'a.long.at(4)

    checkEvaluation(Bin(l1), null, row)
    checkEvaluation(Bin(l2), java.lang.Long.toBinaryString(12), row)
    checkEvaluation(Bin(l3), java.lang.Long.toBinaryString(123), row)
    checkEvaluation(Bin(l4), java.lang.Long.toBinaryString(1234), row)
    checkEvaluation(Bin(l5), java.lang.Long.toBinaryString(-123), row)

    checkEvaluation(Bin(positiveLongLit), java.lang.Long.toBinaryString(positiveLong))
    checkEvaluation(Bin(negativeLongLit), java.lang.Long.toBinaryString(negativeLong))

    checkConsistencyBetweenInterpretedAndCodegen(Bin, LongType)
  }

  test("log2") {
    def f: (Double) => Double = (x: Double) => StrictMath.log(x) / StrictMath.log(2)
    testUnary(Log2, f, (1 to 20).map(_ * 0.1))
    testUnary(Log2, f, (-5 to 0).map(_ * 1.0), expectNull = true)
    checkConsistencyBetweenInterpretedAndCodegen(Log2, DoubleType)
  }

  test("sqrt") {
    testUnary(Sqrt, math.sqrt, (0 to 20).map(_ * 0.1))
    testUnary(Sqrt, math.sqrt, (-5 to -1).map(_ * 1.0), expectNaN = true)

    checkEvaluation(Sqrt(Literal.create(null, DoubleType)), null, create_row(null))
    checkNaN(Sqrt(Literal(-1.0)), EmptyRow)
    checkNaN(Sqrt(Literal(-1.5)), EmptyRow)
    checkConsistencyBetweenInterpretedAndCodegen(Sqrt, DoubleType)
  }

  test("pow") {
    testBinary(Pow, StrictMath.pow, (-5 to 5).map(v => (v * 1.0, v * 1.0)))
    testBinary(Pow, StrictMath.pow, Seq((-1.0, 0.9), (-2.2, 1.7), (-2.2, -1.7)), expectNaN = true)
    checkConsistencyBetweenInterpretedAndCodegen(Pow, DoubleType, DoubleType)
  }

  test("shift left") {
    checkEvaluation(ShiftLeft(Literal.create(null, IntegerType), Literal(1)), null)
    checkEvaluation(ShiftLeft(Literal(21), Literal.create(null, IntegerType)), null)
    checkEvaluation(
      ShiftLeft(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null)
    checkEvaluation(ShiftLeft(Literal(21), Literal(1)), 42)

    checkEvaluation(ShiftLeft(Literal(21.toLong), Literal(1)), 42.toLong)
    checkEvaluation(ShiftLeft(Literal(-21.toLong), Literal(1)), -42.toLong)

    checkEvaluation(ShiftLeft(positiveIntLit, positiveIntLit), positiveInt << positiveInt)
    checkEvaluation(ShiftLeft(positiveIntLit, negativeIntLit), positiveInt << negativeInt)
    checkEvaluation(ShiftLeft(negativeIntLit, positiveIntLit), negativeInt << positiveInt)
    checkEvaluation(ShiftLeft(negativeIntLit, negativeIntLit), negativeInt << negativeInt)
    checkEvaluation(ShiftLeft(positiveLongLit, positiveIntLit), positiveLong << positiveInt)
    checkEvaluation(ShiftLeft(positiveLongLit, negativeIntLit), positiveLong << negativeInt)
    checkEvaluation(ShiftLeft(negativeLongLit, positiveIntLit), negativeLong << positiveInt)
    checkEvaluation(ShiftLeft(negativeLongLit, negativeIntLit), negativeLong << negativeInt)

    checkConsistencyBetweenInterpretedAndCodegen(ShiftLeft, IntegerType, IntegerType)
    checkConsistencyBetweenInterpretedAndCodegen(ShiftLeft, LongType, IntegerType)
  }

  test("shift right") {
    checkEvaluation(ShiftRight(Literal.create(null, IntegerType), Literal(1)), null)
    checkEvaluation(ShiftRight(Literal(42), Literal.create(null, IntegerType)), null)
    checkEvaluation(
      ShiftRight(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null)
    checkEvaluation(ShiftRight(Literal(42), Literal(1)), 21)

    checkEvaluation(ShiftRight(Literal(42.toLong), Literal(1)), 21.toLong)
    checkEvaluation(ShiftRight(Literal(-42.toLong), Literal(1)), -21.toLong)

    checkEvaluation(ShiftRight(positiveIntLit, positiveIntLit), positiveInt >> positiveInt)
    checkEvaluation(ShiftRight(positiveIntLit, negativeIntLit), positiveInt >> negativeInt)
    checkEvaluation(ShiftRight(negativeIntLit, positiveIntLit), negativeInt >> positiveInt)
    checkEvaluation(ShiftRight(negativeIntLit, negativeIntLit), negativeInt >> negativeInt)
    checkEvaluation(ShiftRight(positiveLongLit, positiveIntLit), positiveLong >> positiveInt)
    checkEvaluation(ShiftRight(positiveLongLit, negativeIntLit), positiveLong >> negativeInt)
    checkEvaluation(ShiftRight(negativeLongLit, positiveIntLit), negativeLong >> positiveInt)
    checkEvaluation(ShiftRight(negativeLongLit, negativeIntLit), negativeLong >> negativeInt)

    checkConsistencyBetweenInterpretedAndCodegen(ShiftRight, IntegerType, IntegerType)
    checkConsistencyBetweenInterpretedAndCodegen(ShiftRight, LongType, IntegerType)
  }

  test("shift right unsigned") {
    checkEvaluation(ShiftRightUnsigned(Literal.create(null, IntegerType), Literal(1)), null)
    checkEvaluation(ShiftRightUnsigned(Literal(42), Literal.create(null, IntegerType)), null)
    checkEvaluation(
      ShiftRight(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null)
    checkEvaluation(ShiftRightUnsigned(Literal(42), Literal(1)), 21)

    checkEvaluation(ShiftRightUnsigned(Literal(42.toLong), Literal(1)), 21.toLong)
    checkEvaluation(ShiftRightUnsigned(Literal(-42.toLong), Literal(1)), 9223372036854775787L)

    checkEvaluation(ShiftRightUnsigned(positiveIntLit, positiveIntLit),
      positiveInt >>> positiveInt)
    checkEvaluation(ShiftRightUnsigned(positiveIntLit, negativeIntLit),
      positiveInt >>> negativeInt)
    checkEvaluation(ShiftRightUnsigned(negativeIntLit, positiveIntLit),
      negativeInt >>> positiveInt)
    checkEvaluation(ShiftRightUnsigned(negativeIntLit, negativeIntLit),
      negativeInt >>> negativeInt)
    checkEvaluation(ShiftRightUnsigned(positiveLongLit, positiveIntLit),
      positiveLong >>> positiveInt)
    checkEvaluation(ShiftRightUnsigned(positiveLongLit, negativeIntLit),
      positiveLong >>> negativeInt)
    checkEvaluation(ShiftRightUnsigned(negativeLongLit, positiveIntLit),
      negativeLong >>> positiveInt)
    checkEvaluation(ShiftRightUnsigned(negativeLongLit, negativeIntLit),
      negativeLong >>> negativeInt)

    checkConsistencyBetweenInterpretedAndCodegen(ShiftRightUnsigned, IntegerType, IntegerType)
    checkConsistencyBetweenInterpretedAndCodegen(ShiftRightUnsigned, LongType, IntegerType)
  }

  test("hex") {
    checkEvaluation(Hex(Literal.create(null, LongType)), null)
    checkEvaluation(Hex(Literal(28L)), "1C")
    checkEvaluation(Hex(Literal(-28L)), "FFFFFFFFFFFFFFE4")
    checkEvaluation(Hex(Literal(100800200404L)), "177828FED4")
    checkEvaluation(Hex(Literal(-100800200404L)), "FFFFFFE887D7012C")
    checkEvaluation(Hex(Literal.create(null, BinaryType)), null)
    checkEvaluation(Hex(Literal("helloHex".getBytes(StandardCharsets.UTF_8))), "68656C6C6F486578")
    // scalastyle:off
    // Turn off scala style for non-ascii chars
    checkEvaluation(Hex(Literal("三重的".getBytes(StandardCharsets.UTF_8))), "E4B889E9878DE79A84")
    // scalastyle:on
    Seq(LongType, BinaryType, StringType).foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(Hex.apply _, dt)
    }
  }

  test("unhex") {
    checkEvaluation(Unhex(Literal.create(null, StringType)), null)
    checkEvaluation(Unhex(Literal("737472696E67")), "string".getBytes(StandardCharsets.UTF_8))
    checkEvaluation(Unhex(Literal("")), new Array[Byte](0))
    checkEvaluation(Unhex(Literal("F")), Array[Byte](15))
    checkEvaluation(Unhex(Literal("ff")), Array[Byte](-1))
    checkEvaluation(Unhex(Literal("GG")), null)
    // scalastyle:off
    // Turn off scala style for non-ascii chars
    checkEvaluation(Unhex(Literal("E4B889E9878DE79A84")), "三重的".getBytes(StandardCharsets.UTF_8))
    checkEvaluation(Unhex(Literal("三重的")), null)
    // scalastyle:on
    checkConsistencyBetweenInterpretedAndCodegen(Unhex, StringType)
  }

  test("hypot") {
    testBinary(Hypot, math.hypot)
    checkConsistencyBetweenInterpretedAndCodegen(Hypot, DoubleType, DoubleType)
  }

  test("atan2") {
    testBinary(Atan2, math.atan2)
    checkConsistencyBetweenInterpretedAndCodegen(Atan2, DoubleType, DoubleType)
  }

  test("binary log") {
    val f = (c1: Double, c2: Double) => StrictMath.log(c2) / StrictMath.log(c1)
    val domain = (1 to 20).map(v => (v * 0.1, v * 0.2))

    domain.foreach { case (v1, v2) =>
      checkEvaluation(Logarithm(Literal(v1), Literal(v2)), f(v1 + 0.0, v2 + 0.0), EmptyRow)
      checkEvaluation(Logarithm(Literal(v2), Literal(v1)), f(v2 + 0.0, v1 + 0.0), EmptyRow)
      checkEvaluation(new Logarithm(Literal(v1)), f(math.E, v1 + 0.0), EmptyRow)
    }

    // null input should yield null output
    checkEvaluation(
      Logarithm(Literal.create(null, DoubleType), Literal(1.0)),
      null,
      create_row(null))
    checkEvaluation(
      Logarithm(Literal(1.0), Literal.create(null, DoubleType)),
      null,
      create_row(null))

    // negative input should yield null output
    checkEvaluation(
      Logarithm(Literal(-1.0), Literal(1.0)),
      null,
      create_row(null))
    checkEvaluation(
      Logarithm(Literal(1.0), Literal(-1.0)),
      null,
      create_row(null))
    checkConsistencyBetweenInterpretedAndCodegen(Logarithm, DoubleType, DoubleType)
  }

  test("round/bround") {
    val scales = -6 to 6
    val doublePi: Double = math.Pi
    val shortPi: Short = 31415
    val intPi: Int = 314159265
    val longPi: Long = 31415926535897932L
    val bdPi: BigDecimal = BigDecimal(31415927L, 7)
    val floatPi: Float = 3.1415f

    val doubleResults: Seq[Double] = Seq(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 3.1, 3.14, 3.142,
      3.1416, 3.14159, 3.141593)

    val floatResults: Seq[Float] = Seq(0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 3.0f, 3.1f, 3.14f,
      3.141f, 3.1415f, 3.1415f, 3.1415f)

    val shortResults: Seq[Short] = Seq[Short](0, 0, 30000, 31000, 31400, 31420) ++
      Seq.fill[Short](7)(31415)

    val intResults: Seq[Int] = Seq(314000000, 314200000, 314160000, 314159000, 314159300,
      314159270) ++ Seq.fill(7)(314159265)

    val longResults: Seq[Long] = Seq(31415926536000000L, 31415926535900000L,
      31415926535900000L, 31415926535898000L, 31415926535897900L, 31415926535897930L) ++
      Seq.fill(7)(31415926535897932L)

    val intResultsB: Seq[Int] = Seq(314000000, 314200000, 314160000, 314159000, 314159300,
      314159260) ++ Seq.fill(7)(314159265)

    scales.zipWithIndex.foreach { case (scale, i) =>
      checkEvaluation(Round(doublePi, scale), doubleResults(i), EmptyRow)
      checkEvaluation(Round(shortPi, scale), shortResults(i), EmptyRow)
      checkEvaluation(Round(intPi, scale), intResults(i), EmptyRow)
      checkEvaluation(Round(longPi, scale), longResults(i), EmptyRow)
      checkEvaluation(Round(floatPi, scale), floatResults(i), EmptyRow)
      checkEvaluation(BRound(doublePi, scale), doubleResults(i), EmptyRow)
      checkEvaluation(BRound(shortPi, scale), shortResults(i), EmptyRow)
      checkEvaluation(BRound(intPi, scale), intResultsB(i), EmptyRow)
      checkEvaluation(BRound(longPi, scale), longResults(i), EmptyRow)
      checkEvaluation(BRound(floatPi, scale), floatResults(i), EmptyRow)
    }

    val bdResults: Seq[BigDecimal] = Seq(BigDecimal(3), BigDecimal("3.1"), BigDecimal("3.14"),
      BigDecimal("3.142"), BigDecimal("3.1416"), BigDecimal("3.14159"),
      BigDecimal("3.141593"), BigDecimal("3.1415927"))

    (0 to 7).foreach { i =>
      checkEvaluation(Round(bdPi, i), bdResults(i), EmptyRow)
      checkEvaluation(BRound(bdPi, i), bdResults(i), EmptyRow)
    }
    (8 to 10).foreach { scale =>
      checkEvaluation(Round(bdPi, scale), bdPi, EmptyRow)
      checkEvaluation(BRound(bdPi, scale), bdPi, EmptyRow)
    }

    DataTypeTestUtils.numericTypes.foreach { dataType =>
      checkEvaluation(Round(Literal.create(null, dataType), Literal(2)), null)
      checkEvaluation(Round(Literal.create(null, dataType),
        Literal.create(null, IntegerType)), null)
      checkEvaluation(BRound(Literal.create(null, dataType), Literal(2)), null)
      checkEvaluation(BRound(Literal.create(null, dataType),
        Literal.create(null, IntegerType)), null)
    }

    checkEvaluation(Round(2.5, 0), 3.0)
    checkEvaluation(Round(3.5, 0), 4.0)
    checkEvaluation(Round(-2.5, 0), -3.0)
    checkEvaluation(Round(-3.5, 0), -4.0)
    checkEvaluation(Round(-0.35, 1), -0.4)
    checkEvaluation(Round(-35, -1), -40)
    checkEvaluation(BRound(2.5, 0), 2.0)
    checkEvaluation(BRound(3.5, 0), 4.0)
    checkEvaluation(BRound(-2.5, 0), -2.0)
    checkEvaluation(BRound(-3.5, 0), -4.0)
    checkEvaluation(BRound(-0.35, 1), -0.4)
    checkEvaluation(BRound(-35, -1), -40)
  }
}
