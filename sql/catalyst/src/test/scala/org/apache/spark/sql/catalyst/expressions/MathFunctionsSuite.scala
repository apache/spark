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
import org.apache.spark.sql.types.DoubleType

class MathFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

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
   * @param f The functions in scala.math
   * @param domain The set of values to run the function with
   * @param expectNull Whether the given values should return null or not
   * @tparam T Generic type for primitives
   */
  private def testUnary[T](
      c: Expression => Expression,
      f: T => T,
      domain: Iterable[T] = (-20 to 20).map(_ * 0.1),
      expectNull: Boolean = false): Unit = {
    if (expectNull) {
      domain.foreach { value =>
        checkEvaluation(c(Literal(value)), null, EmptyRow)
      }
    } else {
      domain.foreach { value =>
        checkEvaluation(c(Literal(value)), f(value), EmptyRow)
      }
    }
    checkEvaluation(c(Literal.create(null, DoubleType)), null, create_row(null))
  }

  /**
   * Used for testing binary math expressions.
   *
   * @param c The DataFrame function
   * @param f The functions in scala.math
   * @param domain The set of values to run the function with
   */
  private def testBinary(
      c: (Expression, Expression) => Expression,
      f: (Double, Double) => Double,
      domain: Iterable[(Double, Double)] = (-20 to 20).map(v => (v * 0.1, v * -0.1)),
      expectNull: Boolean = false): Unit = {
    if (expectNull) {
      domain.foreach { case (v1, v2) =>
        checkEvaluation(c(Literal(v1), Literal(v2)), null, create_row(null))
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

  test("e") {
    testLeaf(EulerNumber, math.E)
  }

  test("pi") {
    testLeaf(Pi, math.Pi)
  }

  test("sin") {
    testUnary(Sin, math.sin)
  }

  test("asin") {
    testUnary(Asin, math.asin, (-10 to 10).map(_ * 0.1))
    testUnary(Asin, math.asin, (11 to 20).map(_ * 0.1), expectNull = true)
  }

  test("sinh") {
    testUnary(Sinh, math.sinh)
  }

  test("cos") {
    testUnary(Cos, math.cos)
  }

  test("acos") {
    testUnary(Acos, math.acos, (-10 to 10).map(_ * 0.1))
    testUnary(Acos, math.acos, (11 to 20).map(_ * 0.1), expectNull = true)
  }

  test("cosh") {
    testUnary(Cosh, math.cosh)
  }

  test("tan") {
    testUnary(Tan, math.tan)
  }

  test("atan") {
    testUnary(Atan, math.atan)
  }

  test("tanh") {
    testUnary(Tanh, math.tanh)
  }

  test("toDegrees") {
    testUnary(ToDegrees, math.toDegrees)
  }

  test("toRadians") {
    testUnary(ToRadians, math.toRadians)
  }

  test("cbrt") {
    testUnary(Cbrt, math.cbrt)
  }

  test("ceil") {
    testUnary(Ceil, math.ceil)
  }

  test("floor") {
    testUnary(Floor, math.floor)
  }

  test("rint") {
    testUnary(Rint, math.rint)
  }

  test("exp") {
    testUnary(Exp, math.exp)
  }

  test("expm1") {
    testUnary(Expm1, math.expm1)
  }

  test("signum") {
    testUnary[Double](Signum, math.signum)
  }

  test("log") {
    testUnary(Log, math.log, (0 to 20).map(_ * 0.1))
    testUnary(Log, math.log, (-5 to -1).map(_ * 0.1), expectNull = true)
  }

  test("log10") {
    testUnary(Log10, math.log10, (0 to 20).map(_ * 0.1))
    testUnary(Log10, math.log10, (-5 to -1).map(_ * 0.1), expectNull = true)
  }

  test("log1p") {
    testUnary(Log1p, math.log1p, (-1 to 20).map(_ * 0.1))
    testUnary(Log1p, math.log1p, (-10 to -2).map(_ * 1.0), expectNull = true)
  }

  test("log2") {
    def f: (Double) => Double = (x: Double) => math.log(x) / math.log(2)
    testUnary(Log2, f, (0 to 20).map(_ * 0.1))
    testUnary(Log2, f, (-5 to -1).map(_ * 1.0), expectNull = true)
  }

  test("pow") {
    testBinary(Pow, math.pow, (-5 to 5).map(v => (v * 1.0, v * 1.0)))
    testBinary(Pow, math.pow, Seq((-1.0, 0.9), (-2.2, 1.7), (-2.2, -1.7)), expectNull = true)
  }

  test("hypot") {
    testBinary(Hypot, math.hypot)
  }

  test("atan2") {
    testBinary(Atan2, math.atan2)
  }

  test("binary log") {
    val f = (c1: Double, c2: Double) => math.log(c2) / math.log(c1)
    val domain = (1 to 20).map(v => (v * 0.1, v * 0.2))

    domain.foreach { case (v1, v2) =>
      checkEvaluation(Logarithm(Literal(v1), Literal(v2)), f(v1 + 0.0, v2 + 0.0), EmptyRow)
      checkEvaluation(Logarithm(Literal(v2), Literal(v1)), f(v2 + 0.0, v1 + 0.0), EmptyRow)
      checkEvaluation(new Logarithm(Literal(v1)), f(math.E, v1 + 0.0), EmptyRow)
    }
    checkEvaluation(
      Logarithm(Literal.create(null, DoubleType), Literal(1.0)),
      null,
      create_row(null))
    checkEvaluation(
      Logarithm(Literal(1.0), Literal.create(null, DoubleType)),
      null,
      create_row(null))
  }
}
