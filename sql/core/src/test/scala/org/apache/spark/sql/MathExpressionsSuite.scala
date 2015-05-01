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

package org.apache.spark.sql

import java.lang.{Double => JavaDouble}

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.mathfunctions._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext.implicits._

private[this] object MathExpressionsTestData {

  case class DoubleData(a: JavaDouble, b: JavaDouble)
  val doubleData = TestSQLContext.sparkContext.parallelize(
    (1 to 10).map(i => DoubleData(i * 0.2 - 1, i * -0.2 + 1))).toDF()

  val nnDoubleData = TestSQLContext.sparkContext.parallelize(
    (1 to 10).map(i => DoubleData(i * 0.1, i * -0.1))).toDF()

  case class NullDoubles(a: JavaDouble)
  val nullDoubles =
    TestSQLContext.sparkContext.parallelize(
      NullDoubles(1.0) ::
        NullDoubles(2.0) ::
        NullDoubles(3.0) ::
        NullDoubles(null) :: Nil
    ).toDF()
}

class MathExpressionsSuite extends QueryTest {

  import MathExpressionsTestData._

  def testOneToOneMathFunction[@specialized(Int, Long, Float, Double) T](
      c: Column => Column,
      f: T => T): Unit = {
    checkAnswer(
      doubleData.select(c('a)),
      (1 to 10).map(n => Row(f((n * 0.2 - 1).asInstanceOf[T])))
    )

    checkAnswer(
      doubleData.select(c('b)),
      (1 to 10).map(n => Row(f((-n * 0.2 + 1).asInstanceOf[T])))
    )

    checkAnswer(
      doubleData.select(c(lit(null))),
      (1 to 10).map(_ => Row(null))
    )
  }

  def testOneToOneNonNegativeMathFunction(c: Column => Column, f: Double => Double): Unit = {
    checkAnswer(
      nnDoubleData.select(c('a)),
      (1 to 10).map(n => Row(f(n * 0.1)))
    )

    if (f(-1) === math.log1p(-1)) {
      checkAnswer(
        nnDoubleData.select(c('b)),
        (1 to 9).map(n => Row(f(n * -0.1))) :+ Row(Double.NegativeInfinity)
      )
    } else {
      checkAnswer(
        nnDoubleData.select(c('b)),
        (1 to 10).map(n => Row(null))
      )
    }

    checkAnswer(
      nnDoubleData.select(c(lit(null))),
      (1 to 10).map(_ => Row(null))
    )
  }

  def testTwoToOneMathFunction(
      c: (Column, Column) => Column,
      d: (Column, Double) => Column,
      f: (Double, Double) => Double): Unit = {
    checkAnswer(
      nnDoubleData.select(c('a, 'a)),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), r.getDouble(0))))
    )

    checkAnswer(
      nnDoubleData.select(c('a, 'b)),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), r.getDouble(1))))
    )

    checkAnswer(
      nnDoubleData.select(d('a, 2.0)),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), 2.0)))
    )

    checkAnswer(
      nnDoubleData.select(d('a, -0.5)),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), -0.5)))
    )

    val nonNull = nullDoubles.collect().toSeq.filter(r => r.get(0) != null)

    checkAnswer(
      nullDoubles.select(c('a, 'a)).orderBy('a.asc),
      Row(null) +: nonNull.map(r => Row(f(r.getDouble(0), r.getDouble(0))))
    )
  }

  test("sin") {
    testOneToOneMathFunction(sin, math.sin)
  }

  test("asin") {
    testOneToOneMathFunction(asin, math.asin)
  }

  test("sinh") {
    testOneToOneMathFunction(sinh, math.sinh)
  }

  test("cos") {
    testOneToOneMathFunction(cos, math.cos)
  }

  test("acos") {
    testOneToOneMathFunction(acos, math.acos)
  }

  test("cosh") {
    testOneToOneMathFunction(cosh, math.cosh)
  }

  test("tan") {
    testOneToOneMathFunction(tan, math.tan)
  }

  test("atan") {
    testOneToOneMathFunction(atan, math.atan)
  }

  test("tanh") {
    testOneToOneMathFunction(tanh, math.tanh)
  }

  test("toDeg") {
    testOneToOneMathFunction(toDeg, math.toDegrees)
  }

  test("toRad") {
    testOneToOneMathFunction(toRad, math.toRadians)
  }

  test("cbrt") {
    testOneToOneMathFunction(cbrt, math.cbrt)
  }

  test("ceil") {
    testOneToOneMathFunction(ceil, math.ceil)
  }

  test("floor") {
    testOneToOneMathFunction(floor, math.floor)
  }

  test("rint") {
    testOneToOneMathFunction(rint, math.rint)
  }

  test("exp") {
    testOneToOneMathFunction(exp, math.exp)
  }

  test("expm1") {
    testOneToOneMathFunction(expm1, math.expm1)
  }

  test("signum") {
    testOneToOneMathFunction[Double](signum, math.signum)
  }

  test("pow") {
    testTwoToOneMathFunction(pow, pow, math.pow)
  }

  test("hypot") {
    testTwoToOneMathFunction(hypot, hypot, math.hypot)
  }

  test("atan2") {
    testTwoToOneMathFunction(atan2, atan2, math.atan2)
  }

  test("log") {
    testOneToOneNonNegativeMathFunction(log, math.log)
  }

  test("log10") {
    testOneToOneNonNegativeMathFunction(log10, math.log10)
  }

  test("log1p") {
    testOneToOneNonNegativeMathFunction(log1p, math.log1p)
  }

}
