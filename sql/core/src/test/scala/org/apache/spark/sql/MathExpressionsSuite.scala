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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{log => logarithm}


private object MathExpressionsTestData {
  case class DoubleData(a: java.lang.Double, b: java.lang.Double)
  case class NullDoubles(a: java.lang.Double)
}

class MathExpressionsSuite extends QueryTest {

  import MathExpressionsTestData._

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  private lazy val doubleData = (1 to 10).map(i => DoubleData(i * 0.2 - 1, i * -0.2 + 1)).toDF()

  private lazy val nnDoubleData = (1 to 10).map(i => DoubleData(i * 0.1, i * -0.1)).toDF()

  private lazy val nullDoubles =
    Seq(NullDoubles(1.0), NullDoubles(2.0), NullDoubles(3.0), NullDoubles(null)).toDF()

  private def testOneToOneMathFunction[@specialized(Int, Long, Float, Double) T](
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

  private def testOneToOneNonNegativeMathFunction(c: Column => Column, f: Double => Double): Unit =
  {
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

  private def testTwoToOneMathFunction(
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

  test("toDegrees") {
    testOneToOneMathFunction(toDegrees, math.toDegrees)
    checkAnswer(
      ctx.sql("SELECT degrees(0), degrees(1), degrees(1.5)"),
      Seq((1, 2)).toDF().select(toDegrees(lit(0)), toDegrees(lit(1)), toDegrees(lit(1.5)))
    )
  }

  test("toRadians") {
    testOneToOneMathFunction(toRadians, math.toRadians)
    checkAnswer(
      ctx.sql("SELECT radians(0), radians(1), radians(1.5)"),
      Seq((1, 2)).toDF().select(toRadians(lit(0)), toRadians(lit(1)), toRadians(lit(1.5)))
    )
  }

  test("cbrt") {
    testOneToOneMathFunction(cbrt, math.cbrt)
  }

  test("ceil and ceiling") {
    testOneToOneMathFunction(ceil, math.ceil)
    checkAnswer(
      ctx.sql("SELECT ceiling(0), ceiling(1), ceiling(1.5)"),
      Row(0.0, 1.0, 2.0))
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

  test("signum / sign") {
    testOneToOneMathFunction[Double](signum, math.signum)

    checkAnswer(
      ctx.sql("SELECT sign(10), signum(-11)"),
      Row(1, -1))
  }

  test("pow / power") {
    testTwoToOneMathFunction(pow, pow, math.pow)

    checkAnswer(
      ctx.sql("SELECT pow(1, 2), power(2, 1)"),
      Seq((1, 2)).toDF().select(pow(lit(1), lit(2)), pow(lit(2), lit(1)))
    )
  }

  test("hypot") {
    testTwoToOneMathFunction(hypot, hypot, math.hypot)
  }

  test("atan2") {
    testTwoToOneMathFunction(atan2, atan2, math.atan2)
  }

  test("log / ln") {
    testOneToOneNonNegativeMathFunction(org.apache.spark.sql.functions.log, math.log)
    checkAnswer(
      ctx.sql("SELECT ln(0), ln(1), ln(1.5)"),
      Seq((1, 2)).toDF().select(logarithm(lit(0)), logarithm(lit(1)), logarithm(lit(1.5)))
    )
  }

  test("log10") {
    testOneToOneNonNegativeMathFunction(log10, math.log10)
  }

  test("log1p") {
    testOneToOneNonNegativeMathFunction(log1p, math.log1p)
  }

  test("binary log") {
    val df = Seq[(Integer, Integer)]((123, null)).toDF("a", "b")
    checkAnswer(
      df.select(org.apache.spark.sql.functions.log("a"),
        org.apache.spark.sql.functions.log(2.0, "a"),
        org.apache.spark.sql.functions.log("b")),
      Row(math.log(123), math.log(123) / math.log(2), null))

    checkAnswer(
      df.selectExpr("log(a)", "log(2.0, a)", "log(b)"),
      Row(math.log(123), math.log(123) / math.log(2), null))
  }

  test("abs") {
    val input =
      Seq[(java.lang.Double, java.lang.Double)]((null, null), (0.0, 0.0), (1.5, 1.5), (-2.5, 2.5))
    checkAnswer(
      input.toDF("key", "value").select(abs($"key").alias("a")).sort("a"),
      input.map(pair => Row(pair._2)))

    checkAnswer(
      input.toDF("key", "value").selectExpr("abs(key) a").sort("a"),
      input.map(pair => Row(pair._2)))
  }

  test("log2") {
    val df = Seq((1, 2)).toDF("a", "b")
    checkAnswer(
      df.select(log2("b") + log2("a")),
      Row(1))

    checkAnswer(ctx.sql("SELECT LOG2(8), LOG2(null)"), Row(3, null))
  }

  test("negative") {
    checkAnswer(
      ctx.sql("SELECT negative(1), negative(0), negative(-1)"),
      Row(-1, 0, 1))
  }
}
