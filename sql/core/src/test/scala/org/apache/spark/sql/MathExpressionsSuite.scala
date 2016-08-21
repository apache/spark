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

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{log => logarithm}
import org.apache.spark.sql.test.SharedSQLContext

private object MathExpressionsTestData {
  case class DoubleData(a: java.lang.Double, b: java.lang.Double)
  case class NullDoubles(a: java.lang.Double)
}

class MathExpressionsSuite extends QueryTest with SharedSQLContext {
  import MathExpressionsTestData._
  import testImplicits._

  private lazy val doubleData = (1 to 10).map(i => DoubleData(i * 0.2 - 1, i * -0.2 + 1)).toDF()

  private lazy val nnDoubleData = (1 to 10).map(i => DoubleData(i * 0.1, i * -0.1)).toDF()

  private lazy val nullDoubles =
    Seq(NullDoubles(1.0), NullDoubles(2.0), NullDoubles(3.0), NullDoubles(null)).toDF()

  private def testOneToOneMathFunction[
  @specialized(Int, Long, Float, Double) T,
  @specialized(Int, Long, Float, Double) U](
      c: Column => Column,
      f: T => U): Unit = {
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
        (1 to 9).map(n => Row(f(n * -0.1))) :+ Row(null)
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
      sql("SELECT degrees(0), degrees(1), degrees(1.5)"),
      Seq((1, 2)).toDF().select(toDegrees(lit(0)), toDegrees(lit(1)), toDegrees(lit(1.5)))
    )
  }

  test("toRadians") {
    testOneToOneMathFunction(toRadians, math.toRadians)
    checkAnswer(
      sql("SELECT radians(0), radians(1), radians(1.5)"),
      Seq((1, 2)).toDF().select(toRadians(lit(0)), toRadians(lit(1)), toRadians(lit(1.5)))
    )
  }

  test("cbrt") {
    testOneToOneMathFunction(cbrt, math.cbrt)
  }

  test("ceil and ceiling") {
    testOneToOneMathFunction(ceil, (d: Double) => math.ceil(d).toLong)
    checkAnswer(
      sql("SELECT ceiling(0), ceiling(1), ceiling(1.5)"),
      Row(0L, 1L, 2L))
  }

  test("conv") {
    val df = Seq(("333", 10, 2)).toDF("num", "fromBase", "toBase")
    checkAnswer(df.select(conv('num, 10, 16)), Row("14D"))
    checkAnswer(df.select(conv(lit(100), 2, 16)), Row("4"))
    checkAnswer(df.select(conv(lit(3122234455L), 10, 16)), Row("BA198457"))
    checkAnswer(df.selectExpr("conv(num, fromBase, toBase)"), Row("101001101"))
    checkAnswer(df.selectExpr("""conv("100", 2, 10)"""), Row("4"))
    checkAnswer(df.selectExpr("""conv("-10", 16, -10)"""), Row("-16"))
    checkAnswer(
      df.selectExpr("""conv("9223372036854775807", 36, -16)"""), Row("-1")) // for overflow
  }

  test("floor") {
    testOneToOneMathFunction(floor, (d: Double) => math.floor(d).toLong)
  }

  test("factorial") {
    val df = (0 to 5).map(i => (i, i)).toDF("a", "b")
    checkAnswer(
      df.select(factorial('a)),
      Seq(Row(1), Row(1), Row(2), Row(6), Row(24), Row(120))
    )
    checkAnswer(
      df.selectExpr("factorial(a)"),
      Seq(Row(1), Row(1), Row(2), Row(6), Row(24), Row(120))
    )
  }

  test("rint") {
    testOneToOneMathFunction(rint, math.rint)
  }

  test("round/bround") {
    val df = Seq(5, 55, 555).map(Tuple1(_)).toDF("a")
    checkAnswer(
      df.select(round('a), round('a, -1), round('a, -2)),
      Seq(Row(5, 10, 0), Row(55, 60, 100), Row(555, 560, 600))
    )
    checkAnswer(
      df.select(bround('a), bround('a, -1), bround('a, -2)),
      Seq(Row(5, 0, 0), Row(55, 60, 100), Row(555, 560, 600))
    )

    val pi = "3.1415"
    checkAnswer(
      sql(s"SELECT round($pi, -3), round($pi, -2), round($pi, -1), " +
        s"round($pi, 0), round($pi, 1), round($pi, 2), round($pi, 3)"),
      Seq(Row(BigDecimal("0E3"), BigDecimal("0E2"), BigDecimal("0E1"), BigDecimal(3),
        BigDecimal("3.1"), BigDecimal("3.14"), BigDecimal("3.142")))
    )
    checkAnswer(
      sql(s"SELECT bround($pi, -3), bround($pi, -2), bround($pi, -1), " +
        s"bround($pi, 0), bround($pi, 1), bround($pi, 2), bround($pi, 3)"),
      Seq(Row(BigDecimal("0E3"), BigDecimal("0E2"), BigDecimal("0E1"), BigDecimal(3),
        BigDecimal("3.1"), BigDecimal("3.14"), BigDecimal("3.142")))
    )
  }

  test("exp") {
    testOneToOneMathFunction(exp, math.exp)
  }

  test("expm1") {
    testOneToOneMathFunction(expm1, math.expm1)
  }

  test("signum / sign") {
    testOneToOneMathFunction[Double, Double](signum, math.signum)

    checkAnswer(
      sql("SELECT sign(10), signum(-11)"),
      Row(1, -1))
  }

  test("pow / power") {
    testTwoToOneMathFunction(pow, pow, math.pow)

    checkAnswer(
      sql("SELECT pow(1, 2), power(2, 1)"),
      Seq((1, 2)).toDF().select(pow(lit(1), lit(2)), pow(lit(2), lit(1)))
    )
  }

  test("hex") {
    val data = Seq((28, -28, 100800200404L, "hello")).toDF("a", "b", "c", "d")
    checkAnswer(data.select(hex('a)), Seq(Row("1C")))
    checkAnswer(data.select(hex('b)), Seq(Row("FFFFFFFFFFFFFFE4")))
    checkAnswer(data.select(hex('c)), Seq(Row("177828FED4")))
    checkAnswer(data.select(hex('d)), Seq(Row("68656C6C6F")))
    checkAnswer(data.selectExpr("hex(a)"), Seq(Row("1C")))
    checkAnswer(data.selectExpr("hex(b)"), Seq(Row("FFFFFFFFFFFFFFE4")))
    checkAnswer(data.selectExpr("hex(c)"), Seq(Row("177828FED4")))
    checkAnswer(data.selectExpr("hex(d)"), Seq(Row("68656C6C6F")))
    checkAnswer(data.selectExpr("hex(cast(d as binary))"), Seq(Row("68656C6C6F")))
  }

  test("unhex") {
    val data = Seq(("1C", "737472696E67")).toDF("a", "b")
    checkAnswer(data.select(unhex('a)), Row(Array[Byte](28.toByte)))
    checkAnswer(data.select(unhex('b)), Row("string".getBytes(StandardCharsets.UTF_8)))
    checkAnswer(data.selectExpr("unhex(a)"), Row(Array[Byte](28.toByte)))
    checkAnswer(data.selectExpr("unhex(b)"), Row("string".getBytes(StandardCharsets.UTF_8)))
    checkAnswer(data.selectExpr("""unhex("##")"""), Row(null))
    checkAnswer(data.selectExpr("""unhex("G123")"""), Row(null))
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
      sql("SELECT ln(0), ln(1), ln(1.5)"),
      Seq((1, 2)).toDF().select(logarithm(lit(0)), logarithm(lit(1)), logarithm(lit(1.5)))
    )
  }

  test("log10") {
    testOneToOneNonNegativeMathFunction(log10, math.log10)
  }

  test("log1p") {
    testOneToOneNonNegativeMathFunction(log1p, math.log1p)
  }

  test("shift left") {
    val df = Seq[(Long, Integer, Short, Byte, Integer, Integer)]((21, 21, 21, 21, 21, null))
      .toDF("a", "b", "c", "d", "e", "f")

    checkAnswer(
      df.select(
        shiftLeft('a, 1), shiftLeft('b, 1), shiftLeft('c, 1), shiftLeft('d, 1),
        shiftLeft('f, 1)),
        Row(42.toLong, 42, 42.toShort, 42.toByte, null))

    checkAnswer(
      df.selectExpr(
        "shiftLeft(a, 1)", "shiftLeft(b, 1)", "shiftLeft(b, 1)", "shiftLeft(d, 1)",
        "shiftLeft(f, 1)"),
      Row(42.toLong, 42, 42.toShort, 42.toByte, null))
  }

  test("shift right") {
    val df = Seq[(Long, Integer, Short, Byte, Integer, Integer)]((42, 42, 42, 42, 42, null))
      .toDF("a", "b", "c", "d", "e", "f")

    checkAnswer(
      df.select(
        shiftRight('a, 1), shiftRight('b, 1), shiftRight('c, 1), shiftRight('d, 1),
        shiftRight('f, 1)),
      Row(21.toLong, 21, 21.toShort, 21.toByte, null))

    checkAnswer(
      df.selectExpr(
        "shiftRight(a, 1)", "shiftRight(b, 1)", "shiftRight(c, 1)", "shiftRight(d, 1)",
        "shiftRight(f, 1)"),
      Row(21.toLong, 21, 21.toShort, 21.toByte, null))
  }

  test("shift right unsigned") {
    val df = Seq[(Long, Integer, Short, Byte, Integer, Integer)]((-42, 42, 42, 42, 42, null))
      .toDF("a", "b", "c", "d", "e", "f")

    checkAnswer(
      df.select(
        shiftRightUnsigned('a, 1), shiftRightUnsigned('b, 1), shiftRightUnsigned('c, 1),
        shiftRightUnsigned('d, 1), shiftRightUnsigned('f, 1)),
      Row(9223372036854775787L, 21, 21.toShort, 21.toByte, null))

    checkAnswer(
      df.selectExpr(
        "shiftRightUnsigned(a, 1)", "shiftRightUnsigned(b, 1)", "shiftRightUnsigned(c, 1)",
        "shiftRightUnsigned(d, 1)", "shiftRightUnsigned(f, 1)"),
      Row(9223372036854775787L, 21, 21.toShort, 21.toByte, null))
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

    checkAnswer(
      sql("select abs(0), abs(-1), abs(123), abs(-9223372036854775807), abs(9223372036854775807)"),
      Row(0, 1, 123, 9223372036854775807L, 9223372036854775807L)
    )

    checkAnswer(
      sql("select abs(0.0), abs(-3.14159265), abs(3.14159265)"),
      Row(BigDecimal("0.0"), BigDecimal("3.14159265"), BigDecimal("3.14159265"))
    )
  }

  test("log2") {
    val df = Seq((1, 2)).toDF("a", "b")
    checkAnswer(
      df.select(log2("b") + log2("a")),
      Row(1))

    checkAnswer(sql("SELECT LOG2(8), LOG2(null)"), Row(3, null))
  }

  test("sqrt") {
    val df = Seq((1, 4)).toDF("a", "b")
    checkAnswer(
      df.select(sqrt("a"), sqrt("b")),
      Row(1.0, 2.0))

    checkAnswer(sql("SELECT SQRT(4.0), SQRT(null)"), Row(2.0, null))
    checkAnswer(df.selectExpr("sqrt(a)", "sqrt(b)", "sqrt(null)"), Row(1.0, 2.0, null))
  }

  test("negative") {
    checkAnswer(
      sql("SELECT negative(1), negative(0), negative(-1)"),
      Row(-1, 0, 1))
  }

  test("positive") {
    val df = Seq((1, -1, "abc")).toDF("a", "b", "c")
    checkAnswer(df.selectExpr("positive(a)"), Row(1))
    checkAnswer(df.selectExpr("positive(b)"), Row(-1))
  }
}
