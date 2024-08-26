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
import java.sql.Date
import java.text.SimpleDateFormat
import java.time.Period
import java.util.Locale

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{log => logarithm}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

private object MathFunctionsTestData {
  case class DoubleData(a: java.lang.Double, b: java.lang.Double)
  case class NullDoubles(a: java.lang.Double)
}

class MathFunctionsSuite extends QueryTest with SharedSparkSession {
  import MathFunctionsTestData._
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
      doubleData.select(c($"a")),
      (1 to 10).map(n => Row(f((n * 0.2 - 1).asInstanceOf[T])))
    )

    checkAnswer(
      doubleData.select(c($"b")),
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
      nnDoubleData.select(c($"a")),
      (1 to 10).map(n => Row(f(n * 0.1)))
    )

    if (f(-1) === StrictMath.log1p(-1)) {
      checkAnswer(
        nnDoubleData.select(c($"b")),
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
      nnDoubleData.select(c($"a", $"a")),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), r.getDouble(0))))
    )

    checkAnswer(
      nnDoubleData.select(c($"a", $"b")),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), r.getDouble(1))))
    )

    checkAnswer(
      nnDoubleData.select(d($"a", 2.0)),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), 2.0)))
    )

    checkAnswer(
      nnDoubleData.select(d($"a", -0.5)),
      nnDoubleData.collect().toSeq.map(r => Row(f(r.getDouble(0), -0.5)))
    )

    val nonNull = nullDoubles.collect().toSeq.filter(r => r.get(0) != null)

    checkAnswer(
      nullDoubles.select(c($"a", $"a")).orderBy($"a".asc),
      Row(null) +: nonNull.map(r => Row(f(r.getDouble(0), r.getDouble(0))))
    )
  }

  test("sin") {
    testOneToOneMathFunction(sin, math.sin)
  }

  test("csc") {
    testOneToOneMathFunction(csc,
      (x: Double) => (1 / math.sin(x)) )
  }

  test("asin") {
    testOneToOneMathFunction(asin, math.asin)
  }

  test("sinh") {
    testOneToOneMathFunction(sinh, math.sinh)
  }

  test("asinh") {
    testOneToOneMathFunction(asinh,
      (x: Double) => math.log(x + math.sqrt(x * x + 1)) )
  }

  test("cos") {
    testOneToOneMathFunction(cos, math.cos)
  }

  test("sec") {
    testOneToOneMathFunction(sec,
      (x: Double) => (1 / math.cos(x)) )
  }

  test("acos") {
    testOneToOneMathFunction(acos, math.acos)
  }

  test("cosh") {
    testOneToOneMathFunction(cosh, math.cosh)
  }

  test("acosh") {
    testOneToOneMathFunction(acosh,
      (x: Double) => math.log(x + math.sqrt(x * x - 1)) )
  }

  test("tan") {
    testOneToOneMathFunction(tan, math.tan)
  }

  test("cot") {
    testOneToOneMathFunction(cot,
      (x: Double) => (1 / math.tan(x)) )
  }

  test("atan") {
    testOneToOneMathFunction(atan, math.atan)
  }

  test("tanh") {
    testOneToOneMathFunction(tanh, math.tanh)
  }

  test("atanh") {
    testOneToOneMathFunction(atanh,
      (x: Double) => (0.5 * (math.log1p(x) - math.log1p(-x))) )
  }

  test("degrees") {
    testOneToOneMathFunction(degrees, math.toDegrees)
    checkAnswer(
      sql("SELECT degrees(0), degrees(1), degrees(1.5)"),
      Seq((1, 2)).toDF().select(degrees(lit(0)), degrees(lit(1)), degrees(lit(1.5)))
    )
  }

  test("radians") {
    testOneToOneMathFunction(radians, math.toRadians)
    checkAnswer(
      sql("SELECT radians(0), radians(1), radians(1.5)"),
      Seq((1, 2)).toDF().select(radians(lit(0)), radians(lit(1)), radians(lit(1.5)))
    )
  }

  test("cbrt") {
    testOneToOneMathFunction(cbrt, math.cbrt)
  }

  test("ceil and ceiling") {
    testOneToOneMathFunction(ceil, (d: Double) => math.ceil(d).toLong)
    // testOneToOneMathFunction does not validate the resulting data type
    assert(
      spark.range(1).select(ceil(col("id")).alias("a")).schema ==
          types.StructType(Seq(types.StructField("a", types.LongType))))
    assert(
      spark.range(1).select(ceil(col("id"), lit(0)).alias("a")).schema ==
          types.StructType(Seq(types.StructField("a", types.DecimalType(21, 0)))))
    checkAnswer(
      sql("SELECT ceiling(0), ceiling(1), ceiling(1.5)"),
      Row(0L, 1L, 2L))
  }

  test("conv") {
    val df = Seq(("333", 10, 2)).toDF("num", "fromBase", "toBase")
    checkAnswer(df.select(conv($"num", 10, 16)), Row("14D"))
    checkAnswer(df.select(conv(lit(100), 2, 16)), Row("4"))
    checkAnswer(df.select(conv(lit(3122234455L), 10, 16)), Row("BA198457"))
    checkAnswer(df.selectExpr("conv(num, fromBase, toBase)"), Row("101001101"))
    checkAnswer(df.selectExpr("""conv("100", 2, 10)"""), Row("4"))
    checkAnswer(df.selectExpr("""conv("-10", 16, -10)"""), Row("-16"))
  }

  test("SPARK-33428 conv function should trim input string") {
    val df = Seq(("abc"), ("  abc"), ("abc  "), ("  abc  ")).toDF("num")
    checkAnswer(df.select(conv($"num", 16, 10)),
      Seq(Row("2748"), Row("2748"), Row("2748"), Row("2748")))
    checkAnswer(df.select(conv($"num", 16, -10)),
      Seq(Row("2748"), Row("2748"), Row("2748"), Row("2748")))
  }

  test("SPARK-33428 conv function shouldn't raise error if input string is too big") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> false.toString) {
      val df = Seq((
        "aaaaaaa0aaaaaaa0aaaaaaa0aaaaaaa0aaaaaaa0aaaaaaa0aaaaaaa0aaaaaaa0aaaaaaa0")).toDF("num")
      checkAnswer(df.select(conv($"num", 16, 10)), Row("18446744073709551615"))
      checkAnswer(df.select(conv($"num", 16, -10)), Row("-1"))
    }
  }

  test("SPARK-36229 inconsistently behaviour where returned value is above the 64 char threshold") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> false.toString) {
      val df = Seq(("?" * 64), ("?" * 65), ("a" * 4 + "?" * 60), ("a" * 4 + "?" * 61)).toDF("num")
      val expectedResult = Seq(Row("0"), Row("0"), Row("43690"), Row("43690"))
      checkAnswer(df.select(conv($"num", 16, 10)), expectedResult)
      checkAnswer(df.select(conv($"num", 16, -10)), expectedResult)
    }
  }

  test("SPARK-36229 conv should return result equal to -1 in base of toBase") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> false.toString) {
      val df = Seq(("aaaaaaa0aaaaaaa0a"), ("aaaaaaa0aaaaaaa0")).toDF("num")
      checkAnswer(df.select(conv($"num", 16, 10)),
        Seq(Row("18446744073709551615"), Row("12297829339523361440")))
      checkAnswer(df.select(conv($"num", 16, -10)), Seq(Row("-1"), Row("-6148914734186190176")))
    }
  }

  test("SPARK-44973: conv must allocate enough space for all digits plus negative sign") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> false.toString) {
      val df = Seq(
        ((BigInt(Long.MaxValue) + 1).toString(16)),
        (BigInt(Long.MinValue).toString(16))
      ).toDF("num")
      checkAnswer(df.select(conv($"num", 16, -2)),
        Seq(Row(BigInt(Long.MinValue).toString(2)), Row(BigInt(Long.MinValue).toString(2))))
    }
  }

  test("floor") {
    testOneToOneMathFunction(floor, (d: Double) => math.floor(d).toLong)
    // testOneToOneMathFunction does not validate the resulting data type
    assert(
      spark.range(1).select(floor(col("id")).alias("a")).schema ==
          types.StructType(Seq(types.StructField("a", types.LongType))))
    assert(
      spark.range(1).select(floor(col("id"), lit(0)).alias("a")).schema ==
          types.StructType(Seq(types.StructField("a", types.DecimalType(21, 0)))))
  }

  test("factorial") {
    val df = (0 to 5).map(i => (i, i)).toDF("a", "b")
    checkAnswer(
      df.select(factorial($"a")),
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

  test("round/bround/ceil/floor") {
    val df = Seq(5, 55, 555).map(Tuple1(_)).toDF("a")
    checkAnswer(
      df.select(round($"a"), round($"a", -1), round($"a", -2)),
      Seq(Row(5, 10, 0), Row(55, 60, 100), Row(555, 560, 600))
    )
    checkAnswer(
      df.select(bround($"a"), bround($"a", -1), bround($"a", -2)),
      Seq(Row(5, 0, 0), Row(55, 60, 100), Row(555, 560, 600))
    )
    checkAnswer(
      df.select(ceil($"a"), ceil($"a", lit(-1)), ceil($"a", lit(-2))),
      Seq(Row(5, 10, 100), Row(55, 60, 100), Row(555, 560, 600))
    )
    checkAnswer(
      df.select(floor($"a"), floor($"a", lit(-1)), floor($"a", lit(-2))),
      Seq(Row(5, 0, 0), Row(55, 50, 0), Row(555, 550, 500))
    )

    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
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
      checkAnswer(
        sql(s"SELECT ceil($pi), ceil($pi, -3), ceil($pi, -2), ceil($pi, -1), " +
          s"ceil($pi, 0), ceil($pi, 1), ceil($pi, 2), ceil($pi, 3)"),
        Seq(Row(BigDecimal(4), BigDecimal("1E3"), BigDecimal("1E2"), BigDecimal("1E1"),
          BigDecimal(4), BigDecimal("3.2"), BigDecimal("3.15"), BigDecimal("3.142")))
      )
      checkAnswer(
        sql(s"SELECT floor($pi), floor($pi, -3), floor($pi, -2), floor($pi, -1), " +
          s"floor($pi, 0), floor($pi, 1), floor($pi, 2), floor($pi, 3)"),
        Seq(Row(BigDecimal(3), BigDecimal("0E3"), BigDecimal("0E2"), BigDecimal("0E1"),
          BigDecimal(3), BigDecimal("3.1"), BigDecimal("3.14"), BigDecimal("3.141")))
      )
    }

    val bdPi: BigDecimal = BigDecimal(31415925L, 7)
    checkAnswer(
      sql(s"SELECT round($bdPi, 7), round($bdPi, 8), round($bdPi, 9), round($bdPi, 10), " +
        s"round($bdPi, 100), round($bdPi, 6), round(null, 8)"),
      Seq(Row(bdPi, bdPi, bdPi, bdPi, bdPi, BigDecimal("3.141593"), null))
    )

    checkAnswer(
      sql(s"SELECT bround($bdPi, 7), bround($bdPi, 8), bround($bdPi, 9), bround($bdPi, 10), " +
        s"bround($bdPi, 100), bround($bdPi, 6), bround(null, 8)"),
      Seq(Row(bdPi, bdPi, bdPi, bdPi, bdPi, BigDecimal("3.141592"), null))
    )
    checkAnswer(
      sql(s"SELECT ceil($bdPi, 7), ceil($bdPi, 8), ceil($bdPi, 9), ceil($bdPi, 10), " +
        s"ceil($bdPi, 100), ceil($bdPi, 6), ceil(null, 8)"),
      Seq(Row(bdPi, bdPi, bdPi, bdPi, bdPi, BigDecimal("3.141593"), null))
    )

    checkAnswer(
      sql(s"SELECT floor($bdPi, 7), floor($bdPi, 8), floor($bdPi, 9), floor($bdPi, 10), " +
        s"floor($bdPi, 100), floor($bdPi, 6), floor(null, 8)"),
      Seq(Row(bdPi, bdPi, bdPi, bdPi, bdPi, BigDecimal("3.141592"), null))
    )
  }

  test("round/bround/ceil/floor with data frame from a local Seq of Product") {
    val df = spark.createDataFrame(Seq(Tuple1(BigDecimal("5.9")))).toDF("value")
    checkAnswer(
      df.withColumn("value_rounded", round($"value")),
      Seq(Row(BigDecimal("5.9"), BigDecimal("6")))
    )
    checkAnswer(
      df.withColumn("value_brounded", bround($"value")),
      Seq(Row(BigDecimal("5.9"), BigDecimal("6")))
    )
    checkAnswer(
      df
        .withColumn("value_ceil", ceil($"value"))
        .withColumn("value_ceil1", ceil($"value", lit(0)))
        .withColumn("value_ceil2", ceil($"value", lit(1))),
      Seq(Row(BigDecimal("5.9"), BigDecimal("6"), BigDecimal("6"), BigDecimal("5.9")))
    )
    checkAnswer(
      df
        .withColumn("value_floor", floor($"value"))
        .withColumn("value_floor1", floor($"value", lit(0)))
        .withColumn("value_floor2", floor($"value", lit(1))),
      Seq(Row(BigDecimal("5.9"), BigDecimal("5"), BigDecimal("5"), BigDecimal("5.9")))
    )
  }

  test("round/bround/ceil/floor with table columns") {
    withTable("t") {
      Seq(BigDecimal("5.9")).toDF("i").write.saveAsTable("t")
      checkAnswer(
        sql("select i, round(i) from t"),
        Seq(Row(BigDecimal("5.9"), BigDecimal("6"))))
      checkAnswer(
        sql("select i, bround(i) from t"),
        Seq(Row(BigDecimal("5.9"), BigDecimal("6"))))
      checkAnswer(
        sql("select i, ceil(i) from t"),
        Seq(Row(BigDecimal("5.9"), BigDecimal("6"))))
      checkAnswer(
        sql("select i, ceil(i, 0) from t"),
        Seq(Row(BigDecimal("5.9"), BigDecimal("6"))))
      checkAnswer(
        sql("select i, ceil(i, 1) from t"),
        Seq(Row(BigDecimal("5.9"), BigDecimal("5.9"))))
      checkAnswer(
        sql("select i, floor(i) from t"),
        Seq(Row(BigDecimal("5.9"), BigDecimal("5"))))
      checkAnswer(
        sql("select i, floor(i, 0) from t"),
        Seq(Row(BigDecimal("5.9"), BigDecimal("5"))))
      checkAnswer(
        sql("select i, floor(i, 1) from t"),
        Seq(Row(BigDecimal("5.9"), BigDecimal("5.9"))))
    }
  }

  test("exp") {
    testOneToOneMathFunction(exp, StrictMath.exp)
  }

  test("expm1") {
    testOneToOneMathFunction(expm1, StrictMath.expm1)
  }

  test("signum / sign") {
    testOneToOneMathFunction[Double, Double](signum, math.signum)

    checkAnswer(
      sql("SELECT sign(10), signum(-11)"),
      Row(1, -1))

    checkAnswer(
      Seq((1, 2)).toDF().select(signum(lit(10)), signum(lit(-11))),
      Row(1, -1))
  }

  test("pow / power") {
    testTwoToOneMathFunction(pow, pow, StrictMath.pow)

    checkAnswer(
      sql("SELECT pow(1, 2), power(2, 1)"),
      Seq((1, 2)).toDF().select(pow(lit(1), lit(2)), pow(lit(2), lit(1)))
    )

    checkAnswer(
      sql("SELECT pow(1, 2), power(2, 1)"),
      Seq((1, 2)).toDF().select(power(lit(1), lit(2)), power(lit(2), lit(1)))
    )
  }

  test("hex") {
    val data = Seq((28, -28, 100800200404L, "hello")).toDF("a", "b", "c", "d")
    checkAnswer(data.select(hex($"a")), Seq(Row("1C")))
    checkAnswer(data.select(hex($"b")), Seq(Row("FFFFFFFFFFFFFFE4")))
    checkAnswer(data.select(hex($"c")), Seq(Row("177828FED4")))
    checkAnswer(data.select(hex($"d")), Seq(Row("68656C6C6F")))
    checkAnswer(data.selectExpr("hex(a)"), Seq(Row("1C")))
    checkAnswer(data.selectExpr("hex(b)"), Seq(Row("FFFFFFFFFFFFFFE4")))
    checkAnswer(data.selectExpr("hex(c)"), Seq(Row("177828FED4")))
    checkAnswer(data.selectExpr("hex(d)"), Seq(Row("68656C6C6F")))
    checkAnswer(data.selectExpr("hex(cast(d as binary))"), Seq(Row("68656C6C6F")))
  }

  test("unhex") {
    val data = Seq(("1C", "737472696E67")).toDF("a", "b")
    checkAnswer(data.select(unhex($"a")), Row(Array[Byte](28.toByte)))
    checkAnswer(data.select(unhex($"b")), Row("string".getBytes(StandardCharsets.UTF_8)))
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
    testOneToOneNonNegativeMathFunction(org.apache.spark.sql.functions.log, StrictMath.log)
    checkAnswer(
      sql("SELECT ln(0), ln(1), ln(1.5)"),
      Seq((1, 2)).toDF().select(logarithm(lit(0)), logarithm(lit(1)), logarithm(lit(1.5)))
    )
  }

  test("log10") {
    testOneToOneNonNegativeMathFunction(log10, StrictMath.log10)
  }

  test("log1p") {
    testOneToOneNonNegativeMathFunction(log1p, StrictMath.log1p)
  }

  test("shift left") {
    val df = Seq[(Long, Integer, Short, Byte, Integer, Integer)]((21, 21, 21, 21, 21, null))
      .toDF("a", "b", "c", "d", "e", "f")

    checkAnswer(
      df.select(
        shiftleft($"a", 1), shiftleft($"b", 1), shiftleft($"c", 1), shiftleft($"d", 1),
        shiftLeft($"f", 1)), // test deprecated one.
        Row(42.toLong, 42, 42.toShort, 42.toByte, null))

    checkAnswer(
      df.selectExpr(
        "shiftleft(a, 1)", "shiftleft(b, 1)", "shiftleft(b, 1)", "shiftleft(d, 1)",
        "shiftleft(f, 1)"),
      Row(42.toLong, 42, 42.toShort, 42.toByte, null))
  }

  test("shift right") {
    val df = Seq[(Long, Integer, Short, Byte, Integer, Integer)]((42, 42, 42, 42, 42, null))
      .toDF("a", "b", "c", "d", "e", "f")

    checkAnswer(
      df.select(
        shiftright($"a", 1), shiftright($"b", 1), shiftright($"c", 1), shiftright($"d", 1),
        shiftRight($"f", 1)), // test deprecated one.
      Row(21.toLong, 21, 21.toShort, 21.toByte, null))

    checkAnswer(
      df.selectExpr(
        "shiftright(a, 1)", "shiftright(b, 1)", "shiftright(c, 1)", "shiftright(d, 1)",
        "shiftright(f, 1)"),
      Row(21.toLong, 21, 21.toShort, 21.toByte, null))
  }

  test("shift right unsigned") {
    val df = Seq[(Long, Integer, Short, Byte, Integer, Integer)]((-42, 42, 42, 42, 42, null))
      .toDF("a", "b", "c", "d", "e", "f")

    checkAnswer(
      df.select(
        shiftrightunsigned($"a", 1), shiftrightunsigned($"b", 1), shiftrightunsigned($"c", 1),
        shiftrightunsigned($"d", 1), shiftRightUnsigned($"f", 1)), // test deprecated one.
      Row(9223372036854775787L, 21, 21.toShort, 21.toByte, null))

    checkAnswer(
      df.selectExpr(
        "shiftrightunsigned(a, 1)", "shiftrightunsigned(b, 1)", "shiftrightunsigned(c, 1)",
        "shiftrightunsigned(d, 1)", "shiftrightunsigned(f, 1)"),
      Row(9223372036854775787L, 21, 21.toShort, 21.toByte, null))
  }

  test("binary log") {
    val df = Seq[(Integer, Integer)]((123, null)).toDF("a", "b")
    checkAnswer(
      df.select(org.apache.spark.sql.functions.log("a"),
        org.apache.spark.sql.functions.log(2.0, "a"),
        org.apache.spark.sql.functions.log("b")),
      Row(StrictMath.log(123), StrictMath.log(123) / StrictMath.log(2), null))

    checkAnswer(
      df.selectExpr("log(a)", "log(2.0, a)", "log(b)"),
      Row(StrictMath.log(123), StrictMath.log(123) / StrictMath.log(2), null))
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

    checkAnswer(
      Seq((1, 2)).toDF().select(negative(lit(1)), negative(lit(0)), negative(lit(-1))),
      Row(-1, 0, 1))
  }

  test("positive") {
    val df = Seq((1, -1, "abc")).toDF("a", "b", "c")
    checkAnswer(df.selectExpr("positive(a)"), Row(1))
    checkAnswer(df.selectExpr("positive(b)"), Row(-1))

    checkAnswer(df.select(positive(col("a"))), Row(1))
    checkAnswer(df.select(positive(col("b"))), Row(-1))
  }

  test("SPARK-35926: Support YearMonthIntervalType in width-bucket function") {
    Seq(
      (Period.ofMonths(-1), Period.ofYears(0), Period.ofYears(10), 10) -> 0,
      (Period.ofMonths(0), Period.ofYears(0), Period.ofYears(10), 10) -> 1,
      (Period.ofMonths(13), Period.ofYears(0), Period.ofYears(10), 10) -> 2,
      (Period.ofYears(1), Period.ofYears(0), Period.ofYears(10), 10) -> 2,
      (Period.ofYears(1), Period.ofYears(0), Period.ofYears(1), 10) -> 11,
      (Period.ofMonths(Int.MaxValue), Period.ofYears(0), Period.ofYears(1), 10) -> 11,
      (Period.ofMonths(0), Period.ofMonths(Int.MinValue), Period.ofMonths(Int.MaxValue), 10) -> 6,
      (Period.ofMonths(-1), Period.ofMonths(Int.MinValue), Period.ofMonths(Int.MaxValue), 10) -> 5
    ).foreach { case ((value, start, end, num), expected) =>
      val df = Seq((value, start, end, num)).toDF("v", "s", "e", "n")
      checkAnswer(df.selectExpr("width_bucket(v, s, e, n)"), Row(expected))
      checkAnswer(df.select(width_bucket(col("v"), col("s"), col("e"), col("n"))), Row(expected))
    }
  }

  test("width_bucket with numbers") {
    val df1 = Seq(
      (5.3, 0.2, 10.6, 5), (-2.1, 1.3, 3.4, 3),
      (8.1, 0.0, 5.7, 4), (-0.9, 5.2, 0.5, 2)
    ).toDF("v", "min", "max", "n")

    checkAnswer(
      df1.selectExpr("width_bucket(v, min, max, n)"),
      df1.select(width_bucket(col("v"), col("min"), col("max"), col("n")))
    )
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)

  test("try_add") {
    val df = Seq((1982, 15)).toDF("birth", "age")

    checkAnswer(df.selectExpr("try_add(birth, age)"), Seq(Row(1997)))
    checkAnswer(df.select(try_add(col("birth"), col("age"))), Seq(Row(1997)))

    val d1 = Date.valueOf("2015-09-30")
    val d2 = Date.valueOf("2016-02-29")
    val df1 = Seq((1, d1), (2, d2)).toDF("i", "d")

    checkAnswer(df1.selectExpr("try_add(d, i)"),
      df1.select(try_add(col("d"), col("i"))))
    checkAnswer(df1.selectExpr(s"try_add(d, make_interval(i))"),
      df1.select(try_add(column("d"), make_interval(col("i")))))
    checkAnswer(df1.selectExpr(s"try_add(d, make_interval(0, 0, 0, i))"),
      df1.select(try_add(column("d"), make_interval(lit(0), lit(0), lit(0), col("i")))))
    checkAnswer(df1.selectExpr("try_add(make_interval(i), make_interval(i))"),
      df1.select(try_add(make_interval(col("i")), make_interval(col("i")))))
  }

  test("try_avg") {
    val df = Seq((1982, 15), (1990, 11)).toDF("birth", "age")

    checkAnswer(df.selectExpr("try_avg(age)"), Seq(Row(13)))
    checkAnswer(df.select(try_avg(col("age"))), Seq(Row(13)))
  }

  test("try_divide") {
    val df = Seq((2000, 10), (2050, 5)).toDF("birth", "age")

    checkAnswer(df.selectExpr("try_divide(birth, age)"), Seq(Row(200.0), Row(410.0)))
    checkAnswer(df.select(try_divide(col("birth"), col("age"))), Seq(Row(200.0), Row(410.0)))

    val df1 = Seq((1, 2)).toDF("year", "month")

    checkAnswer(df1.selectExpr(s"try_divide(make_interval(year, month), 2)"),
      df1.select(try_divide(make_interval(col("year"), col("month")), lit(2))))
    checkAnswer(df1.selectExpr(s"try_divide(make_interval(year, month), 0)"),
      df1.select(try_divide(make_interval(col("year"), col("month")), lit(0))))
  }

  test("try_mod") {
    val df = Seq((10, 3), (5, 5), (5, 0)).toDF("birth", "age")
    checkAnswer(df.selectExpr("try_mod(birth, age)"), Seq(Row(1), Row(0), Row(null)))

    val dfDecimal = Seq(
      (BigDecimal(10), BigDecimal(3)),
      (BigDecimal(5), BigDecimal(5)),
      (BigDecimal(5), BigDecimal(0))).toDF("birth", "age")
    checkAnswer(dfDecimal.selectExpr("try_mod(birth, age)"), Seq(Row(1), Row(0), Row(null)))
  }

  test("try_element_at") {
    val df = Seq((Array(1, 2, 3), 2)).toDF("a", "b")
    checkAnswer(df.selectExpr("try_element_at(a, b)"), Seq(Row(2)))
    checkAnswer(df.select(try_element_at(col("a"), col("b"))), Seq(Row(2)))
  }

  test("try_multiply") {
    val df = Seq((2, 3)).toDF("a", "b")

    checkAnswer(df.selectExpr("try_multiply(a, b)"), Seq(Row(6)))
    checkAnswer(df.select(try_multiply(col("a"), col("b"))), Seq(Row(6)))

    checkAnswer(df.selectExpr("try_multiply(make_interval(a), b)"),
      df.select(try_multiply(make_interval(col("a")), col("b"))))
  }

  test("try_subtract") {
    val df = Seq((2, 3)).toDF("a", "b")

    checkAnswer(df.selectExpr("try_subtract(a, b)"), Seq(Row(-1)))
    checkAnswer(df.select(try_subtract(col("a"), col("b"))), Seq(Row(-1)))

    val d1 = Date.valueOf("2015-09-30")
    val d2 = Date.valueOf("2016-02-29")
    val df1 = Seq((1, d1), (2, d2)).toDF("i", "d")

    checkAnswer(df1.selectExpr("try_subtract(d, i)"),
      df1.select(try_subtract(col("d"), col("i"))))
    checkAnswer(df1.selectExpr(s"try_subtract(d, make_interval(i))"),
      df1.select(try_subtract(col("d"), make_interval(col("i")))))
    checkAnswer(df1.selectExpr(s"try_subtract(d, make_interval(0, 0, 0, i))"),
      df1.select(try_subtract(col("d"), make_interval(lit(0), lit(0), lit(0), col("i")))))
    checkAnswer(df1.selectExpr("try_subtract(make_interval(i), make_interval(i))"),
      df1.select(try_subtract(make_interval(col("i")), make_interval(col("i")))))
  }

  test("try_sum") {
    val df = Seq((2, 3), (5, 6)).toDF("a", "b")

    checkAnswer(df.selectExpr("try_sum(a)"), Seq(Row(7)))
    checkAnswer(df.select(try_sum(col("a"))), Seq(Row(7)))
  }
}
