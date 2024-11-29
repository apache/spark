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

import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period}
import java.time.temporal.ChronoUnit
import java.util.Locale

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.UpdateFieldsBenchmark._
import org.apache.spark.sql.catalyst.expressions.{InSet, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{outstandingTimezonesIds, outstandingZoneIds}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.DAY
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

class ColumnExpressionSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private lazy val booleanData = {
    spark.createDataFrame(sparkContext.parallelize(
      Row(false, false) ::
      Row(false, true) ::
      Row(true, false) ::
      Row(true, true) :: Nil),
      StructType(Seq(StructField("a", BooleanType), StructField("b", BooleanType))))
  }

  private lazy val nullData = Seq(
    (Some(1), Some(1)), (Some(1), Some(2)), (Some(1), None), (None, None)).toDF("a", "b")

  test("column names with space") {
    val df = Seq((1, "a")).toDF("name with space", "name.with.dot")

    checkAnswer(
      df.select(df("name with space")),
      Row(1) :: Nil)

    checkAnswer(
      df.select($"name with space"),
      Row(1) :: Nil)

    checkAnswer(
      df.select(col("name with space")),
      Row(1) :: Nil)

    checkAnswer(
      df.select("name with space"),
      Row(1) :: Nil)

    checkAnswer(
      df.select(expr("`name with space`")),
      Row(1) :: Nil)
  }

  test("column names with dot") {
    val df = Seq((1, "a")).toDF("name with space", "name.with.dot").as("a")

    checkAnswer(
      df.select(df("`name.with.dot`")),
      Row("a") :: Nil)

    checkAnswer(
      df.select($"`name.with.dot`"),
      Row("a") :: Nil)

    checkAnswer(
      df.select(col("`name.with.dot`")),
      Row("a") :: Nil)

    checkAnswer(
      df.select("`name.with.dot`"),
      Row("a") :: Nil)

    checkAnswer(
      df.select(expr("`name.with.dot`")),
      Row("a") :: Nil)

    checkAnswer(
      df.select(df("a.`name.with.dot`")),
      Row("a") :: Nil)

    checkAnswer(
      df.select($"a.`name.with.dot`"),
      Row("a") :: Nil)

    checkAnswer(
      df.select(col("a.`name.with.dot`")),
      Row("a") :: Nil)

    checkAnswer(
      df.select("a.`name.with.dot`"),
      Row("a") :: Nil)

    checkAnswer(
      df.select(expr("a.`name.with.dot`")),
      Row("a") :: Nil)
  }

  test("alias and name") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")
    assert(df.select(df("a").as("b")).columns.head === "b")
    assert(df.select(df("a").alias("b")).columns.head === "b")
    assert(df.select(df("a").name("b")).columns.head === "b")
  }

  test("as propagates metadata") {
    val metadata = new MetadataBuilder
    metadata.putString("key", "value")
    val origCol = $"a".as("b", metadata.build())
    val newCol = origCol.as("c")
    assert(newCol.expr.asInstanceOf[NamedExpression].metadata.getString("key") === "value")
  }

  test("SPARK-34805: as propagates metadata from nested column") {
    val metadata = new MetadataBuilder
    metadata.putString("key", "value")
    val df = spark.createDataFrame(sparkContext.emptyRDD[Row],
      StructType(Seq(
        StructField("parent", StructType(Seq(
          StructField("child", StringType, metadata = metadata.build())
        ))))
      ))
    val newCol = df("parent.child")
    assert(newCol.expr.asInstanceOf[NamedExpression].metadata.getString("key") === "value")
  }

  test("collect on column produced by a binary operator") {
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")
    checkAnswer(df.select(df("a") + df("b")), Seq(Row(3)))
    checkAnswer(df.select(df("a") + df("b").as("c")), Seq(Row(3)))
  }

  test("star") {
    checkAnswer(testData.select($"*"), testData.collect().toSeq)
  }

  test("star qualified by data frame object") {
    val df = testData.toDF()
    val goldAnswer = df.collect().toSeq
    checkAnswer(df.select(df("*")), goldAnswer)

    val df1 = df.select(df("*"), lit("abcd").as("litCol"))
    checkAnswer(df1.select(df("*")), goldAnswer)
  }

  test("star qualified by table name") {
    checkAnswer(testData.as("testData").select($"testData.*"), testData.collect().toSeq)
  }

  test("SPARK-34199: star can be qualified by table name inside a non-count function") {
    checkAnswer(
      testData.as("testData").selectExpr("hash(testData.*)"),
      testData.as("testData").selectExpr("hash(testData.key, testData.value)")
    )
  }

  test("SPARK-34199: star cannot be qualified by table name inside a count function") {
    val e = intercept[AnalysisException] {
      testData.as("testData").selectExpr("count(testData.*)").collect()
    }
    assert(e.getMessage.contains(
      "count(testData.*) is not allowed. Please use count(*) or expand the columns manually"))
  }

  test("SPARK-34199: table star can be qualified inside a count function with multiple arguments") {
    checkAnswer(
      testData.as("testData").selectExpr("count(testData.*, testData.key)"),
      testData.as("testData").selectExpr("count(testData.key, testData.value, testData.key)")
    )
  }

  test("+") {
    checkAnswer(
      testData2.select($"a" + 1),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) + 1)))

    checkAnswer(
      testData2.select($"a" + $"b" + 2),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) + r.getInt(1) + 2)))
  }

  test("-") {
    checkAnswer(
      testData2.select($"a" - 1),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) - 1)))

    checkAnswer(
      testData2.select($"a" - $"b" - 2),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) - r.getInt(1) - 2)))
  }

  test("*") {
    checkAnswer(
      testData2.select($"a" * 10),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) * 10)))

    checkAnswer(
      testData2.select($"a" * $"b"),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) * r.getInt(1))))
  }

  test("/") {
    checkAnswer(
      testData2.select($"a" / 2),
      testData2.collect().toSeq.map(r => Row(r.getInt(0).toDouble / 2)))

    checkAnswer(
      testData2.select($"a" / $"b"),
      testData2.collect().toSeq.map(r => Row(r.getInt(0).toDouble / r.getInt(1))))
  }


  test("%") {
    checkAnswer(
      testData2.select($"a" % 2),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) % 2)))

    checkAnswer(
      testData2.select($"a" % $"b"),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) % r.getInt(1))))
  }

  test("unary -") {
    checkAnswer(
      testData2.select(-$"a"),
      testData2.collect().toSeq.map(r => Row(-r.getInt(0))))
  }

  test("unary !") {
    checkAnswer(
      complexData.select(!$"b"),
      complexData.collect().toSeq.map(r => Row(!r.getBoolean(3))))
  }

  test("isNull") {
    checkAnswer(
      nullStrings.toDF().where($"s".isNull),
      nullStrings.collect().toSeq.filter(r => r.getString(1) eq null))

    checkAnswer(
      sql("select isnull(null), isnull(1)"),
      Row(true, false))
  }

  test("isNotNull") {
    checkAnswer(
      nullStrings.toDF().where($"s".isNotNull),
      nullStrings.collect().toSeq.filter(r => r.getString(1) ne null))

    checkAnswer(
      sql("select isnotnull(null), isnotnull('a')"),
      Row(false, true))
  }

  test("isNaN") {
    val testData = spark.createDataFrame(sparkContext.parallelize(
      Row(Double.NaN, Float.NaN) ::
      Row(math.log(-1), math.log(-3).toFloat) ::
      Row(null, null) ::
      Row(Double.MaxValue, Float.MinValue):: Nil),
      StructType(Seq(StructField("a", DoubleType), StructField("b", FloatType))))

    checkAnswer(
      testData.select($"a".isNaN, $"b".isNaN),
      Row(true, true) :: Row(true, true) :: Row(false, false) :: Row(false, false) :: Nil)

    checkAnswer(
      testData.select(isnan($"a"), isnan($"b")),
      Row(true, true) :: Row(true, true) :: Row(false, false) :: Row(false, false) :: Nil)

    if (!conf.ansiEnabled) {
      checkAnswer(
        sql("select isnan(15), isnan('invalid')"),
        Row(false, false))
    }
  }

  test("nanvl") {
    withTempView("t") {
      val testData = spark.createDataFrame(sparkContext.parallelize(
        Row(null, 3.0, Double.NaN, Double.PositiveInfinity, 1.0f, 4) :: Nil),
        StructType(Seq(StructField("a", DoubleType), StructField("b", DoubleType),
          StructField("c", DoubleType), StructField("d", DoubleType),
          StructField("e", FloatType), StructField("f", IntegerType))))

      checkAnswer(
        testData.select(
          nanvl($"a", lit(5)), nanvl($"b", lit(10)), nanvl(lit(10), $"b"),
          nanvl($"c", lit(null).cast(DoubleType)), nanvl($"d", lit(10)),
          nanvl($"b", $"e"), nanvl($"e", $"f")),
        Row(null, 3.0, 10.0, null, Double.PositiveInfinity, 3.0, 1.0)
      )
      testData.createOrReplaceTempView("t")
      checkAnswer(
        sql(
          "select nanvl(a, 5), nanvl(b, 10), nanvl(10, b), nanvl(c, null), nanvl(d, 10), " +
            " nanvl(b, e), nanvl(e, f) from t"),
        Row(null, 3.0, 10.0, null, Double.PositiveInfinity, 3.0, 1.0)
      )
    }
  }

  test("===") {
    checkAnswer(
      testData2.filter($"a" === 1),
      testData2.collect().toSeq.filter(r => r.getInt(0) == 1))

    checkAnswer(
      testData2.filter($"a" === $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) == r.getInt(1)))
  }

  test("<=>") {
    checkAnswer(
      nullData.filter($"b" <=> 1),
      Row(1, 1) :: Nil)

    checkAnswer(
      nullData.filter($"b" <=> null),
      Row(1, null) :: Row(null, null) :: Nil)

    checkAnswer(
      nullData.filter($"a" <=> $"b"),
      Row(1, 1) :: Row(null, null) :: Nil)

    val nullData2 = spark.createDataFrame(sparkContext.parallelize(
        Row("abc") ::
        Row(null)  ::
        Row("xyz") :: Nil),
        StructType(Seq(StructField("a", StringType, true))))

    checkAnswer(
      nullData2.filter($"a" <=> null),
      Row(null) :: Nil)
  }

  test("=!=") {
    checkAnswer(
      nullData.filter($"b" =!= 1),
      Row(1, 2) :: Nil)

    checkAnswer(nullData.filter($"b" =!= null), Nil)

    checkAnswer(
      nullData.filter($"a" =!= $"b"),
      Row(1, 2) :: Nil)
  }

  test(">") {
    checkAnswer(
      testData2.filter($"a" > 1),
      testData2.collect().toSeq.filter(r => r.getInt(0) > 1))

    checkAnswer(
      testData2.filter($"a" > $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) > r.getInt(1)))
  }

  test(">=") {
    checkAnswer(
      testData2.filter($"a" >= 1),
      testData2.collect().toSeq.filter(r => r.getInt(0) >= 1))

    checkAnswer(
      testData2.filter($"a" >= $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) >= r.getInt(1)))
  }

  test("<") {
    checkAnswer(
      testData2.filter($"a" < 2),
      testData2.collect().toSeq.filter(r => r.getInt(0) < 2))

    checkAnswer(
      testData2.filter($"a" < $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) < r.getInt(1)))
  }

  test("<=") {
    checkAnswer(
      testData2.filter($"a" <= 2),
      testData2.collect().toSeq.filter(r => r.getInt(0) <= 2))

    checkAnswer(
      testData2.filter($"a" <= $"b"),
      testData2.collect().toSeq.filter(r => r.getInt(0) <= r.getInt(1)))
  }

  test("between") {
    val testData = sparkContext.parallelize(
      (0, 1, 2) ::
      (1, 2, 3) ::
      (2, 1, 0) ::
      (2, 2, 4) ::
      (3, 1, 6) ::
      (3, 2, 0) :: Nil).toDF("a", "b", "c")
    val expectAnswer = testData.collect().toSeq.
      filter(r => r.getInt(0) >= r.getInt(1) && r.getInt(0) <= r.getInt(2))

    checkAnswer(testData.filter($"a".between($"b", $"c")), expectAnswer)
  }

  test("in") {
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
    checkAnswer(df.filter($"a".isin(1, 2)),
      df.collect().toSeq.filter(r => r.getInt(0) == 1 || r.getInt(0) == 2))
    checkAnswer(df.filter($"a".isin(3, 2)),
      df.collect().toSeq.filter(r => r.getInt(0) == 3 || r.getInt(0) == 2))
    checkAnswer(df.filter($"a".isin(3, 1)),
      df.collect().toSeq.filter(r => r.getInt(0) == 3 || r.getInt(0) == 1))
    checkAnswer(df.filter($"b".isin("y", "x")),
      df.collect().toSeq.filter(r => r.getString(1) == "y" || r.getString(1) == "x"))
    checkAnswer(df.filter($"b".isin("z", "x")),
      df.collect().toSeq.filter(r => r.getString(1) == "z" || r.getString(1) == "x"))
    checkAnswer(df.filter($"b".isin("z", "y")),
      df.collect().toSeq.filter(r => r.getString(1) == "z" || r.getString(1) == "y"))

    // Auto casting should work with mixture of different types in collections
    checkAnswer(df.filter($"a".isin(1.toShort, "2")),
      df.collect().toSeq.filter(r => r.getInt(0) == 1 || r.getInt(0) == 2))
    checkAnswer(df.filter($"a".isin("3", 2.toLong)),
      df.collect().toSeq.filter(r => r.getInt(0) == 3 || r.getInt(0) == 2))
    checkAnswer(df.filter($"a".isin(3, "1")),
      df.collect().toSeq.filter(r => r.getInt(0) == 3 || r.getInt(0) == 1))

    val df2 = Seq((1, Seq(1)), (2, Seq(2)), (3, Seq(3))).toDF("a", "b")
    checkError(
      exception = intercept[AnalysisException] {
        df2.filter($"a".isin($"b"))
      },
      condition = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
      parameters = Map(
        "functionName" -> "`in`",
        "dataType" -> "[\"INT\", \"ARRAY<INT>\"]",
        "sqlExpr" -> "\"(a IN (b))\""),
      context = ExpectedContext(fragment = "isin", callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("IN/INSET with bytes, shorts, ints, dates") {
    def check(): Unit = {
      val values = Seq(
        (Byte.MinValue, Some(Short.MinValue), Int.MinValue, Date.valueOf("2017-01-01")),
        (Byte.MaxValue, None, Int.MaxValue, null))
      val df = values.toDF("b", "s", "i", "d")
      checkAnswer(df.select($"b".isin(Byte.MinValue, Byte.MaxValue)), Seq(Row(true), Row(true)))
      checkAnswer(df.select($"b".isin(-1.toByte, 2.toByte)), Seq(Row(false), Row(false)))
      checkAnswer(df.select($"s".isin(Short.MinValue, 1.toShort)), Seq(Row(true), Row(null)))
      checkAnswer(df.select($"s".isin(0.toShort, null)), Seq(Row(null), Row(null)))
      checkAnswer(df.select($"i".isin(0, Int.MinValue)), Seq(Row(true), Row(false)))
      checkAnswer(df.select($"i".isin(null, Int.MinValue)), Seq(Row(true), Row(null)))
      checkAnswer(
        df.select($"d".isin(Date.valueOf("1950-01-01"), Date.valueOf("2017-01-01"))),
        Seq(Row(true), Row(null)))
      checkAnswer(
        df.select($"d".isin(Date.valueOf("1950-01-01"), null)),
        Seq(Row(null), Row(null)))
    }

    withSQLConf(SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "10") {
      check()
    }

    withSQLConf(
      SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "0",
      SQLConf.OPTIMIZER_INSET_SWITCH_THRESHOLD.key -> "0") {
      check()
    }

    withSQLConf(
      SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "0",
      SQLConf.OPTIMIZER_INSET_SWITCH_THRESHOLD.key -> "20") {
      check()
    }
  }

  test("isInCollection: Scala Collection") {
    Seq(0, 1, 10).foreach { optThreshold =>
      Seq(0, 1, 10).foreach { switchThreshold =>
        withSQLConf(
          SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> optThreshold.toString,
          SQLConf.OPTIMIZER_INSET_SWITCH_THRESHOLD.key -> switchThreshold.toString) {
          val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
          // Test with different types of collections
          checkAnswer(df.filter($"a".isInCollection(Seq(3, 1))),
            df.collect().toSeq.filter(r => r.getInt(0) == 3 || r.getInt(0) == 1))
          checkAnswer(df.filter($"a".isInCollection(Seq(1, 2).toSet)),
            df.collect().toSeq.filter(r => r.getInt(0) == 1 || r.getInt(0) == 2))
          checkAnswer(df.filter($"a".isInCollection(Seq(3, 2).toArray)),
            df.collect().toSeq.filter(r => r.getInt(0) == 3 || r.getInt(0) == 2))
          checkAnswer(df.filter($"a".isInCollection(Seq(3, 1).toList)),
            df.collect().toSeq.filter(r => r.getInt(0) == 3 || r.getInt(0) == 1))

          val df2 = Seq((1, Seq(1)), (2, Seq(2)), (3, Seq(3))).toDF("a", "b")
          checkError(
            exception = intercept[AnalysisException] {
              df2.filter($"a".isInCollection(Seq($"b")))
            },
            condition = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
            parameters = Map(
              "functionName" -> "`in`",
              "dataType" -> "[\"INT\", \"ARRAY<INT>\"]",
              "sqlExpr" -> "\"(a IN (b))\""),
            context = ExpectedContext(
              fragment = "isInCollection",
              callSitePattern = getCurrentClassCallSitePattern)
          )
        }
      }
    }
  }

  test("SPARK-31553: isInCollection - collection element types") {
    val expected = Seq(Row(true), Row(false))
    Seq(0, 1, 10).foreach { optThreshold =>
      Seq(0, 1, 10).foreach { switchThreshold =>
        withSQLConf(
          SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> optThreshold.toString,
          SQLConf.OPTIMIZER_INSET_SWITCH_THRESHOLD.key -> switchThreshold.toString) {
          checkAnswer(Seq(0).toDS().select($"value".isInCollection(Seq(null))), Seq(Row(null)))
          checkAnswer(
            Seq(true).toDS().select($"value".isInCollection(Seq(true, false))),
            Seq(Row(true)))
          checkAnswer(
            Seq(0.toByte, 1.toByte).toDS().select($"value".isInCollection(Seq(0.toByte, 2.toByte))),
            expected)
          checkAnswer(
            Seq(0.toShort, 1.toShort).toDS()
              .select($"value".isInCollection(Seq(0.toShort, 2.toShort))),
            expected)
          checkAnswer(Seq(0, 1).toDS().select($"value".isInCollection(Seq(0, 2))), expected)
          checkAnswer(Seq(0L, 1L).toDS().select($"value".isInCollection(Seq(0L, 2L))), expected)
          checkAnswer(Seq(0.0f, 1.0f).toDS()
            .select($"value".isInCollection(Seq(0.0f, 2.0f))), expected)
          checkAnswer(Seq(0.0D, 1.0D).toDS()
            .select($"value".isInCollection(Seq(0.0D, 2.0D))), expected)
          checkAnswer(
            Seq(BigDecimal(0), BigDecimal(2)).toDS()
              .select($"value".isInCollection(Seq(BigDecimal(0), BigDecimal(1)))),
            expected)
          checkAnswer(
            Seq("abc", "def").toDS().select($"value".isInCollection(Seq("abc", "xyz"))),
            expected)
          checkAnswer(
            Seq(Date.valueOf("2020-04-29"), Date.valueOf("2020-05-01")).toDS()
              .select($"value".isInCollection(
                Seq(Date.valueOf("2020-04-29"), Date.valueOf("2020-04-30")))),
            expected)
          checkAnswer(
            Seq(new Timestamp(0), new Timestamp(2)).toDS()
              .select($"value".isInCollection(Seq(new Timestamp(0), new Timestamp(1)))),
            expected)
          checkAnswer(
            Seq(Array("a", "b"), Array("c", "d")).toDS()
              .select($"value".isInCollection(Seq(Array("a", "b"), Array("x", "z")))),
            expected)
        }
      }
    }
  }

  test("&&") {
    checkAnswer(
      booleanData.filter($"a" && true),
      Row(true, false) :: Row(true, true) :: Nil)

    checkAnswer(
      booleanData.filter($"a" && false),
      Nil)

    checkAnswer(
      booleanData.filter($"a" && $"b"),
      Row(true, true) :: Nil)
  }

  test("||") {
    checkAnswer(
      booleanData.filter($"a" || true),
      booleanData.collect().toImmutableArraySeq)

    checkAnswer(
      booleanData.filter($"a" || false),
      Row(true, false) :: Row(true, true) :: Nil)

    checkAnswer(
      booleanData.filter($"a" || $"b"),
      Row(false, true) :: Row(true, false) :: Row(true, true) :: Nil)
  }

  test("SPARK-7321 when conditional statements") {
    val testData = (1 to 3).map(i => (i, i.toString)).toDF("key", "value")

    checkAnswer(
      testData.select(when($"key" === 1, -1).when($"key" === 2, -2).otherwise(0)),
      Seq(Row(-1), Row(-2), Row(0))
    )

    // Without the ending otherwise, return null for unmatched conditions.
    // Also test putting a non-literal value in the expression.
    checkAnswer(
      testData.select(when($"key" === 1, lit(0) - $"key").when($"key" === 2, -2)),
      Seq(Row(-1), Row(-2), Row(null))
    )

    // Test error handling for invalid expressions.
    intercept[IllegalArgumentException] { $"key".when($"key" === 1, -1) }
    intercept[IllegalArgumentException] { $"key".otherwise(-1) }
    intercept[IllegalArgumentException] { when($"key" === 1, -1).otherwise(-1).otherwise(-1) }
  }

  test("sqrt") {
    checkAnswer(
      testData.select(sqrt($"key")).orderBy($"key".asc),
      (1 to 100).map(n => Row(math.sqrt(n)))
    )

    checkAnswer(
      testData.select(sqrt($"value"), $"key").orderBy($"key".asc, $"value".asc),
      (1 to 100).map(n => Row(math.sqrt(n), n))
    )

    checkAnswer(
      testData.select(sqrt(lit(null))),
      (1 to 100).map(_ => Row(null))
    )
  }

  test("upper") {
    checkAnswer(
      lowerCaseData.select(upper($"l")),
      ('a' to 'd').map(c => Row(c.toString.toUpperCase(Locale.ROOT)))
    )

    checkAnswer(
      testData.select(upper($"value"), $"key"),
      (1 to 100).map(n => Row(n.toString, n))
    )

    checkAnswer(
      testData.select(upper(lit(null))),
      (1 to 100).map(n => Row(null))
    )

    checkAnswer(
      sql("SELECT upper('aB'), ucase('cDe')"),
      Row("AB", "CDE"))
  }

  test("lower") {
    checkAnswer(
      upperCaseData.select(lower($"L")),
      ('A' to 'F').map(c => Row(c.toString.toLowerCase(Locale.ROOT)))
    )

    checkAnswer(
      testData.select(lower($"value"), $"key"),
      (1 to 100).map(n => Row(n.toString, n))
    )

    checkAnswer(
      testData.select(lower(lit(null))),
      (1 to 100).map(n => Row(null))
    )

    checkAnswer(
      sql("SELECT lower('aB'), lcase('cDe')"),
      Row("ab", "cde"))
  }

  test("monotonically_increasing_id") {
    // Make sure we have 2 partitions, each with 2 records.
    val df = sparkContext.parallelize(Seq[Int](), 2).mapPartitions { _ =>
      Iterator(Tuple1(1), Tuple1(2))
    }.toDF("a")
    checkAnswer(
      df.select(monotonically_increasing_id(), expr("monotonically_increasing_id()")),
      Row(0L, 0L) ::
        Row(1L, 1L) ::
        Row((1L << 33) + 0L, (1L << 33) + 0L) ::
        Row((1L << 33) + 1L, (1L << 33) + 1L) :: Nil
    )
  }

  test("spark_partition_id") {
    // Make sure we have 2 partitions, each with 2 records.
    val df = sparkContext.parallelize(Seq[Int](), 2).mapPartitions { _ =>
      Iterator(Tuple1(1), Tuple1(2))
    }.toDF("a")
    checkAnswer(
      df.select(spark_partition_id()),
      Row(0) :: Row(0) :: Row(1) :: Row(1) :: Nil
    )
  }

  test("input_file_name, input_file_block_start, input_file_block_length - more than one source") {
    withTempView("tempView1") {
      withTable("tab1", "tab2") {
        val data = sparkContext.parallelize(0 to 9).toDF("id")
        data.write.saveAsTable("tab1")
        data.write.saveAsTable("tab2")
        data.createOrReplaceTempView("tempView1")
        Seq(
          ("input_file_name", 26),
          ("input_file_block_start", 33),
          ("input_file_block_length", 34)).foreach { case (f, e) =>
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"SELECT *, $f() FROM tab1 JOIN tab2 ON tab1.id = tab2.id")
            },
            condition = "MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION",
            parameters = Map("expr" -> s""""$f()""""),
            context = ExpectedContext(
              fragment = s"$f()",
              start = 10,
              stop = e)
          )
        }

        def checkResult(
            fromClause: String,
            exceptionExpected: Boolean,
            numExpectedRows: Int = 0): Unit = {
          val stmt = s"SELECT *, input_file_name() FROM ($fromClause)"
          if (exceptionExpected) {
            checkError(
              exception = intercept[AnalysisException] {
                sql(stmt)
              },
              condition = "MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION",
              parameters = Map("expr" -> """"input_file_name()""""),
              context = ExpectedContext(
                fragment = s"input_file_name()",
                start = 10,
                stop = 26)
            )
          } else {
            assert(sql(stmt).count() == numExpectedRows)
          }
        }

        checkResult(
          "SELECT * FROM tab1 UNION ALL SELECT * FROM tab2 UNION ALL SELECT * FROM tab2",
          exceptionExpected = false,
          numExpectedRows = 30)

        checkResult(
          "(SELECT * FROM tempView1 NATURAL JOIN tab2) UNION ALL SELECT * FROM tab2",
          exceptionExpected = false,
          numExpectedRows = 20)

        checkResult(
          "(SELECT * FROM tab1 UNION ALL SELECT * FROM tab2) NATURAL JOIN tempView1",
          exceptionExpected = false,
          numExpectedRows = 20)

        checkResult(
          "(SELECT * FROM tempView1 UNION ALL SELECT * FROM tab2) NATURAL JOIN tab2",
          exceptionExpected = true)

        checkResult(
          "(SELECT * FROM tab1 NATURAL JOIN tab2) UNION ALL SELECT * FROM tab2",
          exceptionExpected = true)

        checkResult(
          "(SELECT * FROM tab1 UNION ALL SELECT * FROM tab2) NATURAL JOIN tab2",
          exceptionExpected = true)
      }
    }
  }

  test("input_file_name, input_file_block_start, input_file_block_length - FileScanRDD") {
    withTempPath { dir =>
      val data = sparkContext.parallelize(0 to 10).toDF("id")
      data.write.parquet(dir.getCanonicalPath)

      // Test the 3 expressions when reading from files
      val q = spark.read.parquet(dir.getCanonicalPath).select(
        input_file_name(), expr("input_file_block_start()"), expr("input_file_block_length()"))
      val firstRow = q.head()
      assert(firstRow.getString(0).contains(dir.toURI.getPath))
      assert(firstRow.getLong(1) == 0)
      assert(firstRow.getLong(2) > 0)

      // Now read directly from the original RDD without going through any files to make sure
      // we are returning empty string, -1, and -1.
      checkAnswer(
        data.select(
          input_file_name(), expr("input_file_block_start()"), expr("input_file_block_length()")
        ).limit(1),
        Row("", -1L, -1L))
    }
  }

  test("input_file_name, input_file_block_start, input_file_block_length - HadoopRDD") {
    withTempPath { dir =>
      val data = sparkContext.parallelize((0 to 10).map(_.toString)).toDF()
      data.write.text(dir.getCanonicalPath)
      val df = spark.sparkContext.textFile(dir.getCanonicalPath).toDF()

      // Test the 3 expressions when reading from files
      val q = df.select(
        input_file_name(), expr("input_file_block_start()"), expr("input_file_block_length()"))
      val firstRow = q.head()
      assert(firstRow.getString(0).contains(dir.toURI.getPath))
      assert(firstRow.getLong(1) == 0)
      assert(firstRow.getLong(2) > 0)

      // Now read directly from the original RDD without going through any files to make sure
      // we are returning empty string, -1, and -1.
      checkAnswer(
        data.select(
          input_file_name(), expr("input_file_block_start()"), expr("input_file_block_length()")
        ).limit(1),
        Row("", -1L, -1L))
    }
  }

  test("input_file_name, input_file_block_start, input_file_block_length - NewHadoopRDD") {
    withTempPath { dir =>
      val data = sparkContext.parallelize((0 to 10).map(_.toString)).toDF()
      data.write.text(dir.getCanonicalPath)
      val rdd = spark.sparkContext.newAPIHadoopFile(
        dir.getCanonicalPath,
        classOf[NewTextInputFormat],
        classOf[LongWritable],
        classOf[Text])
      val df = rdd.map(pair => pair._2.toString).toDF()

      // Test the 3 expressions when reading from files
      val q = df.select(
        input_file_name(), expr("input_file_block_start()"), expr("input_file_block_length()"))
      val firstRow = q.head()
      assert(firstRow.getString(0).contains(dir.toURI.getPath))
      assert(firstRow.getLong(1) == 0)
      assert(firstRow.getLong(2) > 0)

      // Now read directly from the original RDD without going through any files to make sure
      // we are returning empty string, -1, and -1.
      checkAnswer(
        data.select(
          input_file_name(), expr("input_file_block_start()"), expr("input_file_block_length()")
        ).limit(1),
        Row("", -1L, -1L))
    }
  }

  test("columns can be compared") {
    assert($"key".desc == $"key".desc)
    assert($"key".desc != $"key".asc)
  }

  test("alias with metadata") {
    val metadata = new MetadataBuilder()
      .putString("originName", "value")
      .build()
    val schema = testData
      .select($"*", col("value").as("abc", metadata))
      .schema
    assert(schema("value").metadata === Metadata.empty)
    assert(schema("abc").metadata === metadata)
  }

  test("rand") {
    val randCol = testData.select($"key", rand(5L).as("rand"))
    randCol.columns.length should be (2)
    val rows = randCol.collect()
    rows.foreach { row =>
      assert(row.getDouble(1) <= 1.0)
      assert(row.getDouble(1) >= 0.0)
    }

    def checkNumProjects(df: DataFrame, expectedNumProjects: Int): Unit = {
      val projects = df.queryExecution.sparkPlan.collect {
        case tungstenProject: ProjectExec => tungstenProject
      }
      assert(projects.size === expectedNumProjects)
    }

    // We first create a plan with two Projects.
    // Project [rand + 1 AS rand1, rand - 1 AS rand2]
    //   Project [key, (Rand 5 + 1) AS rand]
    //     LogicalRDD [key, value]
    // Because Rand function is not deterministic, the column rand is not deterministic.
    // So, in the optimizer, we will not collapse Project [rand + 1 AS rand1, rand - 1 AS rand2]
    // and Project [key, Rand 5 AS rand]. The final plan still has two Projects.
    val dfWithTwoProjects =
      testData
        .select($"key", (rand(5L) + 1).as("rand"))
        .select(($"rand" + 1).as("rand1"), ($"rand" - 1).as("rand2"))
    checkNumProjects(dfWithTwoProjects, 2)

    // Now, we add one more project rand1 - rand2 on top of the query plan.
    // Since rand1 and rand2 are deterministic (they basically apply +/- to the generated
    // rand value), we can collapse rand1 - rand2 to the Project generating rand1 and rand2.
    // So, the plan will be optimized from ...
    // Project [(rand1 - rand2) AS (rand1 - rand2)]
    //   Project [rand + 1 AS rand1, rand - 1 AS rand2]
    //     Project [key, (Rand 5 + 1) AS rand]
    //       LogicalRDD [key, value]
    // to ...
    // Project [((rand + 1 AS rand1) - (rand - 1 AS rand2)) AS (rand1 - rand2)]
    //   Project [key, Rand 5 AS rand]
    //     LogicalRDD [key, value]
    val dfWithThreeProjects = dfWithTwoProjects.select($"rand1" - $"rand2")
    checkNumProjects(dfWithThreeProjects, 2)
    dfWithThreeProjects.collect().foreach { row =>
      assert(row.getDouble(0) === 2.0 +- 0.0001)
    }
  }

  test("randn") {
    val randCol = testData.select($"key", randn(5L).as("rand"))
    randCol.columns.length should be (2)
    val rows = randCol.collect()
    rows.foreach { row =>
      assert(row.getDouble(1) <= 4.0)
      assert(row.getDouble(1) >= -4.0)
    }
  }

  test("bitwiseAND") {
    checkAnswer(
      testData2.select($"a".bitwiseAND(75)),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) & 75)))

    checkAnswer(
      testData2.select($"a".bitwiseAND($"b").bitwiseAND(22)),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) & r.getInt(1) & 22)))
  }

  test("bitwiseOR") {
    checkAnswer(
      testData2.select($"a".bitwiseOR(170)),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) | 170)))

    checkAnswer(
      testData2.select($"a".bitwiseOR($"b").bitwiseOR(42)),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) | r.getInt(1) | 42)))
  }

  test("bitwiseXOR") {
    checkAnswer(
      testData2.select($"a".bitwiseXOR(112)),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) ^ 112)))

    checkAnswer(
      testData2.select($"a".bitwiseXOR($"b").bitwiseXOR(39)),
      testData2.collect().toSeq.map(r => Row(r.getInt(0) ^ r.getInt(1) ^ 39)))
  }

  test("SPARK-37646: lit") {
    assert(lit($"foo") == $"foo")
    assert(lit($"foo") == $"foo")
    assert(lit(1).expr == Literal(1))
    assert(lit(null).expr == Literal(null, NullType))
  }

  test("typedLit") {
    assert(typedLit($"foo") == $"foo")
    assert(typedLit($"foo") == $"foo")
    assert(typedLit(1).expr == Literal(1))
    assert(typedLit[String](null).expr == Literal(null, StringType))

    val df = Seq(Tuple1(0)).toDF("a")
    // Only check the types `lit` cannot handle
    checkAnswer(
      df.select(typedLit(Seq(1, 2, 3))),
      Row(Seq(1, 2, 3)) :: Nil)
    checkAnswer(
      df.select(typedLit(Map("a" -> 1, "b" -> 2))),
      Row(Map("a" -> 1, "b" -> 2)) :: Nil)
    checkAnswer(
      df.select(typedLit(("a", 2, 1.0))),
      Row(Row("a", 2, 1.0)) :: Nil)
  }

  test("SPARK-31563: sql of InSet for UTF8String collection") {
    val inSet = InSet(Literal("a"), Set("a", "b").map(UTF8String.fromString))
    assert(inSet.sql === "('a' IN ('a', 'b'))")
  }

  def checkAnswer(
      df: => DataFrame,
      expectedAnswer: Seq[Row],
      expectedSchema: StructType): Unit = {
    checkAnswer(df, expectedAnswer)
    assert(df.schema == expectedSchema)
  }

  private lazy val structType = StructType(Seq(
    StructField("a", IntegerType, nullable = false),
    StructField("b", IntegerType, nullable = true),
    StructField("c", IntegerType, nullable = false)))

  private lazy val structLevel1: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Row(Row(1, null, 3)) :: Nil),
    StructType(Seq(StructField("a", structType, nullable = false))))

  private lazy val nullableStructLevel1: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Row(null) :: Row(Row(1, null, 3)) :: Nil),
    StructType(Seq(StructField("a", structType, nullable = true))))

  private lazy val structLevel2: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Row(Row(Row(1, null, 3))) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", structType, nullable = false))),
        nullable = false))))

  private lazy val nullableStructLevel2: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Row(null) :: Row(Row(null)) :: Row(Row(Row(1, null, 3))) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", structType, nullable = true))),
        nullable = true))))

  private lazy val structLevel3: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Row(Row(Row(Row(1, null, 3)))) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", structType, nullable = false))),
          nullable = false))),
        nullable = false))))

  test("withField should throw an exception if called on a non-StructType column") {
    checkError(
      exception = intercept[AnalysisException] {
        testData.withColumn("key", $"key".withField("a", lit(2)))
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"update_fields(key, WithField(2))\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"key\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRUCT\""),
      context = ExpectedContext(
        fragment = "withField",
        callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("withField should throw an exception if either fieldName or col argument are null") {
    intercept[IllegalArgumentException] {
      structLevel1.withColumn("a", $"a".withField(null, lit(2)))
    }.getMessage should include("fieldName cannot be null")

    intercept[IllegalArgumentException] {
      structLevel1.withColumn("a", $"a".withField("b", null))
    }.getMessage should include("col cannot be null")

    intercept[IllegalArgumentException] {
      structLevel1.withColumn("a", $"a".withField(null, null))
    }.getMessage should include("fieldName cannot be null")
  }

  test("withField should throw an exception if any intermediate structs don't exist") {
    checkError(
      exception = intercept[AnalysisException] {
        structLevel2.withColumn("a", $"a".withField("x.b", lit(2)))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`x`", "fields" -> "`a`"))

    checkError(
      exception = intercept[AnalysisException] {
        structLevel3.withColumn("a", $"a".withField("a.x.b", lit(2)))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`x`", "fields" -> "`a`"))
  }

  test("withField should throw an exception if intermediate field is not a struct") {
    checkError(
      exception = intercept[AnalysisException] {
        structLevel1.withColumn("a", $"a".withField("b.a", lit(2)))
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"update_fields(a.b, WithField(2))\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"a.b\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRUCT\""),
      context = ExpectedContext(
        fragment = "withField",
        callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("withField should throw an exception if intermediate field reference is ambiguous") {
    checkError(
      exception = intercept[AnalysisException] {
        val structLevel2: DataFrame = spark.createDataFrame(
          sparkContext.parallelize(Row(Row(Row(1, null, 3), 4)) :: Nil),
          StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", structType, nullable = false),
              StructField("a", structType, nullable = false))),
              nullable = false))))

        structLevel2.withColumn("a", $"a".withField("a.b", lit(2)))
      },
      condition = "AMBIGUOUS_REFERENCE_TO_FIELDS",
      sqlState = "42000",
      parameters = Map("field" -> "`a`", "count" -> "2")
    )
  }

  test("withField should add field with no name") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("", lit(4))),
      Row(Row(1, null, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false),
          StructField("", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("withField should add field to struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(4))),
      Row(Row(1, null, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false),
          StructField("d", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("withField should add field to nullable struct") {
    checkAnswer(
      nullableStructLevel1.withColumn("a", $"a".withField("d", lit(4))),
      Row(null) :: Row(Row(1, null, 3, 4)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false),
          StructField("d", IntegerType, nullable = false))),
          nullable = true))))
  }

  test("withField should add field to nested nullable struct") {
    checkAnswer(
      nullableStructLevel2.withColumn("a", $"a".withField("a.d", lit(4))),
      Row(null) :: Row(Row(null)) :: Row(Row(Row(1, null, 3, 4))) :: Nil,
      StructType(
        Seq(StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("b", IntegerType, nullable = true),
            StructField("c", IntegerType, nullable = false),
            StructField("d", IntegerType, nullable = false))),
            nullable = true))),
          nullable = true))))
  }

  test("withField should add null field to struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(null).cast(IntegerType))),
      Row(Row(1, null, 3, null)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false),
          StructField("d", IntegerType, nullable = true))),
          nullable = false))))
  }

  test("withField should add multiple fields to struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(4)).withField("e", lit(5))),
      Row(Row(1, null, 3, 4, 5)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false),
          StructField("d", IntegerType, nullable = false),
          StructField("e", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("withField should add multiple fields to nullable struct") {
    checkAnswer(
      nullableStructLevel1.withColumn("a", $"a"
        .withField("d", lit(4)).withField("e", lit(5))),
      Row(null) :: Row(Row(1, null, 3, 4, 5)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false),
          StructField("d", IntegerType, nullable = false),
          StructField("e", IntegerType, nullable = false))),
          nullable = true))))
  }

  test("withField should add field to nested struct") {
    Seq(
      structLevel2.withColumn("a", $"a".withField("a.d", lit(4))),
      structLevel2.withColumn("a", $"a".withField("a", $"a.a".withField("d", lit(4))))
    ).foreach { df =>
      checkAnswer(
        df,
        Row(Row(Row(1, null, 3, 4))) :: Nil,
        StructType(
          Seq(StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = true),
              StructField("c", IntegerType, nullable = false),
              StructField("d", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))))
    }
  }

  test("withField should add multiple fields to nested struct") {
    Seq(
      col("a").withField("a", $"a.a".withField("d", lit(4)).withField("e", lit(5))),
      col("a").withField("a.d", lit(4)).withField("a.e", lit(5))
    ).foreach { column =>
      checkAnswer(
        structLevel2.select(column.as("a")),
        Row(Row(Row(1, null, 3, 4, 5))) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = true),
              StructField("c", IntegerType, nullable = false),
              StructField("d", IntegerType, nullable = false),
              StructField("e", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))))
    }
  }

  test("withField should add multiple fields to nested nullable struct") {
    Seq(
      col("a").withField("a", $"a.a".withField("d", lit(4)).withField("e", lit(5))),
      col("a").withField("a.d", lit(4)).withField("a.e", lit(5))
    ).foreach { column =>
      checkAnswer(
        nullableStructLevel2.select(column.as("a")),
        Row(null) :: Row(Row(null)) :: Row(Row(Row(1, null, 3, 4, 5))) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = true),
              StructField("c", IntegerType, nullable = false),
              StructField("d", IntegerType, nullable = false),
              StructField("e", IntegerType, nullable = false))),
              nullable = true))),
            nullable = true))))
    }
  }

  test("withField should add field to deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", $"a".withField("a.a.d", lit(4))),
      Row(Row(Row(Row(1, null, 3, 4)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = true),
              StructField("c", IntegerType, nullable = false),
              StructField("d", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))),
          nullable = false))))
  }

  test("withField should replace field in struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("b", lit(2))),
      Row(Row(1, 2, 3)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false),
          StructField("c", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("withField should replace field in nullable struct") {
    checkAnswer(
      nullableStructLevel1.withColumn("a", $"a".withField("b", lit("foo"))),
      Row(null) :: Row(Row(1, "foo", 3)) ::  Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", StringType, nullable = false),
          StructField("c", IntegerType, nullable = false))),
          nullable = true))))
  }

  test("withField should replace field in nested nullable struct") {
    checkAnswer(
      nullableStructLevel2.withColumn("a", $"a".withField("a.b", lit("foo"))),
      Row(null) :: Row(Row(null)) :: Row(Row(Row(1, "foo", 3))) :: Nil,
      StructType(
        Seq(StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("b", StringType, nullable = false),
            StructField("c", IntegerType, nullable = false))),
            nullable = true))),
          nullable = true))))
  }

  test("withField should replace field with null value in struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("c", lit(null).cast(IntegerType))),
      Row(Row(1, null, null)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = true))),
          nullable = false))))
  }

  test("withField should replace multiple fields in struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("a", lit(10)).withField("b", lit(20))),
      Row(Row(10, 20, 3)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false),
          StructField("c", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("withField should replace multiple fields in nullable struct") {
    checkAnswer(
      nullableStructLevel1.withColumn("a", $"a".withField("a", lit(10))
        .withField("b", lit(20))),
      Row(null) :: Row(Row(10, 20, 3)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false),
          StructField("c", IntegerType, nullable = false))),
          nullable = true))))
  }

  test("withField should replace field in nested struct") {
    Seq(
      structLevel2.withColumn("a", $"a".withField("a.b", lit(2))),
      structLevel2.withColumn("a", $"a".withField("a", $"a.a".withField("b", lit(2))))
    ).foreach { df =>
      checkAnswer(
        df,
        Row(Row(Row(1, 2, 3))) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false),
              StructField("c", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))))
    }
  }

  test("withField should replace multiple fields in nested struct") {
    Seq(
      col("a").withField("a", $"a.a".withField("a", lit(10)).withField("b", lit(20))),
      col("a").withField("a.a", lit(10)).withField("a.b", lit(20))
    ).foreach { column =>
      checkAnswer(
        structLevel2.select(column.as("a")),
        Row(Row(Row(10, 20, 3))) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false),
              StructField("c", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))))
    }
  }

  test("withField should replace multiple fields in nested nullable struct") {
    Seq(
      col("a").withField("a", $"a.a".withField("a", lit(10)).withField("b", lit(20))),
      col("a").withField("a.a", lit(10)).withField("a.b", lit(20))
    ).foreach { column =>
      checkAnswer(
        nullableStructLevel2.select(column.as("a")),
        Row(null) :: Row(Row(null)) :: Row(Row(Row(10, 20, 3))) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false),
              StructField("c", IntegerType, nullable = false))),
              nullable = true))),
            nullable = true))))
    }
  }

  test("withField should replace field in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", $"a".withField("a.a.b", lit(2))),
      Row(Row(Row(Row(1, 2, 3)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false),
              StructField("c", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))),
          nullable = false))))
  }

  test("withField should replace all fields with given name in struct") {
    val structLevel1 = spark.createDataFrame(
      sparkContext.parallelize(Row(Row(1, 2, 3)) :: Nil),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false))),
          nullable = false))))

    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("b", lit(100))),
      Row(Row(1, 100, 100)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("withField should replace fields in struct in given order") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("b", lit(2)).withField("b", lit(20))),
      Row(Row(1, 20, 3)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false),
          StructField("c", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("withField should add field and then replace same field in struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(4)).withField("d", lit(5))),
      Row(Row(1, null, 3, 5)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false),
          StructField("d", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("withField should handle fields with dots in their name if correctly quoted") {
    val df: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(Row(Row(Row(1, null, 3))) :: Nil),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a.b", StructType(Seq(
            StructField("c.d", IntegerType, nullable = false),
            StructField("e.f", IntegerType, nullable = true),
            StructField("g.h", IntegerType, nullable = false))),
            nullable = false))),
          nullable = false))))

    checkAnswer(
      df.withColumn("a", $"a".withField("`a.b`.`e.f`", lit(2))),
      Row(Row(Row(1, 2, 3))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a.b", StructType(Seq(
            StructField("c.d", IntegerType, nullable = false),
            StructField("e.f", IntegerType, nullable = false),
            StructField("g.h", IntegerType, nullable = false))),
            nullable = false))),
          nullable = false))))

    checkError(
      exception = intercept[AnalysisException] {
        df.withColumn("a", $"a".withField("a.b.e.f", lit(2)))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`a`", "fields" -> "`a`.`b`"))
  }

  private lazy val mixedCaseStructLevel1: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Row(Row(1, 1)) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("B", IntegerType, nullable = false))),
        nullable = false))))

  test("withField should replace field in struct even if casing is different") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkAnswer(
        mixedCaseStructLevel1.withColumn("a", $"a".withField("A", lit(2))),
        Row(Row(2, 1)) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("A", IntegerType, nullable = false),
            StructField("B", IntegerType, nullable = false))),
            nullable = false))))

      checkAnswer(
        mixedCaseStructLevel1.withColumn("a", $"a".withField("b", lit(2))),
        Row(Row(1, 2)) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("b", IntegerType, nullable = false))),
            nullable = false))))
    }
  }

  test("withField should add field to struct because casing is different") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        mixedCaseStructLevel1.withColumn("a", $"a".withField("A", lit(2))),
        Row(Row(1, 1, 2)) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("B", IntegerType, nullable = false),
            StructField("A", IntegerType, nullable = false))),
            nullable = false))))

      checkAnswer(
        mixedCaseStructLevel1.withColumn("a", $"a".withField("b", lit(2))),
        Row(Row(1, 1, 2)) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("B", IntegerType, nullable = false),
            StructField("b", IntegerType, nullable = false))),
            nullable = false))))
    }
  }

  private lazy val mixedCaseStructLevel2: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Row(Row(Row(1, 1), Row(1, 1))) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false))),
          nullable = false),
        StructField("B", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false))),
          nullable = false))),
        nullable = false))))

  test("withField should replace nested field in struct even if casing is different") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkAnswer(
        mixedCaseStructLevel2.withColumn("a", $"a".withField("A.a", lit(2))),
        Row(Row(Row(2, 1), Row(1, 1))) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("A", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false))),
              nullable = false),
            StructField("B", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))))

      checkAnswer(
        mixedCaseStructLevel2.withColumn("a", $"a".withField("b.a", lit(2))),
        Row(Row(Row(1, 1), Row(2, 1))) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false))),
              nullable = false),
            StructField("b", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))))
    }
  }

  test("withField should throw an exception because casing is different") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkError(
        exception = intercept[AnalysisException] {
          mixedCaseStructLevel2.withColumn("a", $"a".withField("A.a", lit(2)))
        },
        condition = "FIELD_NOT_FOUND",
        parameters = Map("fieldName" -> "`A`", "fields" -> "`a`, `B`"))

      checkError(
        exception = intercept[AnalysisException] {
          mixedCaseStructLevel2.withColumn("a", $"a".withField("b.a", lit(2)))
        },
        condition = "FIELD_NOT_FOUND",
        parameters = Map("fieldName" -> "`b`", "fields" -> "`a`, `B`"))
    }
  }

  test("withField user-facing examples") {
    checkAnswer(
      sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
        .select($"struct_col".withField("c", lit(3))),
      Row(Row(1, 2, 3)))

    checkAnswer(
      sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
        .select($"struct_col".withField("b", lit(3))),
      Row(Row(1, 3)))

    checkAnswer(
      sql("SELECT CAST(NULL AS struct<a:int,b:int>) struct_col")
        .select($"struct_col".withField("c", lit(3))),
      Row(null))

    checkAnswer(
      sql("SELECT named_struct('a', 1, 'b', 2, 'b', 3) struct_col")
        .select($"struct_col".withField("b", lit(100))),
      Row(Row(1, 100, 100)))

    checkAnswer(
      sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
        .select($"struct_col".withField("a.c", lit(3))),
      Row(Row(Row(1, 2, 3))))

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT named_struct('a', named_struct('b', 1), 'a', named_struct('c', 2)) struct_col")
          .select($"struct_col".withField("a.c", lit(3)))
      },
      condition = "AMBIGUOUS_REFERENCE_TO_FIELDS",
      sqlState = "42000",
      parameters = Map("field" -> "`a`", "count" -> "2")
    )

    checkAnswer(
      sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
        .select($"struct_col".withField("a.c", lit(3)).withField("a.d", lit(4))),
      Row(Row(Row(1, 2, 3, 4))))

    checkAnswer(
      sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
        .select($"struct_col".withField("a",
          $"struct_col.a".withField("c", lit(3)).withField("d", lit(4)))),
      Row(Row(Row(1, 2, 3, 4))))
  }

  test("SPARK-32641: extracting field from non-null struct column after withField should return " +
    "field value") {
    // extract newly added field
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(4)).getField("d")),
      Row(4) :: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = false))))

    // extract newly replaced field
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("a", lit(4)).getField("a")),
      Row(4) :: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = false))))

    // add new field, extract another field from original struct
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("d", lit(4)).getField("c")),
      Row(3):: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = false))))

    // replace field, extract another field from original struct
    checkAnswer(
      structLevel1.withColumn("a", $"a".withField("a", lit(4)).getField("c")),
      Row(3):: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = false))))
  }

  test("SPARK-32641: extracting field from null struct column after withField should return " +
    "null if the original struct was null") {
    val nullStructLevel1 = spark.createDataFrame(
      sparkContext.parallelize(Row(null) :: Nil),
      StructType(Seq(StructField("a", structType, nullable = true))))

    // extract newly added field
    checkAnswer(
      nullStructLevel1.withColumn("a", $"a".withField("d", lit(4)).getField("d")),
      Row(null) :: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = true))))

    // extract newly replaced field
    checkAnswer(
      nullStructLevel1.withColumn("a", $"a".withField("a", lit(4)).getField("a")),
      Row(null):: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = true))))

    // add new field, extract another field from original struct
    checkAnswer(
      nullStructLevel1.withColumn("a", $"a".withField("d", lit(4)).getField("c")),
      Row(null):: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = true))))

    // replace field, extract another field from original struct
    checkAnswer(
      nullStructLevel1.withColumn("a", $"a".withField("a", lit(4)).getField("c")),
      Row(null):: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = true))))
  }

  test("SPARK-32641: extracting field from nullable struct column which contains both null and " +
    "non-null values after withField should return null if the original struct was null") {
    val df = spark.createDataFrame(
      sparkContext.parallelize(Row(Row(1, null, 3)) :: Row(null) :: Nil),
      StructType(Seq(StructField("a", structType, nullable = true))))

    // extract newly added field
    checkAnswer(
      df.withColumn("a", $"a".withField("d", lit(4)).getField("d")),
      Row(4) :: Row(null) :: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = true))))

    // extract newly replaced field
    checkAnswer(
      df.withColumn("a", $"a".withField("a", lit(4)).getField("a")),
      Row(4) :: Row(null):: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = true))))

    // add new field, extract another field from original struct
    checkAnswer(
      df.withColumn("a", $"a".withField("d", lit(4)).getField("c")),
      Row(3) :: Row(null):: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = true))))

    // replace field, extract another field from original struct
    checkAnswer(
      df.withColumn("a", $"a".withField("a", lit(4)).getField("c")),
      Row(3) :: Row(null):: Nil,
      StructType(Seq(StructField("a", IntegerType, nullable = true))))
  }

  test("SPARK-35213: chained withField operations should have correct schema for new columns") {
    val df = spark.createDataFrame(
      sparkContext.parallelize(Row(null) :: Nil),
      StructType(Seq(StructField("data", NullType))))

    checkAnswer(
      df.withColumn("data", struct()
        .withField("a", struct())
        .withField("b", struct())
        .withField("a.aa", lit("aa1"))
        .withField("b.ba", lit("ba1"))
        .withField("a.ab", lit("ab1"))),
        Row(Row(Row("aa1", "ab1"), Row("ba1"))) :: Nil,
        StructType(Seq(
          StructField("data", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("aa", StringType, nullable = false),
              StructField("ab", StringType, nullable = false)
            )), nullable = false),
            StructField("b", StructType(Seq(
              StructField("ba", StringType, nullable = false)
            )), nullable = false)
          )), nullable = false)
        ))
    )
  }

  test("SPARK-35213: optimized withField operations should maintain correct nested struct " +
    "ordering") {
    val df = spark.createDataFrame(
      sparkContext.parallelize(Row(null) :: Nil),
      StructType(Seq(StructField("data", NullType))))

    checkAnswer(
      df.withColumn("data", struct()
          .withField("a", struct().withField("aa", lit("aa1")))
          .withField("b", struct().withField("ba", lit("ba1")))
        )
        .withColumn("data", col("data").withField("b.bb", lit("bb1")))
        .withColumn("data", col("data").withField("a.ab", lit("ab1"))),
        Row(Row(Row("aa1", "ab1"), Row("ba1", "bb1"))) :: Nil,
        StructType(Seq(
          StructField("data", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("aa", StringType, nullable = false),
              StructField("ab", StringType, nullable = false)
            )), nullable = false),
            StructField("b", StructType(Seq(
              StructField("ba", StringType, nullable = false),
              StructField("bb", StringType, nullable = false)
            )), nullable = false)
          )), nullable = false)
        ))
    )
  }

  test("dropFields should throw an exception if called on a non-StructType column") {
    checkError(
      exception = intercept[AnalysisException] {
        testData.withColumn("key", $"key".dropFields("a"))
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"update_fields(key, dropfield())\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"key\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRUCT\""),
      context = ExpectedContext(
        fragment = "dropFields",
        callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("dropFields should throw an exception if fieldName argument is null") {
    intercept[IllegalArgumentException] {
      structLevel1.withColumn("a", $"a".dropFields(null))
    }.getMessage should include("fieldName cannot be null")
  }

  test("dropFields should throw an exception if any intermediate structs don't exist") {
    checkError(
      exception = intercept[AnalysisException] {
        structLevel2.withColumn("a", $"a".dropFields("x.b"))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`x`", "fields" -> "`a`"))

    checkError(
      exception = intercept[AnalysisException] {
        structLevel3.withColumn("a", $"a".dropFields("a.x.b"))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`x`", "fields" -> "`a`"))
  }

  test("dropFields should throw an exception if intermediate field is not a struct") {
    checkError(
      exception = intercept[AnalysisException] {
        structLevel1.withColumn("a", $"a".dropFields("b.a"))
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"update_fields(a.b, dropfield())\"",
        "paramIndex" -> "first",
        "inputSql" -> "\"a.b\"",
        "inputType" -> "\"INT\"",
        "requiredType" -> "\"STRUCT\""),
      context = ExpectedContext(
        fragment = "dropFields",
        callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("dropFields should throw an exception if intermediate field reference is ambiguous") {
    checkError(
      exception = intercept[AnalysisException] {
        val structLevel2: DataFrame = spark.createDataFrame(
          sparkContext.parallelize(Row(Row(Row(1, null, 3), 4)) :: Nil),
          StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", structType, nullable = false),
              StructField("a", structType, nullable = false))),
              nullable = false))))

        structLevel2.withColumn("a", $"a".dropFields("a.b"))
      },
      condition = "AMBIGUOUS_REFERENCE_TO_FIELDS",
      sqlState = "42000",
      parameters = Map("field" -> "`a`", "count" -> "2")
    )
  }

  test("dropFields should drop field in struct") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".dropFields("b")),
      Row(Row(1, 3)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("c", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("dropFields should drop field in nullable struct") {
    checkAnswer(
      nullableStructLevel1.withColumn("a", $"a".dropFields("b")),
      Row(null) :: Row(Row(1, 3)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("c", IntegerType, nullable = false))),
          nullable = true))))
  }

  test("dropFields should drop multiple fields in struct") {
    Seq(
      structLevel1.withColumn("a", $"a".dropFields("b", "c")),
      structLevel1.withColumn("a", $"a".dropFields("b").dropFields("c"))
    ).foreach { df =>
      checkAnswer(
        df,
        Row(Row(1)) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false))),
            nullable = false))))
    }
  }

  test("dropFields should throw an exception if no fields will be left in struct") {
    checkError(
      exception = intercept[AnalysisException] {
        structLevel1.withColumn("a", $"a".dropFields("a", "b", "c"))
      },
      condition = "DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS",
      parameters = Map("sqlExpr" -> "\"update_fields(a, dropfield(), dropfield(), dropfield())\""),
      context = ExpectedContext(
        fragment = "dropFields",
        callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("dropFields should drop field with no name in struct") {
    val structType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("", IntegerType, nullable = false)))

    val structLevel1: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(Row(Row(1, 2)) :: Nil),
      StructType(Seq(StructField("a", structType, nullable = false))))

    checkAnswer(
      structLevel1.withColumn("a", $"a".dropFields("")),
      Row(Row(1)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("dropFields should drop field in nested struct") {
    checkAnswer(
      structLevel2.withColumn("a", $"a".dropFields("a.b")),
      Row(Row(Row(1, 3))) :: Nil,
      StructType(
        Seq(StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("c", IntegerType, nullable = false))),
            nullable = false))),
          nullable = false))))
  }

  test("dropFields should drop multiple fields in nested struct") {
    checkAnswer(
      structLevel2.withColumn("a", $"a".dropFields("a.b", "a.c")),
      Row(Row(Row(1))) :: Nil,
      StructType(
        Seq(StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false))),
            nullable = false))),
          nullable = false))))
  }

  test("dropFields should drop field in nested nullable struct") {
    checkAnswer(
      nullableStructLevel2.withColumn("a", $"a".dropFields("a.b")),
      Row(null) :: Row(Row(null)) :: Row(Row(Row(1, 3))) :: Nil,
      StructType(
        Seq(StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("c", IntegerType, nullable = false))),
            nullable = true))),
          nullable = true))))
  }

  test("dropFields should drop multiple fields in nested nullable struct") {
    checkAnswer(
      nullableStructLevel2.withColumn("a", $"a".dropFields("a.b", "a.c")),
      Row(null) :: Row(Row(null)) :: Row(Row(Row(1))) :: Nil,
      StructType(
        Seq(StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false))),
            nullable = true))),
          nullable = true))))
  }

  test("dropFields should drop field in deeply nested struct") {
    checkAnswer(
      structLevel3.withColumn("a", $"a".dropFields("a.a.b")),
      Row(Row(Row(Row(1, 3)))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("c", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))),
          nullable = false))))
  }

  test("dropFields should drop all fields with given name in struct") {
    val structLevel1 = spark.createDataFrame(
      sparkContext.parallelize(Row(Row(1, 2, 3)) :: Nil),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = false))),
          nullable = false))))

    checkAnswer(
      structLevel1.withColumn("a", $"a".dropFields("b")),
      Row(Row(1)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("dropFields should drop field in struct even if casing is different") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkAnswer(
        mixedCaseStructLevel1.withColumn("a", $"a".dropFields("A")),
        Row(Row(1)) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("B", IntegerType, nullable = false))),
            nullable = false))))

      checkAnswer(
        mixedCaseStructLevel1.withColumn("a", $"a".dropFields("b")),
        Row(Row(1)) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false))),
            nullable = false))))
    }
  }

  test("dropFields should not drop field in struct because casing is different") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        mixedCaseStructLevel1.withColumn("a", $"a".dropFields("A")),
        Row(Row(1, 1)) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("B", IntegerType, nullable = false))),
            nullable = false))))

      checkAnswer(
        mixedCaseStructLevel1.withColumn("a", $"a".dropFields("b")),
        Row(Row(1, 1)) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("B", IntegerType, nullable = false))),
            nullable = false))))
    }
  }

  test("dropFields should drop nested field in struct even if casing is different") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkAnswer(
        mixedCaseStructLevel2.withColumn("a", $"a".dropFields("A.a")),
        Row(Row(Row(1), Row(1, 1))) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("A", StructType(Seq(
              StructField("b", IntegerType, nullable = false))),
              nullable = false),
            StructField("B", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))))

      checkAnswer(
        mixedCaseStructLevel2.withColumn("a", $"a".dropFields("b.a")),
        Row(Row(Row(1, 1), Row(1))) :: Nil,
        StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", StructType(Seq(
              StructField("a", IntegerType, nullable = false),
              StructField("b", IntegerType, nullable = false))),
              nullable = false),
            StructField("b", StructType(Seq(
              StructField("b", IntegerType, nullable = false))),
              nullable = false))),
            nullable = false))))
    }
  }

  test("dropFields should throw an exception because casing is different") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkError(
        exception = intercept[AnalysisException] {
          mixedCaseStructLevel2.withColumn("a", $"a".dropFields("A.a"))
        },
        condition = "FIELD_NOT_FOUND",
        parameters = Map("fieldName" -> "`A`", "fields" -> "`a`, `B`"))

      checkError(
        exception = intercept[AnalysisException] {
          mixedCaseStructLevel2.withColumn("a", $"a".dropFields("b.a"))
        },
        condition = "FIELD_NOT_FOUND",
        parameters = Map("fieldName" -> "`b`", "fields" -> "`a`, `B`"))
    }
  }

  test("dropFields should drop only fields that exist") {
    checkAnswer(
      structLevel1.withColumn("a", $"a".dropFields("d")),
      Row(Row(1, null, 3)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false))),
          nullable = false))))

    checkAnswer(
      structLevel1.withColumn("a", $"a".dropFields("b", "d")),
      Row(Row(1, 3)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("c", IntegerType, nullable = false))),
          nullable = false))))

    checkAnswer(
      structLevel2.withColumn("a", $"a".dropFields("a.b", "a.d")),
      Row(Row(Row(1, 3))) :: Nil,
      StructType(
        Seq(StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("c", IntegerType, nullable = false))),
            nullable = false))),
          nullable = false))))
  }

  test("dropFields should drop multiple fields at arbitrary levels of nesting in a single call") {
    val df: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(Row(Row(Row(1, null, 3), 4)) :: Nil),
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", structType, nullable = false),
          StructField("b", IntegerType, nullable = false))),
          nullable = false))))

    checkAnswer(
      df.withColumn("a", $"a".dropFields("a.b", "b")),
      Row(Row(Row(1, 3))) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("c", IntegerType, nullable = false))), nullable = false))),
          nullable = false))))
  }

  test("dropFields user-facing examples") {
    checkAnswer(
      sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
        .select($"struct_col".dropFields("b")),
      Row(Row(1)))

    checkAnswer(
      sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
        .select($"struct_col".dropFields("c")),
      Row(Row(1, 2)))

    checkAnswer(
      sql("SELECT named_struct('a', 1, 'b', 2, 'c', 3) struct_col")
        .select($"struct_col".dropFields("b", "c")),
      Row(Row(1)))

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT named_struct('a', 1, 'b', 2) struct_col")
          .select($"struct_col".dropFields("a", "b"))
      },
      condition = "DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS",
      parameters = Map("sqlExpr" -> "\"update_fields(struct_col, dropfield(), dropfield())\""),
      context = ExpectedContext(
        fragment = "dropFields",
        callSitePattern = getCurrentClassCallSitePattern)
    )

    checkAnswer(
      sql("SELECT CAST(NULL AS struct<a:int,b:int>) struct_col")
        .select($"struct_col".dropFields("b")),
      Row(null))

    checkAnswer(
      sql("SELECT named_struct('a', 1, 'b', 2, 'b', 3) struct_col")
        .select($"struct_col".dropFields("b")),
      Row(Row(1)))

    checkAnswer(
      sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2)) struct_col")
        .select($"struct_col".dropFields("a.b")),
      Row(Row(Row(1))))

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT named_struct('a', named_struct('b', 1), 'a', named_struct('c', 2)) struct_col")
          .select($"struct_col".dropFields("a.c"))
      },
      condition = "AMBIGUOUS_REFERENCE_TO_FIELDS",
      sqlState = "42000",
      parameters = Map("field" -> "`a`", "count" -> "2")
    )

    checkAnswer(
      sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2, 'c', 3)) struct_col")
        .select($"struct_col".dropFields("a.b", "a.c")),
      Row(Row(Row(1))))

    checkAnswer(
      sql("SELECT named_struct('a', named_struct('a', 1, 'b', 2, 'c', 3)) struct_col")
        .select($"struct_col".withField("a", $"struct_col.a".dropFields("b", "c"))),
      Row(Row(Row(1))))
  }

  test("should correctly handle different dropField + withField + getField combinations") {
    val structType = StructType(Seq(
      StructField("a", IntegerType, nullable = false),
      StructField("b", IntegerType, nullable = false)))

    val structLevel1: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(Row(Row(1, 2)) :: Nil),
      StructType(Seq(StructField("a", structType, nullable = false))))

    val nullStructLevel1: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(Row(null) :: Nil),
      StructType(Seq(StructField("a", structType, nullable = true))))

    val nullableStructLevel1: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(Row(Row(1, 2)) :: Row(null) :: Nil),
      StructType(Seq(StructField("a", structType, nullable = true))))

    def check(
      fieldOps: Column => Column,
      getFieldName: String,
      expectedValue: Option[Int]): Unit = {

      def query(df: DataFrame): DataFrame =
        df.select(fieldOps(col("a")).getField(getFieldName).as("res"))

      checkAnswer(
        query(structLevel1),
        Row(expectedValue.orNull) :: Nil,
        StructType(Seq(StructField("res", IntegerType, nullable = expectedValue.isEmpty))))

      checkAnswer(
        query(nullStructLevel1),
        Row(null) :: Nil,
        StructType(Seq(StructField("res", IntegerType, nullable = true))))

      checkAnswer(
        query(nullableStructLevel1),
        Row(expectedValue.orNull) :: Row(null) :: Nil,
        StructType(Seq(StructField("res", IntegerType, nullable = true))))
    }

    // add attribute, extract an attribute from the original struct
    check(_.withField("c", lit(3)), "a", Some(1))
    check(_.withField("c", lit(3)), "b", Some(2))

    // add attribute, extract added attribute
    check(_.withField("c", lit(3)), "c", Some(3))
    check(_.withField("c", col("a.a")), "c", Some(1))
    check(_.withField("c", col("a.b")), "c", Some(2))
    check(_.withField("c", lit(null).cast(IntegerType)), "c", None)

    // replace attribute, extract an attribute from the original struct
    check(_.withField("b", lit(3)), "a", Some(1))
    check(_.withField("a", lit(3)), "b", Some(2))

    // replace attribute, extract replaced attribute
    check(_.withField("b", lit(3)), "b", Some(3))
    check(_.withField("b", lit(null).cast(IntegerType)), "b", None)
    check(_.withField("a", lit(3)), "a", Some(3))
    check(_.withField("a", lit(null).cast(IntegerType)), "a", None)

    // drop attribute, extract an attribute from the original struct
    check(_.dropFields("b"), "a", Some(1))
    check(_.dropFields("a"), "b", Some(2))

    // drop attribute, add attribute, extract an attribute from the original struct
    check(_.dropFields("b").withField("c", lit(3)), "a", Some(1))
    check(_.dropFields("a").withField("c", lit(3)), "b", Some(2))

    // drop attribute, add another attribute, extract added attribute
    check(_.dropFields("a").withField("c", lit(3)), "c", Some(3))
    check(_.dropFields("b").withField("c", lit(3)), "c", Some(3))

    // add attribute, drop attribute, extract an attribute from the original struct
    check(_.withField("c", lit(3)).dropFields("a"), "b", Some(2))
    check(_.withField("c", lit(3)).dropFields("b"), "a", Some(1))

    // add attribute, drop another attribute, extract added attribute
    check(_.withField("c", lit(3)).dropFields("a"), "c", Some(3))
    check(_.withField("c", lit(3)).dropFields("b"), "c", Some(3))

    // replace attribute, drop same attribute, extract an attribute from the original struct
    check(_.withField("b", lit(3)).dropFields("b"), "a", Some(1))
    check(_.withField("a", lit(3)).dropFields("a"), "b", Some(2))

    // add attribute, drop same attribute, extract an attribute from the original struct
    check(_.withField("c", lit(3)).dropFields("c"), "a", Some(1))
    check(_.withField("c", lit(3)).dropFields("c"), "b", Some(2))

    // add attribute, drop another attribute, extract added attribute
    check(_.withField("b", lit(3)).dropFields("a"), "b", Some(3))
    check(_.withField("a", lit(3)).dropFields("b"), "a", Some(3))
    check(_.withField("b", lit(null).cast(IntegerType)).dropFields("a"), "b", None)
    check(_.withField("a", lit(null).cast(IntegerType)).dropFields("b"), "a", None)

    // drop attribute, add same attribute, extract added attribute
    check(_.dropFields("b").withField("b", lit(3)), "b", Some(3))
    check(_.dropFields("a").withField("a", lit(3)), "a", Some(3))
    check(_.dropFields("b").withField("b", lit(null).cast(IntegerType)), "b", None)
    check(_.dropFields("a").withField("a", lit(null).cast(IntegerType)), "a", None)
    check(_.dropFields("c").withField("c", lit(3)), "c", Some(3))

    // add attribute, drop same attribute, add same attribute again, extract added attribute
    check(_.withField("c", lit(3)).dropFields("c").withField("c", lit(4)), "c", Some(4))
  }

  test("should move field up one level of nesting") {
    // move a field up one level
    checkAnswer(
      nullableStructLevel2.select(
        col("a").withField("c", col("a.a.c")).dropFields("a.c").as("res")),
      Row(null) :: Row(Row(null, null)) ::  Row(Row(Row(1, null), 3)) :: Nil,
      StructType(Seq(
        StructField("res", StructType(Seq(
          StructField("a", StructType(Seq(
            StructField("a", IntegerType, nullable = false),
            StructField("b", IntegerType, nullable = true))),
            nullable = true),
          StructField("c", IntegerType, nullable = true))),
          nullable = true))))

    // move a field up one level and then extract it
    checkAnswer(
      nullableStructLevel2.select(
        col("a").withField("c", col("a.a.c")).dropFields("a.c").getField("c").as("res")),
      Row(null) :: Row(null) :: Row(3) :: Nil,
      StructType(Seq(StructField("res", IntegerType, nullable = true))))
  }

  test("should be able to refer to newly added nested column") {
    checkError(
      exception = intercept[AnalysisException] {
        structLevel1.select($"a".withField("d", lit(4)).withField("e", $"a.d" + 1).as("a"))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`d`", "fields" -> "`a`, `b`, `c`"),
      context = ExpectedContext(
        fragment = "$",
        callSitePattern = getCurrentClassCallSitePattern))

    checkAnswer(
      structLevel1
        .select($"a".withField("d", lit(4)).as("a"))
        .select($"a".withField("e", $"a.d" + 1).as("a")),
      Row(Row(1, null, 3, 4, 5)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("c", IntegerType, nullable = false),
          StructField("d", IntegerType, nullable = false),
          StructField("e", IntegerType, nullable = false))),
          nullable = false))))
  }

  test("should be able to drop newly added nested column") {
    Seq(
      structLevel1.select($"a".withField("d", lit(4)).dropFields("d").as("a")),
      structLevel1
        .select($"a".withField("d", lit(4)).as("a"))
        .select($"a".dropFields("d").as("a"))
    ).foreach { query =>
      checkAnswer(
        query,
        Row(Row(1, null, 3)) :: Nil,
        StructType(Seq(
          StructField("a", structType, nullable = false))))
    }
  }

  test("should still be able to refer to dropped column within the same select statement") {
    // we can still access the nested column even after dropping it within the same select statement
    checkAnswer(
      structLevel1.select($"a".dropFields("c").withField("z", $"a.c").as("a")),
      Row(Row(1, null, 3)) :: Nil,
      StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", IntegerType, nullable = true),
          StructField("z", IntegerType, nullable = false))),
          nullable = false))))

    // we can't access the nested column in subsequent select statement after dropping it in a
    // previous select statement
    checkError(
      exception = intercept[AnalysisException]{
        structLevel1
          .select($"a".dropFields("c").as("a"))
          .select($"a".withField("z", $"a.c")).as("a")
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`c`", "fields" -> "`a`, `b`"),
      context = ExpectedContext(
        fragment = "$",
        callSitePattern = getCurrentClassCallSitePattern))
  }

  test("nestedDf should generate nested DataFrames") {
    checkAnswer(
      emptyNestedDf(1, 1, nullable = false),
      Seq.empty[Row],
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", IntegerType, nullable = false))),
        nullable = false))))

    checkAnswer(
      emptyNestedDf(1, 2, nullable = false),
      Seq.empty[Row],
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", IntegerType, nullable = false),
        StructField("nested1Col1", IntegerType, nullable = false))),
        nullable = false))))

    checkAnswer(
      emptyNestedDf(2, 1, nullable = false),
      Seq.empty[Row],
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", StructType(Seq(
          StructField("nested2Col0", IntegerType, nullable = false))),
          nullable = false))),
        nullable = false))))

    checkAnswer(
      emptyNestedDf(2, 2, nullable = false),
      Seq.empty[Row],
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", StructType(Seq(
          StructField("nested2Col0", IntegerType, nullable = false),
          StructField("nested2Col1", IntegerType, nullable = false))),
          nullable = false),
        StructField("nested1Col1", IntegerType, nullable = false))),
        nullable = false))))

    checkAnswer(
      emptyNestedDf(2, 2, nullable = true),
      Seq.empty[Row],
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", StructType(Seq(
          StructField("nested2Col0", IntegerType, nullable = false),
          StructField("nested2Col1", IntegerType, nullable = false))),
          nullable = true),
        StructField("nested1Col1", IntegerType, nullable = false))),
        nullable = true))))
  }

  Seq(Performant, NonPerformant).foreach { method =>
    Seq(false, true).foreach { nullable =>
      test(s"should add and drop 1 column at each depth of nesting using ${method.name} method, " +
        s"nullable = $nullable") {
        val maxDepth = 3

        // dataframe with nested*Col0 to nested*Col2 at each depth
        val inputDf = emptyNestedDf(maxDepth, 3, nullable)

        // add nested*Col3 and drop nested*Col2
        val modifiedColumn = method(
          column = col(nestedColName(0, 0)),
          numsToAdd = Seq(3),
          numsToDrop = Seq(2),
          maxDepth = maxDepth
        ).as(nestedColName(0, 0))
        val resultDf = inputDf.select(modifiedColumn)

        // dataframe with nested*Col0, nested*Col1, nested*Col3 at each depth
        val expectedDf = {
          val colNums = Seq(0, 1, 3)
          val nestedColumnDataType = nestedStructType(colNums, nullable, maxDepth)

          spark.createDataFrame(
            spark.sparkContext.emptyRDD[Row],
            StructType(Seq(StructField(nestedColName(0, 0), nestedColumnDataType, nullable))))
        }

        checkAnswer(resultDf, expectedDf.collect().toImmutableArraySeq, expectedDf.schema)
      }
    }
  }

  test("assert_true") {
    // assert_true(condition, errMsgCol)
    val booleanDf = Seq((true), (false)).toDF("cond")
    checkAnswer(
      booleanDf.filter("cond = true").select(assert_true($"cond")),
      Row(null) :: Nil
    )
    checkError(
      exception = intercept[SparkRuntimeException] {
        booleanDf.select(assert_true($"cond", lit(null.asInstanceOf[String]))).collect()
      },
      condition = "USER_RAISED_EXCEPTION",
      parameters = Map("errorMessage" -> "null"))

    val nullDf = Seq(("first row", None), ("second row", Some(true))).toDF("n", "cond")
    checkAnswer(
      nullDf.filter("cond = true").select(assert_true($"cond", $"cond")),
      Row(null) :: Nil
    )
    checkError(
      exception = intercept[SparkRuntimeException] {
        nullDf.select(assert_true($"cond", $"n")).collect()
      },
      condition = "USER_RAISED_EXCEPTION",
      parameters = Map("errorMessage" -> "first row"))

    // assert_true(condition)
    val intDf = Seq((0, 1)).toDF("a", "b")
    checkAnswer(intDf.select(assert_true($"a" < $"b")), Row(null) :: Nil)
    val e3 = intercept[SparkRuntimeException] {
      intDf.select(assert_true($"a" > $"b")).collect()
    }
    assert(e3.getMessage.matches(
      "\\[USER_RAISED_EXCEPTION\\] '\\(a#\\d+ > b#\\d+\\)' is not true! SQLSTATE: P0001"))
  }

  test("raise_error") {
    val strDf = Seq(("hello")).toDF("a")

    checkError(
      exception = intercept[SparkRuntimeException] {
        strDf.select(raise_error(lit(null.asInstanceOf[String]))).collect()
      },
      condition = "USER_RAISED_EXCEPTION",
      parameters = Map("errorMessage" -> "null"))

    checkError(
      exception = intercept[SparkRuntimeException] {
        strDf.select(raise_error($"a")).collect()
      },
      condition = "USER_RAISED_EXCEPTION",
      parameters = Map("errorMessage" -> "hello"))
  }

  test("SPARK-34677: negate/add/subtract year-month and day-time intervals") {
    import testImplicits._
    val df = Seq((Period.ofMonths(10), Duration.ofDays(10), Period.ofMonths(1), Duration.ofDays(1)))
      .toDF("year-month-A", "day-time-A", "year-month-B", "day-time-B")
    val negatedDF = df.select(-$"year-month-A", -$"day-time-A")
    checkAnswer(negatedDF, Row(Period.ofMonths(-10), Duration.ofDays(-10)))
    val addDF = df.select($"year-month-A" + $"year-month-B", $"day-time-A" + $"day-time-B")
    checkAnswer(addDF, Row(Period.ofMonths(11), Duration.ofDays(11)))
    val subDF = df.select($"year-month-A" - $"year-month-B", $"day-time-A" - $"day-time-B")
    checkAnswer(subDF, Row(Period.ofMonths(9), Duration.ofDays(9)))
  }

  test("SPARK-34721: add a year-month interval to a date") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      outstandingTimezonesIds.foreach { zid =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid) {
          Seq(
            (LocalDate.of(1900, 10, 1), Period.ofMonths(0)) -> LocalDate.of(1900, 10, 1),
            (LocalDate.of(1970, 1, 1), Period.ofMonths(-1)) -> LocalDate.of(1969, 12, 1),
            (LocalDate.of(2021, 3, 11), Period.ofMonths(1)) -> LocalDate.of(2021, 4, 11),
            (LocalDate.of(2020, 12, 31), Period.ofMonths(2)) -> LocalDate.of(2021, 2, 28),
            (LocalDate.of(2021, 5, 31), Period.ofMonths(-3)) -> LocalDate.of(2021, 2, 28),
            (LocalDate.of(2020, 2, 29), Period.ofYears(1)) -> LocalDate.of(2021, 2, 28),
            (LocalDate.of(1, 1, 1), Period.ofYears(2020)) -> LocalDate.of(2021, 1, 1)
          ).foreach { case ((date, period), result) =>
            val df = Seq((date, period)).toDF("date", "interval")
            checkAnswer(
              df.select($"date" + $"interval", $"interval" + $"date"),
              Row(result, result))
          }
        }
      }

      val e = intercept[ArithmeticException] {
        Seq((LocalDate.of(2021, 3, 11), Period.ofMonths(Int.MaxValue)))
          .toDF("date", "interval")
          .select($"date" + $"interval")
          .collect()
      }
      assert(e.getMessage.contains("integer overflow"))
    }
  }

  test("SPARK-34721: subtract a year-month interval from a date") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      outstandingTimezonesIds.foreach { zid =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid) {
          Seq(
            (LocalDate.of(1582, 10, 4), Period.ofMonths(0)) -> LocalDate.of(1582, 10, 4),
            (LocalDate.of(1582, 10, 15), Period.ofMonths(1)) -> LocalDate.of(1582, 9, 15),
            (LocalDate.of(1, 1, 1), Period.ofMonths(-1)) -> LocalDate.of(1, 2, 1),
            (LocalDate.of(9999, 10, 31), Period.ofMonths(-2)) -> LocalDate.of(9999, 12, 31),
            (LocalDate.of(2021, 5, 31), Period.ofMonths(3)) -> LocalDate.of(2021, 2, 28),
            (LocalDate.of(2021, 2, 28), Period.ofYears(1)) -> LocalDate.of(2020, 2, 28),
            (LocalDate.of(2020, 2, 29), Period.ofYears(4)) -> LocalDate.of(2016, 2, 29)
          ).foreach { case ((date, period), result) =>
            val df = Seq((date, period)).toDF("date", "interval")
            checkAnswer(df.select($"date" - $"interval"), Row(result))
          }
        }
      }

      val e = intercept[ArithmeticException] {
        Seq((LocalDate.of(2021, 3, 11), Period.ofMonths(Int.MaxValue)))
          .toDF("date", "interval")
          .select($"date" - $"interval")
          .collect()
      }
      assert(e.getMessage.contains("integer overflow"))
    }
  }

  test("SPARK-34739: add a year-month interval to a timestamp") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      outstandingZoneIds.foreach { zid =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId) {
          Seq(
            (LocalDateTime.of(1900, 1, 1, 0, 0, 0, 123456000), Period.ofMonths(0)) ->
              LocalDateTime.of(1900, 1, 1, 0, 0, 0, 123456000),
            (LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1000), Period.ofMonths(-1)) ->
              LocalDateTime.of(1969, 12, 1, 0, 0, 0, 1000),
            (LocalDateTime.of(2021, 3, 14, 1, 2, 3, 0), Period.ofMonths(1)) ->
              LocalDateTime.of(2021, 4, 14, 1, 2, 3, 0),
            (LocalDateTime.of(2020, 12, 31, 23, 59, 59, 999999000), Period.ofMonths(2)) ->
              LocalDateTime.of(2021, 2, 28, 23, 59, 59, 999999000),
            (LocalDateTime.of(2021, 5, 31, 0, 0, 1, 0), Period.ofMonths(-3)) ->
              LocalDateTime.of(2021, 2, 28, 0, 0, 1, 0),
            (LocalDateTime.of(2020, 2, 29, 12, 13, 14), Period.ofYears(1)) ->
              LocalDateTime.of(2021, 2, 28, 12, 13, 14),
            (LocalDateTime.of(1, 1, 1, 1, 1, 1, 1000), Period.ofYears(2020)) ->
              LocalDateTime.of(2021, 1, 1, 1, 1, 1, 1000)
          ).foreach { case ((ldt, period), expected) =>
            val df = Seq((ldt.atZone(zid).toInstant, period)).toDF("ts", "interval")
            val result = expected.atZone(zid).toInstant
            checkAnswer(df.select($"ts" + $"interval", $"interval" + $"ts"), Row(result, result))
          }
        }
      }

      val e = intercept[SparkException] {
        Seq((Instant.parse("2021-03-14T18:55:00Z"), Period.ofMonths(Int.MaxValue)))
          .toDF("ts", "interval")
          .select($"ts" + $"interval")
          .collect()
      }.getCause
      assert(e.isInstanceOf[ArithmeticException])
      assert(e.getMessage.contains("long overflow"))
    }
  }

  test("SPARK-34739: subtract a year-month interval from a timestamp") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      outstandingZoneIds.foreach { zid =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId) {
          Seq(
            (LocalDateTime.of(1582, 10, 4, 0, 0, 0), Period.ofMonths(0)) ->
              LocalDateTime.of(1582, 10, 4, 0, 0, 0),
            (LocalDateTime.of(1582, 10, 15, 23, 59, 59, 999999000), Period.ofMonths(1)) ->
              LocalDateTime.of(1582, 9, 15, 23, 59, 59, 999999000),
            (LocalDateTime.of(1, 1, 1, 1, 1, 1, 1000), Period.ofMonths(-1)) ->
              LocalDateTime.of(1, 2, 1, 1, 1, 1, 1000),
            (LocalDateTime.of(9999, 10, 31, 23, 59, 59, 999000000), Period.ofMonths(-2)) ->
              LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999000000),
            (LocalDateTime.of(2021, 5, 31, 0, 0, 0, 1000), Period.ofMonths(3)) ->
              LocalDateTime.of(2021, 2, 28, 0, 0, 0, 1000),
            (LocalDateTime.of(2021, 2, 28, 11, 12, 13, 123456000), Period.ofYears(1)) ->
              LocalDateTime.of(2020, 2, 28, 11, 12, 13, 123456000),
            (LocalDateTime.of(2020, 2, 29, 1, 2, 3, 5000), Period.ofYears(4)) ->
              LocalDateTime.of(2016, 2, 29, 1, 2, 3, 5000)
          ).foreach { case ((ldt, period), expected) =>
            val df = Seq((ldt.atZone(zid).toInstant, period)).toDF("ts", "interval")
            checkAnswer(df.select($"ts" - $"interval"), Row(expected.atZone(zid).toInstant))
          }
        }
      }

      val e = intercept[SparkException] {
        Seq((Instant.parse("2021-03-14T18:55:00Z"), Period.ofMonths(Int.MaxValue)))
          .toDF("ts", "interval")
          .select($"ts" - $"interval")
          .collect()
      }.getCause
      assert(e.isInstanceOf[ArithmeticException])
      assert(e.getMessage.contains("long overflow"))
    }
  }

  test("SPARK-34761, SPARK-34903: add/subtract a day-time interval to/from a timestamp") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      outstandingZoneIds.foreach { zid =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId) {
          Seq(
            (LocalDateTime.of(1900, 1, 1, 0, 0, 0, 123456000), Duration.ofDays(0)) ->
              LocalDateTime.of(1900, 1, 1, 0, 0, 0, 123456000),
            (LocalDateTime.of(1970, 1, 1, 0, 0, 0, 100000000), Duration.ofDays(-1)) ->
              LocalDateTime.of(1969, 12, 31, 0, 0, 0, 100000000),
            (LocalDateTime.of(2021, 3, 14, 1, 2, 3), Duration.ofDays(1)) ->
              LocalDateTime.of(2021, 3, 15, 1, 2, 3),
            (LocalDateTime.of(2020, 12, 31, 23, 59, 59, 999000000),
              Duration.ofDays(2 * 30).plusMillis(1)) -> LocalDateTime.of(2021, 3, 2, 0, 0, 0),
            (LocalDateTime.of(2020, 3, 16, 0, 0, 0, 1000), Duration.of(-1, ChronoUnit.MICROS)) ->
              LocalDateTime.of(2020, 3, 16, 0, 0, 0),
            (LocalDateTime.of(2020, 2, 29, 12, 13, 14), Duration.ofDays(365)) ->
              LocalDateTime.of(2021, 2, 28, 12, 13, 14),
            (LocalDateTime.of(1582, 10, 4, 1, 2, 3, 40000000),
              Duration.ofDays(10).plusMillis(60)) ->
              LocalDateTime.of(1582, 10, 14, 1, 2, 3, 100000000)
          ).foreach { case ((ldt, duration), expected) =>
            val ts = ldt.atZone(zid).toInstant
            val result = expected.atZone(zid).toInstant
            val df = Seq((ts, duration, result)).toDF("ts", "interval", "result")
            checkAnswer(
              df.select($"ts" + $"interval", $"interval" + $"ts", $"result" - $"interval",
                $"result" - $"ts"),
              Row(result, result, ts, duration))
          }
        }
      }

      Seq(
        "2021-03-16T18:56:00Z" -> "ts + i",
        "1900-03-16T18:56:00Z" -> "ts - i").foreach { case (instant, op) =>
        val e = intercept[SparkException] {
          Seq(
            (Instant.parse(instant), Duration.of(Long.MaxValue, ChronoUnit.MICROS)))
            .toDF("ts", "i")
            .selectExpr(op)
            .collect()
        }.getCause
        assert(e.isInstanceOf[ArithmeticException])
        assert(e.getMessage.contains("long overflow"))
      }
    }
  }

  test("SPARK-34824: multiply year-month interval by numeric") {
    checkAnswer(
      Seq((Period.ofYears(0), 0)).toDF("i", "n").select($"i" * $"n"),
      Row(Period.ofYears(0)))
    checkAnswer(
      Seq((Period.ofMonths(0), 10.toByte)).toDF("i", "n").select($"i" * $"n"),
      Row(Period.ofMonths(0)))
    checkAnswer(
      Seq((Period.ofMonths(5), 3.toShort)).toDF("i", "n").select($"n" * $"i"),
      Row(Period.ofYears(1).plusMonths(3)))
    checkAnswer(
      Seq((Period.ofYears(1000), "2")).toDF("i", "n").select($"i" * $"n"),
      Row(Period.ofYears(2000)))
    checkAnswer(
      Seq((Period.ofMonths(1), 12L)).toDF("i", "n").select($"n" * $"i"),
      Row(Period.ofYears(1)))
    checkAnswer(
      Seq((Period.ofYears(100).plusMonths(11), Short.MaxValue)).toDF("i", "n").select($"n" * $"i"),
      Row(Period.ofYears(100).plusMonths(11).multipliedBy(Short.MaxValue).normalized()))
    checkAnswer(
      Seq((Period.ofMonths(-1), 0.499f)).toDF("i", "n").select($"i" * $"n"),
      Row(Period.ofMonths(0)))
    checkAnswer(
      Seq((Period.ofMonths(10000000), 0.0000001d)).toDF("i", "n").select($"i" * $"n"),
      Row(Period.ofMonths(1)))
    checkAnswer(
      Seq((Period.ofMonths(-10000000), BigDecimal(0.0000001d))).toDF("i", "n").select($"i" * $"n"),
      Row(Period.ofMonths(-1)))
    checkAnswer(
      Seq((Period.ofMonths(-1), BigDecimal(0.5))).toDF("i", "n").select($"i" * $"n"),
      Row(Period.ofMonths(-1)))

    val e = intercept[SparkException] {
      Seq((Period.ofYears(9999), Long.MinValue)).toDF("i", "n").select($"n" * $"i").collect()
    }.getCause
    assert(e.isInstanceOf[ArithmeticException])
    assert(e.getMessage.contains("overflow"))
  }

  test("SPARK-34850: multiply day-time interval by numeric") {
    checkAnswer(
      Seq((Duration.ofDays(0), 0)).toDF("i", "n").select($"i" * $"n"),
      Row(Duration.ofDays(0)))
    checkAnswer(
      Seq((Duration.ofDays(0), 10.toByte)).toDF("i", "n").select($"i" * $"n"),
      Row(Duration.ofDays(0)))
    checkAnswer(
      Seq((Duration.ofHours(12), 3.toShort)).toDF("i", "n").select($"n" * $"i"),
      Row(Duration.ofDays(1).plusHours(12)))
    checkAnswer(
      Seq((Duration.ofMinutes(1000), "2")).toDF("i", "n").select($"i" * $"n"),
      Row(Duration.ofMinutes(2000)))
    checkAnswer(
      Seq((Duration.ofSeconds(1), 60L)).toDF("i", "n").select($"n" * $"i"),
      Row(Duration.ofMinutes(1)))
    checkAnswer(
      Seq((Duration.of(-1, ChronoUnit.MICROS), 0.499f)).toDF("i", "n").select($"i" * $"n"),
      Row(Duration.of(0, ChronoUnit.MICROS)))
    checkAnswer(
      Seq((Duration.of(-1, ChronoUnit.MICROS), 0.51d)).toDF("i", "n").select($"i" * $"n"),
      Row(Duration.of(-1, ChronoUnit.MICROS)))
    checkAnswer(
      Seq((Duration.of(-10000000, ChronoUnit.MICROS), BigDecimal(0.0000001d)))
        .toDF("i", "n").select($"i" * $"n"),
      Row(Duration.of(-1, ChronoUnit.MICROS)))

    val e = intercept[SparkException] {
      Seq((Duration.ofDays(9999), Long.MinValue)).toDF("i", "n").select($"n" * $"i").collect()
    }.getCause
    assert(e.isInstanceOf[ArithmeticException])
    assert(e.getMessage.contains("overflow"))
  }

  test("SPARK-34868: divide year-month interval by numeric") {
    checkAnswer(
      Seq((Period.ofYears(0), 10.toByte)).toDF("i", "n").select($"i" / $"n"),
      Row(Period.ofYears(0)))
    checkAnswer(
      Seq((Period.ofYears(10), 3.toShort)).toDF("i", "n").select($"i" / $"n"),
      Row(Period.ofYears(3).plusMonths(4)))
    checkAnswer(
      Seq((Period.ofYears(1000), "2")).toDF("i", "n").select($"i" / $"n"),
      Row(Period.ofYears(500)))
    checkAnswer(
      Seq((Period.ofMonths(1).multipliedBy(Int.MaxValue), Int.MaxValue))
        .toDF("i", "n").select($"i" / $"n"),
      Row(Period.ofMonths(1)))
    checkAnswer(
      Seq((Period.ofYears(-1), 12L)).toDF("i", "n").select($"i" / $"n"),
      Row(Period.ofMonths(-1)))
    checkAnswer(
      Seq((Period.ofMonths(-1), 0.499f)).toDF("i", "n").select($"i" / $"n"),
      Row(Period.ofMonths(-2)))
    checkAnswer(
      Seq((Period.ofMonths(10000000), 10000000d)).toDF("i", "n").select($"i" / $"n"),
      Row(Period.ofMonths(1)))
    checkAnswer(
      Seq((Period.ofMonths(-1), BigDecimal(0.5))).toDF("i", "n").select($"i" / $"n"),
      Row(Period.ofMonths(-2)))

    val e = intercept[ArithmeticException] {
      Seq((Period.ofYears(9999), 0)).toDF("i", "n").select($"i" / $"n").collect()
    }
    assert(e.getMessage.contains("Division by zero"))

    val e2 = intercept[ArithmeticException] {
      Seq((Period.ofYears(9999), 0d)).toDF("i", "n").select($"i" / $"n").collect()
    }
    assert(e2.getMessage.contains("Division by zero"))

    val e3 = intercept[ArithmeticException] {
      Seq((Period.ofYears(9999), BigDecimal(0))).toDF("i", "n").select($"i" / $"n").collect()
    }
    assert(e3.getMessage.contains("Division by zero"))
  }

  test("SPARK-34875: divide day-time interval by numeric") {
    checkAnswer(
      Seq((Duration.ZERO, 10.toByte)).toDF("i", "n").select($"i" / $"n"),
      Row(Duration.ZERO))
    checkAnswer(
      Seq((Duration.ofDays(10), 3.toShort)).toDF("i", "n").select($"i" / $"n"),
      Row(Duration.ofDays(10).dividedBy(3)))
    checkAnswer(
      Seq((Duration.ofHours(1000), "2")).toDF("i", "n").select($"i" / $"n"),
      Row(Duration.ofHours(500)))
    checkAnswer(
      Seq((Duration.of(1, ChronoUnit.MICROS).multipliedBy(Long.MaxValue), Long.MaxValue))
        .toDF("i", "n").select($"i" / $"n"),
      Row(Duration.of(1, ChronoUnit.MICROS)))
    checkAnswer(
      Seq((Duration.ofMinutes(-1), 60L)).toDF("i", "n").select($"i" / $"n"),
      Row(Duration.ofSeconds(-1)))
    checkAnswer(
      Seq((Duration.ofDays(-1), 0.5f)).toDF("i", "n").select($"i" / $"n"),
      Row(Duration.ofDays(-2)))
    checkAnswer(
      Seq((Duration.ofMillis(10000000), 10000000d)).toDF("i", "n").select($"i" / $"n"),
      Row(Duration.ofMillis(1)))
    checkAnswer(
      Seq((Duration.of(-1, ChronoUnit.MICROS), BigDecimal(10000.0001)))
        .toDF("i", "n").select($"i" / $"n"),
      Row(Duration.of(-1, ChronoUnit.MICROS).multipliedBy(10000).dividedBy(100000001)))

    val e = intercept[ArithmeticException] {
      Seq((Duration.ofDays(9999), 0)).toDF("i", "n").select($"i" / $"n").collect()
    }
    assert(e.getMessage.contains("Division by zero"))

    val e2 = intercept[ArithmeticException] {
      Seq((Duration.ofDays(9999), 0d)).toDF("i", "n").select($"i" / $"n").collect()
    }
    assert(e2.getMessage.contains("Division by zero"))

    val e3 = intercept[ArithmeticException] {
      Seq((Duration.ofDays(9999), BigDecimal(0))).toDF("i", "n").select($"i" / $"n").collect()
    }
    assert(e3.getMessage.contains("Division by zero"))
  }

  test("SPARK-34896: return day-time interval from dates subtraction") {
    withSQLConf(
      SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true",
      SQLConf.LEGACY_INTERVAL_ENABLED.key -> "false") {
      outstandingTimezonesIds.foreach { zid =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid) {
          Seq(
            (LocalDate.of(1582, 10, 15), LocalDate.of(1582, 10, 4)),
            (LocalDate.of(1900, 10, 1), LocalDate.of(1900, 10, 1)),
            (LocalDate.of(1969, 12, 1), LocalDate.of(1970, 1, 1)),
            (LocalDate.of(2021, 3, 1), LocalDate.of(2020, 2, 29)),
            (LocalDate.of(2021, 3, 15), LocalDate.of(2021, 3, 14)),
            (LocalDate.of(1, 1, 1), LocalDate.of(2021, 3, 29))
          ).foreach { case (end, start) =>
            val df = Seq((end, start)).toDF("end", "start")
            val daysBetween = Duration.ofDays(ChronoUnit.DAYS.between(start, end))
            val r = df.select($"end" - $"start").toDF("diff")
            checkAnswer(r, Row(daysBetween))
            assert(r.schema === new StructType().add("diff", DayTimeIntervalType(DAY)))
          }
        }
      }

      val e = intercept[SparkException] {
        Seq((LocalDate.ofEpochDay(0), LocalDate.of(500000, 1, 1)))
          .toDF("start", "end")
          .select($"end" - $"start")
          .collect()
      }.getCause
      assert(e.isInstanceOf[ArithmeticException])
      assert(e.getMessage.contains("long overflow"))
    }
  }

  test("SPARK-34903: Return day-time interval from timestamps subtraction") {
    outstandingTimezonesIds.foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        checkAnswer(
          sql("select timestamp '2021-03-31 19:11:10' - timestamp '2021-03-01 19:11:10'"),
          Row(Duration.ofDays(30)))
        checkAnswer(
          Seq((Instant.parse("2021-03-31T00:01:02Z"), Instant.parse("2021-04-01T00:00:00Z")))
            .toDF("start", "end").select($"end" - $"start" < Duration.ofDays(1)),
          Row(true))
        checkAnswer(
          Seq((Instant.parse("2021-03-31T00:01:02.777Z"), Duration.ofMillis(333)))
            .toDF("ts", "i")
            .select(($"ts" + $"i") - $"ts"),
          Row(Duration.ofMillis(333)))
        checkAnswer(
          Seq((LocalDateTime.of(2021, 3, 31, 10, 0, 0)
              .atZone(DateTimeUtils.getZoneId(tz)).toInstant, LocalDate.of(2020, 3, 31)))
            .toDF("ts", "d")
          .select($"ts" - $"d"),
          Row(Duration.ofDays(365).plusHours(10)))
      }
    }
  }

  test("SPARK-35051: add/subtract a day-time interval to/from a date") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      outstandingZoneIds.foreach { zid =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId) {
          Seq(
            (LocalDate.of(1, 1, 1), Duration.ofDays(31)) -> LocalDateTime.of(1, 2, 1, 0, 0, 0),
            (LocalDate.of(1582, 9, 15), Duration.ofDays(30).plus(1, ChronoUnit.MICROS)) ->
              LocalDateTime.of(1582, 10, 15, 0, 0, 0, 1000),
            (LocalDate.of(1900, 1, 1), Duration.ofDays(0).plusHours(1)) ->
              LocalDateTime.of(1900, 1, 1, 1, 0, 0),
            (LocalDate.of(1970, 1, 1), Duration.ofDays(-1).minusMinutes(1)) ->
              LocalDateTime.of(1969, 12, 30, 23, 59, 0),
            (LocalDate.of(2021, 3, 14), Duration.ofDays(1)) ->
              LocalDateTime.of(2021, 3, 15, 0, 0, 0),
            (LocalDate.of(2020, 12, 31), Duration.ofDays(4 * 30).plusMinutes(30)) ->
              LocalDateTime.of(2021, 4, 30, 0, 30, 0),
            (LocalDate.of(2020, 2, 29), Duration.ofDays(365).plusSeconds(59)) ->
              LocalDateTime.of(2021, 2, 28, 0, 0, 59),
            (LocalDate.of(10000, 1, 1), Duration.ofDays(-2)) ->
              LocalDateTime.of(9999, 12, 30, 0, 0, 0)
          ).foreach { case ((date, duration), expected) =>
            val result = expected.atZone(zid).toInstant
            val ts = date.atStartOfDay(zid).toInstant
            val df = Seq((date, duration, result)).toDF("date", "interval", "result")
            checkAnswer(
              df.select($"date" + $"interval", $"interval" + $"date", $"result" - $"interval",
                $"result" - $"date"),
              Row(result, result, ts, duration))
          }
        }
      }

      Seq(
        "2021-04-14" -> "date + i",
        "1900-04-14" -> "date - i").foreach { case (date, op) =>
        val e = intercept[SparkException] {
          Seq(
            (LocalDate.parse(date), Duration.of(Long.MaxValue, ChronoUnit.MICROS)))
            .toDF("date", "i")
            .selectExpr(op)
            .collect()
        }.getCause
        assert(e.isInstanceOf[ArithmeticException])
        assert(e.getMessage.contains("long overflow"))
      }
    }
  }

  test("SPARK-35852: add/subtract a interval day to/from a date") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      Seq(
        (LocalDate.of(1, 1, 1), Duration.ofDays(31)),
        (LocalDate.of(1582, 9, 15), Duration.ofDays(30)),
        (LocalDate.of(1900, 1, 1), Duration.ofDays(0)),
        (LocalDate.of(1970, 1, 1), Duration.ofDays(-1)),
        (LocalDate.of(2021, 3, 14), Duration.ofDays(1)),
        (LocalDate.of(2020, 12, 31), Duration.ofDays(4 * 30)),
        (LocalDate.of(2020, 2, 29), Duration.ofDays(365)),
        (LocalDate.of(10000, 1, 1), Duration.ofDays(-2))
      ).foreach { case (date, duration) =>
        val days = duration.toDays
        val add = date.plusDays(days)
        val sub = date.minusDays(days)
        val df = Seq((date, duration)).toDF("start", "diff")
          .select($"start", $"diff" cast DayTimeIntervalType(DAY) as "diff")
          .select($"start" + $"diff", $"diff" + $"start", $"start" - $"diff")
        checkAnswer(df, Row(add, add, sub))
      }
    }
  }

  test("SPARK-36778: add ilike API for scala") {
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    // null handling
    val nullDf = Seq("a", null).toDF("src")
    checkAnswer(nullDf.filter($"src".ilike("A")), Row("a"))
    checkAnswer(nullDf.filter($"src".ilike(null)), spark.emptyDataFrame)
    // simple pattern
    val simpleDf = Seq("a", "A", "abdef", "a_%b", "addb", "abC", "a\nb").toDF("src")
    checkAnswer(simpleDf.filter($"src".ilike("a")), Seq("a", "A").toDF())
    checkAnswer(simpleDf.filter($"src".ilike("A")), Seq("a", "A").toDF())
    checkAnswer(simpleDf.filter($"src".ilike("b")), spark.emptyDataFrame)
    checkAnswer(simpleDf.filter($"src".ilike("aBdef")), Seq("abdef").toDF())
    checkAnswer(simpleDf.filter($"src".ilike("a\\__b")), Seq("a_%b").toDF())
    checkAnswer(simpleDf.filter($"src".ilike("A_%b")), Seq("a_%b", "addb", "a\nb").toDF())
    checkAnswer(simpleDf.filter($"src".ilike("a%")), simpleDf)
    checkAnswer(simpleDf.filter($"src".ilike("a_b")), Seq("a\nb").toDF())
    // double-escaping backslash
    val dEscDf = Seq("""\__""", """\\__""").toDF("src")
    checkAnswer(dEscDf.filter($"src".ilike("""\\\__""")), Seq("""\__""").toDF())
    checkAnswer(dEscDf.filter($"src".ilike("""%\\%\%""")), spark.emptyDataFrame)
    // unicode
    val uncDf = Seq("a\u20ACA", "A€a", "a€AA", "a\u20ACaz", "ЀЁЂѺΏỀ").toDF("src")
    checkAnswer(uncDf.filter($"src".ilike("_\u20AC_")), Seq("a\u20ACA", "A€a").toDF())
    checkAnswer(uncDf.filter($"src".ilike("_€_")), Seq("a\u20ACA", "A€a").toDF())
    checkAnswer(uncDf.filter($"src".ilike("_\u20AC_a")), Seq("a€AA").toDF())
    checkAnswer(uncDf.filter($"src".ilike("_€_Z")), Seq("a\u20ACaz").toDF())
    checkAnswer(uncDf.filter($"src".ilike("ѐёђѻώề")), Seq("ЀЁЂѺΏỀ").toDF())
    // scalastyle:on
  }

  test("SPARK-39093: divide period by integral expression") {
    val df = Seq(((Period.ofMonths(10)), 2)).toDF("pd", "num")
    checkAnswer(df.select($"pd" / ($"num" + 3)),
      Seq((Period.ofMonths(2))).toDF())
  }

  test("SPARK-39093: divide duration by integral expression") {
    val df = Seq(((Duration.ofDays(10)), 2)).toDF("dd", "num")
    checkAnswer(df.select($"dd" / ($"num" + 3)),
      Seq((Duration.ofDays(2))).toDF())
  }
}
