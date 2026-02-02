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

import java.time.LocalDateTime
import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.aggregate.PivotFirst
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DataFramePivotSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("pivot courses") {
    val expected = Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    checkAnswer(
      courseSales.groupBy("year").pivot("course", Seq("dotNET", "Java"))
        .agg(sum($"earnings")),
      expected)
    checkAnswer(
      courseSales.groupBy($"year").pivot($"course", Seq("dotNET", "Java"))
        .agg(sum($"earnings")),
      expected)
  }

  test("pivot year") {
    val expected = Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    checkAnswer(
      courseSales.groupBy("course").pivot("year", Seq(2012, 2013)).agg(sum($"earnings")),
      expected)
    checkAnswer(
      courseSales.groupBy($"course").pivot($"year", Seq(2012, 2013)).agg(sum($"earnings")),
      expected)
  }

  test("pivot courses with multiple aggregations") {
    val expected = Row(2012, 15000.0, 7500.0, 20000.0, 20000.0) ::
      Row(2013, 48000.0, 48000.0, 30000.0, 30000.0) :: Nil
    checkAnswer(
      courseSales.groupBy($"year")
        .pivot("course", Seq("dotNET", "Java"))
        .agg(sum($"earnings"), avg($"earnings")),
      expected)
    checkAnswer(
      courseSales.groupBy($"year")
        .pivot($"course", Seq("dotNET", "Java"))
        .agg(sum($"earnings"), avg($"earnings")),
      expected)
  }

  test("pivot year with string values (cast)") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year", Seq("2012", "2013")).sum("earnings"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }

  test("pivot year with int values") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year", Seq(2012, 2013)).sum("earnings"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }

  test("pivot courses with no values") {
    // Note Java comes before dotNet in sorted order
    val expected = Row(2012, 20000.0, 15000.0) :: Row(2013, 30000.0, 48000.0) :: Nil
    checkAnswer(
      courseSales.groupBy("year").pivot("course").agg(sum($"earnings")),
      expected)
    checkAnswer(
      courseSales.groupBy($"year").pivot($"course").agg(sum($"earnings")),
      expected)
  }

  test("pivot year with no values") {
    val expected = Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    checkAnswer(
      courseSales.groupBy("course").pivot("year").agg(sum($"earnings")),
      expected)
    checkAnswer(
      courseSales.groupBy($"course").pivot($"year").agg(sum($"earnings")),
      expected)
  }

  test("pivot max values enforced") {
    spark.conf.set(SQLConf.DATAFRAME_PIVOT_MAX_VALUES.key, 1)
    intercept[AnalysisException](
      courseSales.groupBy("year").pivot("course")
    )
    spark.conf.set(SQLConf.DATAFRAME_PIVOT_MAX_VALUES.key,
      SQLConf.DATAFRAME_PIVOT_MAX_VALUES.defaultValue.get)
  }

  test("pivot with UnresolvedFunction") {
    checkAnswer(
      courseSales.groupBy("year").pivot("course", Seq("dotNET", "Java"))
        .agg("earnings" -> "sum"),
      Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    )
  }

  // Tests for optimized pivot (with PivotFirst) below

  test("optimized pivot planned") {
    val df = courseSales.groupBy("year")
      // pivot with extra columns to trigger optimization
      .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
      .agg(sum($"earnings"))
    val queryExecution = spark.sessionState.executePlan(df.queryExecution.logical)
    assert(queryExecution.simpleString.contains("pivotfirst"))
  }


  test("optimized pivot courses with literals") {
    checkAnswer(
      courseSales.groupBy("year")
        // pivot with extra columns to trigger optimization
        .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
        .agg(sum($"earnings"))
        .select("year", "dotNET", "Java"),
      Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    )
  }

  test("optimized pivot year with literals") {
    checkAnswer(
      courseSales.groupBy($"course")
        // pivot with extra columns to trigger optimization
        .pivot("year", Seq(2012, 2013) ++ (1 to 10))
        .agg(sum($"earnings"))
        .select("course", "2012", "2013"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }

  test("optimized pivot year with string values (cast)") {
    checkAnswer(
      courseSales.groupBy("course")
        // pivot with extra columns to trigger optimization
        .pivot("year", Seq("2012", "2013") ++ (1 to 10).map(_.toString))
        .sum("earnings")
        .select("course", "2012", "2013"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }

  test("optimized pivot DecimalType") {
    val df = courseSales.select($"course", $"year", $"earnings".cast(DecimalType(10, 2)))
      .groupBy("year")
      // pivot with extra columns to trigger optimization
      .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
      .agg(sum($"earnings"))
      .select("year", "dotNET", "Java")

    assertResult(IntegerType)(df.schema("year").dataType)
    assertResult(DecimalType(20, 2))(df.schema("Java").dataType)
    assertResult(DecimalType(20, 2))(df.schema("dotNET").dataType)

    checkAnswer(df, Row(2012, BigDecimal(1500000, 2), BigDecimal(2000000, 2)) ::
      Row(2013, BigDecimal(4800000, 2), BigDecimal(3000000, 2)) :: Nil)
  }

  test("PivotFirst supported datatypes") {
    val supportedDataTypes: Seq[DataType] = DoubleType :: IntegerType :: LongType :: FloatType ::
      BooleanType :: ShortType :: ByteType :: Nil
    for (datatype <- supportedDataTypes) {
      assertResult(true)(PivotFirst.supportsDataType(datatype))
    }
    assertResult(true)(PivotFirst.supportsDataType(DecimalType(10, 1)))
    assertResult(false)(PivotFirst.supportsDataType(null))
    assertResult(false)(PivotFirst.supportsDataType(ArrayType(IntegerType)))
  }

  test("optimized pivot with multiple aggregations") {
    checkAnswer(
      courseSales.groupBy($"year")
        // pivot with extra columns to trigger optimization
        .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
        .agg(sum($"earnings"), avg($"earnings")),
      Row(Seq(2012, 15000.0, 7500.0, 20000.0, 20000.0) ++ Seq.fill(20)(null): _*) ::
        Row(Seq(2013, 48000.0, 48000.0, 30000.0, 30000.0) ++ Seq.fill(20)(null): _*) :: Nil
    )
  }

  test("pivot with datatype not supported by PivotFirst") {
    val expected = Row(Seq(1, 1, 1), Seq(2, 2, 2)) :: Nil
    checkAnswer(
      complexData.groupBy().pivot("b", Seq(true, false)).agg(max("a")),
      expected)
    checkAnswer(
      complexData.groupBy().pivot($"b", Seq(true, false)).agg(max("a")),
      expected)
  }

  test("pivot with datatype not supported by PivotFirst 2") {
    checkAnswer(
      courseSales.withColumn("e", expr("array(earnings, 7.0d)"))
        .groupBy("year")
        .pivot("course", Seq("dotNET", "Java"))
        .agg(min($"e")),
      Row(2012, Seq(5000.0, 7.0), Seq(20000.0, 7.0)) ::
        Row(2013, Seq(48000.0, 7.0), Seq(30000.0, 7.0)) :: Nil
    )
  }

  test("pivot preserves aliases if given") {
    assertResult(
      Array("year", "dotNET_foo", "dotNET_avg(earnings)", "Java_foo", "Java_avg(earnings)")
    )(
      courseSales.groupBy($"year")
        .pivot("course", Seq("dotNET", "Java"))
        .agg(sum($"earnings").as("foo"), avg($"earnings")).columns
    )
  }

  test("pivot with column definition in groupby") {
    checkAnswer(
      courseSales.groupBy(substring(col("course"), 0, 1).as("foo"))
        .pivot("year", Seq(2012, 2013))
        .sum("earnings"),
      Row("d", 15000.0, 48000.0) :: Row("J", 20000.0, 30000.0) :: Nil
    )
  }

  test("pivot with null should not throw NPE") {
    checkAnswer(
      Seq(Tuple1(None), Tuple1(Some(1))).toDF("a").groupBy($"a").pivot("a").count(),
      Row(null, 1, null) :: Row(1, null, 1) :: Nil)
  }

  test("pivot with null and aggregate type not supported by PivotFirst returns correct result") {
    checkAnswer(
      Seq(Tuple1(None), Tuple1(Some(1))).toDF("a")
        .withColumn("b", expr("array(a, 7)"))
        .groupBy($"a").pivot("a").agg(min($"b")),
      Row(null, Seq(null, 7), null) :: Row(1, null, Seq(1, 7)) :: Nil)
  }

  test("pivot with timestamp and count should not print internal representation") {
    val ts = "2012-12-31 16:00:10.011"
    val tsWithZone = "2013-01-01 00:00:10.011"

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val df = Seq(java.sql.Timestamp.valueOf(ts)).toDF("a").groupBy("a").pivot("a").count()
      val expected = StructType(
        StructField("a", TimestampType) ::
        StructField(tsWithZone, LongType) :: Nil)
      assert(df.schema == expected)
      // String representation of timestamp with timezone should take the time difference
      // into account.
      checkAnswer(df.select($"a".cast(StringType)), Row(tsWithZone))
    }
  }

  test("SPARK-24722: pivoting nested columns") {
    val expected = Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    val df = trainingSales
      .groupBy($"sales.year")
      .pivot(lower($"sales.course"), Seq("dotNet", "Java").map(_.toLowerCase(Locale.ROOT)))
      .agg(sum($"sales.earnings"))

    checkAnswer(df, expected)
  }

  test("SPARK-24722: references to multiple columns in the pivot column") {
    val expected = Row(2012, 10000.0) :: Row(2013, 48000.0) :: Nil
    val df = trainingSales
      .groupBy($"sales.year")
      .pivot(concat_ws("-", $"training", $"sales.course"), Seq("Experts-dotNET"))
      .agg(sum($"sales.earnings"))

    checkAnswer(df, expected)
  }

  test("SPARK-24722: pivoting by a constant") {
    val expected = Row(2012, 35000.0) :: Row(2013, 78000.0) :: Nil
    val df1 = trainingSales
      .groupBy($"sales.year")
      .pivot(lit(123), Seq(123))
      .agg(sum($"sales.earnings"))

    checkAnswer(df1, expected)
  }

  test("SPARK-24722: aggregate as the pivot column") {
    checkError(
      exception = intercept[AnalysisException] {
        trainingSales
          .groupBy($"sales.year")
          .pivot(min($"training"), Seq("Experts"))
          .agg(sum($"sales.earnings"))
      },
      condition = "GROUP_BY_AGGREGATE",
      parameters = Map("sqlExpr" -> "min(training)"),
      context = ExpectedContext(fragment = "min", callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("pivoting column list with values") {
    val expected = Row(2012, 10000.0, null) :: Row(2013, 48000.0, 30000.0) :: Nil
    val df = trainingSales
      .groupBy($"sales.year")
      .pivot(struct(lower($"sales.course"), $"training"), Seq(
        struct(lit("dotnet"), lit("Experts")),
        struct(lit("java"), lit("Dummies")))
      ).agg(sum($"sales.earnings"))

    checkAnswer(df, expected)
  }

  test("SPARK-26403: pivoting by array column") {
    val df = Seq(
      (2, Seq.empty[String]),
      (2, Seq("a", "x")),
      (3, Seq.empty[String]),
      (3, Seq("a", "x"))).toDF("x", "s")
    val expected = Seq((3, 1, 1), (2, 1, 1)).toDF()
    val actual = df.groupBy("x").pivot("s").count()
    checkAnswer(actual, expected)
  }

  test("SPARK-35480: percentile_approx should work with pivot") {
    val actual = Seq(
      ("a", -1.0), ("a", 5.5), ("a", 2.5), ("b", 3.0), ("b", 5.2)).toDF("type", "value")
      .groupBy().pivot("type", Seq("a", "b")).agg(
        percentile_approx(col("value"), array(lit(0.5)), lit(10000)))
    checkAnswer(actual, Row(Array(2.5), Array(3.0)))
  }

  test("SPARK-38133: Grouping by TIMESTAMP_NTZ should not corrupt results") {
    checkAnswer(
      courseSales.withColumn("ts", $"year".cast("string").cast("timestamp_ntz"))
        .groupBy("ts")
        .pivot("course", Seq("dotNET", "Java"))
        .agg(sum($"earnings"))
        .select("ts", "dotNET", "Java"),
      Row(LocalDateTime.of(2012, 1, 1, 0, 0, 0, 0), 15000.0, 20000.0) ::
        Row(LocalDateTime.of(2013, 1, 1, 0, 0, 0, 0), 48000.0, 30000.0) :: Nil
    )
  }

  test("using pivot in streaming is not supported") {
    val df = spark
      .readStream
      .format("rate")
      .load()
      .withColumn("key", expr(s"MOD(value, 10)"))
      .groupBy($"key")

    val e = intercept[AnalysisException] {
      df.pivot("value").count()
    }

    assert(e.getMessage.contains("pivot is not supported on a streaming DataFrames/Datasets"))
  }
}
