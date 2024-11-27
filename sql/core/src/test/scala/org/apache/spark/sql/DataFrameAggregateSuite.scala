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

import java.time.{Duration, LocalDateTime, Period}

import scala.util.Random

import org.scalatest.matchers.must.Matchers.the

import org.apache.spark.{SparkArithmeticException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.catalyst.util.AUTO_GENERATED_ALIAS
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData.DecimalData
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.{DAY, HOUR, MINUTE, SECOND}
import org.apache.spark.sql.types.YearMonthIntervalType.{MONTH, YEAR}
import org.apache.spark.unsafe.types.CalendarInterval

case class Fact(date: Int, hour: Int, minute: Int, room_name: String, temp: Double)

class DataFrameAggregateSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  val absTol = 1e-8

  test("groupBy") {
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b").as("totB")).agg(sum($"totB")),
      Row(9)
    )
    checkAnswer(
      testData2.groupBy("a").agg(count("*")),
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil
    )
    checkAnswer(
      testData2.groupBy("a").agg(Map("*" -> "count")),
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil
    )
    checkAnswer(
      testData2.groupBy("a").agg(Map("b" -> "sum")),
      Row(1, 3) :: Row(2, 3) :: Row(3, 3) :: Nil
    )

    val df1 = Seq(("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d"))
      .toDF("key", "value1", "value2", "rest")

    checkAnswer(
      df1.groupBy("key").min(),
      df1.groupBy("key").min("value1", "value2").collect()
    )
    checkAnswer(
      df1.groupBy("key").min("value2"),
      Seq(Row("a", 0), Row("b", 4))
    )

    checkAnswer(
      decimalData.groupBy("a").agg(sum("b")),
      Seq(Row(new java.math.BigDecimal(1), new java.math.BigDecimal(3)),
        Row(new java.math.BigDecimal(2), new java.math.BigDecimal(3)),
        Row(new java.math.BigDecimal(3), new java.math.BigDecimal(3)))
    )

    val decimalDataWithNulls = spark.sparkContext.parallelize(
      DecimalData(1, 1) ::
      DecimalData(1, null) ::
      DecimalData(2, 1) ::
      DecimalData(2, null) ::
      DecimalData(3, 1) ::
      DecimalData(3, 2) ::
      DecimalData(null, 2) :: Nil).toDF()
    checkAnswer(
      decimalDataWithNulls.groupBy("a").agg(sum("b")),
      Seq(Row(new java.math.BigDecimal(1), new java.math.BigDecimal(1)),
        Row(new java.math.BigDecimal(2), new java.math.BigDecimal(1)),
        Row(new java.math.BigDecimal(3), new java.math.BigDecimal(3)),
        Row(null, new java.math.BigDecimal(2)))
    )
  }

  test("SPARK-17124 agg should be ordering preserving") {
    val df = spark.range(2)
    val ret = df.groupBy("id").agg("id" -> "sum", "id" -> "count", "id" -> "min")
    assert(ret.schema.map(_.name) == Seq("id", "sum(id)", "count(id)", "min(id)"))
    checkAnswer(
      ret,
      Row(0, 0, 1, 0) :: Row(1, 1, 1, 1) :: Nil
    )
  }

  test("SPARK-18952: regexes fail codegen when used as keys due to bad forward-slash escapes") {
    val df = Seq(("some[thing]", "random-string")).toDF("key", "val")

    checkAnswer(
      df.groupBy(regexp_extract($"key", "([a-z]+)\\[", 1)).count(),
      Row("some", 1) :: Nil
    )
  }

  test("rollup") {
    checkAnswer(
      courseSales.rollup("course", "year").sum("earnings"),
      Row("Java", 2012, 20000.0) ::
        Row("Java", 2013, 30000.0) ::
        Row("Java", null, 50000.0) ::
        Row("dotNET", 2012, 15000.0) ::
        Row("dotNET", 2013, 48000.0) ::
        Row("dotNET", null, 63000.0) ::
        Row(null, null, 113000.0) :: Nil
    )
  }

  test("cube") {
    checkAnswer(
      courseSales.cube("course", "year").sum("earnings"),
      Row("Java", 2012, 20000.0) ::
        Row("Java", 2013, 30000.0) ::
        Row("Java", null, 50000.0) ::
        Row("dotNET", 2012, 15000.0) ::
        Row("dotNET", 2013, 48000.0) ::
        Row("dotNET", null, 63000.0) ::
        Row(null, 2012, 35000.0) ::
        Row(null, 2013, 78000.0) ::
        Row(null, null, 113000.0) :: Nil
    )

    val df0 = spark.sparkContext.parallelize(Seq(
      Fact(20151123, 18, 35, "room1", 18.6),
      Fact(20151123, 18, 35, "room2", 22.4),
      Fact(20151123, 18, 36, "room1", 17.4),
      Fact(20151123, 18, 36, "room2", 25.6))).toDF()

    val cube0 = df0.cube("date", "hour", "minute", "room_name").agg(Map("temp" -> "avg"))
    assert(cube0.where("date IS NULL").count() > 0)
  }

  test("SPARK-45929 support grouping set operation in dataframe api") {
    checkAnswer(
      courseSales
        .groupingSets(
          Seq(Seq(Column("course"), Column("year")), Seq()),
          Column("course"),
          Column("year"))
        .agg(sum(Column("earnings")), grouping_id()),
      Row("Java", 2012, 20000.0, 0) ::
        Row("Java", 2013, 30000.0, 0) ::
        Row("dotNET", 2012, 15000.0, 0) ::
        Row("dotNET", 2013, 48000.0, 0) ::
        Row(null, null, 113000.0, 3) :: Nil)
  }

  test("grouping and grouping_id") {
    checkAnswer(
      courseSales.cube("course", "year")
        .agg(grouping("course"), grouping("year"), grouping_id("course", "year")),
      Row("Java", 2012, 0, 0, 0) ::
        Row("Java", 2013, 0, 0, 0) ::
        Row("Java", null, 0, 1, 1) ::
        Row("dotNET", 2012, 0, 0, 0) ::
        Row("dotNET", 2013, 0, 0, 0) ::
        Row("dotNET", null, 0, 1, 1) ::
        Row(null, 2012, 1, 0, 2) ::
        Row(null, 2013, 1, 0, 2) ::
        Row(null, null, 1, 1, 3) :: Nil
    )

    // use column reference in `grouping_id` instead of column name
    checkAnswer(
      courseSales.cube("course", "year")
        .agg(grouping_id(courseSales("course"), courseSales("year"))),
      Row("Java", 2012, 0) ::
        Row("Java", 2013, 0) ::
        Row("Java", null, 1) ::
        Row("dotNET", 2012, 0) ::
        Row("dotNET", 2013, 0) ::
        Row("dotNET", null, 1) ::
        Row(null, 2012, 2) ::
        Row(null, 2013, 2) ::
        Row(null, null, 3) :: Nil
    )

    intercept[AnalysisException] {
      courseSales.agg(grouping("course")).explain()
    }

    intercept[AnalysisException] {
      courseSales.agg(grouping_id("course")).explain()
    }
  }

  test("grouping/grouping_id inside window function") {
    checkAnswer(
      courseSales.cube("course", "year")
        .agg(sum("earnings"),
          grouping_id("course", "year"),
          rank().over(Window.partitionBy(grouping_id("course", "year")).orderBy(sum("earnings")))),
      Row("Java", 2012, 20000.0, 0, 2) ::
        Row("Java", 2013, 30000.0, 0, 3) ::
        Row("Java", null, 50000.0, 1, 1) ::
        Row("dotNET", 2012, 15000.0, 0, 1) ::
        Row("dotNET", 2013, 48000.0, 0, 4) ::
        Row("dotNET", null, 63000.0, 1, 2) ::
        Row(null, 2012, 35000.0, 2, 1) ::
        Row(null, 2013, 78000.0, 2, 2) ::
        Row(null, null, 113000.0, 3, 1) :: Nil
    )
  }

  test("SPARK-21980: References in grouping functions should be indexed with semanticEquals") {
    checkAnswer(
      courseSales.cube("course", "year")
        .agg(grouping("CouRse"), grouping("year")),
      Row("Java", 2012, 0, 0) ::
        Row("Java", 2013, 0, 0) ::
        Row("Java", null, 0, 1) ::
        Row("dotNET", 2012, 0, 0) ::
        Row("dotNET", 2013, 0, 0) ::
        Row("dotNET", null, 0, 1) ::
        Row(null, 2012, 1, 0) ::
        Row(null, 2013, 1, 0) ::
        Row(null, null, 1, 1) :: Nil
    )
  }

  test("rollup overlapping columns") {
    checkAnswer(
      testData2.rollup($"a" + $"b" as "foo", $"b" as "bar").agg(sum($"a" - $"b") as "foo"),
      Row(2, 1, 0) :: Row(3, 2, -1) :: Row(3, 1, 1) :: Row(4, 2, 0) :: Row(4, 1, 2) :: Row(5, 2, 1)
        :: Row(2, null, 0) :: Row(3, null, 0) :: Row(4, null, 2) :: Row(5, null, 1)
        :: Row(null, null, 3) :: Nil
    )

    checkAnswer(
      testData2.rollup("a", "b").agg(sum("b")),
      Row(1, 1, 1) :: Row(1, 2, 2) :: Row(2, 1, 1) :: Row(2, 2, 2) :: Row(3, 1, 1) :: Row(3, 2, 2)
        :: Row(1, null, 3) :: Row(2, null, 3) :: Row(3, null, 3)
        :: Row(null, null, 9) :: Nil
    )
  }

  test("cube overlapping columns") {
    checkAnswer(
      testData2.cube($"a" + $"b", $"b").agg(sum($"a" - $"b")),
      Row(2, 1, 0) :: Row(3, 2, -1) :: Row(3, 1, 1) :: Row(4, 2, 0) :: Row(4, 1, 2) :: Row(5, 2, 1)
        :: Row(2, null, 0) :: Row(3, null, 0) :: Row(4, null, 2) :: Row(5, null, 1)
        :: Row(null, 1, 3) :: Row(null, 2, 0)
        :: Row(null, null, 3) :: Nil
    )

    checkAnswer(
      testData2.cube("a", "b").agg(sum("b")),
      Row(1, 1, 1) :: Row(1, 2, 2) :: Row(2, 1, 1) :: Row(2, 2, 2) :: Row(3, 1, 1) :: Row(3, 2, 2)
        :: Row(1, null, 3) :: Row(2, null, 3) :: Row(3, null, 3)
        :: Row(null, 1, 3) :: Row(null, 2, 6)
        :: Row(null, null, 9) :: Nil
    )
  }

  test("spark.sql.retainGroupColumns config") {
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )

    spark.conf.set(SQLConf.DATAFRAME_RETAIN_GROUP_COLUMNS.key, false)
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),
      Seq(Row(3), Row(3), Row(3))
    )
    spark.conf.set(SQLConf.DATAFRAME_RETAIN_GROUP_COLUMNS.key, true)
  }

  test("agg without groups") {
    checkAnswer(
      testData2.agg(sum($"b")),
      Row(9)
    )
  }

  test("agg without groups and functions") {
    checkAnswer(
      testData2.agg(lit(1)),
      Row(1)
    )
  }

  test("average") {
    checkAnswer(
      testData2.agg(avg($"a"), mean($"a")),
      Row(2.0, 2.0))

    checkAnswer(
      testData2.agg(avg($"a"), sumDistinct($"a")), // non-partial and test deprecated version
      Row(2.0, 6.0) :: Nil)

    checkAnswer(
      decimalData.agg(avg($"a")),
      Row(new java.math.BigDecimal(2)))

    checkAnswer(
      decimalData.agg(avg($"a"), sum_distinct($"a")), // non-partial
      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)

    checkAnswer(
      decimalData.agg(avg($"a" cast DecimalType(10, 2))),
      Row(new java.math.BigDecimal(2)))
    // non-partial
    checkAnswer(
      decimalData.agg(
        avg($"a" cast DecimalType(10, 2)), sum_distinct($"a" cast DecimalType(10, 2))),
      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)

    checkAnswer(
      emptyTestData.agg(avg($"key" cast DecimalType(10, 0))),
      Row(null))
  }

  test("null average") {
    checkAnswer(
      testData3.agg(avg($"b")),
      Row(2.0))

    checkAnswer(
      testData3.agg(avg($"b"), count_distinct($"b")),
      Row(2.0, 1))

    checkAnswer(
      testData3.agg(avg($"b"), sum_distinct($"b")), // non-partial
      Row(2.0, 2.0))
  }

  test("zero average") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(avg($"a")),
      Row(null))

    checkAnswer(
      emptyTableData.agg(avg($"a"), sum_distinct($"b")), // non-partial
      Row(null, null))
  }

  test("count") {
    assert(testData2.count() === testData2.rdd.map(_ => 1).count())

    checkAnswer(
      testData2.agg(count($"a"), sum_distinct($"a")), // non-partial
      Row(6, 6.0))
  }

  test("null count") {
    checkAnswer(
      testData3.groupBy($"a").agg(count($"b")),
      Seq(Row(1, 0), Row(2, 1))
    )

    checkAnswer(
      testData3.groupBy($"a").agg(count($"a" + $"b")),
      Seq(Row(1, 0), Row(2, 1))
    )

    checkAnswer(
      testData3.agg(
        count($"a"), count($"b"), count(lit(1)), count_distinct($"a"), count_distinct($"b")),
      Row(2, 1, 2, 2, 1)
    )

    checkAnswer(
      testData3.agg(count($"b"), count_distinct($"b"), sum_distinct($"b")), // non-partial
      Row(1, 1, 2)
    )
  }

  test("multiple column distinct count") {
    val df1 = Seq(
      ("a", "b", "c"),
      ("a", "b", "c"),
      ("a", "b", "d"),
      ("x", "y", "z"),
      ("x", "q", null.asInstanceOf[String]))
      .toDF("key1", "key2", "key3")

    checkAnswer(
      df1.agg(count_distinct($"key1", $"key2")),
      Row(3)
    )

    checkAnswer(
      df1.agg(count_distinct($"key1", $"key2", $"key3")),
      Row(3)
    )

    checkAnswer(
      df1.groupBy($"key1").agg(count_distinct($"key2", $"key3")),
      Seq(Row("a", 2), Row("x", 1))
    )
  }

  test("zero count") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(count($"a"), sum_distinct($"a")), // non-partial
      Row(0, null))
  }

  test("stddev") {
    val testData2ADev = math.sqrt(4.0 / 5.0)
    checkAnswer(
      testData2.agg(stddev($"a"), stddev_pop($"a"), stddev_samp($"a")),
      Row(testData2ADev, math.sqrt(4 / 6.0), testData2ADev))
    checkAnswer(
      testData2.agg(stddev("a"), stddev_pop("a"), stddev_samp("a")),
      Row(testData2ADev, math.sqrt(4 / 6.0), testData2ADev))
  }

  test("zero stddev") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
    emptyTableData.agg(stddev($"a"), stddev_pop($"a"), stddev_samp($"a")),
    Row(null, null, null))
  }

  test("zero sum") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(sum($"a")),
      Row(null))
  }

  test("zero sum distinct") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(sum_distinct($"a")),
      Row(null))
  }

  test("moments") {

    val sparkVariance = testData2.agg(variance($"a"))
    checkAggregatesWithTol(sparkVariance, Row(4.0 / 5.0), absTol)

    val sparkVariancePop = testData2.agg(var_pop($"a"))
    checkAggregatesWithTol(sparkVariancePop, Row(4.0 / 6.0), absTol)

    val sparkVarianceSamp = testData2.agg(var_samp($"a"))
    checkAggregatesWithTol(sparkVarianceSamp, Row(4.0 / 5.0), absTol)

    val sparkSkewness = testData2.agg(skewness($"a"))
    checkAggregatesWithTol(sparkSkewness, Row(0.0), absTol)

    val sparkKurtosis = testData2.agg(kurtosis($"a"))
    checkAggregatesWithTol(sparkKurtosis, Row(-1.5), absTol)
  }

  test("linear regression") {
    checkAnswer(testData2.agg(regr_avgx($"a", $"b")), testData2.selectExpr("regr_avgx(a, b)"))
    checkAnswer(testData2.agg(regr_avgy($"a", $"b")), testData2.selectExpr("regr_avgy(a, b)"))
    checkAnswer(testData2.agg(regr_count($"a", $"b")), testData2.selectExpr("regr_count(a, b)"))
    checkAnswer(
      testData2.agg(regr_intercept($"a", $"b")), testData2.selectExpr("regr_intercept(a, b)"))
    checkAnswer(testData2.agg(regr_r2($"a", $"b")), testData2.selectExpr("regr_r2(a, b)"))
    checkAnswer(testData2.agg(regr_slope($"a", $"b")), testData2.selectExpr("regr_slope(a, b)"))
    checkAnswer(testData2.agg(regr_sxx($"a", $"b")), testData2.selectExpr("regr_sxx(a, b)"))
    checkAnswer(testData2.agg(regr_sxy($"a", $"b")), testData2.selectExpr("regr_sxy(a, b)"))
    checkAnswer(testData2.agg(regr_syy($"a", $"b")), testData2.selectExpr("regr_syy(a, b)"))
  }

  test("every | bool_and | some | any | bool_or") {
    checkAnswer(complexData.agg(every($"b")), complexData.selectExpr("every(b)"))
    checkAnswer(complexData.agg(bool_and($"b")), complexData.selectExpr("bool_and(b)"))
    checkAnswer(complexData.agg(some($"b")), complexData.selectExpr("some(b)"))
    checkAnswer(complexData.agg(any($"b")), complexData.selectExpr("any(b)"))
    checkAnswer(complexData.agg(bool_or($"b")), complexData.selectExpr("bool_or(b)"))
  }

  test("bit aggregate") {
    checkAnswer(testData2.agg(bit_and($"b")), testData2.selectExpr("bit_and(b)"))
    checkAnswer(testData2.agg(bit_or($"b")), testData2.selectExpr("bit_or(b)"))
    checkAnswer(testData2.agg(bit_xor($"b")), testData2.selectExpr("bit_xor(b)"))
  }

  test("zero moments") {
    withSQLConf(SQLConf.LEGACY_STATISTICAL_AGGREGATE.key -> "true") {
      val input = Seq((1, 2)).toDF("a", "b")
      checkAnswer(
        input.agg(stddev($"a"), stddev_samp($"a"), stddev_pop($"a"), variance($"a"),
          var_samp($"a"), var_pop($"a"), skewness($"a"), kurtosis($"a")),
        Row(Double.NaN, Double.NaN, 0.0, Double.NaN, Double.NaN, 0.0,
          Double.NaN, Double.NaN))

      checkAnswer(
        input.agg(
          expr("stddev(a)"),
          expr("stddev_samp(a)"),
          expr("stddev_pop(a)"),
          expr("variance(a)"),
          expr("var_samp(a)"),
          expr("var_pop(a)"),
          expr("skewness(a)"),
          expr("kurtosis(a)")),
        Row(Double.NaN, Double.NaN, 0.0, Double.NaN, Double.NaN, 0.0,
          Double.NaN, Double.NaN))
    }
  }

  test("SPARK-13860: zero moments LEGACY_STATISTICAL_AGGREGATE off") {
    withSQLConf(SQLConf.LEGACY_STATISTICAL_AGGREGATE.key -> "false") {
      val input = Seq((1, 2)).toDF("a", "b")
      checkAnswer(
        input.agg(stddev($"a"), stddev_samp($"a"), stddev_pop($"a"), variance($"a"),
          var_samp($"a"), var_pop($"a"), skewness($"a"), kurtosis($"a")),
        Row(null, null, 0.0, null, null, 0.0,
          null, null))

      checkAnswer(
        input.agg(
          expr("stddev(a)"),
          expr("stddev_samp(a)"),
          expr("stddev_pop(a)"),
          expr("variance(a)"),
          expr("var_samp(a)"),
          expr("var_pop(a)"),
          expr("skewness(a)"),
          expr("kurtosis(a)")),
        Row(null, null, 0.0, null, null, 0.0,
          null, null))
    }
  }

  test("null moments") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(emptyTableData.agg(
      variance($"a"), var_samp($"a"), var_pop($"a"), skewness($"a"), kurtosis($"a")),
      Row(null, null, null, null, null))

    checkAnswer(
      emptyTableData.agg(
        expr("variance(a)"),
        expr("var_samp(a)"),
        expr("var_pop(a)"),
        expr("skewness(a)"),
        expr("kurtosis(a)")),
      Row(null, null, null, null, null))
  }

  test("collect functions") {
    val df = Seq((1, 2), (2, 2), (3, 4)).toDF("a", "b")
    checkAnswer(
      df.select(collect_list($"a"), collect_list($"b")),
      Seq(Row(Seq(1, 2, 3), Seq(2, 2, 4)))
    )
    checkAnswer(
      df.select(collect_set($"a"), collect_set($"b")),
      Seq(Row(Seq(1, 2, 3), Seq(2, 4)))
    )

    checkDataset(
      df.select(collect_set($"a").as("aSet")).as[Set[Int]],
      Set(1, 2, 3))
    checkDataset(
      df.select(collect_set($"b").as("bSet")).as[Set[Int]],
      Set(2, 4))
    checkDataset(
      df.select(collect_set($"a"), collect_set($"b")).as[(Set[Int], Set[Int])],
      Seq(Set(1, 2, 3) -> Set(2, 4)): _*)
  }

  test("array_agg function") {
    val df = Seq((1, 2), (2, 2), (3, 4)).toDF("a", "b")
    checkAnswer(
      df.selectExpr("array_agg(a)", "array_agg(b)"),
      Seq(Row(Seq(1, 2, 3), Seq(2, 2, 4)))
    )
    checkAnswer(
      df.select(array_agg($"a"), array_agg($"b")),
      Seq(Row(Seq(1, 2, 3), Seq(2, 2, 4)))
    )
  }

  test("collect functions structs") {
    val df = Seq((1, 2, 2), (2, 2, 2), (3, 4, 1))
      .toDF("a", "x", "y")
      .select($"a", struct($"x", $"y").as("b"))
    checkAnswer(
      df.select(collect_list($"a"), sort_array(collect_list($"b"))),
      Seq(Row(Seq(1, 2, 3), Seq(Row(2, 2), Row(2, 2), Row(4, 1))))
    )
    checkAnswer(
      df.select(collect_set($"a"), sort_array(collect_set($"b"))),
      Seq(Row(Seq(1, 2, 3), Seq(Row(2, 2), Row(4, 1))))
    )
  }

  test("SPARK-31500: collect_set() of BinaryType returns duplicate elements") {
    val bytesTest1 = "test1".getBytes
    val bytesTest2 = "test2".getBytes
    val df = Seq(bytesTest1, bytesTest1, bytesTest2).toDF("a")
    checkAnswer(df.select(size(collect_set($"a"))), Row(2) :: Nil)

    val a = "aa".getBytes
    val b = "bb".getBytes
    val c = "cc".getBytes
    val d = "dd".getBytes
    val df1 = Seq((a, b), (a, b), (c, d))
      .toDF("x", "y")
      .select(struct($"x", $"y").as("a"))
    checkAnswer(df1.select(size(collect_set($"a"))), Row(2) :: Nil)
  }

  test("collect_set functions cannot have maps") {
    val df = Seq((1, 3, 0), (2, 3, 0), (3, 4, 1))
      .toDF("a", "x", "y")
      .select($"a", map($"x", $"y").as("b"))
    val error = intercept[AnalysisException] {
      df.select(collect_set($"a"), collect_set($"b"))
    }
    checkError(
      exception = error,
      condition = "DATATYPE_MISMATCH.UNSUPPORTED_INPUT_TYPE",
      parameters = Map(
        "functionName" -> "`collect_set`",
        "dataType" -> "\"MAP\" or \"COLLATED STRING\"",
        "sqlExpr" -> "\"collect_set(b)\""
      ),
      context = ExpectedContext(
        fragment = "collect_set",
        callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("SPARK-17641: collect functions should not collect null values") {
    val df = Seq(("1", 2), (null, 2), ("1", 4)).toDF("a", "b")
    checkAnswer(
      df.select(collect_list($"a"), collect_list($"b")),
      Seq(Row(Seq("1", "1"), Seq(2, 2, 4)))
    )
    checkAnswer(
      df.select(collect_set($"a"), collect_set($"b")),
      Seq(Row(Seq("1"), Seq(2, 4)))
    )
  }

  test("collect functions should be able to cast to array type with no null values") {
    val df = Seq(1, 2).toDF("a")
    checkAnswer(df.select(collect_list("a") cast ArrayType(IntegerType, false)),
      Seq(Row(Seq(1, 2))))
    checkAnswer(df.select(collect_set("a") cast ArrayType(FloatType, false)),
      Seq(Row(Seq(1.0, 2.0))))
  }

  test("SPARK-14664: Decimal sum/avg over window should work.") {
    checkAnswer(
      spark.sql("select sum(a) over () from values 1.0, 2.0, 3.0 T(a)"),
      Row(6.0) :: Row(6.0) :: Row(6.0) :: Nil)
    checkAnswer(
      spark.sql("select avg(a) over () from values 1.0, 2.0, 3.0 T(a)"),
      Row(2.0) :: Row(2.0) :: Row(2.0) :: Nil)
  }

  test("SQL decimal test (used for catching certain decimal handling bugs in aggregates)") {
    checkAnswer(
      decimalData.groupBy($"a" cast DecimalType(10, 2)).agg(avg($"b" cast DecimalType(10, 2))),
      Seq(Row(new java.math.BigDecimal(1), new java.math.BigDecimal("1.5")),
        Row(new java.math.BigDecimal(2), new java.math.BigDecimal("1.5")),
        Row(new java.math.BigDecimal(3), new java.math.BigDecimal("1.5"))))
  }

  test("SPARK-17616: distinct aggregate combined with a non-partial aggregate") {
    val df = Seq((1, 3, "a"), (1, 2, "b"), (3, 4, "c"), (3, 4, "c"), (3, 5, "d"))
      .toDF("x", "y", "z")
    checkAnswer(
      df.groupBy($"x").agg(count_distinct($"y"), sort_array(collect_list($"z"))),
      Seq(Row(1, 2, Seq("a", "b")), Row(3, 2, Seq("c", "c", "d"))))
  }

  test("SPARK-18004 limit + aggregates") {
    val df = Seq(("a", 1), ("b", 2), ("c", 1), ("d", 5)).toDF("id", "value")
    val limit2Df = df.limit(2)
    checkAnswer(
      limit2Df.groupBy("id").count().select($"id"),
      limit2Df.select($"id"))
  }

  test("SPARK-17237 remove backticks in a pivot result schema") {
    val df = Seq((2, 3, 4), (3, 4, 5)).toDF("a", "x", "y")
    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
      checkAnswer(
        df.groupBy("a").pivot("x").agg(count("y"), avg("y")).na.fill(0),
        Seq(Row(3, 0, 0.0, 1, 5.0), Row(2, 1, 4.0, 0, 0.0))
      )
    }
  }

  test("aggregate function in GROUP BY") {
    checkError(
      exception = intercept[AnalysisException] {
        testData.groupBy(sum($"key")).count()
      },
      condition = "GROUP_BY_AGGREGATE",
      parameters = Map("sqlExpr" -> "sum(key)"),
      context = ExpectedContext(fragment = "sum", callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  private def assertNoExceptions(c: Column): Unit = {
    for ((wholeStage, useObjectHashAgg) <-
         Seq((true, true), (true, false), (false, true), (false, false))) {
      withSQLConf(
        (SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, wholeStage.toString),
        (SQLConf.USE_OBJECT_HASH_AGG.key, useObjectHashAgg.toString)) {

        val df = Seq(("1", 1), ("1", 2), ("2", 3), ("2", 4)).toDF("x", "y")

        // test case for HashAggregate
        val hashAggDF = df.groupBy("x").agg(c, sum("y"))
        hashAggDF.collect()
        val hashAggPlan = hashAggDF.queryExecution.executedPlan
        if (wholeStage) {
          assert(find(hashAggPlan) {
            case WholeStageCodegenExec(_: HashAggregateExec) => true
            case _ => false
          }.isDefined)
        } else {
          assert(stripAQEPlan(hashAggPlan).isInstanceOf[HashAggregateExec])
        }

        // test case for ObjectHashAggregate and SortAggregate
        val objHashAggOrSortAggDF = df.groupBy("x").agg(c, collect_list("y"))
        objHashAggOrSortAggDF.collect()
        val objHashAggOrSortAggPlan =
          stripAQEPlan(objHashAggOrSortAggDF.queryExecution.executedPlan)
        if (useObjectHashAgg) {
          assert(objHashAggOrSortAggPlan.isInstanceOf[ObjectHashAggregateExec])
        } else {
          assert(objHashAggOrSortAggPlan.isInstanceOf[SortAggregateExec])
        }
      }
    }
  }

  test("SPARK-19471: AggregationIterator does not initialize the generated result projection" +
    " before using it") {
    Seq(
      monotonically_increasing_id(), spark_partition_id(),
      rand(Random.nextLong()), randn(Random.nextLong())
    ).foreach(assertNoExceptions)
  }

  test("SPARK-21580 ints in aggregation expressions are taken as group-by ordinal.") {
    checkAnswer(
      testData2.groupBy(lit(3), lit(4)).agg(lit(6), lit(7), sum("b")),
      Seq(Row(3, 4, 6, 7, 9)))
    checkAnswer(
      testData2.groupBy(lit(3), lit(4)).agg(lit(6), $"b", sum("b")),
      Seq(Row(3, 4, 6, 1, 3), Row(3, 4, 6, 2, 6)))

    checkAnswer(
      spark.sql("SELECT 3, 4, SUM(b) FROM testData2 GROUP BY 1, 2"),
      Seq(Row(3, 4, 9)))
    checkAnswer(
      spark.sql("SELECT 3 AS c, 4 AS d, SUM(b) FROM testData2 GROUP BY c, d"),
      Seq(Row(3, 4, 9)))
  }

  test("SPARK-22223: ObjectHashAggregate should not introduce unnecessary shuffle") {
    withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "true") {
      val df = Seq(("1", "2", 1), ("1", "2", 2), ("2", "3", 3), ("2", "3", 4)).toDF("a", "b", "c")
        .repartition(col("a"))

      val objHashAggDF = df
        .withColumn("d", expr("(a, b, c)"))
        .groupBy("a", "b").agg(collect_list("d").as("e"))
        .withColumn("f", expr("(b, e)"))
        .groupBy("a").agg(collect_list("f").as("g"))
      val aggPlan = objHashAggDF.queryExecution.executedPlan

      val sortAggPlans = collect(aggPlan) {
        case sortAgg: SortAggregateExec => sortAgg
      }
      assert(sortAggPlans.isEmpty)

      val objHashAggPlans = collect(aggPlan) {
        case objHashAgg: ObjectHashAggregateExec => objHashAgg
      }
      assert(objHashAggPlans.nonEmpty)

      val exchangePlans = collect(aggPlan) {
        case shuffle: ShuffleExchangeExec => shuffle
      }
      assert(exchangePlans.length == 1)
    }
  }

  testWithWholeStageCodegenOnAndOff("SPARK-22951: dropDuplicates on empty dataFrames " +
    "should produce correct aggregate") { _ =>
    // explicit global aggregations
    val emptyAgg = Map.empty[String, String]
    checkAnswer(spark.emptyDataFrame.agg(emptyAgg), Seq(Row()))
    checkAnswer(spark.emptyDataFrame.agg(emptyAgg), Seq(Row()))
    checkAnswer(spark.emptyDataFrame.agg(count("*")), Seq(Row(0)))
    checkAnswer(spark.emptyDataFrame.dropDuplicates().agg(emptyAgg), Seq(Row()))
    checkAnswer(spark.emptyDataFrame.dropDuplicates().agg(emptyAgg), Seq(Row()))
    checkAnswer(spark.emptyDataFrame.dropDuplicates().agg(count("*")), Seq(Row(0)))

    // global aggregation is converted to grouping aggregation:
    assert(spark.emptyDataFrame.dropDuplicates().count() == 0)
  }

  test("SPARK-21896: Window functions inside aggregate functions") {
    def checkWindowError(df: => DataFrame): Unit = {
      val thrownException = the [AnalysisException] thrownBy {
        df.queryExecution.analyzed
      }
      assert(thrownException.message.contains("not allowed to use a window function"))
    }

    checkWindowError(testData2.select(min(avg($"b").over(Window.partitionBy($"a")))))
    checkWindowError(testData2.agg(sum($"b"), max(rank().over(Window.orderBy($"a")))))
    checkWindowError(testData2.groupBy($"a").agg(sum($"b"), max(rank().over(Window.orderBy($"b")))))
    checkWindowError(testData2.groupBy($"a").agg(max(sum(sum($"b")).over(Window.orderBy($"a")))))
    checkWindowError(testData2.groupBy($"a").agg(
      sum($"b").as("s"), max(count("*").over())).where($"s" === 3))
    checkAnswer(testData2.groupBy($"a").agg(
      max($"b"), sum($"b").as("s"), count("*").over()).where($"s" === 3),
      Row(1, 2, 3, 3) :: Row(2, 2, 3, 3) :: Row(3, 2, 3, 3) :: Nil)

    checkWindowError(sql("SELECT MIN(AVG(b) OVER(PARTITION BY a)) FROM testData2"))
    checkWindowError(sql("SELECT SUM(b), MAX(RANK() OVER(ORDER BY a)) FROM testData2"))
    checkWindowError(sql("SELECT SUM(b), MAX(RANK() OVER(ORDER BY b)) FROM testData2 GROUP BY a"))
    checkWindowError(sql("SELECT MAX(SUM(SUM(b)) OVER(ORDER BY a)) FROM testData2 GROUP BY a"))
    checkWindowError(
      sql("SELECT MAX(RANK() OVER(ORDER BY b)) FROM testData2 GROUP BY a HAVING SUM(b) = 3"))
    checkAnswer(
      sql("SELECT a, MAX(b), RANK() OVER(ORDER BY a) FROM testData2 GROUP BY a HAVING SUM(b) = 3"),
      Row(1, 2, 1) :: Row(2, 2, 2) :: Row(3, 2, 3) :: Nil)
  }

  test("SPARK-24788: RelationalGroupedDataset.toString with unresolved exprs should not fail") {
    // Checks if these raise no exception
    assert(testData.groupBy($"key").toString.contains(
      "[grouping expressions: [key], value: [key: int, value: string], type: GroupBy]"))
    assert(testData.groupBy(col("key")).toString.contains(
      "[grouping expressions: [key], value: [key: int, value: string], type: GroupBy]"))
    assert(testData.groupBy(current_date()).toString.contains(
      "grouping expressions: ['current_date()], value: [key: int, value: string], " +
        "type: GroupBy]"))
  }

  test("SPARK-26021: NaN and -0.0 in grouping expressions") {
    checkAnswer(
      Seq(0.0f, -0.0f, 0.0f/0.0f, Float.NaN).toDF("f").groupBy("f").count(),
      Row(0.0f, 2) :: Row(Float.NaN, 2) :: Nil)
    checkAnswer(
      Seq(0.0d, -0.0d, 0.0d/0.0d, Double.NaN).toDF("d").groupBy("d").count(),
      Row(0.0d, 2) :: Row(Double.NaN, 2) :: Nil)

    // test with complicated type grouping expressions
    checkAnswer(
      Seq(0.0f, -0.0f, 0.0f/0.0f, Float.NaN).toDF("f")
        .groupBy(array("f"), struct("f")).count(),
      Row(Seq(0.0f), Row(0.0f), 2) ::
        Row(Seq(Float.NaN), Row(Float.NaN), 2) :: Nil)
    checkAnswer(
      Seq(0.0d, -0.0d, 0.0d/0.0d, Double.NaN).toDF("d")
        .groupBy(array("d"), struct("d")).count(),
      Row(Seq(0.0d), Row(0.0d), 2) ::
        Row(Seq(Double.NaN), Row(Double.NaN), 2) :: Nil)

    checkAnswer(
      Seq(0.0f, -0.0f, 0.0f/0.0f, Float.NaN).toDF("f")
        .groupBy(array(struct("f")), struct(array("f"))).count(),
      Row(Seq(Row(0.0f)), Row(Seq(0.0f)), 2) ::
        Row(Seq(Row(Float.NaN)), Row(Seq(Float.NaN)), 2) :: Nil)
    checkAnswer(
      Seq(0.0d, -0.0d, 0.0d/0.0d, Double.NaN).toDF("d")
        .groupBy(array(struct("d")), struct(array("d"))).count(),
      Row(Seq(Row(0.0d)), Row(Seq(0.0d)), 2) ::
        Row(Seq(Row(Double.NaN)), Row(Seq(Double.NaN)), 2) :: Nil)

    // test with complicated type grouping columns
    val df = Seq(
      (Array(-0.0f, 0.0f), Tuple2(-0.0d, Double.NaN), Seq(Tuple2(-0.0d, Double.NaN))),
      (Array(0.0f, -0.0f), Tuple2(0.0d, Double.NaN), Seq(Tuple2(0.0d, 0.0/0.0)))
    ).toDF("arr", "stru", "arrOfStru")
    checkAnswer(
      df.groupBy("arr", "stru", "arrOfStru").count(),
      Row(Seq(0.0f, 0.0f), Row(0.0d, Double.NaN), Seq(Row(0.0d, Double.NaN)), 2)
    )
  }

  test("SPARK-27581: DataFrame count_distinct(\"*\") shouldn't fail with AnalysisException") {
    val df = sql("select id % 100 from range(100000)")
    val distinctCount1 = df.select(expr("count(distinct(*))"))
    val distinctCount2 = df.select(countDistinct("*"))
    checkAnswer(distinctCount1, distinctCount2)

    val countAndDistinct = df.select(count("*"), countDistinct("*"))
    checkAnswer(countAndDistinct, Row(100000, 100))
  }

  test("max_by") {
    val yearOfMaxEarnings =
      sql("SELECT course, max_by(year, earnings) FROM courseSales GROUP BY course")
    checkAnswer(yearOfMaxEarnings, Row("dotNET", 2013) :: Row("Java", 2013) :: Nil)

    checkAnswer(
      courseSales.groupBy("course").agg(max_by(col("year"), col("earnings"))),
      Row("dotNET", 2013) :: Row("Java", 2013) :: Nil
    )

    checkAnswer(
      sql("SELECT max_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y)"),
      Row("b") :: Nil
    )

    checkAnswer(
      sql("SELECT max_by(x, y) FROM VALUES (('a', 10)), (('b', null)), (('c', 20)) AS tab(x, y)"),
      Row("c") :: Nil
    )

    checkAnswer(
      sql("SELECT max_by(x, y) FROM VALUES (('a', null)), (('b', null)), (('c', 20)) AS tab(x, y)"),
      Row("c") :: Nil
    )

    checkAnswer(
      sql("SELECT max_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', null)) AS tab(x, y)"),
      Row("b") :: Nil
    )

    checkAnswer(
      sql("SELECT max_by(x, y) FROM VALUES (('a', null)), (('b', null)) AS tab(x, y)"),
      Row(null) :: Nil
    )

    // structs as ordering value.
    checkAnswer(
      sql("select max_by(x, y) FROM VALUES (('a', (10, 20))), (('b', (10, 50))), " +
        "(('c', (10, 60))) AS tab(x, y)"),
      Row("c") :: Nil
    )

    checkAnswer(
      sql("select max_by(x, y) FROM VALUES (('a', (10, 20))), (('b', (10, 50))), " +
        "(('c', null)) AS tab(x, y)"),
      Row("b") :: Nil
    )

    withTempView("tempView") {
      val dfWithMap = Seq((0, "a"), (1, "b"), (2, "c"))
        .toDF("x", "y")
        .select($"x", map($"x", $"y").as("y"))
        .createOrReplaceTempView("tempView")
      val error = intercept[AnalysisException] {
        sql("SELECT max_by(x, y) FROM tempView").show()
      }
      checkError(
        exception = error,
        condition = "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
        sqlState = None,
        parameters = Map(
          "functionName" -> "`max_by`",
          "dataType" -> "\"MAP<INT, STRING>\"",
          "sqlExpr" -> "\"max_by(x, y)\""
        ),
        context = ExpectedContext(fragment = "max_by(x, y)", start = 7, stop = 18)
      )
    }
  }

  test("min_by") {
    val yearOfMinEarnings =
      sql("SELECT course, min_by(year, earnings) FROM courseSales GROUP BY course")
    checkAnswer(yearOfMinEarnings, Row("dotNET", 2012) :: Row("Java", 2012) :: Nil)

    checkAnswer(
      courseSales.groupBy("course").agg(min_by(col("year"), col("earnings"))),
      Row("dotNET", 2012) :: Row("Java", 2012) :: Nil
    )

    checkAnswer(
      sql("SELECT min_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', 20)) AS tab(x, y)"),
      Row("a") :: Nil
    )

    checkAnswer(
      sql("SELECT min_by(x, y) FROM VALUES (('a', 10)), (('b', null)), (('c', 20)) AS tab(x, y)"),
      Row("a") :: Nil
    )

    checkAnswer(
      sql("SELECT min_by(x, y) FROM VALUES (('a', null)), (('b', null)), (('c', 20)) AS tab(x, y)"),
      Row("c") :: Nil
    )

    checkAnswer(
      sql("SELECT min_by(x, y) FROM VALUES (('a', 10)), (('b', 50)), (('c', null)) AS tab(x, y)"),
      Row("a") :: Nil
    )

    checkAnswer(
      sql("SELECT min_by(x, y) FROM VALUES (('a', null)), (('b', null)) AS tab(x, y)"),
      Row(null) :: Nil
    )

    // structs as ordering value.
    checkAnswer(
      sql("select min_by(x, y) FROM VALUES (('a', (10, 20))), (('b', (10, 50))), " +
        "(('c', (10, 60))) AS tab(x, y)"),
      Row("a") :: Nil
    )

    checkAnswer(
      sql("select min_by(x, y) FROM VALUES (('a', null)), (('b', (10, 50))), " +
        "(('c', (10, 60))) AS tab(x, y)"),
      Row("b") :: Nil
    )

    withTempView("tempView") {
      val dfWithMap = Seq((0, "a"), (1, "b"), (2, "c"))
        .toDF("x", "y")
        .select($"x", map($"x", $"y").as("y"))
        .createOrReplaceTempView("tempView")
      val error = intercept[AnalysisException] {
        sql("SELECT min_by(x, y) FROM tempView").show()
      }
      checkError(
        exception = error,
        condition = "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
        sqlState = None,
        parameters = Map(
          "functionName" -> "`min_by`",
          "dataType" -> "\"MAP<INT, STRING>\"",
          "sqlExpr" -> "\"min_by(x, y)\""
        ),
        context = ExpectedContext(fragment = "min_by(x, y)", start = 7, stop = 18)
      )
    }
  }

  test("percentile_like") {
    // percentile
    checkAnswer(
      courseSales.groupBy("course").agg(
        percentile(col("year"), lit(0.3)),
        percentile(col("year"), lit(Array(0.25, 0.75))),
        percentile(col("year"), lit(0.3), lit(2)),
        percentile(col("year"), lit(Array(0.25, 0.75)), lit(2))
      ),
      Row("Java", 2012.2999999999997, Seq(2012.25, 2012.75), 2012.0, Seq(2012.0, 2013.0)) ::
        Row("dotNET", 2012.0, Seq(2012.0, 2012.5), 2012.0, Seq(2012.0, 2012.75)) :: Nil
    )

    // median
    checkAnswer(
      courseSales.groupBy("course").agg(
        median(col("year"))
      ),
      Row("Java", 2012.5) ::
        Row("dotNET", 2012.0) :: Nil
    )
  }

  test("SPARK-45599: Neither 0.0 nor -0.0 should be dropped when computing percentile") {
    // To reproduce the bug described in SPARK-45599, we need exactly these rows in roughly
    // this order in a DataFrame with exactly 1 partition.
    // scalastyle:off line.size.limit
    // See: https://issues.apache.org/jira/browse/SPARK-45599?focusedCommentId=17806954&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-17806954
    // scalastyle:on line.size.limit
    val spark45599Repro: DataFrame = Seq(
        0.0,
        2.0,
        153.0,
        168.0,
        3252411229536261.0,
        7.205759403792794e+16,
        1.7976931348623157e+308,
        0.25,
        Double.NaN,
        Double.NaN,
        -0.0,
        -128.0,
        Double.NaN,
        Double.NaN
      ).toDF("val").coalesce(1)

    checkAnswer(
      spark45599Repro.agg(
        percentile(col("val"), lit(0.1))
      ),
      // With the buggy implementation of OpenHashSet, this returns `0.050000000000000044`
      // instead of `-0.0`.
      List(Row(-0.0))
    )
  }

  test("any_value") {
    checkAnswer(
      courseSales.groupBy("course").agg(
        any_value(col("year")),
        any_value(col("year"), lit(true))
      ),
      Row("Java", 2012, 2012) :: Row("dotNET", 2012, 2012) :: Nil
    )
  }

  test("approx_percentile") {
    checkAnswer(
      courseSales.groupBy("course").agg(
        approx_percentile(col("earnings"), lit(0.3), lit(10000)),
        approx_percentile(col("earnings"), array(lit(0.3), lit(0.6)), lit(10000)),
        approx_percentile(col("earnings"), lit(0.3), lit(1)),
        approx_percentile(col("earnings"), array(lit(0.3), lit(0.6)), lit(1))
      ),
      Row("Java", 20000.0, Seq(20000.0, 30000.0), 20000.0, Seq(20000.0, 20000.0)) ::
        Row("dotNET", 5000.0, Seq(5000.0, 10000.0), 5000.0, Seq(5000.0, 5000.0)) :: Nil
    )
  }

  test("count_if") {
    withTempView("tempView") {
      Seq(("a", None), ("a", Some(1)), ("a", Some(2)), ("a", Some(3)),
        ("b", None), ("b", Some(4)), ("b", Some(5)), ("b", Some(6)))
        .toDF("x", "y")
        .createOrReplaceTempView("tempView")

      checkAnswer(
        sql("SELECT COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), " +
          "COUNT_IF(y IS NULL) FROM tempView"),
        Row(0L, 3L, 3L, 2L))

      checkAnswer(
        sql("SELECT x, COUNT_IF(NULL), COUNT_IF(y % 2 = 0), COUNT_IF(y % 2 <> 0), " +
          "COUNT_IF(y IS NULL) FROM tempView GROUP BY x"),
        Row("a", 0L, 1L, 2L, 1L) :: Row("b", 0L, 2L, 1L, 1L) :: Nil)

      checkAnswer(
        sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 1"),
        Row("a"))

      checkAnswer(
        sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y % 2 = 0) = 2"),
        Row("b"))

      checkAnswer(
        sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(y IS NULL) > 0"),
        Row("a") :: Row("b") :: Nil)

      checkAnswer(
        sql("SELECT x FROM tempView GROUP BY x HAVING COUNT_IF(NULL) > 0"),
        Nil)

      // When ANSI mode is on, it will implicit cast the string as boolean and throw a runtime
      // error. Here we simply test with ANSI mode off.
      if (!conf.ansiEnabled) {
        checkError(
          exception = intercept[AnalysisException] {
            sql("SELECT COUNT_IF(x) FROM tempView")
          },
          condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
          sqlState = None,
          parameters = Map(
            "sqlExpr" -> "\"count_if(x)\"",
            "paramIndex" -> "first",
            "inputSql" -> "\"x\"",
            "inputType" -> "\"STRING\"",
            "requiredType" -> "\"BOOLEAN\""),
          context = ExpectedContext(fragment = "COUNT_IF(x)", start = 7, stop = 17))
      }
    }

    checkAnswer(
      courseSales.groupBy("course").agg(
        count_if((col("earnings").gt(10000))),
        count_if(col("course").equalTo("Java").and(col("earnings").gt(10000)))
      ),
      Row("Java", 2, 2) :: Row("dotNET", 1, 0) :: Nil
    )
  }

  test("first_value") {
    checkAnswer(
      nullStrings.orderBy(col("n").desc).agg(
        first_value(col("s")),
        first_value(col("s"), lit(true))
      ),
      Row(null, "ABC") :: Nil
    )
  }

  test("last_value") {
    checkAnswer(
      nullStrings.agg(
        last_value(col("s")),
        last_value(col("s"), lit(true))
      ),
      Row(null, "ABC") :: Nil
    )
  }

  test("histogram_numeric") {
    checkAnswer(
      courseSales.groupBy("course").agg(
        histogram_numeric(col("earnings"), lit(5))
      ),
      Row("Java", Seq(Row(20000.0, 1.0), Row(30000.0, 1.0))) ::
        Row("dotNET", Seq(Row(5000.0, 1.0), Row(10000.0, 1.0), Row(48000.0, 1.0))) :: Nil
    )
  }

  Seq(true, false).foreach { value =>
    test(s"SPARK-31620: agg with subquery (whole-stage-codegen = $value)") {
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> value.toString) {
        withTempView("t1", "t2") {
          sql("create temporary view t1 as select * from values (1, 2) as t1(a, b)")
          sql("create temporary view t2 as select * from values (3, 4) as t2(c, d)")

          // test without grouping keys
          checkAnswer(sql("select sum(if(c > (select a from t1), d, 0)) as csum from t2"),
            Row(4) :: Nil)

          // test with grouping keys
          checkAnswer(sql("select c, sum(if(c > (select a from t1), d, 0)) as csum from " +
            "t2 group by c"), Row(3, 4) :: Nil)

          // test with distinct
          checkAnswer(sql("select avg(distinct(d)), sum(distinct(if(c > (select a from t1)," +
            " d, 0))) as csum from t2 group by c"), Row(4, 4) :: Nil)

          // test subquery with agg
          checkAnswer(sql("select sum(distinct(if(c > (select sum(distinct(a)) from t1)," +
            " d, 0))) as csum from t2 group by c"), Row(4) :: Nil)

          // test SortAggregateExec
          var df = sql("select max(if(c > (select a from t1), 'str1', 'str2')) as csum from t2")
          assert(find(df.queryExecution.executedPlan)(_.isInstanceOf[SortAggregateExec]).isDefined)
          checkAnswer(df, Row("str1") :: Nil)

          // test ObjectHashAggregateExec
          df = sql("select collect_list(d), sum(if(c > (select a from t1), d, 0)) as csum from t2")
          assert(
            find(df.queryExecution.executedPlan)(_.isInstanceOf[ObjectHashAggregateExec]).isDefined)
          checkAnswer(df, Row(Array(4), 4) :: Nil)
        }
      }
    }
  }

  test("SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate") {
    withTempView("view") {
      val nan1 = java.lang.Float.intBitsToFloat(0x7f800001)
      val nan2 = java.lang.Float.intBitsToFloat(0x7fffffff)

      Seq(("mithunr", Float.NaN),
        ("mithunr", nan1),
        ("mithunr", nan2),
        ("abellina", 1.0f),
        ("abellina", 2.0f)).toDF("uid", "score").createOrReplaceTempView("view")

      val df = spark.sql("select uid, count(distinct score) from view group by 1 order by 1 asc")
      checkAnswer(df, Row("abellina", 2) :: Row("mithunr", 1) :: Nil)
    }
  }

  test("SPARK-32136: NormalizeFloatingNumbers should work on null struct") {
    val df = Seq(
      A(None),
      A(Some(B(None))),
      A(Some(B(Some(1.0))))).toDF()
    val groupBy = df.groupBy("b").agg(count("*"))
    checkAnswer(groupBy, Row(null, 1) :: Row(Row(null), 1) :: Row(Row(1.0), 1) :: Nil)
  }

  test("SPARK-32344: Unevaluable's set to FIRST/LAST ignoreNullsExpr in distinct aggregates") {
    val queryTemplate = (agg: String) =>
      s"SELECT $agg(DISTINCT v) FROM (SELECT v FROM VALUES 1, 2, 3 t(v) ORDER BY v)"
    checkAnswer(sql(queryTemplate("FIRST")), Row(1))
    checkAnswer(sql(queryTemplate("LAST")), Row(3))
  }

  test("SPARK-32906: struct field names should not change after normalizing floats") {
    val df = Seq(Tuple1(Tuple2(-0.0d, Double.NaN)), Tuple1(Tuple2(0.0d, Double.NaN))).toDF("k")
    val aggs = df.distinct().queryExecution.sparkPlan.collect { case a: HashAggregateExec => a }
    assert(aggs.length == 2)
    assert(aggs.head.output.map(_.dataType.simpleString).head ===
      aggs.last.output.map(_.dataType.simpleString).head)
  }

  test("SPARK-33726: Aggregation on a table where a column name is reused") {
    val query =
      """|with T as (
         |select id as a, -id as x from range(3)),
         |U as (
         |select id as b, cast(id as string) as x from range(3))
         |select T.x, U.x, min(a) as ma, min(b) as mb
         |from T join U on a=b
         |group by U.x, T.x
      """.stripMargin
    val df = spark.sql(query)
    checkAnswer(df, Row(0, "0", 0, 0) :: Row(-1, "1", 1, 1) :: Row(-2, "2", 2, 2) :: Nil)
  }

  test("SPARK-34713: group by CreateStruct with ExtractValue") {
    val structDF = Seq(Tuple1(1 -> 1)).toDF("col")
    checkAnswer(structDF.groupBy(struct($"col._1")).count().select("count"), Row(1))

    val arrayOfStructDF = Seq(Tuple1(Seq(1 -> 1))).toDF("col")
    checkAnswer(arrayOfStructDF.groupBy(struct($"col._1")).count().select("count"), Row(1))

    val mapDF = Seq(Tuple1(Map("a" -> "a"))).toDF("col")
    checkAnswer(mapDF.groupBy(struct($"col.a")).count().select("count"), Row(1))

    if (!conf.ansiEnabled) {
      val nonStringMapDF = Seq(Tuple1(Map(1 -> 1))).toDF("col")
      // Spark implicit casts string literal "a" to int to match the key type.
      checkAnswer(nonStringMapDF.groupBy(struct($"col.a")).count().select("count"), Row(1))
    }

    checkError(
      exception = intercept[AnalysisException] {
        Seq(Tuple1(Seq(1))).toDF("col").groupBy(struct($"col.a")).count()
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"col[a]\"",
        "paramIndex" -> "second",
        "inputSql" -> "\"a\"",
        "inputType" -> "\"STRING\"",
        "requiredType" -> "\"INTEGRAL\""),
      context = ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("SPARK-34716: Support ANSI SQL intervals by the aggregate function `sum`") {
    val sumDF = intervalData.select(
      sum($"year-month"),
      sum($"year"),
      sum($"month"),
      sum($"day-second"),
      sum($"day-minute"),
      sum($"day-hour"),
      sum($"day"),
      sum($"hour-second"),
      sum($"hour-minute"),
      sum($"hour"),
      sum($"minute-second"),
      sum($"minute"),
      sum($"second"))
    checkAnswer(sumDF,
      Row(
        Period.of(2, 5, 0),
        Period.ofYears(28),
        Period.of(1, 1, 0),
        Duration.ofDays(9).plusHours(23).plusMinutes(29).plusSeconds(4),
        Duration.ofDays(23).plusHours(8).plusMinutes(27),
        Duration.ofDays(-8).plusHours(-7),
        Duration.ofDays(1),
        Duration.ofDays(1).plusHours(12).plusMinutes(2).plusSeconds(33),
        Duration.ofMinutes(43),
        Duration.ofHours(12),
        Duration.ofMinutes(18).plusSeconds(3),
        Duration.ofMinutes(52),
        Duration.ofSeconds(20)))
    assert(find(sumDF.queryExecution.executedPlan)(_.isInstanceOf[HashAggregateExec]).isDefined)
    assert(sumDF.schema == StructType(Seq(
      StructField("sum(year-month)", YearMonthIntervalType()),
      StructField("sum(year)", YearMonthIntervalType(YEAR)),
      StructField("sum(month)", YearMonthIntervalType(MONTH)),
      StructField("sum(day-second)", DayTimeIntervalType()),
      StructField("sum(day-minute)", DayTimeIntervalType(DAY, MINUTE)),
      StructField("sum(day-hour)", DayTimeIntervalType(DAY, HOUR)),
      StructField("sum(day)", DayTimeIntervalType(DAY)),
      StructField("sum(hour-second)", DayTimeIntervalType(HOUR, SECOND)),
      StructField("sum(hour-minute)", DayTimeIntervalType(HOUR, MINUTE)),
      StructField("sum(hour)", DayTimeIntervalType(HOUR)),
      StructField("sum(minute-second)", DayTimeIntervalType(MINUTE, SECOND)),
      StructField("sum(minute)", DayTimeIntervalType(MINUTE)),
      StructField("sum(second)", DayTimeIntervalType(SECOND)))))

    val sumDF2 =
      intervalData.groupBy($"class").agg(
        sum($"year-month"),
        sum($"year"),
        sum($"month"),
        sum($"day-second"),
        sum($"day-minute"),
        sum($"day-hour"),
        sum($"day"),
        sum($"hour-second"),
        sum($"hour-minute"),
        sum($"hour"),
        sum($"minute-second"),
        sum($"minute"),
        sum($"second"))
    checkAnswer(sumDF2,
      Row(1,
        Period.ofMonths(10),
        Period.ofYears(8),
        Period.ofMonths(10),
        Duration.ofDays(7).plusHours(13).plusMinutes(3).plusSeconds(18),
        Duration.ofDays(5).plusHours(21).plusMinutes(12),
        Duration.ofDays(1).plusHours(8),
        Duration.ofDays(10),
        Duration.ofHours(20).plusMinutes(11).plusSeconds(33),
        Duration.ofHours(3).plusMinutes(18),
        Duration.ofHours(13),
        Duration.ofMinutes(2).plusSeconds(59),
        Duration.ofMinutes(38),
        Duration.ofSeconds(5)) ::
      Row(2,
        Period.ofMonths(1),
        Period.ofYears(1),
        Period.ofMonths(1),
        Duration.ofSeconds(1),
        Duration.ofMinutes(1),
        Duration.ofHours(1),
        Duration.ofDays(1),
        Duration.ofSeconds(1),
        Duration.ofMinutes(1),
        Duration.ofHours(1),
        Duration.ofSeconds(1),
        Duration.ofMinutes(1),
        Duration.ofSeconds(1)) ::
      Row(3,
        Period.of(1, 6, 0),
        Period.ofYears(19),
        Period.ofMonths(2),
        Duration.ofDays(2).plusHours(10).plusMinutes(25).plusSeconds(45),
        Duration.ofDays(17).plusHours(11).plusMinutes(14),
        Duration.ofDays(-9).plusHours(-16),
        Duration.ofDays(-10),
        Duration.ofHours(15).plusMinutes(50).plusSeconds(59),
        Duration.ofHours(-2).plusMinutes(-36),
        Duration.ofHours(-2),
        Duration.ofMinutes(15).plusSeconds(3),
        Duration.ofMinutes(13),
        Duration.ofSeconds(14)) ::
      Nil)
    assert(find(sumDF2.queryExecution.executedPlan)(_.isInstanceOf[HashAggregateExec]).isDefined)
    val metadata = new MetadataBuilder().putString(AUTO_GENERATED_ALIAS, "true").build()
    assert(sumDF2.schema == StructType(Seq(StructField("class", IntegerType, false),
      StructField("sum(year-month)", YearMonthIntervalType(), metadata = metadata),
      StructField("sum(year)", YearMonthIntervalType(YEAR), metadata = metadata),
      StructField("sum(month)", YearMonthIntervalType(MONTH), metadata = metadata),
      StructField("sum(day-second)", DayTimeIntervalType(), metadata = metadata),
      StructField("sum(day-minute)", DayTimeIntervalType(DAY, MINUTE), metadata = metadata),
      StructField("sum(day-hour)", DayTimeIntervalType(DAY, HOUR), metadata = metadata),
      StructField("sum(day)", DayTimeIntervalType(DAY), metadata = metadata),
      StructField("sum(hour-second)", DayTimeIntervalType(HOUR, SECOND), metadata = metadata),
      StructField("sum(hour-minute)", DayTimeIntervalType(HOUR, MINUTE), metadata = metadata),
      StructField("sum(hour)", DayTimeIntervalType(HOUR), metadata = metadata),
      StructField("sum(minute-second)", DayTimeIntervalType(MINUTE, SECOND), metadata = metadata),
      StructField("sum(minute)", DayTimeIntervalType(MINUTE), metadata = metadata),
      StructField("sum(second)", DayTimeIntervalType(SECOND), metadata = metadata))))

    val df2 = Seq((Period.ofMonths(Int.MaxValue), Duration.ofDays(106751991)),
      (Period.ofMonths(10), Duration.ofDays(10)))
      .toDF("year-month", "day")

    checkError(
      exception = intercept[SparkArithmeticException] {
        checkAnswer(df2.select(sum($"year-month")), Nil)
      },
      condition = "INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION",
      parameters = Map("functionName" -> toSQLId("try_add"))
    )

    checkError(
      exception = intercept[SparkArithmeticException] {
        checkAnswer(df2.select(sum($"day")), Nil)
      },
      condition = "INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION",
      parameters = Map("functionName" -> toSQLId("try_add"))
    )
  }

  test("SPARK-34837: Support ANSI SQL intervals by the aggregate function `avg`") {
    val avgDF = intervalData.select(
      avg($"year-month"),
      avg($"year"),
      avg($"month"),
      avg($"day-second"),
      avg($"day-minute"),
      avg($"day-hour"),
      avg($"day"),
      avg($"hour-second"),
      avg($"hour-minute"),
      avg($"hour"),
      avg($"minute-second"),
      avg($"minute"),
      avg($"second"))
    checkAnswer(avgDF,
      Row(Period.ofMonths(7),
        Period.of(5, 7, 0),
        Period.ofMonths(3),
        Duration.ofDays(2).plusHours(11).plusMinutes(52).plusSeconds(16),
        Duration.ofDays(4).plusHours(16).plusMinutes(5).plusSeconds(24),
        Duration.ofDays(-1).plusHours(-15).plusMinutes(-48),
        Duration.ofHours(4).plusMinutes(48),
        Duration.ofHours(9).plusSeconds(38).plusMillis(250),
        Duration.ofMinutes(8).plusSeconds(36),
        Duration.ofHours(2).plusMinutes(24),
        Duration.ofMinutes(4).plusSeconds(30).plusMillis(750),
        Duration.ofMinutes(10).plusSeconds(24),
        Duration.ofSeconds(5)))
    assert(find(avgDF.queryExecution.executedPlan)(_.isInstanceOf[HashAggregateExec]).isDefined)
    assert(avgDF.schema == StructType(Seq(
      StructField("avg(year-month)", YearMonthIntervalType()),
      StructField("avg(year)", YearMonthIntervalType()),
      StructField("avg(month)", YearMonthIntervalType()),
      StructField("avg(day-second)", DayTimeIntervalType()),
      StructField("avg(day-minute)", DayTimeIntervalType()),
      StructField("avg(day-hour)", DayTimeIntervalType()),
      StructField("avg(day)", DayTimeIntervalType()),
      StructField("avg(hour-second)", DayTimeIntervalType()),
      StructField("avg(hour-minute)", DayTimeIntervalType()),
      StructField("avg(hour)", DayTimeIntervalType()),
      StructField("avg(minute-second)", DayTimeIntervalType()),
      StructField("avg(minute)", DayTimeIntervalType()),
      StructField("avg(second)", DayTimeIntervalType()))))

    val avgDF2 =
      intervalData.groupBy($"class").agg(
        avg($"year-month"),
        avg($"year"),
        avg($"month"),
        avg($"day-second"),
        avg($"day-minute"),
        avg($"day-hour"),
        avg($"day"),
        avg($"hour-second"),
        avg($"hour-minute"),
        avg($"hour"),
        avg($"minute-second"),
        avg($"minute"),
        avg($"second"))
    checkAnswer(avgDF2,
      Row(1,
        Period.ofMonths(10),
        Period.ofYears(8),
        Period.ofMonths(10),
        Duration.ofDays(7).plusHours(13).plusMinutes(3).plusSeconds(18),
        Duration.ofDays(5).plusHours(21).plusMinutes(12),
        Duration.ofDays(1).plusHours(8),
        Duration.ofDays(10),
        Duration.ofHours(20).plusMinutes(11).plusSeconds(33),
        Duration.ofHours(3).plusMinutes(18),
        Duration.ofHours(13),
        Duration.ofMinutes(2).plusSeconds(59),
        Duration.ofMinutes(38),
        Duration.ofSeconds(5)) ::
      Row(2,
        Period.ofMonths(1),
        Period.ofYears(1),
        Period.ofMonths(1),
        Duration.ofSeconds(1),
        Duration.ofMinutes(1),
        Duration.ofHours(1),
        Duration.ofDays(1),
        Duration.ofSeconds(1),
        Duration.ofMinutes(1),
        Duration.ofHours(1),
        Duration.ofSeconds(1),
        Duration.ofMinutes(1),
        Duration.ofSeconds(1)) ::
      Row(3,
        Period.ofMonths(9),
        Period.of(6, 4, 0),
        Period.ofMonths(1),
        Duration.ofDays(1).plusHours(5).plusMinutes(12).plusSeconds(52).plusMillis(500),
        Duration.ofDays(5).plusHours(19).plusMinutes(44).plusSeconds(40),
        Duration.ofDays(-3).plusHours(-5).plusMinutes(-20),
        Duration.ofDays(-3).plusHours(-8),
        Duration.ofHours(7).plusMinutes(55).plusSeconds(29).plusMillis(500),
        Duration.ofMinutes(-52),
        Duration.ofMinutes(-40),
        Duration.ofMinutes(7).plusSeconds(31).plusMillis(500),
        Duration.ofMinutes(4).plusSeconds(20),
        Duration.ofSeconds(7)) :: Nil)
    assert(find(avgDF2.queryExecution.executedPlan)(_.isInstanceOf[HashAggregateExec]).isDefined)
    val metadata = new MetadataBuilder().putString(AUTO_GENERATED_ALIAS, "true").build()
    assert(avgDF2.schema == StructType(Seq(
      StructField("class", IntegerType, false),
      StructField("avg(year-month)", YearMonthIntervalType(), metadata = metadata),
      StructField("avg(year)", YearMonthIntervalType(), metadata = metadata),
      StructField("avg(month)", YearMonthIntervalType(), metadata = metadata),
      StructField("avg(day-second)", DayTimeIntervalType(), metadata = metadata),
      StructField("avg(day-minute)", DayTimeIntervalType(), metadata = metadata),
      StructField("avg(day-hour)", DayTimeIntervalType(), metadata = metadata),
      StructField("avg(day)", DayTimeIntervalType(), metadata = metadata),
      StructField("avg(hour-second)", DayTimeIntervalType(), metadata = metadata),
      StructField("avg(hour-minute)", DayTimeIntervalType(), metadata = metadata),
      StructField("avg(hour)", DayTimeIntervalType(), metadata = metadata),
      StructField("avg(minute-second)", DayTimeIntervalType(), metadata = metadata),
      StructField("avg(minute)", DayTimeIntervalType(), metadata = metadata),
      StructField("avg(second)", DayTimeIntervalType(), metadata = metadata))))

    val df2 = Seq((Period.ofMonths(Int.MaxValue), Duration.ofDays(106751991)),
      (Period.ofMonths(10), Duration.ofDays(10)))
      .toDF("year-month", "day")

    checkError(
      exception = intercept[SparkArithmeticException] {
        checkAnswer(df2.select(avg($"year-month")), Nil)
      },
      condition = "INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION",
      parameters = Map("functionName" -> toSQLId("try_add"))
    )

    checkError(
      exception = intercept[SparkArithmeticException] {
        checkAnswer(df2.select(avg($"day")), Nil)
      },
      condition = "INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION",
      parameters = Map("functionName" -> toSQLId("try_add"))
    )

    val df3 = intervalData.filter($"class" > 4)
    val avgDF3 = df3.select(avg($"year-month"), avg($"day"))
    checkAnswer(avgDF3, Row(null, null) :: Nil)

    val avgDF4 = df3.groupBy($"class").agg(avg($"year-month"), avg($"day"))
    checkAnswer(avgDF4, Nil)
  }

  test("SPARK-35412: groupBy of year-month/day-time intervals should work") {
    val df1 = Seq(Duration.ofDays(1)).toDF("a").groupBy("a").count()
    checkAnswer(df1, Row(Duration.ofDays(1), 1))
    val df2 = Seq(Period.ofYears(1)).toDF("a").groupBy("a").count()
    checkAnswer(df2, Row(Period.ofYears(1), 1))
  }

  test("SPARK-36054: Support group by TimestampNTZ column") {
    val ts1 = "2021-01-01T00:00:00"
    val ts2 = "2021-01-01T00:00:01"
    val localDateTime = Seq(ts1, ts1, ts2).map(LocalDateTime.parse)
    val df = localDateTime.toDF("ts").groupBy("ts").count().orderBy("ts")
    val expectedSchema =
      new StructType().add(StructField("ts", TimestampNTZType)).add("count", LongType, false)
    assert (df.schema == expectedSchema)
    checkAnswer(df, Seq(Row(LocalDateTime.parse(ts1), 2), Row(LocalDateTime.parse(ts2), 1)))
  }

  test("SPARK-36926: decimal average mistakenly overflow") {
    val df = (1 to 10).map(_ => "9999999999.99").toDF("d")
    val res = df.select($"d".cast("decimal(12, 2)").as("d")).agg(avg($"d").cast("string"))
    checkAnswer(res, Row("9999999999.990000"))
  }

  test("SPARK-38185: Fix data incorrect if aggregate function is empty") {
    val emptyAgg = Map.empty[String, String]
    assert(spark.range(2).where("id > 2").agg(emptyAgg).limit(1).count() == 1)
  }

  test("SPARK-45685: group by `LazyList` of complex expressions should not fail") {
    val df = Seq(1).toDF("id").groupBy(LazyList($"id" + 1, $"id" + 2): _*).sum("id")
    checkAnswer(df, Row(2, 3, 1))
  }

  test("SPARK-38221: group by `Stream` of complex expressions should not fail") {
    @scala.annotation.nowarn("cat=deprecation")
    val df = Seq(1).toDF("id").groupBy(Stream($"id" + 1, $"id" + 2): _*).sum("id")
    checkAnswer(df, Row(2, 3, 1))
  }

  test("SPARK-40382: Distinct aggregation expression grouping by semantic equivalence") {
   Seq(
      (1, 1, 3),
      (1, 2, 3),
      (1, 2, 3),
      (2, 1, 1),
      (2, 2, 5)
    ).toDF("k", "c1", "c2").createOrReplaceTempView("df")

    // all distinct aggregation children are semantically equivalent
    val res1 = sql(
      """select k, sum(distinct c1 + 1), avg(distinct 1 + c1), count(distinct 1 + C1)
        |from df
        |group by k
        |""".stripMargin)
    checkAnswer(res1, Row(1, 5, 2.5, 2) :: Row(2, 5, 2.5, 2) :: Nil)

    // some distinct aggregation children are semantically equivalent
    val res2 = sql(
      """select k, sum(distinct c1 + 2), avg(distinct 2 + c1), count(distinct c2)
        |from df
        |group by k
        |""".stripMargin)
    checkAnswer(res2, Row(1, 7, 3.5, 1) :: Row(2, 7, 3.5, 2) :: Nil)

    // no distinct aggregation children are semantically equivalent
    val res3 = sql(
      """select k, sum(distinct c1 + 2), avg(distinct 3 + c1), count(distinct c2)
        |from df
        |group by k
        |""".stripMargin)
    checkAnswer(res3, Row(1, 7, 4.5, 1) :: Row(2, 7, 4.5, 2) :: Nil)
  }

  test("SPARK-41035: Reuse of literal in distinct aggregations should work") {
    val res = sql(
      """select a, count(distinct 100), count(distinct b, 100)
        |from values (1, 2), (4, 5), (4, 6) as data(a, b)
        |group by a;
        |""".stripMargin
    )
    checkAnswer(res, Row(1, 1, 1) :: Row(4, 1, 2) :: Nil)
  }

  test("SPARK-42851: common subexpression should consistently handle aggregate and result exprs") {
    val res = sql(
      "select max(transform(array(id), x -> x)), max(transform(array(id), x -> x)) from range(2)"
    )
    checkAnswer(res, Row(Array(1), Array(1)))
  }

  test("SPARK-16484: hll_*_agg + hll_union + hll_sketch_estimate positive tests") {

    val df1 = Seq(
      (1, "a"), (1, "a"), (1, "a"),
      (1, "b"),
      (1, "c"), (1, "c"),
      (1, "d")
    ).toDF("id", "value")
    df1.createOrReplaceTempView("df1")

    val df2 = Seq(
      (1, "a"),
      (1, "c"),
      (1, "d"), (1, "d"), (1, "d"),
      (1, "e"), (1, "e"),
      (1, "f")
    ).toDF("id", "value")
    df2.createOrReplaceTempView("df2")

    // first test hll_sketch_agg, hll_sketch_estimate via dataframe + sql,
    // with and without configs, via both DF and SQL implementations
    val res1 = df1.groupBy("id")
      .agg(
        count("value").as("count"),
        hll_sketch_agg("value").as("sketch_1"),
        hll_sketch_agg("value", 20).as("sketch_2")
      )
      .withColumn("distinct_count_1", hll_sketch_estimate("sketch_1"))
      .withColumn("distinct_count_2", hll_sketch_estimate("sketch_2"))
      .drop("sketch_1", "sketch_2")
    checkAnswer(res1, Row(1, 7, 4, 4))

    val res2 = sql(
      """with sketches as (
        |select
        | id,
        | count(value) as count,
        | hll_sketch_agg(value) as sketch_1,
        | hll_sketch_agg(value, 20) as sketch_2
        |from df1
        |group by 1
        |)
        |
        |select
        | id,
        | count,
        | hll_sketch_estimate(sketch_1) as distinct_count_1,
        | hll_sketch_estimate(sketch_2) as distinct_count_2
        |from
        | sketches
        |""".stripMargin)
    checkAnswer(res2, Row(1, 7, 4, 4))

    // now test hll_union_agg via dataframe + sql, with and without configs,
    // unioning together sketches with default, non-default and different configurations
    val df3 = df1.groupBy("id")
      .agg(
        count("value").as("count"),
        hll_sketch_agg("value").as("hllsketch_1"),
        hll_sketch_agg("value", 20).as("hllsketch_2"),
        hll_sketch_agg("value").as("hllsketch_3")
      )
    df3.createOrReplaceTempView("df3")

    val df4 = sql(
      """select
        | id,
        | count(value) as count,
        | hll_sketch_agg(value) as hllsketch_1,
        | hll_sketch_agg(value, 20) as hllsketch_2,
        | hll_sketch_agg(value, 20) as hllsketch_3
        |from df2
        |group by 1
        |""".stripMargin)
    df4.createOrReplaceTempView("df4")

    val res3 = df3.union(df4).groupBy("id")
      .agg(
        sum("count").as("count"),
        hll_sketch_estimate(hll_union_agg("hllsketch_1")).as("distinct_count_1"),
        hll_sketch_estimate(hll_union_agg("hllsketch_2")).as("distinct_count_2"),
        hll_sketch_estimate(hll_union_agg("hllsketch_3", true)).as("distinct_count_3")
      )
    checkAnswer(res3, Row(1, 15, 6, 6, 6))

    val res4 = sql(
      """select
        | id,
        | sum(count) as count,
        | hll_sketch_estimate(hll_union_agg(hllsketch_1)) as distinct_count_1,
        | hll_sketch_estimate(hll_union_agg(hllsketch_2)) as distinct_count_2,
        | hll_sketch_estimate(hll_union_agg(hllsketch_3, true)) as distinct_count_3
        |from (select * from df3 union all select * from df4)
        |group by 1
        |""".stripMargin)
    checkAnswer(res4, Row(1, 15, 6, 6, 6))

    // add tests to ensure hll_union works via both DF and SQL too
    val df5 = df3.drop("count")
    df5.createOrReplaceTempView("df5")

    val df6 = df4.drop("count")
      .withColumnRenamed("hllsketch_1", "hllsketch_4")
      .withColumnRenamed("hllsketch_2", "hllsketch_5")
      .withColumnRenamed("hllsketch_3", "hllsketch_6")
    df6.createOrReplaceTempView("df6")

    val res5 = df5.join(df6, "id")
      .withColumn("distinct_count_1",
        hll_sketch_estimate(hll_union("hllsketch_1", "hllsketch_4")))
      .withColumn("distinct_count_2",
        hll_sketch_estimate(hll_union("hllsketch_2", "hllsketch_5")))
      .withColumn("distinct_count_3",
        hll_sketch_estimate(hll_union("hllsketch_3", "hllsketch_6", true)))
      .drop("hllsketch_1", "hllsketch_2", "hllsketch_3",
        "hllsketch_4", "hllsketch_5", "hllsketch_6")
    checkAnswer(res5, Row(1, 6, 6, 6))

    val res6 = sql(
      """with joined as (
        |  select
        |    l.id,
        |    l.hllsketch_1,
        |    l.hllsketch_2,
        |    l.hllsketch_3,
        |    r.hllsketch_4,
        |    r.hllsketch_5,
        |    r.hllsketch_6
        |  from
        |    df5 l
        |    join
        |    df6 r
        |     on l.id = r.id
        | )
        |
        |select
        |  id,
        |  hll_sketch_estimate(hll_union(hllsketch_1, hllsketch_4)) as distinct_count_1,
        |  hll_sketch_estimate(hll_union(hllsketch_2, hllsketch_5)) as distinct_count_2,
        |  hll_sketch_estimate(hll_union(hllsketch_3, hllsketch_6, true)) as distinct_count_3
        |from
        | joined
        |""".stripMargin)
    checkAnswer(res6, Row(1, 6, 6, 6))

    val df7 = Seq(
      (1, "a"), (1, "a"), (1, "a"),
      (1, "b"),
      (1, null),
      (2, null), (2, null), (2, null)
    ).toDF("id", "value")

    // empty column test
    val res7 = df7.where(expr("id = 2")).groupBy("id")
      .agg(
        hll_sketch_estimate(hll_sketch_agg("value")).as("distinct_count")
      )
    checkAnswer(res7, Row(2, 0))

    // partial empty column test
    val res8 = df7.groupBy("id")
      .agg(
        hll_sketch_estimate(hll_sketch_agg("value")).as("distinct_count")
      )
    checkAnswer(res8, Seq(Row(1, 2), Row(2, 0)))
  }

  test("SPARK-16484: hll_*_agg + hll_union negative tests") {

    val df1 = Seq(
      (1, "a"), (1, "a"), (1, "a"),
      (1, "b"),
      (1, "c"), (1, "c"),
      (1, "d")
    ).toDF("id", "value")
    df1.createOrReplaceTempView("df1")

    val df2 = Seq(
      (1, "a"),
      (1, "c"),
      (1, "d"), (1, "d"), (1, "d"),
      (1, "e"), (1, "e"),
      (1, "f")
    ).toDF("id", "value")
    df2.createOrReplaceTempView("df2")

    // validate that the functions error out when lgConfigK < 4 or > 24
    checkError(
      exception = intercept[SparkRuntimeException] {
        df1.groupBy("id")
          .agg(
            hll_sketch_agg("value", 1).as("hllsketch")
          )
          .collect()
      },
      condition = "HLL_INVALID_LG_K",
      parameters = Map(
        "function" -> "`hll_sketch_agg`",
        "min" -> "4",
        "max" -> "21",
        "value" -> "1"
      ))

    checkError(
      exception = intercept[SparkRuntimeException] {
        df1.groupBy("id")
          .agg(
            hll_sketch_agg("value", 25).as("hllsketch")
          )
          .collect()
      },
      condition = "HLL_INVALID_LG_K",
      parameters = Map(
        "function" -> "`hll_sketch_agg`",
        "min" -> "4",
        "max" -> "21",
        "value" -> "25"
      ))

    // validate that unions error out by default for different lgConfigK sketches
    checkError(
      exception = intercept[SparkRuntimeException] {
        val i1 = df1.groupBy("id")
          .agg(
            hll_sketch_agg("value").as("hllsketch_left")
          )
        val i2 = df2.groupBy("id")
          .agg(
            hll_sketch_agg("value", 20).as("hllsketch_right")
          )
        i1.join(i2)
          .withColumn("union", hll_union("hllsketch_left", "hllsketch_right"))
          .collect()
      },
      condition = "HLL_UNION_DIFFERENT_LG_K",
      parameters = Map(
        "left" -> "12",
        "right" -> "20",
        "function" -> "`hll_union`"
      ))

    checkError(
      exception = intercept[SparkRuntimeException] {
        val i1 = df1.groupBy("id")
          .agg(
            hll_sketch_agg("value").as("hllsketch")
          )
        val i2 = df2.groupBy("id")
          .agg(
            hll_sketch_agg("value", 20).as("hllsketch")
          )
        i1.union(i2).groupBy("id")
          .agg(
            hll_union_agg("hllsketch")
          )
          .collect()
      },
      condition = "HLL_UNION_DIFFERENT_LG_K",
      parameters = Map(
        "left" -> "12",
        "right" -> "20",
        "function" -> "`hll_union_agg`"
      ))

    // validate that the functions error out when provided unexpected types
    checkError(
      exception = intercept[AnalysisException] {
        val res = sql(
          """
            |select
            | id,
            | hll_sketch_agg(value, 'text')
            |from
            | df1
            |group by 1
            |""".stripMargin)
        checkAnswer(res, Nil)
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"hll_sketch_agg(value, text)\"",
        "paramIndex" -> "second",
        "inputSql" -> "\"text\"",
        "inputType" -> "\"STRING\"",
        "requiredType" -> "\"INT\""
      ),
      context = ExpectedContext(
        fragment = "hll_sketch_agg(value, 'text')",
        start = 14,
        stop = 42))

    checkError(
      exception = intercept[AnalysisException] {
        val res = sql(
          """with sketch_cte as (
            |select
            | id,
            | hll_sketch_agg(value) as sketch
            |from
            | df1
            |group by 1
            |)
            |
            |select hll_union_agg(sketch, 'Hll_4') from sketch_cte
            |""".stripMargin)
        checkAnswer(res, Nil)
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"hll_union_agg(sketch, Hll_4)\"",
        "paramIndex" -> "second",
        "inputSql" -> "\"Hll_4\"",
        "inputType" -> "\"STRING\"",
        "requiredType" -> "\"BOOLEAN\""
      ),
      context = ExpectedContext(
        fragment = "hll_union_agg(sketch, 'Hll_4')",
        start = 97,
        stop = 126))

    // validate that unions error out by default for different lgConfigK sketches
    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(
          """with cte1 as (
            |select
            | id,
            | hll_sketch_agg(value) as sketch
            |from
            | df1
            |group by 1
            |),
            |
            |cte2 as (
            |select
            | id,
            | hll_sketch_agg(value, 20) as sketch
            |from
            | df2
            |group by 1
            |)
            |
            |select
            | cte1.id,
            | hll_union(cte1.sketch, cte2.sketch) as sketch
            |from
            | cte1 join cte2 on cte1.id = cte2.id
            |""".stripMargin).collect()
      },
      condition = "HLL_UNION_DIFFERENT_LG_K",
      parameters = Map(
        "left" -> "12",
        "right" -> "20",
        "function" -> "`hll_union`"
      ))

    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(
          """with cte1 as (
            |select
            | id,
            | hll_sketch_agg(value) as sketch
            |from
            | df1
            |group by 1
            |),
            |
            |cte2 as (
            |select
            | id,
            | hll_sketch_agg(value, 20) as sketch
            |from
            | df2
            |group by 1
            |)
            |
            |select
            | id,
            | hll_union_agg(sketch) as sketch
            |from
            | (select * from cte1 union all select * from cte2)
            |group by 1
            |""".stripMargin).collect()
      },
      condition = "HLL_UNION_DIFFERENT_LG_K",
      parameters = Map(
        "left" -> "12",
        "right" -> "20",
        "function" -> "`hll_union_agg`"
      ))
  }

  test("SPARK-43876: Enable fast hashmap for distinct queries") {
    withSQLConf(SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "true") {
      val df = testData2.distinct()
      checkAnswer(df, testData2)
      val output = new java.io.ByteArrayOutputStream()
      Console.withOut(output) {
        df.explain("codegen")
      }
      assert(output.toString().contains("public class hashAgg_FastHashMap_0"))
    }
  }

  test("hll_sketch_agg") {
    val df = Seq(1, 1, 2, 2, 3).toDF("col")
    checkAnswer(
      df.selectExpr("hll_sketch_estimate(hll_sketch_agg(col, 12))"),
      Seq(Row(3))
    )
    checkAnswer(
      df.select(hll_sketch_estimate(hll_sketch_agg(col("col"), lit(12)))),
      Seq(Row(3))
    )
  }

  test("hll_union_agg") {
    val df = Seq(1).toDF("col")
    checkAnswer(
      df.selectExpr("hll_sketch_agg(col) as sketch").unionAll(
        df.selectExpr("hll_sketch_agg(col, 20) as sketch")).
        selectExpr("hll_sketch_estimate(hll_union_agg(sketch, true))"),
      Seq(Row(1))
    )
    checkAnswer(
      df.select(hll_sketch_agg(col("col")).as("sketch")).unionAll(
        df.select(hll_sketch_agg(col("col"), lit(20)).as("sketch"))).
        select(hll_sketch_estimate(hll_union_agg(col("sketch"), lit(true)))),
      Seq(Row(1))
    )
  }

  private def assertAggregateOnDataframe(
      df: => DataFrame,
      expected: Int): Unit = {
    val configurations = Seq(
      Seq.empty[(String, String)], // hash aggregate is used by default
      Seq(SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
        "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 10"),
      Seq("spark.sql.test.forceApplyObjectHashAggregate" -> "true"),
      Seq(
        "spark.sql.test.forceApplyObjectHashAggregate" -> "true",
        SQLConf.OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD.key -> "1"),
      Seq("spark.sql.test.forceApplySortAggregate" -> "true")
    )

    // Make tests faster
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "3") {
      for (conf <- configurations) {
        withSQLConf(conf: _*) {
          assert(df.count() == expected, df.queryExecution.simpleString)
        }
      }
    }
  }

  test("SPARK-47430 Support GROUP BY MapType") {
    def genMapData(dataType: String): String = {
      s"""
        |case when id % 4 == 0 then map()
        |when id % 4 == 1 then map(cast(0 as $dataType), cast(0 as $dataType))
        |when id % 4 == 2 then map(cast(0 as $dataType), cast(0 as $dataType),
        |  cast(1 as $dataType), cast(1 as $dataType))
        |else map(cast(1 as $dataType), cast(1 as $dataType),
        |  cast(0 as $dataType), cast(0 as $dataType))
        |end
        |""".stripMargin
    }
    Seq("int", "long", "float", "double", "decimal(10, 2)", "string", "varchar(6)").foreach { dt =>
      withTempView("v") {
        spark.range(20)
          .selectExpr(
            s"cast(1 as $dt) as c1",
            s"${genMapData(dt)} as c2",
            "map(c1, null) as c3",
            s"cast(null as map<$dt, $dt>) as c4")
          .createOrReplaceTempView("v")

        assertAggregateOnDataframe(
          spark.sql("SELECT count(*) FROM v GROUP BY c2"),
          3)
        assertAggregateOnDataframe(
          spark.sql("SELECT c2, count(*) FROM v GROUP BY c2"),
          3)
        assertAggregateOnDataframe(
          spark.sql("SELECT c1, c2, count(*) FROM v GROUP BY c1, c2"),
          3)
        assertAggregateOnDataframe(
          spark.sql("SELECT map(c1, c1) FROM v GROUP BY map(c1, c1)"),
          1)
        assertAggregateOnDataframe(
          spark.sql("SELECT map(c1, c1), count(*) FROM v GROUP BY map(c1, c1)"),
          1)
        assertAggregateOnDataframe(
          spark.sql("SELECT c3, count(*) FROM v GROUP BY c3"),
          1)
        assertAggregateOnDataframe(
          spark.sql("SELECT c4, count(*) FROM v GROUP BY c4"),
          1)
        assertAggregateOnDataframe(
          spark.sql("SELECT c1, c2, c3, c4, count(*) FROM v GROUP BY c1, c2, c3, c4"),
          3)
      }
    }
  }

  test("SPARK-46536 Support GROUP BY CalendarIntervalType") {
    val numRows = 50

    val dfSame = (0 until numRows)
      .map(_ => Tuple1(new CalendarInterval(1, 2, 3)))
      .toDF("c0")
      .groupBy($"c0")
      .count()
    assertAggregateOnDataframe(dfSame, 1)

    val dfDifferent = (0 until numRows)
      .map(i => Tuple1(new CalendarInterval(i, i, i)))
      .toDF("c0")
      .groupBy($"c0")
      .count()
    assertAggregateOnDataframe(dfDifferent, numRows)
  }

  test("SPARK-46779: Group by subquery with a cached relation") {
    withTempView("data") {
      sql(
        """create or replace temp view data(c1, c2) as values
          |(1, 2),
          |(1, 3),
          |(3, 7)""".stripMargin)
      sql("cache table data")
      val df = sql(
        """select c1, (select count(*) from data d1 where d1.c1 = d2.c1), count(c2)
          |from data d2 group by all""".stripMargin)
      checkAnswer(df, Row(1, 2, 2) :: Row(3, 1, 1) :: Nil)
    }
  }

  private def assertDecimalSumOverflow(
      df: DataFrame, ansiEnabled: Boolean, fnName: String, expectedAnswer: Row): Unit = {
    if (!ansiEnabled) {
      checkAnswer(df, expectedAnswer)
    } else {
      val e = intercept[ArithmeticException] {
        df.collect()
      }
      assert(e.getMessage.contains("cannot be represented as Decimal") ||
        e.getMessage.contains(s"Overflow in sum of decimals. Use 'try_$fnName' to tolerate " +
          s"overflow and return NULL instead."))
    }
  }

  def checkAggResultsForDecimalOverflow(aggFn: Column => Column, fnName: String): Unit = {
    Seq("true", "false").foreach { wholeStageEnabled =>
      withSQLConf((SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, wholeStageEnabled)) {
        Seq(true, false).foreach { ansiEnabled =>
          withSQLConf((SQLConf.ANSI_ENABLED.key, ansiEnabled.toString)) {
            val df0 = Seq(
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 2)).toDF("decNum", "intNum")
            val df1 = Seq(
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2),
              (BigDecimal("10000000000000000000"), 2)).toDF("decNum", "intNum")
            val df = df0.union(df1)
            val df2 = df.withColumnRenamed("decNum", "decNum2").
              join(df, "intNum").agg(aggFn($"decNum"))

            val expectedAnswer = Row(null)
            assertDecimalSumOverflow(df2, ansiEnabled, fnName, expectedAnswer)

            val decStr = "1" + "0" * 19
            val d1 = spark.range(0, 12, 1, 1)
            val d2 = d1.select(expr(s"cast('$decStr' as decimal (38, 18)) as d")).agg(aggFn($"d"))
            assertDecimalSumOverflow(d2, ansiEnabled, fnName, expectedAnswer)

            val d3 = spark.range(0, 1, 1, 1).union(spark.range(0, 11, 1, 1))
            val d4 = d3.select(expr(s"cast('$decStr' as decimal (38, 18)) as d")).agg(aggFn($"d"))
            assertDecimalSumOverflow(d4, ansiEnabled, fnName, expectedAnswer)

            val d5 = d3.select(expr(s"cast('$decStr' as decimal (38, 18)) as d"),
              lit(1).as("key")).groupBy("key").agg(aggFn($"d").alias("aggd")).select($"aggd")
            assertDecimalSumOverflow(d5, ansiEnabled, fnName, expectedAnswer)

            val nullsDf = spark.range(1, 4, 1).select(expr(s"cast(null as decimal(38,18)) as d"))

            val largeDecimals = Seq(BigDecimal("1"* 20 + ".123"), BigDecimal("9"* 20 + ".123")).
              toDF("d")
            assertDecimalSumOverflow(
              nullsDf.union(largeDecimals).agg(aggFn($"d")), ansiEnabled, fnName, expectedAnswer)

            val df3 = Seq(
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("50000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 2)).toDF("decNum", "intNum")

            val df4 = Seq(
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 2)).toDF("decNum", "intNum")

            val df5 = Seq(
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("10000000000000000000"), 1),
              (BigDecimal("20000000000000000000"), 2)).toDF("decNum", "intNum")

            val df6 = df3.union(df4).union(df5)
            val df7 = df6.groupBy("intNum").agg(aggFn($"decNum"), countDistinct("decNum")).
              filter("intNum == 1")
            assertDecimalSumOverflow(df7, ansiEnabled, fnName, Row(1, null, 2))
          }
        }
      }
    }
  }

  test("SPARK-28067: Aggregate sum should not return wrong results for decimal overflow") {
    checkAggResultsForDecimalOverflow(c => sum(c), "sum")
  }

  test("SPARK-35955: Aggregate avg should not return wrong results for decimal overflow") {
    checkAggResultsForDecimalOverflow(c => avg(c), "avg")
  }

  test("SPARK-28224: Aggregate sum big decimal overflow") {
    val largeDecimals = spark.sparkContext.parallelize(
      DecimalData(BigDecimal("1"* 20 + ".123"), BigDecimal("1"* 20 + ".123")) ::
        DecimalData(BigDecimal("9"* 20 + ".123"), BigDecimal("9"* 20 + ".123")) :: Nil).toDF()

    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf((SQLConf.ANSI_ENABLED.key, ansiEnabled.toString)) {
        val structDf = largeDecimals.select("a").agg(sum("a"))
        assertDecimalSumOverflow(structDf, ansiEnabled, "sum", Row(null))
      }
    }
  }

  test("SPARK-32761: aggregating multiple distinct CONSTANT columns") {
     checkAnswer(sql("select count(distinct 2), count(distinct 2,3)"), Row(1, 1))
  }

  test("aggregating with various distinct expressions") {
    abstract class AggregateTestCaseBase(
        val query: String,
        val resultSeq: Seq[Seq[Row]],
        val hasExpandNodeInPlan: Boolean)
    case class AggregateTestCase(
        override val query: String,
        override val resultSeq: Seq[Seq[Row]],
        override val hasExpandNodeInPlan: Boolean)
      extends AggregateTestCaseBase(query, resultSeq, hasExpandNodeInPlan)
    case class AggregateTestCaseDefault(
        override val query: String)
      extends AggregateTestCaseBase(
        query,
        Seq(Seq(Row(0)), Seq(Row(1)), Seq(Row(1))),
        hasExpandNodeInPlan = true)

    val t = "t"
    val testCases: Seq[AggregateTestCaseBase] = Seq(
      AggregateTestCaseDefault(
        s"""SELECT COUNT(DISTINCT "col") FROM $t"""
      ),
      AggregateTestCaseDefault(
        s"SELECT COUNT(DISTINCT 1) FROM $t"
      ),
      AggregateTestCaseDefault(
        s"SELECT COUNT(DISTINCT 1 + 2) FROM $t"
      ),
      AggregateTestCaseDefault(
        s"SELECT COUNT(DISTINCT 1, 2, 1 + 2) FROM $t"
      ),
      AggregateTestCase(
        s"SELECT COUNT(1), COUNT(DISTINCT 1) FROM $t",
        Seq(Seq(Row(0, 0)), Seq(Row(1, 1)), Seq(Row(2, 1))),
        hasExpandNodeInPlan = true
      ),
      AggregateTestCaseDefault(
        s"""SELECT COUNT(DISTINCT 1, "col") FROM $t"""
      ),
      AggregateTestCaseDefault(
        s"""SELECT COUNT(DISTINCT collation("abc")) FROM $t"""
      ),
      AggregateTestCaseDefault(
        s"""SELECT COUNT(DISTINCT current_date()) FROM $t"""
      ),
      AggregateTestCaseDefault(
        s"""SELECT COUNT(DISTINCT array(1, 2)[1]) FROM $t"""
      ),
      AggregateTestCaseDefault(
        s"""SELECT COUNT(DISTINCT map(1, 2)[1]) FROM $t"""
      ),
      AggregateTestCaseDefault(
        s"""SELECT COUNT(DISTINCT struct(1, 2).col1) FROM $t"""
      ),
      AggregateTestCase(
        s"SELECT COUNT(DISTINCT 1) FROM $t GROUP BY col",
        Seq(Seq(), Seq(Row(1)), Seq(Row(1), Row(1))),
        hasExpandNodeInPlan = false
      ),
      AggregateTestCaseDefault(
        s"SELECT COUNT(DISTINCT 1) FROM $t WHERE 1 = 1"
      ),
      AggregateTestCase(
        s"SELECT COUNT(DISTINCT 1) FROM $t WHERE 1 = 0",
        Seq(Seq(Row(0)), Seq(Row(0)), Seq(Row(0))),
        hasExpandNodeInPlan = false
      ),
      AggregateTestCase(
        s"SELECT SUM(DISTINCT 1) FROM (SELECT COUNT(DISTINCT 1) FROM $t)",
        Seq(Seq(Row(1)), Seq(Row(1)), Seq(Row(1))),
        hasExpandNodeInPlan = false
      ),
      AggregateTestCase(
        s"SELECT SUM(DISTINCT 1) FROM (SELECT COUNT(1) FROM $t)",
        Seq(Seq(Row(1)), Seq(Row(1)), Seq(Row(1))),
        hasExpandNodeInPlan = false
      ),
      AggregateTestCase(
        s"SELECT SUM(1) FROM (SELECT COUNT(DISTINCT 1) FROM $t)",
        Seq(Seq(Row(1)), Seq(Row(1)), Seq(Row(1))),
        hasExpandNodeInPlan = false
      ),
      AggregateTestCaseDefault(
        s"SELECT SUM(x) FROM (SELECT COUNT(DISTINCT 1) AS x FROM $t)"),
      AggregateTestCase(
        s"""SELECT COUNT(DISTINCT 1), COUNT(DISTINCT "col") FROM $t""",
        Seq(Seq(Row(0, 0)), Seq(Row(1, 1)), Seq(Row(1, 1))),
        hasExpandNodeInPlan = true
      ),
      AggregateTestCase(
        s"""SELECT COUNT(DISTINCT 1), COUNT(DISTINCT col) FROM $t""",
        Seq(Seq(Row(0, 0)), Seq(Row(1, 1)), Seq(Row(1, 2))),
        hasExpandNodeInPlan = true
      )
    )
    withTable(t) {
      sql(s"create table $t(col int) using parquet")
      Seq(0, 1, 2).foreach(columnValue => {
        if (columnValue != 0) {
          sql(s"insert into $t(col) values($columnValue)")
        }
        testCases.foreach(testCase => {
          val query = sql(testCase.query)
          checkAnswer(query, testCase.resultSeq(columnValue))
          val hasExpandNodeInPlan = query.queryExecution.optimizedPlan.collectFirst {
            case _: Expand => true
          }.nonEmpty
          assert(hasExpandNodeInPlan == testCase.hasExpandNodeInPlan)
        })
      })
    }
  }

  test("SPARK-49261: Literals in grouping expressions shouldn't result in unresolved aggregation") {
    val data = Seq((1, 1.001d, 2), (2, 3.001d, 4), (2, 3.001, 4)).toDF("a", "b", "c")
    withTempView("v1") {
      data.createOrReplaceTempView("v1")
      val df =
        sql("""SELECT
              |  ROUND(SUM(b), 6) AS sum1,
              |  COUNT(DISTINCT a) AS count1,
              |  COUNT(DISTINCT c) AS count2
              |FROM (
              |  SELECT
              |    6 AS gb,
              |    *
              |  FROM v1
              |)
              |GROUP BY a, gb
              |""".stripMargin)
      checkAnswer(df, Row(1.001d, 1, 1) :: Row(6.002d, 1, 1) :: Nil)
    }
  }
}

case class B(c: Option[Double])
case class A(b: Option[B])
