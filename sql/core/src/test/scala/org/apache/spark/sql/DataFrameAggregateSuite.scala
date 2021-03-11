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

import scala.util.Random

import org.scalatest.matchers.must.Matchers.the

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
    assert(cube0.where("date IS NULL").count > 0)
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
      courseSales.groupBy().agg(grouping("course")).explain()
    }
    intercept[AnalysisException] {
      courseSales.groupBy().agg(grouping_id("course")).explain()
    }
  }

  test("grouping/grouping_id inside window function") {

    val w = Window.orderBy(sum("earnings"))
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
      testData2.agg(avg($"a"), sumDistinct($"a")), // non-partial
      Row(2.0, 6.0) :: Nil)

    checkAnswer(
      decimalData.agg(avg($"a")),
      Row(new java.math.BigDecimal(2)))

    checkAnswer(
      decimalData.agg(avg($"a"), sumDistinct($"a")), // non-partial
      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)

    checkAnswer(
      decimalData.agg(avg($"a" cast DecimalType(10, 2))),
      Row(new java.math.BigDecimal(2)))
    // non-partial
    checkAnswer(
      decimalData.agg(
        avg($"a" cast DecimalType(10, 2)), sumDistinct($"a" cast DecimalType(10, 2))),
      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)
  }

  test("null average") {
    checkAnswer(
      testData3.agg(avg($"b")),
      Row(2.0))

    checkAnswer(
      testData3.agg(avg($"b"), countDistinct($"b")),
      Row(2.0, 1))

    checkAnswer(
      testData3.agg(avg($"b"), sumDistinct($"b")), // non-partial
      Row(2.0, 2.0))
  }

  test("zero average") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(avg($"a")),
      Row(null))

    checkAnswer(
      emptyTableData.agg(avg($"a"), sumDistinct($"b")), // non-partial
      Row(null, null))
  }

  test("count") {
    assert(testData2.count() === testData2.rdd.map(_ => 1).count())

    checkAnswer(
      testData2.agg(count($"a"), sumDistinct($"a")), // non-partial
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
        count($"a"), count($"b"), count(lit(1)), countDistinct($"a"), countDistinct($"b")),
      Row(2, 1, 2, 2, 1)
    )

    checkAnswer(
      testData3.agg(count($"b"), countDistinct($"b"), sumDistinct($"b")), // non-partial
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
      df1.agg(countDistinct($"key1", $"key2")),
      Row(3)
    )

    checkAnswer(
      df1.agg(countDistinct($"key1", $"key2", $"key3")),
      Row(3)
    )

    checkAnswer(
      df1.groupBy($"key1").agg(countDistinct($"key2", $"key3")),
      Seq(Row("a", 2), Row("x", 1))
    )
  }

  test("zero count") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(count($"a"), sumDistinct($"a")), // non-partial
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
      emptyTableData.agg(sumDistinct($"a")),
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
    assert(error.message.contains("collect_set() cannot have map type data"))
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
      df.groupBy($"x").agg(countDistinct($"y"), sort_array(collect_list($"z"))),
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
    val e = intercept[AnalysisException] {
      testData.groupBy(sum($"key")).count()
    }
    assert(e.message.contains("aggregate functions are not allowed in GROUP BY"))
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
    checkAnswer(spark.emptyDataFrame.groupBy().agg(emptyAgg), Seq(Row()))
    checkAnswer(spark.emptyDataFrame.groupBy().agg(count("*")), Seq(Row(0)))
    checkAnswer(spark.emptyDataFrame.dropDuplicates().agg(emptyAgg), Seq(Row()))
    checkAnswer(spark.emptyDataFrame.dropDuplicates().groupBy().agg(emptyAgg), Seq(Row()))
    checkAnswer(spark.emptyDataFrame.dropDuplicates().groupBy().agg(count("*")), Seq(Row(0)))

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
      "grouping expressions: [current_date(None)], value: [key: int, value: string], " +
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

  test("SPARK-27581: DataFrame countDistinct(\"*\") shouldn't fail with AnalysisException") {
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
        sql("SELECT max_by(x, y) FROM tempView").show
      }
      assert(
        error.message.contains("function max_by does not support ordering on type map<int,string>"))
    }
  }

  test("min_by") {
    val yearOfMinEarnings =
      sql("SELECT course, min_by(year, earnings) FROM courseSales GROUP BY course")
    checkAnswer(yearOfMinEarnings, Row("dotNET", 2012) :: Row("Java", 2012) :: Nil)

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
        sql("SELECT min_by(x, y) FROM tempView").show
      }
      assert(
        error.message.contains("function min_by does not support ordering on type map<int,string>"))
    }
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

      val error = intercept[AnalysisException] {
        sql("SELECT COUNT_IF(x) FROM tempView")
      }
      assert(error.message.contains("function count_if requires boolean type"))
    }
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
      A(Some(B(Some(1.0))))).toDF
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

    val nonStringMapDF = Seq(Tuple1(Map(1 -> 1))).toDF("col")
    // Spark implicit casts string literal "a" to int to match the key type.
    checkAnswer(nonStringMapDF.groupBy(struct($"col.a")).count().select("count"), Row(1))

    val arrayDF = Seq(Tuple1(Seq(1))).toDF("col")
    val e = intercept[AnalysisException](arrayDF.groupBy(struct($"col.a")).count())
    assert(e.message.contains("requires integral type"))
  }
}

case class B(c: Option[Double])
case class A(b: Option[B])
