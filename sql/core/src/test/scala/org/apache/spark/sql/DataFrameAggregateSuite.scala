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

import org.scalatest.Matchers.the

import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData.DecimalData
import org.apache.spark.sql.types.DecimalType

case class Fact(date: Int, hour: Int, minute: Int, room_name: String, temp: Double)

class DataFrameAggregateSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  val absTol = 1e-8

  test("groupBy") {
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b").as("totB")).agg(sum('totB)),
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
      df.groupBy(regexp_extract('key, "([a-z]+)\\[", 1)).count(),
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
      testData2.agg(sum('b)),
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
      testData2.agg(avg('a), mean('a)),
      Row(2.0, 2.0))

    checkAnswer(
      testData2.agg(avg('a), sumDistinct('a)), // non-partial
      Row(2.0, 6.0) :: Nil)

    checkAnswer(
      decimalData.agg(avg('a)),
      Row(new java.math.BigDecimal(2)))

    checkAnswer(
      decimalData.agg(avg('a), sumDistinct('a)), // non-partial
      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)

    checkAnswer(
      decimalData.agg(avg('a cast DecimalType(10, 2))),
      Row(new java.math.BigDecimal(2)))
    // non-partial
    checkAnswer(
      decimalData.agg(avg('a cast DecimalType(10, 2)), sumDistinct('a cast DecimalType(10, 2))),
      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)
  }

  test("null average") {
    checkAnswer(
      testData3.agg(avg('b)),
      Row(2.0))

    checkAnswer(
      testData3.agg(avg('b), countDistinct('b)),
      Row(2.0, 1))

    checkAnswer(
      testData3.agg(avg('b), sumDistinct('b)), // non-partial
      Row(2.0, 2.0))
  }

  test("zero average") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(avg('a)),
      Row(null))

    checkAnswer(
      emptyTableData.agg(avg('a), sumDistinct('b)), // non-partial
      Row(null, null))
  }

  test("count") {
    assert(testData2.count() === testData2.rdd.map(_ => 1).count())

    checkAnswer(
      testData2.agg(count('a), sumDistinct('a)), // non-partial
      Row(6, 6.0))
  }

  test("null count") {
    checkAnswer(
      testData3.groupBy('a).agg(count('b)),
      Seq(Row(1, 0), Row(2, 1))
    )

    checkAnswer(
      testData3.groupBy('a).agg(count('a + 'b)),
      Seq(Row(1, 0), Row(2, 1))
    )

    checkAnswer(
      testData3.agg(count('a), count('b), count(lit(1)), countDistinct('a), countDistinct('b)),
      Row(2, 1, 2, 2, 1)
    )

    checkAnswer(
      testData3.agg(count('b), countDistinct('b), sumDistinct('b)), // non-partial
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
      df1.agg(countDistinct('key1, 'key2)),
      Row(3)
    )

    checkAnswer(
      df1.agg(countDistinct('key1, 'key2, 'key3)),
      Row(3)
    )

    checkAnswer(
      df1.groupBy('key1).agg(countDistinct('key2, 'key3)),
      Seq(Row("a", 2), Row("x", 1))
    )
  }

  test("zero count") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(count('a), sumDistinct('a)), // non-partial
      Row(0, null))
  }

  test("stddev") {
    val testData2ADev = math.sqrt(4.0 / 5.0)
    checkAnswer(
      testData2.agg(stddev('a), stddev_pop('a), stddev_samp('a)),
      Row(testData2ADev, math.sqrt(4 / 6.0), testData2ADev))
    checkAnswer(
      testData2.agg(stddev("a"), stddev_pop("a"), stddev_samp("a")),
      Row(testData2ADev, math.sqrt(4 / 6.0), testData2ADev))
  }

  test("zero stddev") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
    emptyTableData.agg(stddev('a), stddev_pop('a), stddev_samp('a)),
    Row(null, null, null))
  }

  test("zero sum") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(sum('a)),
      Row(null))
  }

  test("zero sum distinct") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(sumDistinct('a)),
      Row(null))
  }

  test("moments") {

    val sparkVariance = testData2.agg(variance('a))
    checkAggregatesWithTol(sparkVariance, Row(4.0 / 5.0), absTol)

    val sparkVariancePop = testData2.agg(var_pop('a))
    checkAggregatesWithTol(sparkVariancePop, Row(4.0 / 6.0), absTol)

    val sparkVarianceSamp = testData2.agg(var_samp('a))
    checkAggregatesWithTol(sparkVarianceSamp, Row(4.0 / 5.0), absTol)

    val sparkSkewness = testData2.agg(skewness('a))
    checkAggregatesWithTol(sparkSkewness, Row(0.0), absTol)

    val sparkKurtosis = testData2.agg(kurtosis('a))
    checkAggregatesWithTol(sparkKurtosis, Row(-1.5), absTol)
  }

  test("zero moments") {
    val input = Seq((1, 2)).toDF("a", "b")
    checkAnswer(
      input.agg(stddev('a), stddev_samp('a), stddev_pop('a), variance('a),
        var_samp('a), var_pop('a), skewness('a), kurtosis('a)),
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

  test("null moments") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      emptyTableData.agg(variance('a), var_samp('a), var_pop('a), skewness('a), kurtosis('a)),
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
      decimalData.groupBy('a cast DecimalType(10, 2)).agg(avg('b cast DecimalType(10, 2))),
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
        val hashAggPlan = hashAggDF.queryExecution.executedPlan
        if (wholeStage) {
          assert(hashAggPlan.find {
            case WholeStageCodegenExec(_: HashAggregateExec) => true
            case _ => false
          }.isDefined)
        } else {
          assert(hashAggPlan.isInstanceOf[HashAggregateExec])
        }
        hashAggDF.collect()

        // test case for ObjectHashAggregate and SortAggregate
        val objHashAggOrSortAggDF = df.groupBy("x").agg(c, collect_list("y"))
        val objHashAggOrSortAggPlan = objHashAggOrSortAggDF.queryExecution.executedPlan
        if (useObjectHashAgg) {
          assert(objHashAggOrSortAggPlan.isInstanceOf[ObjectHashAggregateExec])
        } else {
          assert(objHashAggOrSortAggPlan.isInstanceOf[SortAggregateExec])
        }
        objHashAggOrSortAggDF.collect()
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
      testData2.groupBy(lit(3), lit(4)).agg(lit(6), 'b, sum("b")),
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

      val sortAggPlans = aggPlan.collect {
        case sortAgg: SortAggregateExec => sortAgg
      }
      assert(sortAggPlans.isEmpty)

      val objHashAggPlans = aggPlan.collect {
        case objHashAgg: ObjectHashAggregateExec => objHashAgg
      }
      assert(objHashAggPlans.nonEmpty)

      val exchangePlans = aggPlan.collect {
        case shuffle: ShuffleExchangeExec => shuffle
      }
      assert(exchangePlans.length == 1)
    }
  }

  Seq(true, false).foreach { codegen =>
    test("SPARK-22951: dropDuplicates on empty dataFrames should produce correct aggregate " +
      s"results when codegen is enabled: $codegen") {
      withSQLConf((SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, codegen.toString)) {
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
    }
  }

  test("SPARK-21896: Window functions inside aggregate functions") {
    def checkWindowError(df: => DataFrame): Unit = {
      val thrownException = the [AnalysisException] thrownBy {
        df.queryExecution.analyzed
      }
      assert(thrownException.message.contains("not allowed to use a window function"))
    }

    checkWindowError(testData2.select(min(avg('b).over(Window.partitionBy('a)))))
    checkWindowError(testData2.agg(sum('b), max(rank().over(Window.orderBy('a)))))
    checkWindowError(testData2.groupBy('a).agg(sum('b), max(rank().over(Window.orderBy('b)))))
    checkWindowError(testData2.groupBy('a).agg(max(sum(sum('b)).over(Window.orderBy('a)))))
    checkWindowError(
      testData2.groupBy('a).agg(sum('b).as("s"), max(count("*").over())).where('s === 3))
    checkAnswer(
      testData2.groupBy('a).agg(max('b), sum('b).as("s"), count("*").over()).where('s === 3),
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
    assert(testData.groupBy('key).toString.contains(
      "[grouping expressions: [key], value: [key: int, value: string], type: GroupBy]"))
    assert(testData.groupBy(col("key")).toString.contains(
      "[grouping expressions: [key], value: [key: int, value: string], type: GroupBy]"))
    assert(testData.groupBy(current_date()).toString.contains(
      "grouping expressions: [current_date(None)], value: [key: int, value: string], " +
        "type: GroupBy]"))
  }

  test("SPARK-32344: Unevaluable's set to FIRST/LAST ignoreNullsExpr in distinct aggregates") {
    val queryTemplate = (agg: String) =>
      s"SELECT $agg(DISTINCT v) FROM (SELECT v FROM VALUES 1, 2, 3 t(v) ORDER BY v)"
    checkAnswer(sql(queryTemplate("FIRST")), Row(1))
    checkAnswer(sql(queryTemplate("LAST")), Row(3))
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
}
