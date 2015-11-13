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
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.DecimalType


class DataFrameAggregateSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

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
  }

  test("spark.sql.retainGroupColumns config") {
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )

    sqlContext.conf.setConf(SQLConf.DATAFRAME_RETAIN_GROUP_COLUMNS, false)
    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),
      Seq(Row(3), Row(3), Row(3))
    )
    sqlContext.conf.setConf(SQLConf.DATAFRAME_RETAIN_GROUP_COLUMNS, true)
  }

  test("agg without groups") {
    checkAnswer(
      testData2.agg(sum('b)),
      Row(9)
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
      Row(new java.math.BigDecimal(2.0)))

    checkAnswer(
      decimalData.agg(avg('a), sumDistinct('a)), // non-partial
      Row(new java.math.BigDecimal(2.0), new java.math.BigDecimal(6)) :: Nil)

    checkAnswer(
      decimalData.agg(avg('a cast DecimalType(10, 2))),
      Row(new java.math.BigDecimal(2.0)))
    // non-partial
    checkAnswer(
      decimalData.agg(avg('a cast DecimalType(10, 2)), sumDistinct('a cast DecimalType(10, 2))),
      Row(new java.math.BigDecimal(2.0), new java.math.BigDecimal(6)) :: Nil)
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
    assert(testData2.count() === testData2.map(_ => 1).count())

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
  }

  test("zero stddev") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
    emptyTableData.agg(stddev('a), stddev_pop('a), stddev_samp('a)),
    Row(Double.NaN, Double.NaN, Double.NaN))
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
    val absTol = 1e-8

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
      input.agg(variance('a), var_samp('a), var_pop('a), skewness('a), kurtosis('a)),
      Row(Double.NaN, Double.NaN, 0.0, Double.NaN, Double.NaN))

    checkAnswer(
      input.agg(
        expr("variance(a)"),
        expr("var_samp(a)"),
        expr("var_pop(a)"),
        expr("skewness(a)"),
        expr("kurtosis(a)")),
      Row(Double.NaN, Double.NaN, 0.0, Double.NaN, Double.NaN))
  }

  test("null moments") {
    val emptyTableData = Seq.empty[(Int, Int)].toDF("a", "b")

    checkAnswer(
      emptyTableData.agg(variance('a), var_samp('a), var_pop('a), skewness('a), kurtosis('a)),
      Row(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN))

    checkAnswer(
      emptyTableData.agg(
        expr("variance(a)"),
        expr("var_samp(a)"),
        expr("var_pop(a)"),
        expr("skewness(a)"),
        expr("kurtosis(a)")),
      Row(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN))
  }
}
