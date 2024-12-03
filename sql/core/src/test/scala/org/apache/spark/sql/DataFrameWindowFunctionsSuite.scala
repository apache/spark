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

import org.scalatest.matchers.must.Matchers.the

import org.apache.spark.TestUtils.{assertNotSpilled, assertSpilled}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Lag, Literal, NonFoldableLiteral}
import org.apache.spark.sql.catalyst.optimizer.TransposeWindow
import org.apache.spark.sql.catalyst.plans.logical.{Window => LogicalWindow}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.SlowSQLTest

/**
 * Window function testing for DataFrame API.
 */
@SlowSQLTest
class DataFrameWindowFunctionsSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("rank functions in unspecific window") {
    withTempView("window_table") {
      val df = Seq((1, "1"), (2, "2"), (1, "2"), (2, "2")).toDF("key", "value")
      df.createOrReplaceTempView("window_table")
      checkAnswer(
        df.select(
          $"key",
          max("key").over(Window.partitionBy("value").orderBy("key")),
          min("key").over(Window.partitionBy("value").orderBy("key")),
          mean("key").over(Window.partitionBy("value").orderBy("key")),
          count("key").over(Window.partitionBy("value").orderBy("key")),
          sum("key").over(Window.partitionBy("value").orderBy("key")),
          ntile(2).over(Window.partitionBy("value").orderBy("key")),
          row_number().over(Window.partitionBy("value").orderBy("key")),
          dense_rank().over(Window.partitionBy("value").orderBy("key")),
          rank().over(Window.partitionBy("value").orderBy("key")),
          cume_dist().over(Window.partitionBy("value").orderBy("key")),
          percent_rank().over(Window.partitionBy("value").orderBy("key"))),
        Row(1, 1, 1, 1.0d, 1, 1, 1, 1, 1, 1, 1.0d, 0.0d) ::
          Row(1, 1, 1, 1.0d, 1, 1, 1, 1, 1, 1, 1.0d / 3.0d, 0.0d) ::
          Row(2, 2, 1, 5.0d / 3.0d, 3, 5, 1, 2, 2, 2, 1.0d, 0.5d) ::
          Row(2, 2, 1, 5.0d / 3.0d, 3, 5, 2, 3, 2, 2, 1.0d, 0.5d) :: Nil)
    }
  }

  test("window function should fail if order by clause is not specified") {
    val df = Seq((1, "1"), (2, "2"), (1, "2"), (2, "2")).toDF("key", "value")
    val e = intercept[AnalysisException](
      // Here we missed .orderBy("key")!
      df.select(row_number().over(Window.partitionBy("value"))).collect())
    assert(e.message.contains("requires window to be ordered"))
  }

  test("corr, covar_pop, stddev_pop functions in specific window") {
    withSQLConf(SQLConf.LEGACY_STATISTICAL_AGGREGATE.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "false") {
      val df = Seq(
        ("a", "p1", 10.0, 20.0),
        ("b", "p1", 20.0, 10.0),
        ("c", "p2", 20.0, 20.0),
        ("d", "p2", 20.0, 20.0),
        ("e", "p3", 0.0, 0.0),
        ("f", "p3", 6.0, 12.0),
        ("g", "p3", 6.0, 12.0),
        ("h", "p3", 8.0, 16.0),
        ("i", "p4", 5.0, 5.0)).toDF("key", "partitionId", "value1", "value2")
      checkAnswer(
        df.select(
          $"key",
          corr("value1", "value2").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          covar_pop("value1", "value2")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          var_pop("value1")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev_pop("value1")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          var_pop("value2")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev_pop("value2")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))),

        // As stddev_pop(expr) = sqrt(var_pop(expr))
        // the "stddev_pop" column can be calculated from the "var_pop" column.
        //
        // As corr(expr1, expr2) = covar_pop(expr1, expr2) / (stddev_pop(expr1) * stddev_pop(expr2))
        // the "corr" column can be calculated from the "covar_pop" and the two "stddev_pop" columns
        Seq(
          Row("a", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
          Row("b", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
          Row("c", null, 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("d", null, 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("e", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("f", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("g", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("h", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("i", Double.NaN, 0.0, 0.0, 0.0, 0.0, 0.0)))
    }
  }

  test("SPARK-13860: " +
    "corr, covar_pop, stddev_pop functions in specific window " +
    "LEGACY_STATISTICAL_AGGREGATE off") {
    withSQLConf(SQLConf.LEGACY_STATISTICAL_AGGREGATE.key -> "false",
      SQLConf.ANSI_ENABLED.key -> "false") {
      val df = Seq(
        ("a", "p1", 10.0, 20.0),
        ("b", "p1", 20.0, 10.0),
        ("c", "p2", 20.0, 20.0),
        ("d", "p2", 20.0, 20.0),
        ("e", "p3", 0.0, 0.0),
        ("f", "p3", 6.0, 12.0),
        ("g", "p3", 6.0, 12.0),
        ("h", "p3", 8.0, 16.0),
        ("i", "p4", 5.0, 5.0)).toDF("key", "partitionId", "value1", "value2")
      checkAnswer(
        df.select(
          $"key",
          corr("value1", "value2").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          covar_pop("value1", "value2")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          var_pop("value1")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev_pop("value1")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          var_pop("value2")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev_pop("value2")
            .over(Window.partitionBy("partitionId")
              .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))),

        // As stddev_pop(expr) = sqrt(var_pop(expr))
        // the "stddev_pop" column can be calculated from the "var_pop" column.
        //
        // As corr(expr1, expr2) = covar_pop(expr1, expr2) / (stddev_pop(expr1) * stddev_pop(expr2))
        // the "corr" column can be calculated from the "covar_pop" and the two "stddev_pop" columns
        Seq(
          Row("a", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
          Row("b", -1.0, -25.0, 25.0, 5.0, 25.0, 5.0),
          Row("c", null, 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("d", null, 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("e", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("f", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("g", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("h", 1.0, 18.0, 9.0, 3.0, 36.0, 6.0),
          Row("i", null, 0.0, 0.0, 0.0, 0.0, 0.0)))
    }
  }

  test("covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window") {
    withSQLConf(SQLConf.LEGACY_STATISTICAL_AGGREGATE.key -> "true") {
      val df = Seq(
        ("a", "p1", 10.0, 20.0),
        ("b", "p1", 20.0, 10.0),
        ("c", "p2", 20.0, 20.0),
        ("d", "p2", 20.0, 20.0),
        ("e", "p3", 0.0, 0.0),
        ("f", "p3", 6.0, 12.0),
        ("g", "p3", 6.0, 12.0),
        ("h", "p3", 8.0, 16.0),
        ("i", "p4", 5.0, 5.0)).toDF("key", "partitionId", "value1", "value2")
      checkAnswer(
        df.select(
          $"key",
          covar_samp("value1", "value2").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          var_samp("value1").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          variance("value1").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev_samp("value1").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev("value1").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
        ),
        Seq(
          Row("a", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
          Row("b", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
          Row("c", 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("d", 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("e", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("f", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("g", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("h", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("i", Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN)))
    }
  }

  test("SPARK-13860: " +
    "covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window " +
    "LEGACY_STATISTICAL_AGGREGATE off") {
    withSQLConf(SQLConf.LEGACY_STATISTICAL_AGGREGATE.key -> "false") {
      val df = Seq(
        ("a", "p1", 10.0, 20.0),
        ("b", "p1", 20.0, 10.0),
        ("c", "p2", 20.0, 20.0),
        ("d", "p2", 20.0, 20.0),
        ("e", "p3", 0.0, 0.0),
        ("f", "p3", 6.0, 12.0),
        ("g", "p3", 6.0, 12.0),
        ("h", "p3", 8.0, 16.0),
        ("i", "p4", 5.0, 5.0)).toDF("key", "partitionId", "value1", "value2")
      checkAnswer(
        df.select(
          $"key",
          covar_samp("value1", "value2").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          var_samp("value1").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          variance("value1").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev_samp("value1").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
          stddev("value1").over(Window.partitionBy("partitionId")
            .orderBy("key").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
        ),
        Seq(
          Row("a", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
          Row("b", -50.0, 50.0, 50.0, 7.0710678118654755, 7.0710678118654755),
          Row("c", 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("d", 0.0, 0.0, 0.0, 0.0, 0.0),
          Row("e", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("f", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("g", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("h", 24.0, 12.0, 12.0, 3.4641016151377544, 3.4641016151377544),
          Row("i", null, null, null, null, null)))
    }
  }

  test("collect_list in ascending ordered window") {
    val df = Seq(
      ("a", "p1", "1"),
      ("b", "p1", "2"),
      ("c", "p1", "2"),
      ("d", "p1", null),
      ("e", "p1", "3"),
      ("f", "p2", "10"),
      ("g", "p2", "11"),
      ("h", "p3", "20"),
      ("i", "p4", null)).toDF("key", "partition", "value")
    checkAnswer(
      df.select(
        $"key",
        sort_array(
          collect_list("value").over(Window.partitionBy($"partition").orderBy($"value")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))),
      Seq(
        Row("a", Array("1", "2", "2", "3")),
        Row("b", Array("1", "2", "2", "3")),
        Row("c", Array("1", "2", "2", "3")),
        Row("d", Array("1", "2", "2", "3")),
        Row("e", Array("1", "2", "2", "3")),
        Row("f", Array("10", "11")),
        Row("g", Array("10", "11")),
        Row("h", Array("20")),
        Row("i", Array())))
  }

  test("collect_list in descending ordered window") {
    val df = Seq(
      ("a", "p1", "1"),
      ("b", "p1", "2"),
      ("c", "p1", "2"),
      ("d", "p1", null),
      ("e", "p1", "3"),
      ("f", "p2", "10"),
      ("g", "p2", "11"),
      ("h", "p3", "20"),
      ("i", "p4", null)).toDF("key", "partition", "value")
    checkAnswer(
      df.select(
        $"key",
        sort_array(
          collect_list("value").over(Window.partitionBy($"partition").orderBy($"value".desc)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))),
      Seq(
        Row("a", Array("1", "2", "2", "3")),
        Row("b", Array("1", "2", "2", "3")),
        Row("c", Array("1", "2", "2", "3")),
        Row("d", Array("1", "2", "2", "3")),
        Row("e", Array("1", "2", "2", "3")),
        Row("f", Array("10", "11")),
        Row("g", Array("10", "11")),
        Row("h", Array("20")),
        Row("i", Array())))
  }

  test("collect_set in window") {
    val df = Seq(
      ("a", "p1", "1"),
      ("b", "p1", "2"),
      ("c", "p1", "2"),
      ("d", "p1", "3"),
      ("e", "p1", "3"),
      ("f", "p2", "10"),
      ("g", "p2", "11"),
      ("h", "p3", "20")).toDF("key", "partition", "value")
    checkAnswer(
      df.select(
        $"key",
        sort_array(
          collect_set("value").over(Window.partitionBy($"partition").orderBy($"value")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))),
      Seq(
        Row("a", Array("1", "2", "3")),
        Row("b", Array("1", "2", "3")),
        Row("c", Array("1", "2", "3")),
        Row("d", Array("1", "2", "3")),
        Row("e", Array("1", "2", "3")),
        Row("f", Array("10", "11")),
        Row("g", Array("10", "11")),
        Row("h", Array("20"))))
  }

  test("skewness and kurtosis functions in window") {
    val df = Seq(
      ("a", "p1", 1.0),
      ("b", "p1", 1.0),
      ("c", "p1", 2.0),
      ("d", "p1", 2.0),
      ("e", "p1", 3.0),
      ("f", "p1", 3.0),
      ("g", "p1", 3.0),
      ("h", "p2", 1.0),
      ("i", "p2", 2.0),
      ("j", "p2", 5.0)).toDF("key", "partition", "value")
    checkAnswer(
      df.select(
        $"key",
        skewness("value").over(Window.partitionBy("partition").orderBy($"key")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
        kurtosis("value").over(Window.partitionBy("partition").orderBy($"key")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))),
      // results are checked by scipy.stats.skew() and scipy.stats.kurtosis()
      Seq(
        Row("a", -0.27238010581457267, -1.506920415224914),
        Row("b", -0.27238010581457267, -1.506920415224914),
        Row("c", -0.27238010581457267, -1.506920415224914),
        Row("d", -0.27238010581457267, -1.506920415224914),
        Row("e", -0.27238010581457267, -1.506920415224914),
        Row("f", -0.27238010581457267, -1.506920415224914),
        Row("g", -0.27238010581457267, -1.506920415224914),
        Row("h", 0.5280049792181881, -1.5000000000000013),
        Row("i", 0.5280049792181881, -1.5000000000000013),
        Row("j", 0.5280049792181881, -1.5000000000000013)))
  }

  test("aggregation function on invalid column") {
    val df = Seq((1, "1")).toDF("key", "value")
    val e = intercept[AnalysisException](
      df.select($"key", count("invalid").over()))
    checkError(
      exception = e,
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map(
        "objectName" -> "`invalid`",
        "proposal" -> "`value`, `key`"),
      context = ExpectedContext(
        fragment = "count",
        callSitePattern = getCurrentClassCallSitePattern))
  }

  test("numerical aggregate functions on string column") {
    if (!conf.ansiEnabled) {
      val df = Seq((1, "a", "b")).toDF("key", "value1", "value2")
      checkAnswer(
        df.select($"key",
          var_pop("value1").over(),
          variance("value1").over(),
          stddev_pop("value1").over(),
          stddev("value1").over(),
          sum("value1").over(),
          mean("value1").over(),
          avg("value1").over(),
          corr("value1", "value2").over(),
          covar_pop("value1", "value2").over(),
          covar_samp("value1", "value2").over(),
          skewness("value1").over(),
          kurtosis("value1").over()),
        Seq(Row(1, null, null, null, null, null, null, null, null, null, null, null, null)))
    }
  }

  test("statistical functions") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2)).
      toDF("key", "value")
    val window = Window.partitionBy($"key")
    checkAnswer(
      df.select(
        $"key",
        var_pop($"value").over(window),
        var_samp($"value").over(window),
        approx_count_distinct($"value").over(window)),
      Seq.fill(4)(Row("a", 1.0d / 4.0d, 1.0d / 3.0d, 2))
        ++ Seq.fill(3)(Row("b", 2.0d / 3.0d, 1.0d, 3)))
  }

  test("window function with aggregates") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2)).
      toDF("key", "value")
    val window = Window.orderBy()
    checkAnswer(
      df.groupBy($"key")
        .agg(
          sum($"value"),
          sum(sum($"value")).over(window) - sum($"value")),
      Seq(Row("a", 6, 9), Row("b", 9, 6)))
  }

  test("SPARK-16195 empty over spec") {
    withTempView("window_table") {
      val df = Seq(("a", 1), ("a", 1), ("a", 2), ("b", 2)).
        toDF("key", "value")
      df.createOrReplaceTempView("window_table")
      checkAnswer(
        df.select($"key", $"value", sum($"value").over(), avg($"value").over()),
        Seq(Row("a", 1, 6, 1.5), Row("a", 1, 6, 1.5), Row("a", 2, 6, 1.5), Row("b", 2, 6, 1.5)))
      checkAnswer(
        sql("select key, value, sum(value) over(), avg(value) over() from window_table"),
        Seq(Row("a", 1, 6, 1.5), Row("a", 1, 6, 1.5), Row("a", 2, 6, 1.5), Row("b", 2, 6, 1.5)))
    }
  }

  test("window function with udaf") {
    val udaf = new UserDefinedAggregateFunction {
      def inputSchema: StructType = new StructType()
        .add("a", LongType)
        .add("b", LongType)

      def bufferSchema: StructType = new StructType()
        .add("product", LongType)

      def dataType: DataType = LongType

      def deterministic: Boolean = true

      def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
      }

      def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (!(input.isNullAt(0) || input.isNullAt(1))) {
          buffer(0) = buffer.getLong(0) + input.getLong(0) * input.getLong(1)
        }
      }

      def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      }

      def evaluate(buffer: Row): Any =
        buffer.getLong(0)
    }
    val df = Seq(
      ("a", 1, 1),
      ("a", 1, 5),
      ("a", 2, 10),
      ("a", 2, -1),
      ("b", 4, 7),
      ("b", 3, 8),
      ("b", 2, 4))
      .toDF("key", "a", "b")
    val window = Window.partitionBy($"key").orderBy($"a").rangeBetween(Long.MinValue, 0L)
    checkAnswer(
      df.select(
        $"key",
        $"a",
        $"b",
        udaf($"a", $"b").over(window)),
      Seq(
        Row("a", 1, 1, 6),
        Row("a", 1, 5, 6),
        Row("a", 2, 10, 24),
        Row("a", 2, -1, 24),
        Row("b", 4, 7, 60),
        Row("b", 3, 8, 32),
        Row("b", 2, 4, 8)))
  }

  test("window function with aggregator") {
    val agg = udaf(new Aggregator[(Long, Long), Long, Long] {
      def zero: Long = 0L
      def reduce(b: Long, a: (Long, Long)): Long = b + (a._1 * a._2)
      def merge(b1: Long, b2: Long): Long = b1 + b2
      def finish(r: Long): Long = r
      def bufferEncoder: Encoder[Long] = Encoders.scalaLong
      def outputEncoder: Encoder[Long] = Encoders.scalaLong
    })

    val df = Seq(
      ("a", 1, 1),
      ("a", 1, 5),
      ("a", 2, 10),
      ("a", 2, -1),
      ("b", 4, 7),
      ("b", 3, 8),
      ("b", 2, 4))
      .toDF("key", "a", "b")
    val window = Window.partitionBy($"key").orderBy($"a").rangeBetween(Long.MinValue, 0L)
    checkAnswer(
      df.select(
        $"key",
        $"a",
        $"b",
        agg($"a", $"b").over(window)),
      Seq(
        Row("a", 1, 1, 6),
        Row("a", 1, 5, 6),
        Row("a", 2, 10, 24),
        Row("a", 2, -1, 24),
        Row("b", 4, 7, 60),
        Row("b", 3, 8, 32),
        Row("b", 2, 4, 8)))
  }

  test("null inputs") {
    val df = Seq(("a", 1), ("a", 1), ("a", 2), ("a", 2), ("b", 4), ("b", 3), ("b", 2))
      .toDF("key", "value")
    val window = Window.orderBy()
    checkAnswer(
      df.select(
        $"key",
        $"value",
        avg(lit(null)).over(window),
        sum(lit(null)).over(window)),
      Seq(
        Row("a", 1, null, null),
        Row("a", 1, null, null),
        Row("a", 2, null, null),
        Row("a", 2, null, null),
        Row("b", 4, null, null),
        Row("b", 3, null, null),
        Row("b", 2, null, null)))
  }

  test("last/first with ignoreNulls") {
    val nullStr: String = null
    val df = Seq(
      ("a", 0, nullStr),
      ("a", 1, "x"),
      ("a", 2, "y"),
      ("a", 3, "z"),
      ("a", 4, nullStr),
      ("b", 1, nullStr),
      ("b", 2, nullStr)).
      toDF("key", "order", "value")
    val window = Window.partitionBy($"key").orderBy($"order")
    checkAnswer(
      df.select(
        $"key",
        $"order",
        first($"value").over(window),
        first($"value", ignoreNulls = false).over(window),
        first($"value", ignoreNulls = true).over(window),
        last($"value").over(window),
        last($"value", ignoreNulls = false).over(window),
        last($"value", ignoreNulls = true).over(window)),
      Seq(
        Row("a", 0, null, null, null, null, null, null),
        Row("a", 1, null, null, "x", "x", "x", "x"),
        Row("a", 2, null, null, "x", "y", "y", "y"),
        Row("a", 3, null, null, "x", "z", "z", "z"),
        Row("a", 4, null, null, "x", null, null, "z"),
        Row("b", 1, null, null, null, null, null, null),
        Row("b", 2, null, null, null, null, null, null)))
  }

  test("last/first on descending ordered window") {
    val nullStr: String = null
    val df = Seq(
      ("a", 0, nullStr),
      ("a", 1, "x"),
      ("a", 2, "y"),
      ("a", 3, "z"),
      ("a", 4, "v"),
      ("b", 1, "k"),
      ("b", 2, "l"),
      ("b", 3, nullStr)).
      toDF("key", "order", "value")
    val window = Window.partitionBy($"key").orderBy($"order".desc)
    checkAnswer(
      df.select(
        $"key",
        $"order",
        first($"value").over(window),
        first($"value", ignoreNulls = false).over(window),
        first($"value", ignoreNulls = true).over(window),
        last($"value").over(window),
        last($"value", ignoreNulls = false).over(window),
        last($"value", ignoreNulls = true).over(window)),
      Seq(
        Row("a", 0, "v", "v", "v", null, null, "x"),
        Row("a", 1, "v", "v", "v", "x", "x", "x"),
        Row("a", 2, "v", "v", "v", "y", "y", "y"),
        Row("a", 3, "v", "v", "v", "z", "z", "z"),
        Row("a", 4, "v", "v", "v", "v", "v", "v"),
        Row("b", 1, null, null, "l", "k", "k", "k"),
        Row("b", 2, null, null, "l", "l", "l", "l"),
        Row("b", 3, null, null, null, null, null, null)))
  }

  test("nth_value with ignoreNulls") {
    val nullStr: String = null
    val df = Seq(
      ("a", 0, nullStr),
      ("a", 1, "x"),
      ("a", 2, "y"),
      ("a", 3, "z"),
      ("a", 4, nullStr),
      ("b", 1, nullStr),
      ("b", 2, nullStr)).
      toDF("key", "order", "value")
    val window = Window.partitionBy($"key").orderBy($"order")
    checkAnswer(
      df.select(
        $"key",
        $"order",
        nth_value($"value", 2).over(window),
        nth_value($"value", 2, ignoreNulls = false).over(window),
        nth_value($"value", 2, ignoreNulls = true).over(window),
        nth_value($"value", 3, ignoreNulls = false).over(window)),
      Seq(
        Row("a", 0, null, null, null, null),
        Row("a", 1, "x", "x", null, null),
        Row("a", 2, "x", "x", "y", "y"),
        Row("a", 3, "x", "x", "y", "y"),
        Row("a", 4, "x", "x", "y", "y"),
        Row("b", 1, null, null, null, null),
        Row("b", 2, null, null, null, null)))
  }

  test("nth_value with ignoreNulls over offset window frame") {
    val nullStr: String = null
    val df = Seq(
      ("a", 0, nullStr),
      ("a", 1, "x"),
      ("a", 2, "y"),
      ("a", 3, "z"),
      ("a", 4, nullStr),
      ("b", 1, nullStr),
      ("b", 2, nullStr)).
      toDF("key", "order", "value")
    val window1 = Window.partitionBy($"key").orderBy($"order")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val window2 = Window.partitionBy($"key").orderBy($"order")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    checkAnswer(
      df.select(
        $"key",
        $"order",
        nth_value($"value", 2).over(window1),
        nth_value($"value", 2, ignoreNulls = false).over(window1),
        nth_value($"value", 2, ignoreNulls = true).over(window1),
        nth_value($"value", 2).over(window2),
        nth_value($"value", 2, ignoreNulls = false).over(window2),
        nth_value($"value", 2, ignoreNulls = true).over(window2)),
      Seq(
        Row("a", 0, "x", "x", "y", null, null, null),
        Row("a", 1, "x", "x", "y", "x", "x", null),
        Row("a", 2, "x", "x", "y", "x", "x", "y"),
        Row("a", 3, "x", "x", "y", "x", "x", "y"),
        Row("a", 4, "x", "x", "y", "x", "x", "y"),
        Row("b", 1, null, null, null, null, null, null),
        Row("b", 2, null, null, null, null, null, null)))

    val df2 = Seq(
      ("a", 1, "x"),
      ("a", 2, "y"),
      ("a", 3, "z")).
      toDF("key", "order", "value")
    checkAnswer(
      df2.select(
        $"key",
        $"order",
        nth_value($"value", 2).over(window1),
        nth_value($"value", 2, ignoreNulls = true).over(window1),
        nth_value($"value", 2).over(window2),
        nth_value($"value", 2, ignoreNulls = true).over(window2),
        nth_value($"value", 3).over(window1),
        nth_value($"value", 3, ignoreNulls = true).over(window1),
        nth_value($"value", 3).over(window2),
        nth_value($"value", 3, ignoreNulls = true).over(window2),
        nth_value($"value", 4).over(window1),
        nth_value($"value", 4, ignoreNulls = true).over(window1),
        nth_value($"value", 4).over(window2),
        nth_value($"value", 4, ignoreNulls = true).over(window2)),
      Seq(
        Row("a", 1, "y", "y", null, null, "z", "z", null, null, null, null, null, null),
        Row("a", 2, "y", "y", "y", "y", "z", "z", null, null, null, null, null, null),
        Row("a", 3, "y", "y", "y", "y", "z", "z", "z", "z", null, null, null, null)))

    val df3 = Seq(
      ("a", 1, "x"),
      ("a", 2, nullStr),
      ("a", 3, "z")).
      toDF("key", "order", "value")
    checkAnswer(
      df3.select(
        $"key",
        $"order",
        nth_value($"value", 3).over(window1),
        nth_value($"value", 3, ignoreNulls = true).over(window1),
        nth_value($"value", 3).over(window2),
        nth_value($"value", 3, ignoreNulls = true).over(window2)),
      Seq(
        Row("a", 1, "z", null, null, null),
        Row("a", 2, "z", null, null, null),
        Row("a", 3, "z", null, "z", null)))
  }

  test("nth_value on descending ordered window") {
    val nullStr: String = null
    val df = Seq(
      ("a", 0, nullStr),
      ("a", 1, "x"),
      ("a", 2, "y"),
      ("a", 3, "z"),
      ("a", 4, "v"),
      ("b", 1, "k"),
      ("b", 2, "l"),
      ("b", 3, nullStr)).
      toDF("key", "order", "value")
    val window = Window.partitionBy($"key").orderBy($"order".desc)
    checkAnswer(
      df.select(
        $"key",
        $"order",
        nth_value($"value", 2).over(window),
        nth_value($"value", 2, ignoreNulls = false).over(window),
        nth_value($"value", 2, ignoreNulls = true).over(window)),
      Seq(
        Row("a", 0, "z", "z", "z"),
        Row("a", 1, "z", "z", "z"),
        Row("a", 2, "z", "z", "z"),
        Row("a", 3, "z", "z", "z"),
        Row("a", 4, null, null, null),
        Row("b", 1, "l", "l", "k"),
        Row("b", 2, "l", "l", null),
        Row("b", 3, null, null, null)))
  }

  test("lead/lag with ignoreNulls") {
    val nullStr: String = null
    val df = Seq(
      ("a", 0, nullStr),
      ("a", 1, "x"),
      ("b", 2, nullStr),
      ("c", 3, nullStr),
      ("a", 4, "y"),
      ("b", 5, nullStr),
      ("a", 6, "z"),
      ("a", 7, "v"),
      ("a", 8, nullStr)).
      toDF("key", "order", "value")
    val window = Window.orderBy($"order")
    checkAnswer(
      df.select(
        $"key",
        $"order",
        $"value",
        lead($"value", 1).over(window),
        lead($"value", 2).over(window),
        lead($"value", 0, null, true).over(window),
        lead($"value", 1, null, true).over(window),
        lead($"value", 2, null, true).over(window),
        lead($"value", 3, null, true).over(window),
        // offset > rowCount: SPARK-45430
        lead($"value", 100, null, true).over(window),
        lead(concat($"value", $"key"), 1, null, true).over(window),
        lag($"value", 1).over(window),
        lag($"value", 2).over(window),
        lag($"value", 0, null, true).over(window),
        lag($"value", 1, null, true).over(window),
        lag($"value", 2, null, true).over(window),
        lag($"value", 3, null, true).over(window),
        // abs(offset) > rowCount: SPARK-45430
        lag($"value", -100, null, true).over(window),
        lag(concat($"value", $"key"), 1, null, true).over(window))
        .orderBy($"order"),
      Seq(
        Row("a", 0, null, "x", null, null, "x", "y", "z", null, "xa",
          null, null, null, null, null, null, null, null),
        Row("a", 1, "x", null, null, "x", "y", "z", "v", null, "ya",
          null, null, "x", null, null, null, null, null),
        Row("b", 2, null, null, "y", null, "y", "z", "v", null, "ya",
          "x", null, null, "x", null, null, null, "xa"),
        Row("c", 3, null, "y", null, null, "y", "z", "v", null, "ya",
          null, "x", null, "x", null, null, null, "xa"),
        Row("a", 4, "y", null, "z", "y", "z", "v", null, null, "za",
          null, null, "y", "x", null, null, null, "xa"),
        Row("b", 5, null, "z", "v", null, "z", "v", null, null, "za",
          "y", null, null, "y", "x", null, null, "ya"),
        Row("a", 6, "z", "v", null, "z", "v", null, null, null, "va",
          null, "y", "z", "y", "x", null, null, "ya"),
        Row("a", 7, "v", null, null, "v", null, null, null, null, null,
          "z", null, "v", "z", "y", "x", null, "za"),
        Row("a", 8, null, null, null, null, null, null, null, null, null,
          "v", "z", null, "v", "z", "y", null, "va")))
  }

  test("lag - Offset expression <offset> must be a literal") {
    val nullStr: String = null
    val df = Seq(
      ("a", 0, nullStr),
      ("a", 1, "x"),
      ("b", 2, nullStr),
      ("c", 3, nullStr),
      ("a", 4, "y"),
      ("b", 5, nullStr),
      ("a", 6, "z"),
      ("a", 7, "v"),
      ("a", 8, nullStr)).
      toDF("key", "order", "value")
    val window = Window.orderBy($"order")
    checkError(
      exception = intercept[AnalysisException] {
        df.select(
          $"key",
          $"order",
          $"value",
          lead($"value", 1).over(window),
          lead($"value", 2).over(window),
          lead($"value", 0, null, true).over(window),
          lead($"value", 1, null, true).over(window),
          lead($"value", 2, null, true).over(window),
          lead($"value", 3, null, true).over(window),
          lead(concat($"value", $"key"), 1, null, true).over(window),
          Column(Lag($"value".expr, NonFoldableLiteral(1), Literal(null), true)).over(window),
          lag($"value", 2).over(window),
          lag($"value", 0, null, true).over(window),
          lag($"value", 1, null, true).over(window),
          lag($"value", 2, null, true).over(window),
          lag($"value", 3, null, true).over(window),
          lag(concat($"value", $"key"), 1, null, true).over(window)).orderBy($"order").collect()
      },
      condition = "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT",
      parameters = Map(
        "sqlExpr" -> "\"lag(value, nonfoldableliteral(), NULL)\"",
        "inputName" -> "`offset`",
        "inputType" -> "\"INT\"",
        "inputExpr" -> "\"(- nonfoldableliteral())\""
      )
    )
  }

  test("SPARK-12989 ExtractWindowExpressions treats alias as regular attribute") {
    val src = Seq((0, 3, 5)).toDF("a", "b", "c")
      .withColumn("Data", struct("a", "b"))
      .drop("a")
      .drop("b")
    val winSpec = Window.partitionBy("Data.a", "Data.b").orderBy($"c".desc)
    val df = src.select($"*", max("c").over(winSpec) as "max")
    checkAnswer(df, Row(5, Row(0, 3), 5))
  }

  test("aggregation and rows between with unbounded + predicate pushdown") {
    withTempView("window_table") {
      val df = Seq((1, "1"), (2, "2"), (2, "3"), (1, "3"), (3, "2"), (4, "3")).toDF("key", "value")
      df.createOrReplaceTempView("window_table")
      val selectList = Seq($"key", $"value",
        last("key").over(
          Window.partitionBy($"value").orderBy($"key").rowsBetween(0, Long.MaxValue)),
        last("key").over(
          Window.partitionBy($"value").orderBy($"key").rowsBetween(Long.MinValue, 0)),
        last("key").over(Window.partitionBy($"value").orderBy($"key").rowsBetween(-1, 1)))

      checkAnswer(
        df.select(selectList: _*).where($"value" < "3"),
        Seq(Row(1, "1", 1, 1, 1), Row(2, "2", 3, 2, 3), Row(3, "2", 3, 3, 3)))
    }
  }

  test("aggregation and range between with unbounded + predicate pushdown") {
    withTempView("window_table") {
      val df = Seq((5, "1"), (5, "2"), (4, "2"), (6, "2"), (3, "1"), (2, "2")).toDF("key", "value")
      df.createOrReplaceTempView("window_table")
      val selectList = Seq($"key", $"value",
        last("value").over(
          Window.partitionBy($"value").orderBy($"key").rangeBetween(-2, -1)).equalTo("2")
          .as("last_v"),
        avg("key").over(Window.partitionBy("value").orderBy("key").rangeBetween(Long.MinValue, 1))
          .as("avg_key1"),
        avg("key").over(Window.partitionBy("value").orderBy("key").rangeBetween(0, Long.MaxValue))
          .as("avg_key2"),
        avg("key").over(Window.partitionBy("value").orderBy("key").rangeBetween(-1, 1))
          .as("avg_key3"))

      checkAnswer(
        df.select(selectList: _*).where($"value" < 2),
        Seq(Row(3, "1", null, 3.0, 4.0, 3.0), Row(5, "1", false, 4.0, 5.0, 5.0)))
    }
  }

  test("Window spill with less than the inMemoryThreshold") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    withSQLConf(SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "2",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "2") {
      assertNotSpilled(sparkContext, "select") {
        df.select($"key", sum("value").over(window)).collect()
      }
    }
  }

  test("Window spill with more than the inMemoryThreshold but less than the spillThreshold") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    withSQLConf(SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "2") {
      assertNotSpilled(sparkContext, "select") {
        df.select($"key", sum("value").over(window)).collect()
      }
    }
  }

  test("Window spill with more than the inMemoryThreshold and spillThreshold") {
    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
    val window = Window.partitionBy($"key").orderBy($"value")

    withSQLConf(SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1") {
      assertSpilled(sparkContext, "select") {
        df.select($"key", sum("value").over(window)).collect()
      }
    }
  }

  test("SPARK-21258: complex object in combination with spilling") {
    // Make sure we trigger the spilling path.
    withSQLConf(SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "17") {
      val sampleSchema = new StructType().
        add("f0", StringType).
        add("f1", LongType).
        add("f2", ArrayType(new StructType().
          add("f20", StringType))).
        add("f3", ArrayType(new StructType().
          add("f30", StringType)))

      val w0 = Window.partitionBy("f0").orderBy("f1")
      val w1 = w0.rowsBetween(Long.MinValue, Long.MaxValue)

      val c0 = first(struct($"f2", $"f3")).over(w0) as "c0"
      val c1 = last(struct($"f2", $"f3")).over(w1) as "c1"

      val input =
        """{"f1":1497820153720,"f2":[{"f20":"x","f21":0}],"f3":[{"f30":"x","f31":0}]}
          |{"f1":1497802179638}
          |{"f1":1497802189347}
          |{"f1":1497802189593}
          |{"f1":1497802189597}
          |{"f1":1497802189599}
          |{"f1":1497802192103}
          |{"f1":1497802193414}
          |{"f1":1497802193577}
          |{"f1":1497802193709}
          |{"f1":1497802202883}
          |{"f1":1497802203006}
          |{"f1":1497802203743}
          |{"f1":1497802203834}
          |{"f1":1497802203887}
          |{"f1":1497802203893}
          |{"f1":1497802203976}
          |{"f1":1497820168098}
          |""".stripMargin.split("\n").toSeq

      import testImplicits._

      assertSpilled(sparkContext, "select") {
        spark.read.schema(sampleSchema).json(input.toDS()).select(c0, c1).foreach { _ => () }
      }
    }
  }

  test("SPARK-24575: Window functions inside WHERE and HAVING clauses") {
    def checkAnalysisError(df: => DataFrame, clause: String): Unit = {
      val thrownException = the[AnalysisException] thrownBy {
        df.queryExecution.analyzed
      }
      assert(thrownException.message.contains(s"window functions inside $clause clause"))
    }

    checkAnalysisError(
      testData2.select("a").where(rank().over(Window.orderBy($"b")) === 1), "WHERE")
    checkAnalysisError(
      testData2.where($"b" === 2 && rank().over(Window.orderBy($"b")) === 1), "WHERE")
    checkAnalysisError(
      testData2.groupBy($"a")
        .agg(avg($"b").as("avgb"))
        .where($"a" > $"avgb" && rank().over(Window.orderBy($"a")) === 1), "WHERE")
    checkAnalysisError(
      testData2.groupBy($"a")
        .agg(max($"b").as("maxb"), sum($"b").as("sumb"))
        .where(rank().over(Window.orderBy($"a")) === 1), "WHERE")
    checkAnalysisError(
      testData2.groupBy($"a")
        .agg(max($"b").as("maxb"), sum($"b").as("sumb"))
        .where($"sumb" === 5 && rank().over(Window.orderBy($"a")) === 1), "WHERE")

    checkAnalysisError(sql("SELECT a FROM testData2 WHERE RANK() OVER(ORDER BY b) = 1"), "WHERE")
    checkAnalysisError(
      sql("SELECT * FROM testData2 WHERE b = 2 AND RANK() OVER(ORDER BY b) = 1"), "WHERE")
    checkAnalysisError(
      sql("SELECT * FROM testData2 GROUP BY a HAVING a > AVG(b) AND RANK() OVER(ORDER BY a) = 1"),
      "HAVING")
    checkAnalysisError(
      sql("SELECT a, MAX(b), SUM(b) FROM testData2 GROUP BY a HAVING RANK() OVER(ORDER BY a) = 1"),
      "HAVING")
    checkAnalysisError(
      sql(
        s"""SELECT a, MAX(b)
           |FROM testData2
           |GROUP BY a
           |HAVING SUM(b) = 5 AND RANK() OVER(ORDER BY a) = 1""".stripMargin),
      "HAVING")
  }

  test("window functions in multiple selects") {
    val df = Seq(
      ("S1", "P1", 100),
      ("S1", "P1", 700),
      ("S2", "P1", 200),
      ("S2", "P2", 300)
    ).toDF("sno", "pno", "qty")

    Seq(true, false).foreach { transposeWindowEnabled =>
      val excludedRules = if (transposeWindowEnabled) "" else TransposeWindow.ruleName
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRules) {
        val w1 = Window.partitionBy("sno")
        val w2 = Window.partitionBy("sno", "pno")

        val select = df.select($"sno", $"pno", $"qty", sum($"qty").over(w2).alias("sum_qty_2"))
          .select($"sno", $"pno", $"qty", col("sum_qty_2"), sum("qty").over(w1).alias("sum_qty_1"))

        val expectedNumExchanges = if (transposeWindowEnabled) 1 else 2
        val actualNumExchanges = stripAQEPlan(select.queryExecution.executedPlan).collect {
          case e: Exchange => e
        }.length
        assert(actualNumExchanges == expectedNumExchanges)

        checkAnswer(
          select,
          Seq(
            Row("S1", "P1", 100, 800, 800),
            Row("S1", "P1", 700, 800, 800),
            Row("S2", "P1", 200, 200, 500),
            Row("S2", "P2", 300, 300, 500)))
      }
    }
  }

  test("NaN and -0.0 in window partition keys") {
    val df = Seq(
      (Float.NaN, Double.NaN),
      (0.0f/0.0f, 0.0/0.0),
      (0.0f, 0.0),
      (-0.0f, -0.0)).toDF("f", "d")

    checkAnswer(
      df.select($"f", count(lit(1)).over(Window.partitionBy("f", "d"))),
      Seq(
        Row(Float.NaN, 2),
        Row(0.0f/0.0f, 2),
        Row(0.0f, 2),
        Row(-0.0f, 2)))

    // test with complicated window partition keys.
    val windowSpec1 = Window.partitionBy(array("f"), struct("d"))
    checkAnswer(
      df.select($"f", count(lit(1)).over(windowSpec1)),
      Seq(
        Row(Float.NaN, 2),
        Row(0.0f/0.0f, 2),
        Row(0.0f, 2),
        Row(-0.0f, 2)))

    val windowSpec2 = Window.partitionBy(array(struct("f")), struct(array("d")))
    checkAnswer(
      df.select($"f", count(lit(1)).over(windowSpec2)),
      Seq(
        Row(Float.NaN, 2),
        Row(0.0f/0.0f, 2),
        Row(0.0f, 2),
        Row(-0.0f, 2)))

    // test with df with complicated-type columns.
    val df2 = Seq(
      (Array(-0.0f, 0.0f), Tuple2(-0.0d, Double.NaN), Seq(Tuple2(-0.0d, Double.NaN))),
      (Array(0.0f, -0.0f), Tuple2(0.0d, Double.NaN), Seq(Tuple2(0.0d, 0.0/0.0)))
    ).toDF("arr", "stru", "arrOfStru")
    val windowSpec3 = Window.partitionBy("arr", "stru", "arrOfStru")
    checkAnswer(
      df2.select($"arr", $"stru", $"arrOfStru", count(lit(1)).over(windowSpec3)),
      Seq(
        Row(Seq(-0.0f, 0.0f), Row(-0.0d, Double.NaN), Seq(Row(-0.0d, Double.NaN)), 2),
        Row(Seq(0.0f, -0.0f), Row(0.0d, Double.NaN), Seq(Row(0.0d, 0.0/0.0)), 2)))
  }

  test("SPARK-38237: require all cluster keys for child required distribution for window query") {
    def partitionExpressionsColumns(expressions: Seq[Expression]): Seq[String] = {
      expressions.flatMap {
        case ref: AttributeReference => Some(ref.name)
      }
    }

    def isShuffleExecByRequirement(
        plan: ShuffleExchangeExec,
        desiredClusterColumns: Seq[String]): Boolean = plan match {
      case ShuffleExchangeExec(op: HashPartitioning, _, ENSURE_REQUIREMENTS, _) =>
        partitionExpressionsColumns(op.expressions) === desiredClusterColumns
      case _ => false
    }

    val df = Seq(("a", 1, 1), ("a", 2, 2), ("b", 1, 3), ("b", 1, 4)).toDF("key1", "key2", "value")
    val windowSpec = Window.partitionBy("key1", "key2").orderBy("value")

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true") {

      val windowed = df
        // repartition by subset of window partitionBy keys which satisfies ClusteredDistribution
        .repartition($"key1")
        .select(
          lead($"key1", 1).over(windowSpec),
          lead($"value", 1).over(windowSpec))

      checkAnswer(windowed, Seq(Row("b", 4), Row(null, null), Row(null, null), Row(null, null)))

      val shuffleByRequirement = windowed.queryExecution.executedPlan.exists {
        case w: WindowExec =>
          w.child.exists {
            case s: ShuffleExchangeExec => isShuffleExecByRequirement(s, Seq("key1", "key2"))
            case _ => false
          }
        case _ => false
      }

      assert(shuffleByRequirement, "Can't find desired shuffle node from the query plan")
    }
  }

  test("SPARK-38308: Properly handle Stream of window expressions") {
    val df = Seq(
      (1, 2, 3),
      (1, 3, 4),
      (2, 4, 5),
      (2, 5, 6)
    ).toDF("a", "b", "c")

    val w = Window.partitionBy("a").orderBy("b")
    val selectExprs = LazyList(
      sum("c").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)).as("sumc"),
      avg("c").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)).as("avgc")
    )
    checkAnswer(
      df.select(selectExprs: _*),
      Seq(
        Row(3, 3),
        Row(7, 3.5),
        Row(5, 5),
        Row(11, 5.5)
      )
    )
  }

  test("SPARK-38614: percent_rank should apply before limit") {
    val df = Seq.tabulate(101)(identity).toDF("id")
    val w = Window.orderBy("id")
    checkAnswer(
      df.select($"id", percent_rank().over(w)).limit(3),
      Seq(
        Row(0, 0.0d),
        Row(1, 0.01d),
        Row(2, 0.02d)
      )
    )
  }

  test("SPARK-40002: ntile should apply before limit") {
    val df = Seq.tabulate(101)(identity).toDF("id")
    val w = Window.orderBy("id")
    checkAnswer(
      df.select($"id", ntile(10).over(w)).limit(3),
      Seq(
        Row(0, 1),
        Row(1, 1),
        Row(2, 1)
      )
    )
  }

  test("SPARK-37099: Insert window group limit node for top-k computation") {

    val nullStr: String = null
    val df = Seq(
      ("a", 0, "c", 1.0),
      ("a", 1, "x", 2.0),
      ("a", 2, "y", 3.0),
      ("a", 3, "z", -1.0),
      ("a", 4, "", 2.0),
      ("a", 4, "", 2.0),
      ("b", 1, "h", Double.NaN),
      ("b", 1, "n", Double.PositiveInfinity),
      ("c", 1, "z", -2.0),
      ("c", 1, "a", -4.0),
      ("c", 2, nullStr, 5.0),
      ("d", 0, "1", 1.0),
      ("d", 1, "1", 2.0),
      ("d", 2, "2", 3.0),
      ("d", 3, "2", -1.0),
      ("d", 4, "2", 2.0),
      ("d", 4, "3", 2.0)).toDF("key", "value", "order", "value2")

    val window = Window.partitionBy($"key").orderBy($"order".asc_nulls_first)
    val window2 = Window.partitionBy($"key").orderBy($"order".desc_nulls_first)
    val window3 = Window.orderBy($"order".asc_nulls_first)

    Seq(true, false).foreach { enableEvaluator =>
      withSQLConf(SQLConf.USE_PARTITION_EVALUATOR.key -> enableEvaluator.toString) {
        Seq(-1, 100).foreach { threshold =>
          withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_THRESHOLD.key -> threshold.toString) {
            Seq($"rn" === 0, $"rn" < 1, $"rn" <= 0).foreach { condition =>
              checkAnswer(df.withColumn("rn", row_number().over(window)).where(condition),
                Seq.empty[Row]
              )
            }

            Seq($"rn" === 1, $"rn" < 2, $"rn" <= 1).foreach { condition =>
              checkAnswer(df.withColumn("rn", row_number().over(window)).where(condition),
                Seq(
                  Row("a", 4, "", 2.0, 1),
                  Row("b", 1, "h", Double.NaN, 1),
                  Row("c", 2, null, 5.0, 1),
                  Row("d", 0, "1", 1.0, 1)
                )
              )

              checkAnswer(df.withColumn("rn", rank().over(window)).where(condition),
                Seq(
                  Row("a", 4, "", 2.0, 1),
                  Row("a", 4, "", 2.0, 1),
                  Row("b", 1, "h", Double.NaN, 1),
                  Row("c", 2, null, 5.0, 1),
                  Row("d", 0, "1", 1.0, 1),
                  Row("d", 1, "1", 2.0, 1)
                )
              )

              checkAnswer(df.withColumn("rn", dense_rank().over(window)).where(condition),
                Seq(
                  Row("a", 4, "", 2.0, 1),
                  Row("a", 4, "", 2.0, 1),
                  Row("b", 1, "h", Double.NaN, 1),
                  Row("c", 2, null, 5.0, 1),
                  Row("d", 0, "1", 1.0, 1),
                  Row("d", 1, "1", 2.0, 1)
                )
              )

              checkAnswer(df.withColumn("rn", row_number().over(window3)).where(condition),
                Seq(
                  Row("c", 2, null, 5.0, 1)
                )
              )

              checkAnswer(df.withColumn("rn", rank().over(window3)).where(condition),
                Seq(
                  Row("c", 2, null, 5.0, 1)
                )
              )

              checkAnswer(df.withColumn("rn", dense_rank().over(window3)).where(condition),
                Seq(
                  Row("c", 2, null, 5.0, 1)
                )
              )
            }

            Seq($"rn" < 3, $"rn" <= 2).foreach { condition =>
              checkAnswer(df.withColumn("rn", row_number().over(window)).where(condition),
                Seq(
                  Row("a", 4, "", 2.0, 1),
                  Row("a", 4, "", 2.0, 2),
                  Row("b", 1, "h", Double.NaN, 1),
                  Row("b", 1, "n", Double.PositiveInfinity, 2),
                  Row("c", 1, "a", -4.0, 2),
                  Row("c", 2, null, 5.0, 1),
                  Row("d", 0, "1", 1.0, 1),
                  Row("d", 1, "1", 2.0, 2)
                )
              )

              checkAnswer(df.withColumn("rn", rank().over(window)).where(condition),
                Seq(
                  Row("a", 4, "", 2.0, 1),
                  Row("a", 4, "", 2.0, 1),
                  Row("b", 1, "h", Double.NaN, 1),
                  Row("b", 1, "n", Double.PositiveInfinity, 2),
                  Row("c", 1, "a", -4.0, 2),
                  Row("c", 2, null, 5.0, 1),
                  Row("d", 0, "1", 1.0, 1),
                  Row("d", 1, "1", 2.0, 1)
                )
              )

              checkAnswer(df.withColumn("rn", dense_rank().over(window)).where(condition),
                Seq(
                  Row("a", 0, "c", 1.0, 2),
                  Row("a", 4, "", 2.0, 1),
                  Row("a", 4, "", 2.0, 1),
                  Row("b", 1, "h", Double.NaN, 1),
                  Row("b", 1, "n", Double.PositiveInfinity, 2),
                  Row("c", 1, "a", -4.0, 2),
                  Row("c", 2, null, 5.0, 1),
                  Row("d", 0, "1", 1.0, 1),
                  Row("d", 1, "1", 2.0, 1),
                  Row("d", 2, "2", 3.0, 2),
                  Row("d", 3, "2", -1.0, 2),
                  Row("d", 4, "2", 2.0, 2)
                )
              )

              checkAnswer(df.withColumn("rn", row_number().over(window3)).where(condition),
                Seq(
                  Row("a", 4, "", 2.0, 2),
                  Row("c", 2, null, 5.0, 1)
                )
              )

              checkAnswer(df.withColumn("rn", rank().over(window3)).where(condition),
                Seq(
                  Row("a", 4, "", 2.0, 2),
                  Row("a", 4, "", 2.0, 2),
                  Row("c", 2, null, 5.0, 1)
                )
              )

              checkAnswer(df.withColumn("rn", dense_rank().over(window3)).where(condition),
                Seq(
                  Row("a", 4, "", 2.0, 2),
                  Row("a", 4, "", 2.0, 2),
                  Row("c", 2, null, 5.0, 1)
                )
              )
            }

            val condition = $"rn" === 2 && $"value2" > 0.5
            checkAnswer(df.withColumn("rn", row_number().over(window)).where(condition),
              Seq(
                Row("a", 4, "", 2.0, 2),
                Row("b", 1, "n", Double.PositiveInfinity, 2),
                Row("d", 1, "1", 2.0, 2)
              )
            )

            checkAnswer(df.withColumn("rn", rank().over(window)).where(condition),
              Seq(
                Row("b", 1, "n", Double.PositiveInfinity, 2)
              )
            )

            checkAnswer(df.withColumn("rn", dense_rank().over(window)).where(condition),
              Seq(
                Row("a", 0, "c", 1.0, 2),
                Row("b", 1, "n", Double.PositiveInfinity, 2),
                Row("d", 2, "2", 3.0, 2),
                Row("d", 4, "2", 2.0, 2)
              )
            )

            val multipleRowNumbers = df
              .withColumn("rn", row_number().over(window))
              .withColumn("rn2", row_number().over(window))
              .where(Symbol("rn") < 2 && Symbol("rn2") < 3)
            checkAnswer(multipleRowNumbers,
              Seq(
                Row("a", 4, "", 2.0, 1, 1),
                Row("b", 1, "h", Double.NaN, 1, 1),
                Row("c", 2, null, 5.0, 1, 1),
                Row("d", 0, "1", 1.0, 1, 1)
              )
            )

            val multipleRanks = df
              .withColumn("rn", rank().over(window))
              .withColumn("rn2", rank().over(window))
              .where(Symbol("rn") < 2 && Symbol("rn2") < 3)
            checkAnswer(multipleRanks,
              Seq(
                Row("a", 4, "", 2.0, 1, 1),
                Row("a", 4, "", 2.0, 1, 1),
                Row("b", 1, "h", Double.NaN, 1, 1),
                Row("c", 2, null, 5.0, 1, 1),
                Row("d", 0, "1", 1.0, 1, 1),
                Row("d", 1, "1", 2.0, 1, 1)
              )
            )

            val multipleDenseRanks = df
              .withColumn("rn", dense_rank().over(window))
              .withColumn("rn2", dense_rank().over(window))
              .where(Symbol("rn") < 2 && Symbol("rn2") < 3)
            checkAnswer(multipleDenseRanks,
              Seq(
                Row("a", 4, "", 2.0, 1, 1),
                Row("a", 4, "", 2.0, 1, 1),
                Row("b", 1, "h", Double.NaN, 1, 1),
                Row("c", 2, null, 5.0, 1, 1),
                Row("d", 0, "1", 1.0, 1, 1),
                Row("d", 1, "1", 2.0, 1, 1)
              )
            )

            val multipleWindows = df
              .withColumn("rn2", row_number().over(window2))
              .withColumn("rn", row_number().over(window))
              .where(Symbol("rn") < 2 && Symbol("rn2") < 3)
            checkAnswer(multipleWindows,
              Seq(
                Row("b", 1, "h", Double.NaN, 2, 1),
                Row("c", 2, null, 5.0, 1, 1)
              )
            )
          }
        }
      }
    }
  }

  test("SPARK-42525: collapse two adjacent windows with the same partition/order in subquery") {
    withTempView("t1") {
      Seq((1, 1), (2, 2)).toDF("a", "b").createOrReplaceTempView("t1")
      val df = sql(
        """
          |SELECT a, b, rk, row_number() OVER (PARTITION BY a ORDER BY b) AS rn
          |FROM   (SELECT a, b, rank() OVER (PARTITION BY a ORDER BY b) AS rk
          |        FROM t1) t2
          |""".stripMargin)

      val windows = df.queryExecution.optimizedPlan.collect { case w: LogicalWindow => w }
      assert(windows.size === 1)
    }
  }

  test("SPARK-45543: InferWindowGroupLimit causes bug " +
    "if the other window functions haven't the same window frame as the rank-like functions") {
    val df = Seq(
      (1, "Dave", 1, 2020),
      (2, "Dave", 1, 2021),
      (3, "Dave", 2, 2022),
      (4, "Dave", 3, 2023),
      (5, "Dave", 3, 2024),
      (6, "Mark", 2, 2022),
      (7, "Mark", 3, 2023),
      (8, "Mark", 3, 2024),
      (9, "Amy", 6, 2021),
      (10, "Amy", 5, 2022),
      (11, "Amy", 6, 2023),
      (12, "Amy", 7, 2024),
      (13, "John", 7, 2024)).toDF("id", "name", "score", "year")

    val window = Window.partitionBy($"year").orderBy($"score".desc)
    val window2 = window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val window3 = window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    Seq(-1, 100).foreach { threshold =>
      withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_THRESHOLD.key -> threshold.toString) {
        // The other window functions have the same window frame as the rank-like functions.
        // df2, df3 and df4 can apply InferWindowGroupLimit
        val df2 = df
          .withColumn("rn", row_number().over(window))
          .withColumn("all_scores", collect_list($"score").over(window2))
          .sort($"year")

        checkAnswer(df2.filter("rn=1"), Seq(
          Row(1, "Dave", 1, 2020, 1, Array(1)),
          Row(9, "Amy", 6, 2021, 1, Array(6)),
          Row(10, "Amy", 5, 2022, 1, Array(5)),
          Row(11, "Amy", 6, 2023, 1, Array(6)),
          Row(12, "Amy", 7, 2024, 1, Array(7))
        ))

        val df3 = df
          .withColumn("rank", rank().over(window))
          .withColumn("all_scores", collect_list($"score").over(window2))
          .sort($"year")

        checkAnswer(df3.filter("rank=2"), Seq(
          Row(2, "Dave", 1, 2021, 2, Array(6, 1)),
          Row(3, "Dave", 2, 2022, 2, Array(5, 2)),
          Row(6, "Mark", 2, 2022, 2, Array(5, 2, 2)),
          Row(4, "Dave", 3, 2023, 2, Array(6, 3)),
          Row(7, "Mark", 3, 2023, 2, Array(6, 3, 3))
        ))

        val df4 = df
          .withColumn("rank", dense_rank().over(window))
          .withColumn("all_scores", collect_list($"score").over(window2))
          .sort($"year")

        checkAnswer(df4.filter("rank=2"), Seq(
          Row(2, "Dave", 1, 2021, 2, Array(6, 1)),
          Row(3, "Dave", 2, 2022, 2, Array(5, 2)),
          Row(6, "Mark", 2, 2022, 2, Array(5, 2, 2)),
          Row(4, "Dave", 3, 2023, 2, Array(6, 3)),
          Row(7, "Mark", 3, 2023, 2, Array(6, 3, 3)),
          Row(5, "Dave", 3, 2024, 2, Array(7, 7, 3)),
          Row(8, "Mark", 3, 2024, 2, Array(7, 7, 3, 3))
        ))

        // The other window functions haven't the same window frame as the rank-like functions.
        // df5, df6 and df7 cannot apply InferWindowGroupLimit
        val df5 = df
          .withColumn("rn", row_number().over(window))
          .withColumn("all_scores", collect_list($"score").over(window3))
          .sort($"year")

        checkAnswer(df5.filter("rn=1"), Seq(
          Row(1, "Dave", 1, 2020, 1, Array(1)),
          Row(9, "Amy", 6, 2021, 1, Array(6, 1)),
          Row(10, "Amy", 5, 2022, 1, Array(5, 2, 2)),
          Row(11, "Amy", 6, 2023, 1, Array(6, 3, 3)),
          Row(12, "Amy", 7, 2024, 1, Array(7, 7, 3, 3))
        ))

        val df6 = df
          .withColumn("rank", rank().over(window))
          .withColumn("all_scores", collect_list($"score").over(window3))
          .sort($"year")

        checkAnswer(df6.filter("rank=2"), Seq(
          Row(2, "Dave", 1, 2021, 2, Array(6, 1)),
          Row(3, "Dave", 2, 2022, 2, Array(5, 2, 2)),
          Row(6, "Mark", 2, 2022, 2, Array(5, 2, 2)),
          Row(4, "Dave", 3, 2023, 2, Array(6, 3, 3)),
          Row(7, "Mark", 3, 2023, 2, Array(6, 3, 3))
        ))

        val df7 = df
          .withColumn("rank", dense_rank().over(window))
          .withColumn("all_scores", collect_list($"score").over(window3))
          .sort($"year")

        checkAnswer(df7.filter("rank=2"), Seq(
          Row(2, "Dave", 1, 2021, 2, Array(6, 1)),
          Row(3, "Dave", 2, 2022, 2, Array(5, 2, 2)),
          Row(6, "Mark", 2, 2022, 2, Array(5, 2, 2)),
          Row(4, "Dave", 3, 2023, 2, Array(6, 3, 3)),
          Row(7, "Mark", 3, 2023, 2, Array(6, 3, 3)),
          Row(5, "Dave", 3, 2024, 2, Array(7, 7, 3, 3)),
          Row(8, "Mark", 3, 2024, 2, Array(7, 7, 3, 3))
        ))
      }
    }
  }

  test("SPARK-46941: Can't insert window group limit node for top-k computation if contains " +
    "SizeBasedWindowFunction") {
    val df = Seq(
      (1, "Dave", 1, 2020),
      (2, "Mark", 2, 2020),
      (3, "Amy", 3, 2020),
      (4, "Dave", 1, 2021),
      (5, "Mark", 2, 2021),
      (6, "Amy", 3, 2021),
      (7, "John", 4, 2021)).toDF("id", "name", "score", "year")

    val window = Window.partitionBy($"year").orderBy($"score".desc)

    Seq(-1, 100).foreach { threshold =>
      withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_THRESHOLD.key -> threshold.toString) {
        val df2 = df
          .withColumn("rank", rank().over(window))
          .withColumn("percent_rank", percent_rank().over(window))
          .sort($"year")
        checkAnswer(df2.filter("rank=2"), Seq(
          Row(2, "Mark", 2, 2020, 2, 0.5),
          Row(6, "Amy", 3, 2021, 2, 0.3333333333333333)
        ))
      }
    }
  }
}
