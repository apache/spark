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
import java.time.{Duration, LocalDateTime, LocalTime, Period}

import org.apache.spark.SparkArithmeticException
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression,
  ApproximatePercentile, Final, Partial}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.PercentileDigest
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.ReusedSubqueryExec
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, TimeType}
import org.apache.spark.tags.SlowSQLTest

/**
 * End-to-end tests for approximate percentile aggregate function.
 */
@SlowSQLTest
class ApproximatePercentileQuerySuite extends SharedSparkSession {
  import testImplicits._

  private val table = "percentile_approx"

  test("compatible scalar percentiles share one physical percentile digest") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      val query = spark.sql(
        s"""SELECT
           |  percentile_approx(col, 0.5, 10000) AS p50,
           |  percentile_approx(col, 0.9, 10000) AS p90,
           |  percentile_approx(col, 0.95, 10000) AS p95
           |FROM $table
           |""".stripMargin)

      checkAnswer(query, Row(500, 900, 950))

      val aggregates = query.queryExecution.sparkPlan.collect {
        case aggregate: ObjectHashAggregateExec => aggregate
      }
      assert(aggregates.nonEmpty)
      val aggregateModes = aggregates.flatMap { aggregate =>
        aggregate.aggregateExpressions.collect {
          case expression @ AggregateExpression(_: ApproximatePercentile, _, _, _, _) =>
            expression.mode
        }
      }.toSet
      assert(aggregateModes == Set(Partial, Final))
      aggregates.foreach { aggregate =>
        val percentiles = aggregate.aggregateExpressions.collect {
          case expression @ AggregateExpression(_: ApproximatePercentile, _, _, _, _) =>
            expression
        }
        assert(percentiles.size == 1)
      }
    }
  }

  test("do not fuse duplicate percentages already shared by physical planning") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      val query = spark.sql(
        s"""SELECT
           |  percentile_approx(col, 0.5D),
           |  percentile_approx(col, 0.25D + 0.25D)
           |FROM $table
           |""".stripMargin)

      checkAnswer(query, Row(500, 500))
      val percentiles = query.queryExecution.sparkPlan.collect {
        case aggregate: ObjectHashAggregateExec =>
          aggregate.aggregateExpressions.collect {
            case AggregateExpression(
                percentile: ApproximatePercentile, _, _, _, _) => percentile
          }
      }.flatten
      assert(percentiles.nonEmpty)
      assert(percentiles.forall(_.percentageExpression.dataType == DoubleType))
    }
  }

  test("do not combine scalar percentiles with differently associated floating-point inputs") {
    checkAnswer(
      spark.sql(
        """SELECT
          |  percentile_approx((a + b) + c, 0.5D),
          |  percentile_approx(a + (b + c), 0.9D)
          |FROM VALUES (
          |  CAST(10000000000000000 AS DOUBLE),
          |  CAST(-10000000000000000 AS DOUBLE),
          |  CAST(1 AS DOUBLE)
          |) AS t(a, b, c)
          |""".stripMargin),
      Row(1.0d, 0.0d))
  }

  test("do not combine scalar percentiles with differently associated filters") {
    checkAnswer(
      spark.sql(
        """SELECT
          |  percentile_approx(v, 0.5D)
          |    FILTER (WHERE (a + b) + c = 1D),
          |  percentile_approx(v, 0.9D)
          |    FILTER (WHERE a + (b + c) = 1D)
          |FROM VALUES (
          |  7,
          |  CAST(10000000000000000 AS DOUBLE),
          |  CAST(-10000000000000000 AS DOUBLE),
          |  CAST(1 AS DOUBLE)
          |) AS t(v, a, b, c)
          |""".stripMargin),
      Row(7, null))
  }

  test("do not combine scalar percentiles when reassociation suppresses ANSI overflow") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val exception = intercept[SparkArithmeticException] {
        spark.sql(
          """SELECT
            |  percentile_approx(a + (b + c), 0.5D),
            |  percentile_approx((a + b) + c, 0.9D)
            |FROM VALUES (
            |  CAST(2147483647 AS INT),
            |  CAST(1 AS INT),
            |  CAST(-1 AS INT)
            |) AS t(a, b, c)
            |""".stripMargin).collect()
      }

      assert(exception.getCondition == "ARITHMETIC_OVERFLOW")
    }
  }

  test("do not fuse input groups when physical deduplication suppresses ANSI overflow") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val exception = intercept[SparkArithmeticException] {
        spark.sql(
          """SELECT
            |  percentile_approx(a + (b + c), 0.5D),
            |  percentile_approx((a + b) + c, 0.5D),
            |  percentile_approx((a + b) + c, 0.9D),
            |  percentile_approx(a + (b + c), 0.9D)
            |FROM VALUES (
            |  CAST(2147483647 AS INT),
            |  CAST(1 AS INT),
            |  CAST(-1 AS INT)
            |) AS t(a, b, c)
            |""".stripMargin).collect()
      }

      assert(exception.getCondition == "ARITHMETIC_OVERFLOW")
    }
  }

  test("do not fuse inputs that collide with a scalar percentile") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkAnswer(
        spark.sql(
          """SELECT
            |  percentile_approx(a + (b + c), 0.5D),
            |  percentile_approx((a + b) + c, 0.5D),
            |  percentile_approx(a + (b + c), 0.9D)
            |FROM VALUES (
            |  CAST(2147483647 AS INT),
            |  CAST(1 AS INT),
            |  CAST(-1 AS INT)
            |) AS t(a, b, c)
            |""".stripMargin),
        Row(Int.MaxValue, Int.MaxValue, Int.MaxValue))
    }
  }

  test("do not fuse inputs that collide with an existing array percentile") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val exception = intercept[SparkArithmeticException] {
        spark.sql(
          """SELECT
            |  percentile_approx(a + (b + c), 0.5D),
            |  percentile_approx((a + b) + c, array(0.5D, 0.9D)),
            |  percentile_approx(a + (b + c), 0.9D)
            |FROM VALUES (
            |  CAST(2147483647 AS INT),
            |  CAST(1 AS INT),
            |  CAST(-1 AS INT)
            |) AS t(a, b, c)
            |""".stripMargin).collect()
      }

      assert(exception.getCondition == "ARITHMETIC_OVERFLOW")
    }
  }

  test("do not fuse floating-point inputs that collide during physical deduplication") {
    checkAnswer(
      spark.sql(
        """SELECT
          |  percentile_approx(a + (b + c), 0.5D),
          |  percentile_approx((a + b) + c, 0.5D),
          |  percentile_approx((a + b) + c, 0.9D),
          |  percentile_approx(a + (b + c), 0.9D)
          |FROM VALUES (
          |  CAST(10000000000000000 AS DOUBLE),
          |  CAST(-10000000000000000 AS DOUBLE),
          |  CAST(1 AS DOUBLE)
          |) AS t(a, b, c)
          |""".stripMargin),
      Row(0.0d, 0.0d, 1.0d, 1.0d))
  }

  test("do not fuse floating-point inputs that collide with an existing array percentile") {
    checkAnswer(
      spark.sql(
        """SELECT
          |  percentile_approx(a + (b + c), array(0.5D, 0.9D)),
          |  percentile_approx((a + b) + c, 0.5D),
          |  percentile_approx((a + b) + c, 0.9D)
          |FROM VALUES (
          |  CAST(10000000000000000 AS DOUBLE),
          |  CAST(-10000000000000000 AS DOUBLE),
          |  CAST(1 AS DOUBLE)
          |) AS t(a, b, c)
          |""".stripMargin),
      Row(Seq(0.0d, 0.0d), 1.0d, 1.0d))
  }

  test("do not fuse filter groups when physical deduplication suppresses ANSI overflow") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val exception = intercept[SparkArithmeticException] {
        spark.sql(
          """SELECT
            |  percentile_approx(v, 0.5D) FILTER (WHERE a + (b + c) > 0),
            |  percentile_approx(v, 0.5D) FILTER (WHERE (a + b) + c > 0),
            |  percentile_approx(v, 0.9D) FILTER (WHERE (a + b) + c > 0),
            |  percentile_approx(v, 0.9D) FILTER (WHERE a + (b + c) > 0)
            |FROM VALUES (
            |  7,
            |  CAST(2147483647 AS INT),
            |  CAST(1 AS INT),
            |  CAST(-1 AS INT)
            |) AS t(v, a, b, c)
            |""".stripMargin).collect()
      }

      assert(exception.getCondition == "ARITHMETIC_OVERFLOW")
    }
  }

  test("do not fuse filters that collide with an existing array percentile") {
    checkAnswer(
      spark.sql(
        """SELECT
          |  percentile_approx(v, array(0.5D, 0.9D))
          |    FILTER (WHERE a + (b + c) = 0D),
          |  percentile_approx(v, 0.5D)
          |    FILTER (WHERE (a + b) + c = 0D),
          |  percentile_approx(v, 0.9D)
          |    FILTER (WHERE (a + b) + c = 0D)
          |FROM VALUES (
          |  7,
          |  CAST(10000000000000000 AS DOUBLE),
          |  CAST(-10000000000000000 AS DOUBLE),
          |  CAST(1 AS DOUBLE)
          |) AS t(v, a, b, c)
          |""".stripMargin),
      Row(Seq(7, 7), null, null))
  }

  test("do not fuse canonically equivalent but differently evaluated accuracies") {
    val constantFoldingRule = "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
    val fusionRule = "org.apache.spark.sql.catalyst.optimizer.CombineApproximatePercentiles"
    val existingRules = spark.sessionState.conf.optimizerExcludedRules.toSeq
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)
    val withoutConstantFolding = (existingRules :+ constantFoldingRule).distinct
    val sql =
      """SELECT
        |  percentile_approx(
        |    v, 0.5D, CAST((1e16D + -1e16D) + 3D AS INT)),
        |  percentile_approx(
        |    v, 0.5D, CAST(1e16D + (-1e16D + 3D) AS INT)),
        |  percentile_approx(
        |    v, 0.9D, CAST(1e16D + (-1e16D + 3D) AS INT)),
        |  percentile_approx(
        |    v, 0.9D, CAST((1e16D + -1e16D) + 3D AS INT))
        |FROM range(100) AS t(v)
        |""".stripMargin

    def assertTwoDigests(query: DataFrame): Unit = {
      val counts = query.queryExecution.sparkPlan.collect {
        case aggregate: ObjectHashAggregateExec =>
          aggregate.aggregateExpressions.count(
            _.aggregateFunction.isInstanceOf[ApproximatePercentile])
      }
      assert(counts.nonEmpty)
      assert(counts.forall(_ == 2))
    }

    val baseline = withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (withoutConstantFolding :+ fusionRule).distinct.mkString(",")) {
      val query = spark.sql(sql)
      assertTwoDigests(query)
      query.collect().toSeq
    }

    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        withoutConstantFolding.filterNot(_ == fusionRule).mkString(",")) {
      val query = spark.sql(sql)
      checkAnswer(query, baseline)
      assertTwoDigests(query)
    }
  }

  test("do not fuse percentages that collide with an existing array percentile") {
    val constantFoldingRule = "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
    val fusionRule = "org.apache.spark.sql.catalyst.optimizer.CombineApproximatePercentiles"
    val existingRules = spark.sessionState.conf.optimizerExcludedRules.toSeq
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)
    val withoutConstantFolding = (existingRules :+ constantFoldingRule).distinct
    val sql =
      """SELECT
        |  percentile_approx(
        |    id, array((1e16D + -1e16D) + 0.5D, 0.9D)),
        |  percentile_approx(
        |    id, 1e16D + (-1e16D + 0.5D)),
        |  percentile_approx(id, 0.9D)
        |FROM range(100)
        |""".stripMargin

    def assertThreeDigests(query: DataFrame): Unit = {
      val counts = query.queryExecution.sparkPlan.collect {
        case aggregate: ObjectHashAggregateExec =>
          aggregate.aggregateExpressions.count(
            _.aggregateFunction.isInstanceOf[ApproximatePercentile])
      }
      assert(counts.nonEmpty)
      assert(counts.forall(_ == 3))
    }

    val baseline = withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (withoutConstantFolding :+ fusionRule).distinct.mkString(",")) {
      val query = spark.sql(sql)
      assertThreeDigests(query)
      query.collect().toSeq
    }

    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        withoutConstantFolding.filterNot(_ == fusionRule).mkString(",")) {
      val query = spark.sql(sql)
      checkAnswer(query, baseline)
      assertThreeDigests(query)
    }
  }

  test("do not fuse canonically equivalent but differently evaluated scalar percentages") {
    val constantFoldingRule = "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
    val fusionRule = "org.apache.spark.sql.catalyst.optimizer.CombineApproximatePercentiles"
    val existingRules = spark.sessionState.conf.optimizerExcludedRules.toSeq
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)
    val withoutConstantFolding = (existingRules :+ constantFoldingRule).distinct
    val sql =
      """SELECT
        |  percentile_approx(id, (1e16D + -1e16D) + 0.5D),
        |  percentile_approx(id, 1e16D + (-1e16D + 0.5D)),
        |  percentile_approx(id, 0.9D)
        |FROM range(100)
        |""".stripMargin

    def assertTwoDigests(query: DataFrame): Unit = {
      val counts = query.queryExecution.sparkPlan.collect {
        case aggregate: ObjectHashAggregateExec =>
          aggregate.aggregateExpressions.count(
            _.aggregateFunction.isInstanceOf[ApproximatePercentile])
      }
      assert(counts.nonEmpty)
      assert(counts.forall(_ == 2))
    }

    val baseline = withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (withoutConstantFolding :+ fusionRule).distinct.mkString(",")) {
      val query = spark.sql(sql)
      assertTwoDigests(query)
      query.collect().toSeq
    }

    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        withoutConstantFolding.filterNot(_ == fusionRule).mkString(",")) {
      val query = spark.sql(sql)
      checkAnswer(query, baseline)
      assertTwoDigests(query)
    }
  }

  test("do not fuse with an existing array that collides after distinct removal") {
    checkAnswer(
      spark.sql(
        """SELECT
          |  percentile_approx(
          |    DISTINCT a + (b + c), array(0.5D, 0.9D)),
          |  percentile_approx(DISTINCT a + (b + c), 0.1D),
          |  percentile_approx((a + b) + c, 0.5D),
          |  percentile_approx((a + b) + c, 0.9D)
          |FROM VALUES (
          |  CAST(10000000000000000 AS DOUBLE),
          |  CAST(-10000000000000000 AS DOUBLE),
          |  CAST(1 AS DOUBLE)
          |) AS t(a, b, c)
          |""".stripMargin),
      Row(Seq(0.0d, 0.0d), 0.0d, 1.0d, 1.0d))
  }

  test("preserve percentile inputs across scalar subquery reuse") {
    val query =
      """SELECT
        |  (SELECT named_struct(
        |    'p50', percentile_approx((a + b) + c, 0.5D),
        |    'p90', percentile_approx((a + b) + c, 0.9D))
        |   FROM VALUES (
        |     CAST(10000000000000000 AS DOUBLE),
        |     CAST(-10000000000000000 AS DOUBLE),
        |     CAST(1 AS DOUBLE)
        |   ) AS t(a, b, c)),
        |  (SELECT named_struct(
        |    'p50', get(percentile_approx(
        |      a + (b + c), array(0.5D, 0.9D)), 0),
        |    'p90', get(percentile_approx(
        |      a + (b + c), array(0.5D, 0.9D)), 1))
        |   FROM VALUES (
        |     CAST(10000000000000000 AS DOUBLE),
        |     CAST(-10000000000000000 AS DOUBLE),
        |     CAST(1 AS DOUBLE)
        |   ) AS t(a, b, c))
        |""".stripMargin

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      val result = spark.sql(query)
      checkAnswer(result, Row(Row(1.0d, 1.0d), Row(0.0d, 0.0d)))
    }
  }

  test("preserve grouping expressions across exchange reuse") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {
      checkAnswer(
        spark.sql(
          """SELECT
            |  (a + b) + c,
            |  array(
            |    percentile_approx(1D, 0.5D),
            |    percentile_approx(1D, 0.9D))
            |FROM VALUES (
            |  CAST(10000000000000000 AS DOUBLE),
            |  CAST(-10000000000000000 AS DOUBLE),
            |  CAST(1 AS DOUBLE)
            |) AS t1(a, b, c)
            |GROUP BY (a + b) + c
            |UNION ALL
            |SELECT
            |  a + (b + c),
            |  percentile_approx(1D, array(0.5D, 0.9D))
            |FROM VALUES (
            |  CAST(10000000000000000 AS DOUBLE),
            |  CAST(-10000000000000000 AS DOUBLE),
            |  CAST(1 AS DOUBLE)
            |) AS t2(a, b, c)
            |GROUP BY a + (b + c)
            |""".stripMargin),
        Seq(
          Row(1.0d, Seq(1.0d, 1.0d)),
          Row(0.0d, Seq(1.0d, 1.0d))))
    }
  }

  test("preserve original percentile parameters across exchange reuse") {
    val constantFoldingRule = "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"
    val fusionRule = "org.apache.spark.sql.catalyst.optimizer.CombineApproximatePercentiles"
    val existingRules = spark.sessionState.conf.optimizerExcludedRules.toSeq
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)
    val excludedRules = (existingRules :+ constantFoldingRule)
      .filterNot(_ == fusionRule)
      .distinct
      .mkString(",")
    val parameterPairs = Seq(
      ("(1e16D + -1e16D) + 0.5D", "100"),
      ("0.5D", "CAST((1e16D + -1e16D) + 100D AS INT)"))

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRules) {
      parameterPairs.foreach { case (firstPercentage, secondAccuracy) =>
        val query = spark.sql(
          s"""SELECT
             |  (a + b) + c,
             |  array(
             |    percentile_approx(1D, $firstPercentage, 100),
             |    percentile_approx(1D, 0.9D, $secondAccuracy))
             |FROM VALUES (
             |  CAST(10000000000000000 AS DOUBLE),
             |  CAST(-10000000000000000 AS DOUBLE),
             |  CAST(1 AS DOUBLE)
             |) AS t1(a, b, c)
             |GROUP BY (a + b) + c
             |UNION ALL
             |SELECT
             |  a + (b + c),
             |  array(
             |    percentile_approx(1D, 0.5D, 100),
             |    percentile_approx(1D, 0.9D, 100))
             |FROM VALUES (
             |  CAST(10000000000000000 AS DOUBLE),
             |  CAST(-10000000000000000 AS DOUBLE),
             |  CAST(1 AS DOUBLE)
             |) AS t2(a, b, c)
             |GROUP BY a + (b + c)
             |""".stripMargin)

        checkAnswer(
          query,
          Seq(
            Row(1.0d, Seq(1.0d, 1.0d)),
            Row(0.0d, Seq(1.0d, 1.0d))))
        assert(query.queryExecution.executedPlan.collect {
          case _: ReusedExchangeExec => true
        }.isEmpty)
      }
    }
  }

  test("identical fused percentiles remain reusable") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {
      val subquery =
        """(SELECT array(
          |  percentile_approx(v, 0D),
          |  percentile_approx(v, 1D))
          | FROM VALUES (1), (2), (3) AS t(v))
          |""".stripMargin
      val query = spark.sql(s"SELECT $subquery, $subquery")

      checkAnswer(query, Row(Seq(1, 3), Seq(1, 3)))
      assert(query.queryExecution.executedPlan.collectWithSubqueries {
        case _: ReusedSubqueryExec => true
      }.nonEmpty)

      val branch =
        """SELECT
          |  id % 2 AS k,
          |  array(
          |    percentile_approx(id, 0D),
          |    percentile_approx(id, 1D)) AS percentiles
          |FROM range(10)
          |GROUP BY id % 2
          |""".stripMargin
      val union = spark.sql(s"$branch UNION ALL $branch")

      checkAnswer(
        union,
        Seq(
          Row(0L, Seq(0L, 8L)),
          Row(1L, Seq(1L, 9L)),
          Row(0L, Seq(0L, 8L)),
          Row(1L, Seq(1L, 9L))))
      assert(union.queryExecution.executedPlan.collect {
        case _: ReusedExchangeExec => true
      }.nonEmpty)
    }
  }

  test("fused distinct percentiles keep a single distinct group") {
    val query = spark.sql(
      """SELECT
        |  count(DISTINCT id),
        |  percentile_approx(DISTINCT id, 0.5D),
        |  percentile_approx(DISTINCT id, 0.9D)
        |FROM range(10)
        |""".stripMargin)

    checkAnswer(query, Row(10L, 4L, 8L))
    assert(query.queryExecution.optimizedPlan.collect {
      case _: Expand => true
    }.isEmpty)
  }

  test("combined distinct literal percentiles preserve results") {
    withTempView(table) {
      Seq.empty[Int].toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(DISTINCT 1D, 0.5D),
             |  percentile_approx(DISTINCT 1D, 0.9D)
             |FROM $table
             |""".stripMargin),
        Row(null, null))

      Seq(1, 2).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(DISTINCT 1D, 0.5D),
             |  percentile_approx(DISTINCT 1D, 0.9D)
             |FROM $table
             |""".stripMargin),
        Row(1.0d, 1.0d))
    }
  }

  test("combined scalar percentiles preserve empty-input nulls with ANSI enabled") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withTempView(table) {
        Seq.empty[Int].toDF("col").createOrReplaceTempView(table)
        checkAnswer(
          spark.sql(
            s"""SELECT
               |  percentile_approx(col, 0.5),
               |  percentile_approx(col, 0.9),
               |  percentile_approx(col, 0.95)
               |FROM $table
               |""".stripMargin),
          Row(null, null, null))
      }
    }
  }

  test("combined scalar percentiles preserve all-null groups") {
    withTempView(table) {
      Seq((0, null: Integer), (0, null: Integer), (1, Integer.valueOf(10)))
        .toDF("group_key", "col")
        .createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  group_key,
             |  percentile_approx(col, 0.5),
             |  percentile_approx(col, 0.9)
             |FROM $table
             |GROUP BY group_key
             |""".stripMargin),
        Seq(Row(0, null, null), Row(1, 10, 10)))
    }
  }

  test("combined scalar percentiles preserve grouping sets") {
    withTempView(table) {
      Seq((0, 1), (0, 2), (1, 3), (1, 4))
        .toDF("group_key", "col")
        .createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  group_key,
             |  percentile_approx(col, 0.5),
             |  percentile_approx(col, 1.0)
             |FROM $table
             |GROUP BY GROUPING SETS ((group_key), ())
             |""".stripMargin),
        Seq(Row(0, 1, 2), Row(1, 3, 4), Row(null, 2, 4)))
    }
  }

  test("combined scalar percentiles preserve decimal and temporal result types") {
    withTempView(table) {
      (1 to 4).map { value =>
        (
          new java.math.BigDecimal(value),
          DateTimeUtils.toJavaDate(value),
          DateTimeUtils.toJavaTimestamp(value.toLong),
          DateTimeUtils.microsToLocalDateTime(value.toLong),
          Period.ofMonths(value),
          Duration.ofSeconds(value.toLong))
      }.toDF("decimal_col", "date_col", "timestamp_col", "timestamp_ntz_col",
        "year_month_col", "day_time_col")
        .createOrReplaceTempView(table)

      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(decimal_col, 0.5),
             |  percentile_approx(decimal_col, 1.0),
             |  percentile_approx(date_col, 0.5),
             |  percentile_approx(date_col, 1.0),
             |  percentile_approx(timestamp_col, 0.5),
             |  percentile_approx(timestamp_col, 1.0),
             |  percentile_approx(timestamp_ntz_col, 0.5),
             |  percentile_approx(timestamp_ntz_col, 1.0),
             |  percentile_approx(year_month_col, 0.5),
             |  percentile_approx(year_month_col, 1.0),
             |  percentile_approx(day_time_col, 0.5),
             |  percentile_approx(day_time_col, 1.0)
             |FROM $table
             |""".stripMargin),
        Row(
          new java.math.BigDecimal("2.000000000000000000"),
          new java.math.BigDecimal("4.000000000000000000"),
          DateTimeUtils.toJavaDate(2),
          DateTimeUtils.toJavaDate(4),
          DateTimeUtils.toJavaTimestamp(2L),
          DateTimeUtils.toJavaTimestamp(4L),
          DateTimeUtils.microsToLocalDateTime(2L),
          DateTimeUtils.microsToLocalDateTime(4L),
          Period.ofMonths(2).normalized(),
          Period.ofMonths(4).normalized(),
          Duration.ofSeconds(2L),
          Duration.ofSeconds(4L)))
    }
  }

  test("combined scalar percentiles preserve TIME result types") {
    withTempView(table) {
      spark.sql(
        s"""SELECT * FROM VALUES
           |  (TIME '01:00:00'), (TIME '02:00:00'), (TIME '03:00:00'),
           |  (TIME '04:00:00'), (TIME '05:00:00') AS tab(c)
           |""".stripMargin).createOrReplaceTempView(table)
      val query = spark.sql(
        s"""SELECT
           |  percentile_approx(c, 0.2D) AS p20,
           |  percentile_approx(c, 0.5D) AS p50,
           |  percentile_approx(c, 0.8D) AS p80
           |FROM $table
           |""".stripMargin)

      assert(query.schema.fields.map(_.dataType).toSeq ==
        Seq(TimeType(), TimeType(), TimeType()))
      checkAnswer(query, Row(LocalTime.of(1, 0), LocalTime.of(3, 0), LocalTime.of(4, 0)))

      val aggregates = query.queryExecution.sparkPlan.collect {
        case aggregate: ObjectHashAggregateExec => aggregate
      }
      assert(aggregates.nonEmpty)
      aggregates.foreach { aggregate =>
        assert(aggregate.aggregateExpressions.count {
          case AggregateExpression(_: ApproximatePercentile, _, _, _, _) => true
          case _ => false
        } == 1)
      }
    }
  }

  test("combine scalar percentiles without changing an existing array percentile") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(col, array(0.25, 0.75)),
             |  percentile_approx(col, 0.5),
             |  percentile_approx(col, 0.9)
             |FROM $table
             |""".stripMargin),
        Row(Seq(250, 750), 500, 900))
    }
  }

  test("combined scalar percentiles preserve filtered and distinct results") {
    withTempView(table) {
      Seq(1, 1, 2, 3, 4, 5).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(col, 0.5) FILTER (WHERE col > 1),
             |  percentile_approx(col, 1.0) FILTER (WHERE col > 1),
             |  percentile_approx(DISTINCT col, 0.5),
             |  percentile_approx(DISTINCT col, 1.0)
             |FROM $table
             |""".stripMargin),
        Row(3, 5, 3, 5))
    }
  }

  test("combined scalar percentiles preserve non-monotonic and repeated percentages") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(col, 0.9),
             |  percentile_approx(col, 0.5),
             |  percentile_approx(col, 0.9)
             |FROM $table
             |""".stripMargin),
        Row(900, 500, 900))
    }
  }

  test("combine approximate percentile function aliases") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(col, 0.5),
             |  approx_percentile(col, 0.9),
             |  percentile_approx(col, 0.95)
             |FROM $table
             |""".stripMargin),
        Row(500, 900, 950))
    }
  }

  test("combine scalar percentiles with constant-folded equivalent accuracies") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      val query = spark.sql(
        s"""SELECT
           |  percentile_approx(col, 0.5, 10000),
           |  percentile_approx(col, 0.9, 1000 * 10),
           |  percentile_approx(col, 0.95, 5000 + 5000)
           |FROM $table
           |""".stripMargin)

      checkAnswer(query, Row(500, 900, 950))

      val aggregates = query.queryExecution.sparkPlan.collect {
        case aggregate: ObjectHashAggregateExec => aggregate
      }
      assert(aggregates.nonEmpty)
      aggregates.foreach { aggregate =>
        assert(aggregate.aggregateExpressions.count {
          case AggregateExpression(_: ApproximatePercentile, _, _, _, _) => true
          case _ => false
        } == 1)
      }
    }
  }

  test("do not combine scalar percentiles with differently evaluated accuracies") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      checkAnswer(
        spark.sql(
          """SELECT
            |  percentile_approx(
            |    id,
            |    0.5D,
            |    CAST((1e16D + -1e16D) + 3D AS INT)
            |  ),
            |  percentile_approx(
            |    id,
            |    0.7D,
            |    CAST(1e16D + (-1e16D + 3D) AS INT)
            |  )
            |FROM range(0, 3, 1, 1)
            |""".stripMargin),
        Row(0L, 1L))
    }
  }

  test("percentile_approx, single percentile value") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""
             |SELECT
             |  percentile_approx(col, 0.25),
             |  percentile_approx(col, 0.5),
             |  percentile_approx(col, 0.75d),
             |  percentile_approx(col, 0.0),
             |  percentile_approx(col, 1.0),
             |  percentile_approx(col, 0),
             |  percentile_approx(col, 1)
             |FROM $table
           """.stripMargin),
        Row(250D, 500D, 750D, 1D, 1000D, 1D, 1000D)
      )
    }
  }

  test("percentile_approx, the first element satisfies small percentages") {
    withTempView(table) {
      (1 to 10).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""
             |SELECT
             |  percentile_approx(col, array(0.01, 0.1, 0.11))
             |FROM $table
           """.stripMargin),
        Row(Seq(1, 1, 2))
      )
    }
  }

  test("percentile_approx, array of percentile value") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(col, array(0.25, 0.5, 0.75D)),
             |  count(col),
             |  percentile_approx(col, array(0.0, 1.0)),
             |  sum(col)
             |FROM $table
           """.stripMargin),
        Row(Seq(250D, 500D, 750D), 1000, Seq(1D, 1000D), 500500)
      )
    }
  }

  test("percentile_approx, different column types") {
    withTempView(table) {
      val intSeq = 1 to 1000
      val data: Seq[(java.math.BigDecimal, Date, Timestamp, LocalDateTime)] = intSeq.map { i =>
        (new java.math.BigDecimal(i), DateTimeUtils.toJavaDate(i),
          DateTimeUtils.toJavaTimestamp(i), DateTimeUtils.microsToLocalDateTime(i))
      }
      data.toDF("cdecimal", "cdate", "ctimestamp", "ctimestampntz").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(cdecimal, array(0.25, 0.5, 0.75D)),
             |  percentile_approx(cdate, array(0.25, 0.5, 0.75D)),
             |  percentile_approx(ctimestamp, array(0.25, 0.5, 0.75D)),
             |  percentile_approx(ctimestampntz, array(0.25, 0.5, 0.75D))
             |FROM $table
           """.stripMargin),
        Row(
          Seq("250.000000000000000000", "500.000000000000000000", "750.000000000000000000")
              .map(i => new java.math.BigDecimal(i)),
          Seq(250, 500, 750).map(DateTimeUtils.toJavaDate),
          Seq(250, 500, 750).map(i => DateTimeUtils.toJavaTimestamp(i.toLong)),
          Seq(250, 500, 750).map(i => DateTimeUtils.microsToLocalDateTime(i.toLong)))
      )
    }
  }

  test("SPARK-57557: percentile_approx supports TIME type") {
    withTempView(table) {
      spark.sql(
        s"""SELECT * FROM VALUES
           |  (TIME '01:00:00'), (TIME '02:00:00'), (TIME '03:00:00'),
           |  (TIME '04:00:00'), (TIME '05:00:00') AS tab(c)
         """.stripMargin).createOrReplaceTempView(table)
      val scalarDf = spark.sql(s"SELECT percentile_approx(c, 0.5) FROM $table")
      // The result type is TIME, mirroring the input column type.
      assert(scalarDf.schema.head.dataType === TimeType())
      checkAnswer(scalarDf, Row(LocalTime.of(3, 0)))
      checkAnswer(
        spark.sql(s"SELECT percentile_approx(c, array(0.2, 0.5, 0.8D)) FROM $table"),
        Row(Seq(LocalTime.of(1, 0), LocalTime.of(3, 0), LocalTime.of(4, 0))))
    }
  }

  test("percentile_approx, multiple records with the minimum value in a partition") {
    withTempView(table) {
      spark.sparkContext.makeRDD(Seq(1, 1, 2, 1, 1, 3, 1, 1, 4, 1, 1, 5), 4).toDF("col")
        .createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT percentile_approx(col, array(0.5)) FROM $table"),
        Row(Seq(1.0D))
      )
    }
  }

  test("percentile_approx, with different accuracies") {

    withTempView(table) {
      val tableCount = 1000
      (1 to tableCount).toDF("col").createOrReplaceTempView(table)

      // With different accuracies
      val accuracies = Array(1, 10, 100, 1000, 10000)
      val expectedPercentiles = Array(100D, 200D, 250D, 314D, 777D)
      for (accuracy <- accuracies) {
        for (expectedPercentile <- expectedPercentiles) {
          val df = spark.sql(
            s"""SELECT
               | percentile_approx(col, $expectedPercentile/$tableCount, $accuracy)
               |FROM $table
             """.stripMargin)
          val approximatePercentile = df.collect().head.getInt(0)
          val error = Math.abs(approximatePercentile - expectedPercentile)
          assert(error <= math.floor(tableCount.toDouble / accuracy.toDouble))
        }
      }
    }
  }

  test("percentile_approx, supports constant folding for parameter accuracy and percentages") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT percentile_approx(col, array(0.25 + 0.25D), 200 + 800) FROM $table"),
        Row(Seq(500))
      )
    }
  }

  test("percentile_approx(), aggregation on empty input table, no group by") {
    withTempView(table) {
      Seq.empty[Int].toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT sum(col), percentile_approx(col, 0.5) FROM $table"),
        Row(null, null)
      )
    }
  }

  test("percentile_approx(), aggregation on empty input table, with group by") {
    withTempView(table) {
      Seq.empty[Int].toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT sum(col), percentile_approx(col, 0.5) FROM $table GROUP BY col"),
        Seq.empty[Row]
      )
    }
  }

  test("percentile_approx(null), aggregation with group by") {
    withTempView(table) {
      (1 to 1000).map(x => (x % 3, x)).toDF("key", "value").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  key,
             |  percentile_approx(null, 0.5)
             |FROM $table
             |GROUP BY key
           """.stripMargin),
        Seq(
          Row(0, null),
          Row(1, null),
          Row(2, null))
      )
    }
  }

  test("percentile_approx(null), aggregation without group by") {
    withTempView(table) {
      (1 to 1000).map(x => (x % 3, x)).toDF("key", "value").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
              |  percentile_approx(null, 0.5),
              |  sum(null),
              |  percentile_approx(null, 0.5)
              |FROM $table
           """.stripMargin),
         Row(null, null, null)
      )
    }
  }

  test("percentile_approx(col, ...), input rows contains null, with out group by") {
    withTempView(table) {
      (1 to 1000).map(Integer.valueOf(_)).flatMap(Seq(null: Integer, _)).toDF("col")
        .createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
              |  percentile_approx(col, 0.5),
              |  sum(null),
              |  percentile_approx(col, 0.5)
              |FROM $table
           """.stripMargin),
        Row(500D, null, 500D))
    }
  }

  test("percentile_approx(col, ...), input rows contains null, with group by") {
    withTempView(table) {
      val rand = new java.util.Random()
      (1 to 1000)
        .map(Integer.valueOf(_))
        .map(v => (Integer.valueOf(v % 2), v))
        // Add some nulls
        .flatMap(Seq(_, (null: Integer, null: Integer)))
        .toDF("key", "value").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
              |  percentile_approx(value, 0.5),
              |  sum(value),
              |  percentile_approx(value, 0.5)
              |FROM $table
              |GROUP BY key
           """.stripMargin),
        Seq(
          Row(499.0D, 250000, 499.0D),
          Row(500.0D, 250500, 500.0D),
          Row(null, null, null))
      )
    }
  }

  test("percentile_approx(col, ...) works in window function") {
    withTempView(table) {
      val data = (1 to 10).map(v => (v % 2, v))
      data.toDF("key", "value").createOrReplaceTempView(table)

      val query = spark.sql(
        s"""
           |SElECT percentile_approx(value, 0.5)
           |OVER
           |  (PARTITION BY key ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
           |    AS percentile
           |FROM $table
           """.stripMargin)

      val expected = data.groupBy(_._1).toSeq.flatMap { group =>
        val (key, values) = group
        val sortedValues = values.map(_._2).sorted

        var outputRows = Seq.empty[Row]
        var i = 0

        val percentile = new PercentileDigest(1.0 / DEFAULT_PERCENTILE_ACCURACY)
        sortedValues.foreach { value =>
          percentile.add(value)
          outputRows :+= Row(percentile.getPercentiles(Array(0.5D)).head)
        }
        outputRows
      }

      checkAnswer(query, expected)
    }
  }

  test("SPARK-24013: unneeded compress can cause performance issues with sorted input") {
    val buffer = new PercentileDigest(1.0D / ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
    var compressCounts = 0
    (1 to 10000000).foreach { i =>
      buffer.add(i)
      if (buffer.isCompressed) compressCounts += 1
    }
    assert(compressCounts > 0)
    buffer.quantileSummaries
    assert(buffer.isCompressed)
  }

  test("SPARK-32908: maximum target error in percentile_approx") {
    withTempView(table) {
      spark.read
        .schema("col int")
        .csv(testFile("test-data/percentile_approx-input.csv.bz2"))
        .repartition(1)
        .createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(col, 0.77, 1000),
             |  percentile_approx(col, 0.77, 10000),
             |  percentile_approx(col, 0.77, 100000),
             |  percentile_approx(col, 0.77, 1000000)
             |FROM $table""".stripMargin),
        Row(18, 17, 17, 17))
    }
  }

  test("SPARK-37138: Support Ansi Interval type in ApproximatePercentile") {
    withTempView(table) {
      Seq((Period.ofMonths(100), Duration.ofSeconds(100L)),
        (Period.ofMonths(200), Duration.ofSeconds(200L)),
        (Period.ofMonths(300), Duration.ofSeconds(300L)))
        .toDF("col1", "col2").createOrReplaceTempView(table)
        checkAnswer(
          spark.sql(
            s"""SELECT
               |  percentile_approx(col1, 0.5),
               |  SUM(null),
               |  percentile_approx(col2, 0.5)
               |FROM $table
           """.stripMargin),
          Row(Period.ofMonths(200).normalized(), null, Duration.ofSeconds(200L)))
    }
  }

  test("SPARK-45079: NULL arguments of percentile_approx") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(
          """
            |SELECT percentile_approx(col, array(0.5, 0.4, 0.1), NULL)
            |FROM VALUES (0), (1), (2), (10) AS tab(col);
            |""".stripMargin).collect()
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      parameters = Map(
        "exprName" -> "accuracy",
        "sqlExpr" -> "\"percentile_approx(col, array(0.5, 0.4, 0.1), NULL)\""),
      context = ExpectedContext(
        "", "", 8, 57, "percentile_approx(col, array(0.5, 0.4, 0.1), NULL)"))
    checkError(
      exception = intercept[AnalysisException] {
        sql(
          """
            |SELECT percentile_approx(col, NULL, 100)
            |FROM VALUES (0), (1), (2), (10) AS tab(col);
            |""".stripMargin).collect()
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      parameters = Map(
        "exprName" -> "percentage",
        "sqlExpr" -> "\"percentile_approx(col, NULL, 100)\""),
      context = ExpectedContext(
        "", "", 8, 40, "percentile_approx(col, NULL, 100)"))
  }

  test("SPARK-54750: percentile_approx returns NULL for certain decimal values") {
    // Regression test: ROUND(PERCENTILE_APPROX(2150/1000.0, 0.95), 3) should return 2.15
    checkAnswer(
      spark.sql("SELECT ROUND(PERCENTILE_APPROX(2150 / 1000.0, 0.95), 3) as p95"),
      Row(2.15)
    )
    checkAnswer(
      spark.sql("SELECT ROUND(PERCENTILE_APPROX(2151 / 1000.0, 0.95), 3) as p95"),
      Row(2.151)
    )
  }
}
