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
import org.apache.spark.sql.types.{ArrayType, DoubleType, TimeType}
import org.apache.spark.tags.SlowSQLTest

/**
 * End-to-end tests for approximate percentile aggregate function.
 */
@SlowSQLTest
class ApproximatePercentileQuerySuite extends SharedSparkSession {
  import testImplicits._

  private val table = "percentile_approx"
  private val constantFoldingRule =
    "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"

  private def fusionTest(name: String)(body: => Unit): Unit = test(name) {
    withSQLConf(SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED.key -> "true") {
      body
    }
  }

  private def excludedRules: Seq[String] = {
    spark.sessionState.conf.optimizerExcludedRules.toSeq
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)
  }

  private def assertPercentileDigestCount(query: DataFrame, expected: Int): Unit = {
    val counts = query.queryExecution.sparkPlan.collect {
      case aggregate: ObjectHashAggregateExec =>
        aggregate.aggregateExpressions.count(
          _.aggregateFunction.isInstanceOf[ApproximatePercentile])
    }
    assert(counts.nonEmpty)
    assert(counts.forall(_ == expected), counts)
  }

  private def checkMatchesUnfusedBaseline(sql: String, expectedDigests: Int): Unit = {
    val withoutConstantFolding = (excludedRules :+ constantFoldingRule).distinct
    val baseline = withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> withoutConstantFolding.mkString(","),
      SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED.key -> "false") {
      spark.sql(sql).collect().toSeq
    }

    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> withoutConstantFolding.mkString(",")) {
      val query = spark.sql(sql)
      checkAnswer(query, baseline)
      assertPercentileDigestCount(query, expectedDigests)
    }
  }

  test("approximate percentile fusion can be disabled") {
    withSQLConf(SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED.key -> "false") {
      val query = spark.sql(
        "SELECT percentile_approx(id, 0.5D), percentile_approx(id, 0.9D) FROM range(10)")
      checkAnswer(query, Row(4L, 8L))
      assertPercentileDigestCount(query, 2)
    }
  }

  fusionTest("compatible scalar percentiles share one physical percentile digest") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      val query = spark.sql(
        s"""SELECT
           |  approx_percentile(col, 0.5, 10000),
           |  approx_percentile(col, 0.9, 10000),
           |  approx_percentile(col, 0.95, 10000)
           |FROM $table
           |""".stripMargin)

      checkAnswer(query, Row(500, 900, 950))
      assertPercentileDigestCount(query, 1)
      val optimizedPercentiles = query.queryExecution.optimizedPlan.expressions.flatMap {
        _.collect { case percentile: ApproximatePercentile => percentile }
      }
      assert(optimizedPercentiles.nonEmpty)
      assert(optimizedPercentiles.forall(_.prettyName == "approx_percentile"))
      val modes = query.queryExecution.sparkPlan.collect {
        case aggregate: ObjectHashAggregateExec =>
          aggregate.aggregateExpressions.collect {
            case expression @ AggregateExpression(_: ApproximatePercentile, _, _, _, _) =>
              expression.mode
          }
      }.flatten.toSet
      assert(modes == Set(Partial, Final))
    }
  }

  fusionTest("do not fuse duplicate percentages already shared by physical planning") {
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

  fusionTest("preserve structural input and filter evaluation") {
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

  fusionTest("do not fuse canonical input or filter collisions") {
    checkMatchesUnfusedBaseline(
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
        |""".stripMargin,
      expectedDigests = 2)

    val crossDistinctCollision =
      """SELECT
        |  percentile_approx(DISTINCT a + (b + c), 0.5D),
        |  percentile_approx(DISTINCT a + (b + c), 0.9D),
        |  percentile_approx((a + b) + c, 0.5D),
        |  percentile_approx((a + b) + c, 0.9D)
        |FROM VALUES (
        |  CAST(10000000000000000 AS DOUBLE),
        |  CAST(-10000000000000000 AS DOUBLE),
        |  CAST(1 AS DOUBLE)
        |) AS t(a, b, c)
        |""".stripMargin
    val unfusedBaseline = withSQLConf(
      SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED.key -> "false") {
      spark.sql(crossDistinctCollision).collect().toSeq
    }
    checkAnswer(spark.sql(crossDistinctCollision), unfusedBaseline)

    val filteredArrayCollision = spark.sql(
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
          |""".stripMargin)
    checkAnswer(filteredArrayCollision, Row(Seq(7, 7), null, null))
    assertPercentileDigestCount(filteredArrayCollision, 2)
  }

  fusionTest("preserve canonically colliding parameters") {
    val cases = Seq(
      (
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
          |""".stripMargin,
        2),
      (
        """SELECT
          |  percentile_approx(
          |    id, array((1e16D + -1e16D) + 0.5D, 0.9D)),
          |  percentile_approx(
          |    id, 1e16D + (-1e16D + 0.5D)),
          |  percentile_approx(id, 0.9D)
          |FROM range(100)
          |""".stripMargin,
        2))

    cases.foreach { case (sql, expectedDigests) =>
      checkMatchesUnfusedBaseline(sql, expectedDigests)
    }
  }

  fusionTest("preserve existing arrays that collide after distinct removal") {
    val query = spark.sql(
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
          |""".stripMargin)
    checkAnswer(query, Row(Seq(0.0d, 0.0d), 0.0d, 1.0d, 1.0d))
    assertPercentileDigestCount(query, 3)
  }

  fusionTest("fused percentiles use fresh result IDs across CTE references") {
    val query = spark.sql(
      """WITH c AS (
          |  SELECT
          |    percentile_approx(v, 0.5D) AS a,
          |    percentile_approx(v, 0.9D) AS b
          |  FROM range(1, 6) AS t(v)
          |)
          |SELECT c1.a, c1.b, c2.a
          |FROM c AS c1 JOIN c AS c2
          |""".stripMargin)
    checkAnswer(query, Row(3L, 5L, 3L))
    val optimizedPlan = query.queryExecution.optimizedPlan
    assert(optimizedPlan.subqueriesAll.exists(_.exists(_.expressions.exists(_.exists {
      case percentile: ApproximatePercentile =>
        percentile.percentageExpression.dataType.isInstanceOf[ArrayType]
      case _ => false
    }))), optimizedPlan.numberedTreeString)
  }

  fusionTest("preserve percentile inputs across scalar subquery reuse") {
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
      checkAnswer(
        spark.sql(query),
        Row(Row(1.0d, 1.0d), Row(0.0d, 0.0d)))
    }
  }

  fusionTest("preserve pre-fusion identity across exchange reuse") {
    val parameterPairs = Seq(
      ("(1e16D + -1e16D) + 0.5D", "100"),
      ("0.5D", "CAST((1e16D + -1e16D) + 100D AS INT)"),
      ("0.5D", "100L"))
    val activeRules = (excludedRules :+ constantFoldingRule).distinct.mkString(",")

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> activeRules) {
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

  fusionTest("identical fused percentiles remain reusable") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
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
    }
  }

  fusionTest("fused distinct percentiles keep a single distinct group") {
    val query = spark.sql(
      """SELECT
        |  count(DISTINCT id),
        |  percentile_approx(DISTINCT id, 0.5D),
        |  percentile_approx(DISTINCT id, 0.9D)
        |FROM range(10)
        |""".stripMargin)

    checkAnswer(query, Row(10L, 4L, 8L))
    assertPercentileDigestCount(query, 1)
    assert(query.queryExecution.optimizedPlan.collect {
      case _: Expand => true
    }.isEmpty)
  }

  fusionTest("combined scalar percentiles preserve empty-input nulls") {
    withTempView(table) {
      Seq.empty[Int].toDF("col").createOrReplaceTempView(table)
      val query = spark.sql(
        s"""SELECT
           |  percentile_approx(col, 0.5D),
           |  percentile_approx(col, 0.9D)
           |FROM $table
           |""".stripMargin)
      checkAnswer(query, Row(null, null))
      assertPercentileDigestCount(query, 1)
    }
  }

  fusionTest("preserve compressed and merged low-accuracy percentile digests") {
    val sql =
      """SELECT
        |  percentile_approx(id, 0.1D, 100),
        |  percentile_approx(id, 0.5D, 100),
        |  percentile_approx(id, 0.9D, 100)
        |FROM range(0, 50000, 1, 4)
        |""".stripMargin
    val baseline = withSQLConf(
      SQLConf.COMBINE_APPROXIMATE_PERCENTILES_ENABLED.key -> "false") {
      spark.sql(sql).collect().toSeq
    }

    val query = spark.sql(sql)
    checkAnswer(query, baseline)
    assertPercentileDigestCount(query, 1)
  }

  fusionTest("fuse compatible percentiles introduced by merged scalar subqueries") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
      val query = spark.sql(
        """SELECT
          |  (SELECT percentile_approx(id, 0.5D) FROM range(10)),
          |  (SELECT percentile_approx(id, 0.9D) FROM range(10))
          |""".stripMargin)

      checkAnswer(query, Row(4L, 8L))
      val digestCounts = query.queryExecution.executedPlan.collectWithSubqueries {
        case aggregate: ObjectHashAggregateExec =>
          aggregate.aggregateExpressions.count(
            _.aggregateFunction.isInstanceOf[ApproximatePercentile])
      }
      assert(digestCounts.nonEmpty && digestCounts.forall(_ == 1), digestCounts)
    }
  }

  fusionTest("fuse canonical input groups with disjoint percentages independently") {
    val query = spark.sql(
      """SELECT
        |  percentile_approx(a + b, 0.5D),
        |  percentile_approx(a + b, 0.9D),
        |  percentile_approx(b + a, 0.25D),
        |  percentile_approx(b + a, 0.75D)
        |FROM VALUES (1D, 2D), (3D, 4D), (5D, 6D) AS t(a, b)
        |""".stripMargin)

    checkAnswer(query, Row(7.0d, 11.0d, 3.0d, 11.0d))
    assertPercentileDigestCount(query, 2)
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
