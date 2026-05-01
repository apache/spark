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
package org.apache.spark.sql.execution.metric

import scala.util.Random

import org.apache.spark.internal.config
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.execution.adaptive.{AQETestHelper, DisableAdaptiveExecutionSuite}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class MetricsFailureInjectionSuite
  extends SharedSparkSession
  with SQLMetricsTestUtils
  // Need to control AQE per-test to ensure expected plan shapes.
  with DisableAdaptiveExecutionSuite {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Disable re-use, since it interferes with the forced replanning.
    spark.conf.set(SQLConf.EXCHANGE_REUSE_ENABLED, false)
  }

  def setUpTestTable(tableName: String): Unit = {
    val rand = new Random(1)
    val randomPrefix = rand.nextString(30)
    spark.range(300).map { id =>
        (id, (id % 5).toInt, randomPrefix + (id % 111))
      }.toDF("id", "low_cardinality_col", "large_col")
      .write.format("parquet").saveAsTable(tableName)
    val numRecords = spark.read.table(tableName).count()
    assert(numRecords === 300)
  }

  for {
    useAQE <- BOOLEAN_DOMAIN
  } test(s"Two stage metrics AQE cancellation injection - useAQE=$useAQE") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> useAQE.toString) {
      val stage1Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 1 counter")
      val stage2Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 2 counter")
      val stage1SLAMetric =
        SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 1 SLAM")
      val stage2SLAMetric =
        SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 2 SLAM")

      def runQueryWithMetrics(
          triggerMetrics: SQLMetric*)(
          postRunChecks: Dataset[_] => Unit): Unit = {
        assert(stage1Metric.value === 0)
        assert(stage2Metric.value === 0)
        withTable("test_table") {
          setUpTestTable("test_table")
          AQETestHelper.withForcedCancellation(triggerMetrics: _*) {
            val stage1MetricsExpr = incrementMetrics(Seq(stage1Metric, stage1SLAMetric))
            val stage1 = spark.read.table("test_table").filter(Column(stage1MetricsExpr))
            val stage2MetricsExpr = incrementMetrics(Seq(stage2Metric, stage2SLAMetric))
            val stage2 =
              stage1.groupBy("low_cardinality_col").count().filter(Column(stage2MetricsExpr))
            val finalDf = stage2.as[(Int, Long)]
            val result = finalDf.collect()

            assert(result.toMap === (0 until 5).map(v => (v, 300 / 5)).toMap)
            postRunChecks(finalDf)
            stage1Metric.reset()
            stage2Metric.reset()
          }
        }
      }

      // SLAM values don't change with retries, so we can reuse the same assertions for all cases.
      def assertSLAM(finalDf: Dataset[_]): Unit = {
        assert(stage1SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
        assert(stage2SLAMetric.lastAttemptValueForHighestRDDId() === Some(5))

        assert(stage1SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
        assert(stage2SLAMetric.lastAttemptValueForDataset(finalDf) === Some(5))
      }

      // Case 1: No forced replanning.
      runQueryWithMetrics() { finalDf =>
        assert(stage1Metric.value === 300)
        assert(stage2Metric.value === 5)

        assertSLAM(finalDf)
      }

      // Case 2: Replan on stage1Metric.
      runQueryWithMetrics(stage1Metric) { finalDf =>
        if (useAQE) {
          assert(stage1Metric.value > 300)
        } else {
          assert(stage1Metric.value === 300)
        }
        assert(stage2Metric.value === 5)

        assertSLAM(finalDf)
      }

      // Case 3: Replan on stage2Metric (will be ignored, because this is a result stage).
      runQueryWithMetrics(stage2Metric) { finalDf =>
        assert(stage1Metric.value === 300)
        assert(stage2Metric.value === 5)

        assertSLAM(finalDf)
      }

      // Case 4: Replan on both metrics (only first will actually trigger).
      runQueryWithMetrics(stage1Metric, stage2Metric) { finalDf =>
        if (useAQE) {
          assert(stage1Metric.value > 300)
        } else {
          assert(stage1Metric.value === 300)
        }
        assert(stage2Metric.value === 5)

        assertSLAM(finalDf)
      }
    }
  }

  for {
    useAQE <- BOOLEAN_DOMAIN
  } test(s"Three stage metrics AQE cancellation injection - useAQE=$useAQE") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> useAQE.toString) {
      val stage1Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 1 counter")
      val stage2Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 2 counter")
      val stage3Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 3 counter")
      val stage1SLAMetric =
        SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 1 SLAM")
      val stage2SLAMetric =
        SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 2 SLAM")
      val stage3SLAMetric =
        SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 3 SLAM")

      def runQueryWithMetrics(
          triggerMetrics: SQLMetric*)(postRunChecks: Dataset[_] => Unit): Unit = {
        assert(stage1Metric.value === 0)
        assert(stage2Metric.value === 0)
        withTable("primary_table", "secondary_table") {
          // Use the same layout for both. Makes the query a non-obvious self-join essentially.
          setUpTestTable("primary_table")
          setUpTestTable("secondary_table")
          AQETestHelper.withForcedCancellation(triggerMetrics: _*) {
            val stage1MetricsExpr = incrementMetrics(Seq(stage1Metric, stage1SLAMetric))
            val stage1 = spark.read.table("primary_table")
              .filter(Column(stage1MetricsExpr))
            val stage2MetricsExpr = incrementMetrics(Seq(stage2Metric, stage2SLAMetric))
            val stage2 = stage1.join(
                spark.read.table("secondary_table"),
                usingColumn = "id",
                joinType = "fullOuter")
              .filter(Column(stage2MetricsExpr))
            val stage3MetricsExpr = incrementMetrics(Seq(stage3Metric, stage3SLAMetric))
            val stage3 = stage2
              .groupBy("primary_table.low_cardinality_col")
              .count()
              .filter(Column(stage3MetricsExpr))
            val finalDf = stage3.as[(Int, Long)]
            val result = finalDf.collect()
            assert(result.toMap === (0 until 5).map(v => (v, 300 / 5)).toMap)
            postRunChecks(finalDf)
            stage1Metric.reset()
            stage2Metric.reset()
            stage3Metric.reset()
          }
        }
      }

      // SLAM values don't change with retries, so we can reuse the same assertions for all cases.
      def assertSLAM(finalDf: Dataset[_]): Unit = {
        assert(stage1SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
        assert(stage2SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
        assert(stage3SLAMetric.lastAttemptValueForHighestRDDId() === Some(5))

        assert(stage1SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
        assert(stage2SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
        assert(stage3SLAMetric.lastAttemptValueForDataset(finalDf) === Some(5))
      }

      // Case 1: No forced replanning.
      runQueryWithMetrics() { finalDf =>
        assert(stage1Metric.value === 300)
        assert(stage2Metric.value === 300)
        assert(stage3Metric.value === 5)

        assertSLAM(finalDf)
      }

      // Case 2: Replan on stage1Metric.
      runQueryWithMetrics(stage1Metric) { finalDf =>
        if (useAQE) {
          assert(stage1Metric.value > 300)
        } else {
          assert(stage1Metric.value === 300)
        }
        assert(stage2Metric.value === 300)
        assert(stage3Metric.value === 5)

        assertSLAM(finalDf)
      }

      // Case 3: Replan on stage2Metric (will also re-run the first stage).
      runQueryWithMetrics(stage2Metric) { finalDf =>
        if (useAQE) {
          assert(stage1Metric.value > 300)
          assert(stage2Metric.value > 300)
        } else {
          assert(stage1Metric.value === 300)
          assert(stage2Metric.value === 300)
        }
        assert(stage3Metric.value === 5)

        assertSLAM(finalDf)
      }

      // Case 4: Replan on all metrics (only first will actually trigger).
      runQueryWithMetrics(stage1Metric, stage2Metric, stage3Metric) { finalDf =>
        if (useAQE) {
          assert(stage1Metric.value > 300)
        } else {
          assert(stage1Metric.value === 300)
        }
        assert(stage2Metric.value === 300)
        assert(stage3Metric.value === 5)

        assertSLAM(finalDf)
      }
    }
  }

  for {
    injectFailure <- BOOLEAN_DOMAIN
  } test(s"Two stage metrics block failure injection - injectFailure=$injectFailure") {
    val stage1Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 1 counter")
    val stage2Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 2 counter")
    val stage1SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 1 SLAM")
    val stage2SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 2 SLAM")

    def runQueryWithMetrics(
        triggerMetrics: SQLMetric*)(postRunChecks: Dataset[_] => Unit): Unit = {
      assert(stage1Metric.value === 0)
      assert(stage2Metric.value === 0)
      withTable("test_table") {
        setUpTestTable("test_table")
        withSparkContextConf(
            config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> injectFailure.toString) {
          val stage1MetricsExpr = incrementMetrics(Seq(stage1Metric, stage1SLAMetric))
          val stage1 = spark.read.table("test_table").filter(Column(stage1MetricsExpr))
          val stage2MetricsExpr = incrementMetrics(Seq(stage2Metric, stage2SLAMetric))
          val stage2 =
            stage1.groupBy("low_cardinality_col").count().filter(Column(stage2MetricsExpr))
          val finalDf = stage2.as[(Int, Long)]
          val result = finalDf.collect()
          assert(result.toMap === (0 until 5).map(v => (v, 300 / 5)).toMap)
          postRunChecks(finalDf)
          stage1Metric.reset()
          stage2Metric.reset()
        }
      }
    }

    runQueryWithMetrics() { finalDf =>
      if (injectFailure) {
        assert(stage1Metric.value > 300)
      } else {
        assert(stage1Metric.value === 300)
      }
      // Stage2 doesn't have a downstream shuffle stage we can fail.
      assert(stage2Metric.value === 5)

      assert(stage1SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
      assert(stage2SLAMetric.lastAttemptValueForHighestRDDId() === Some(5))

      assert(stage1SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
      assert(stage2SLAMetric.lastAttemptValueForDataset(finalDf) === Some(5))
    }
  }

  for {
    injectFailure <- BOOLEAN_DOMAIN
  } test(s"Non-deterministic stage block failure injection - injectFailure=$injectFailure") {
    val stage1Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 1 counter")
    val stage2Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 2 counter")
    val stage1SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 1 SLAM")
    val stage2SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 2 SLAM")

    def runQueryWithMetrics(
        triggerMetrics: SQLMetric*)(postRunChecks: Dataset[_] => Unit): Unit = {
      assert(stage1Metric.value === 0)
      assert(stage2Metric.value === 0)
      withTable("test_table") {
        setUpTestTable("test_table")
        withSparkContextConf(
            config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> injectFailure.toString) {
          val stage1MetricsExpr = incrementMetrics(Seq(stage1Metric, stage1SLAMetric))
          val udfRand =
            udf {
              () => {
                new Random().nextDouble()
              }
            }.asNondeterministic().apply().expr
          val stage1 = spark.read.table("test_table")
            .withColumn("non_deterministic_col", Column(udfRand))
            .filter(Column(stage1MetricsExpr))
          val stage2MetricsExpr = incrementMetrics(Seq(stage2Metric, stage2SLAMetric))
          val stage2 = stage1
            .groupBy("low_cardinality_col")
            .avg("non_deterministic_col")
            .filter(Column(stage2MetricsExpr))
          // Add an extra stage with a single task to avoid flaky failures. If a ResultTask
          // returns non-deterministic results to the client, it forces the query to abort
          // instead of retrying the input stages.
          val finalDf = stage2.repartition(1).as[(Int, Double)]
          val result = finalDf.collect()
          // Don't compare the second value, since it's random.
          assert(result.map(_._1).toSet === (0 until 5).toSet)
          postRunChecks(finalDf)
          stage1Metric.reset()
          stage2Metric.reset()
        }
      }
    }

    runQueryWithMetrics() { finalDf =>
      if (injectFailure) {
        assert(stage1Metric.value > 300)
      } else {
        assert(stage1Metric.value === 300)
      }
      // Stage2 doesn't have a downstream shuffle stage we can fail.
      assert(stage2Metric.value === 5)

      assert(stage1SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
      assert(stage2SLAMetric.lastAttemptValueForHighestRDDId() === Some(5))

      assert(stage1SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
      assert(stage2SLAMetric.lastAttemptValueForDataset(finalDf) === Some(5))
    }
  }
}
