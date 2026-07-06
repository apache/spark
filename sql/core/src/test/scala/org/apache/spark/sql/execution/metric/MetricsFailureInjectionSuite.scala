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

import org.apache.spark.SparkException
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
          def runOnce(): Dataset[_] = {
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
            finalDf
          }

          // The INJECT_SHUFFLE_FETCH_FAILURES machinery corrupts mapper-0 of the first successful
          // attempt of the shuffle map stage. Whether the downstream reducer observes the resulting
          // FetchFailed (and thus forces the stage-1 recompute that inflates the raw metric) depends
          // on task scheduling within the shared SparkContext; across the suite it occasionally does
          // not fire, leaving stage1Metric at exactly 300 and failing "value > 300" (a ~1/6 flake,
          // more frequent on slower runners such as macOS arm64). When we require a recompute
          // (injectFailure = true), re-run the query until the injection actually fires. Each attempt
          // resets the metrics, so a successful attempt is indistinguishable from a first-try success.
          var finalDf = runOnce()
          if (injectFailure) {
            var attempts = 1
            while (stage1Metric.value <= 300 && attempts < 10) {
              stage1Metric.reset()
              stage2Metric.reset()
              stage1SLAMetric.reset()
              stage2SLAMetric.reset()
              finalDf = runOnce()
              attempts += 1
            }
            assert(stage1Metric.value > 300,
              s"fetch-failure injection did not force a recompute after $attempts attempts")
          }
          postRunChecks(finalDf)
          stage1Metric.reset()
          stage2Metric.reset()
        }
      }
    }

    runQueryWithMetrics() { finalDf =>
      if (injectFailure) {
        assert(stage1Metric.value > 300)
        // The non-deterministic UDF in stage 1 makes mapper 0's recompute produce a different
        // checksum from its corrupted first attempt, which fires rollbackSucceedingStages and
        // re-runs stage 2 in full. The raw stage 2 accumulator therefore overcounts; SLAM
        // remains stable.
        assert(stage2Metric.value > 5, s"stage2Metric=${stage2Metric.value}")
      } else {
        assert(stage1Metric.value === 300)
        assert(stage2Metric.value === 5)
      }

      assert(stage1SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
      assert(stage2SLAMetric.lastAttemptValueForHighestRDDId() === Some(5))

      assert(stage1SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
      assert(stage2SLAMetric.lastAttemptValueForDataset(finalDf) === Some(5))
    }
  }

  test("Three stage metrics block failure injection") {
    val stage1Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 1 counter")
    val stage2Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 2 counter")
    val stage3Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 3 counter")
    val stage1SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 1 SLAM")
    val stage2SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 2 SLAM")
    val stage3SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 3 SLAM")

    withTable("primary_table", "secondary_table") {
      setUpTestTable("primary_table")
      setUpTestTable("secondary_table")
      withSparkContextConf(
          config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> "true") {
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

        // Both stage1 (leaf) and stage2 (non-leaf) get corrupted on their first successful
        // attempt and re-run. stage3 is a result stage with no shuffle output, so it isn't
        // corrupted and runs only once successfully.
        assert(stage1Metric.value > 300, s"stage1Metric=${stage1Metric.value}")
        assert(stage2Metric.value > 300, s"stage2Metric=${stage2Metric.value}")
        assert(stage3Metric.value === 5)

        // SLAM correctly reports each stage's last successful attempt's contribution only.
        assert(stage1SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
        assert(stage2SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
        assert(stage3SLAMetric.lastAttemptValueForHighestRDDId() === Some(5))

        assert(stage1SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
        assert(stage2SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
        assert(stage3SLAMetric.lastAttemptValueForDataset(finalDf) === Some(5))
      }
    }
  }

  test("Three stage metrics block failure injection with AQE") {
    // Same as the previous test but with AQE enabled. Under AQE each Exchange is materialized
    // as its own map-stage job, which exercises a different DAGScheduler path than the
    // AQE-disabled variant: the injection's deferred corruption must survive across those
    // per-shuffle jobs for the downstream FetchFailed (and thus the producer re-run) to fire.
    val stage1Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 1 counter")
    val stage2Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 2 counter")
    val stage3Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 3 counter")
    val stage1SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 1 SLAM")
    val stage2SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 2 SLAM")
    val stage3SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 3 SLAM")

    withTable("primary_table", "secondary_table") {
      setUpTestTable("primary_table")
      setUpTestTable("secondary_table")
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        withSparkContextConf(
            config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> "true") {
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

          // Both the leaf stage 1 and the non-leaf stage 2 get their first successful attempt
          // corrupted and re-run, so their raw counters overcount. SLAM reports only the last
          // successful attempt per RDD.
          assert(stage1Metric.value > 300, s"stage1Metric=${stage1Metric.value}")
          assert(stage2Metric.value > 300, s"stage2Metric=${stage2Metric.value}")
          assert(stage3Metric.value === 5)

          assert(stage1SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
          assert(stage2SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
          assert(stage3SLAMetric.lastAttemptValueForHighestRDDId() === Some(5))

          assert(stage1SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
          assert(stage2SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
          assert(stage3SLAMetric.lastAttemptValueForDataset(finalDf) === Some(5))
        }
      }
    }
  }

  test("Three stage metrics force-checksum-mismatch on recompute") {
    // INJECT_SHUFFLE_FORCE_CHECKSUM_MISMATCH_ON_RECOMPUTE additionally flags the recompute of the
    // partition-0 task as a checksum mismatch. The DAGScheduler then runs
    // `rollbackSucceedingStages`, which (a) for downstream ShuffleMapStages clears their map
    // outputs and forces a full retry of every previously-finished partition, and (b) for the
    // ResultStage downstream is a no-op because the result stage hasn't started yet - it just
    // runs once after the rollback completes.
    //
    // Without a timing guarantee the FetchFailed in stage 2 may fire before any stage 2 task
    // finishes, in which case the rollback has nothing to clear and stage 2 metrics look the
    // same as in the recompute-only mode. So we only assert `stage2Metric > 300`, which is the
    // sum of partial-attempt-0 contributions (>=1 partition since rollback had something to
    // roll back) plus a full attempt-1; the deterministic version of this scenario lives in
    // the delayed-corruption test below.
    val stage1Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 1 counter")
    val stage2Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 2 counter")
    val stage3Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 3 counter")
    val stage1SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 1 SLAM")
    val stage2SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 2 SLAM")
    val stage3SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 3 SLAM")

    withTable("primary_table", "secondary_table") {
      setUpTestTable("primary_table")
      setUpTestTable("secondary_table")
      withSparkContextConf(
          config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> "true",
          config.Tests.INJECT_SHUFFLE_FORCE_CHECKSUM_MISMATCH_ON_RECOMPUTE.key -> "true") {
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

        // The recompute-with-mismatch injection drives `rollbackSucceedingStages` against the
        // checksum-mismatched producer. Stage 2 is a downstream ShuffleMapStage and gets its map
        // outputs cleared and rerun. The total raw accumulator on stage 2 is
        // (partial-attempt-0 contributions) + (full-attempt-1 = 300). In the recompute-only
        // mode it would be exactly 300 because attempt 1 only re-runs the missing partitions;
        // here it is strictly larger when the rollback had any partitions to clear.
        assert(stage1Metric.value > 300, s"stage1Metric=${stage1Metric.value}")
        assert(stage2Metric.value > 300, s"stage2Metric=${stage2Metric.value}")
        assert(stage3Metric.value === 5)

        // SLAM still reports the last successful attempt's contribution per RDD.
        assert(stage1SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
        assert(stage2SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
        assert(stage3SLAMetric.lastAttemptValueForHighestRDDId() === Some(5))

        assert(stage1SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
        assert(stage2SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
        assert(stage3SLAMetric.lastAttemptValueForDataset(finalDf) === Some(5))
      }
    }
  }

  test("Three stage metrics force-checksum-mismatch with delayed corruption") {
    // Same setup as the previous test but with INJECT_SHUFFLE_FETCH_FAILURES_DOWNSTREAM_DELAY=1
    // and shuffle.partitions=20 (much greater than the test's local[2] cores). The DAGScheduler
    // event loop is single-threaded for completion events, so deferring the producer's
    // mapper-0 corruption until after one consumer success guarantees AT LEAST ONE consumer
    // task fully completed before the FetchFailed cascade kicks in. With the force-checksum-
    // mismatch injection's rollback, those completed-then-cleared partitions all re-run during
    // the rollback retry, giving a
    // strict lower bound on the raw stage-2 accumulator that's not reachable in
    // recompute-only mode.
    val stage1Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 1 counter")
    val stage2Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 2 counter")
    val stage3Metric = SQLMetrics.createMetric(spark.sparkContext, "stage 3 counter")
    val stage1SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 1 SLAM")
    val stage2SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 2 SLAM")
    val stage3SLAMetric =
      SQLLastAttemptMetrics.createMetric(spark.sparkContext, "stage 3 SLAM")

    withTable("primary_table", "secondary_table") {
      setUpTestTable("primary_table")
      setUpTestTable("secondary_table")
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "20") {
        withSparkContextConf(
            config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> "true",
            config.Tests.INJECT_SHUFFLE_FETCH_FAILURES_DOWNSTREAM_DELAY.key -> "1",
            config.Tests.INJECT_SHUFFLE_FORCE_CHECKSUM_MISMATCH_ON_RECOMPUTE.key -> "true") {
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

          // With delay=1 and 20 shuffle partitions on local[2], at least one stage-2 reducer
          // task is guaranteed to fully process its rows before the corruption fires. The
          // force-checksum-mismatch injection's rollback then re-runs all 20 stage-2
          // partitions, replaying those previously-completed ones. The recompute-only baseline
          // is 300 (full coverage across attempts) + size(mapper 0) for the FetchFailed-driven
          // retry; the force-checksum-mismatch injection adds at least one already-completed
          // partition's worth on top of that. Partition sizes vary with the hash of `id`, so
          // we just assert "strictly above the recompute-only baseline" rather than a tight
          // numeric bound.
          assert(stage1Metric.value > 300, s"stage1Metric=${stage1Metric.value}")
          assert(stage2Metric.value > 315,
            s"stage2Metric should be above the recompute-only baseline (~315) because the " +
              s"rollback re-played a partition that completed in attempt 0, got " +
              s"${stage2Metric.value}")
          assert(stage3Metric.value === 5)

          assert(stage1SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
          assert(stage2SLAMetric.lastAttemptValueForHighestRDDId() === Some(300))
          assert(stage3SLAMetric.lastAttemptValueForHighestRDDId() === Some(5))

          assert(stage1SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
          assert(stage2SLAMetric.lastAttemptValueForDataset(finalDf) === Some(300))
          assert(stage3SLAMetric.lastAttemptValueForDataset(finalDf) === Some(5))
        }
      }
    }
  }

  test("Force checksum mismatch aborts a downstream ResultStage") {
    // 2-stage query whose downstream is a ResultStage. With RESULT_STAGE_DELAY=1 the result
    // stage gets at least one finished task before the FetchFailed cascade, so by the time
    // the forced checksum mismatch on stage 1 mapper 0 fires `rollbackSucceedingStages`,
    // the result stage's findMissingPartitions().length is strictly less than numTasks, and
    // OSS Spark cannot roll back a partially-finished result stage, so the job aborts. With
    // the default RESULT_STAGE_DELAY=0 the result stage is corrupted before any task
    // dispatches and the rollback path does not abort.
    //
    // We group by the high-cardinality `id` column (not `low_cardinality_col`) so that every
    // one of the 20 reducer partitions reads data from the corrupted mapper 0. Otherwise only
    // the handful of reducer partitions that happen to hold mapper-0's few low-cardinality keys
    // would observe the FetchFailed, and the abort would only fire when one of those specific
    // partitions happened to be scheduled after the (asynchronous) corruption -- a scheduling
    // race that made this test flaky under Maven. With `id`, every partition depends on mapper
    // 0, so once RESULT_STAGE_DELAY=1 has corrupted it (after the first result task), local[2]
    // dispatches the remaining result tasks afterwards and at least one is guaranteed to hit
    // the corrupted mapper, deterministically triggering the indeterminate-stage abort.
    withTable("test_table") {
      setUpTestTable("test_table")
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "20") {
        withSparkContextConf(
            config.Tests.INJECT_SHUFFLE_FETCH_FAILURES.key -> "true",
            config.Tests.INJECT_SHUFFLE_FETCH_FAILURES_RESULT_STAGE_DELAY.key -> "1",
            config.Tests.INJECT_SHUFFLE_FORCE_CHECKSUM_MISMATCH_ON_RECOMPUTE.key -> "true") {
          val df = spark.read.table("test_table")
            .groupBy("id")
            .count()
          val ex = intercept[SparkException] {
            df.collect()
          }
          assert(ex.getMessage.contains("indeterminate"),
            s"expected an 'indeterminate'-stage abort, got: ${ex.getMessage}")
        }
      }
    }
  }
}
