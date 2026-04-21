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

package org.apache.spark.sql.execution.window

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SQLMetrics visibility coverage for [[SegmentTreeWindowFunctionFrame]].
 *
 * segtree-path: segtree path bumps `numSegmentTreeFrames`.
 * fallback-path: fallback path bumps `numSegmentTreeFallbackFrames`.
 * feature-flag-off: feature-flag off -> both counters stay at 0 (no false positives).
 *
 * See `the PR description (SQLMetrics exposure)`.
 */
class SegmentTreeWindowMetricsSuite
    extends QueryTest with SharedSparkSession with SQLMetricsTestUtils {

  import testImplicits._

  /**
   * Run `df` and return the first Window node's metric values for the two
   * segment-tree counters, parsed to Long. (Other metrics like `spillSize`
   * are UI-pretty-printed strings like "0.0 B (0.0 B, 0.0 B, ...)" and
   * are skipped.)
   */
  private def windowMetricValues(df: org.apache.spark.sql.DataFrame): Map[String, Long] = {
    val previousExecutionIds = currentExecutionIds()
    df.collect()
    sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionId = currentExecutionIds().diff(previousExecutionIds).head
    val metricValues = statusStore.executionMetrics(executionId)
    val graph = SparkPlanGraph(SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan))
    val windowNode = graph.allNodes.find(_.name == "Window").getOrElse {
      fail(s"No Window node in plan:\n${df.queryExecution.executedPlan}")
    }
    val wanted = Set(
      "number of segment-tree frames",
      "number of segment-tree fallback frames")
    windowNode.metrics.filter(m => wanted.contains(m.name)).map { m =>
      // For sum metrics the UI value is either a plain integer ("3") or the
      // aggregated form "total (min, med, max (stageId: taskId))\nN (...)".
      // Grab the first integer run of digits, which is the grand total.
      val raw = metricValues(m.accumulatorId)
      val total = "-?\\d+".r.findFirstIn(raw).getOrElse {
        fail(s"Could not parse integer from metric '${m.name}' value: $raw")
      }
      m.name -> total.toLong
    }.toMap
  }

  private def baseDF = spark.range(0, 120).selectExpr(
    "id", "(id % 3) AS pk", "CAST(id AS INT) AS v")

  private val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-3, 3)

  test("segment-tree path increments numSegmentTreeFrames (one per frame per partition)") {
    withSQLConf(
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // Two aggregates sharing one Window -> one frame, 3 partitions => 3 frames.
      val df = baseDF.select($"id",
        min($"v").over(winSpec).as("mn"),
        max($"v").over(winSpec).as("mx"))
      val m = windowMetricValues(df)
      assert(m("number of segment-tree frames") === 3L,
        s"expected 3 segtree frames (one per partition), got metrics = $m")
      assert(m("number of segment-tree fallback frames") === 0L,
        s"fallback counter must be 0 when all partitions take segtree path, got $m")
    }
  }

  test("fallback path increments numSegmentTreeFallbackFrames") {
    withSQLConf(
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        // Threshold > partition size (40 rows/partition) forces fallback on every partition.
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1000",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = baseDF.select($"id", min($"v").over(winSpec).as("mn"))
      val m = windowMetricValues(df)
      assert(m("number of segment-tree fallback frames") === 3L,
        s"expected 3 fallback frames (one per partition under threshold), got $m")
      assert(m("number of segment-tree frames") === 0L,
        s"segtree counter must be 0 when all partitions fall back, got $m")
    }
  }

  test("feature flag off: both segment-tree counters stay at zero") {
    withSQLConf(
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = baseDF.select($"id", min($"v").over(winSpec).as("mn"))
      val m = windowMetricValues(df)
      assert(m("number of segment-tree frames") === 0L,
        s"segtree counter must be 0 when feature flag disabled, got $m")
      assert(m("number of segment-tree fallback frames") === 0L,
        s"fallback counter must be 0 when feature flag disabled, got $m")
    }
  }

  // ---------------------------------------------------------------------
  // T1-T4: regression coverage for the removed idempotency guard in
  // `SegmentTreeWindowFunctionFrame.prepare()` (see the class documentation).
  // The guard keyed on `(System.identityHashCode(rows), rows.length)`, but
  // `WindowPartitionEvaluator` reuses a single `ExternalAppendOnlyUnsafeRowArray`
  // across all partitions in a task, so the identity hash is constant per
  // task and the discriminator collapsed to `rows.length` -- silently
  // deduping any two consecutive partitions of equal row count.
  //
  // All four tests pin `minPartitionRows = 64` (landed default) so future
  // default changes don't silently rebucket the fixtures, and use a single
  // shuffle partition so the Window operator sees `numPartitions >
  // numTasks` -- the exact shape the segtree and fallback fixtures happened to avoid.
  // ---------------------------------------------------------------------

  test("T1 (G1) numPartitions > numTasks, identical length: every partition counted") {
    withSQLConf(
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "64",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // 3 window partitions x 100 rows, all >= 64 => segtree path, one task.
      val df = spark.range(0, 300).selectExpr("id", "(id % 3) AS pk", "CAST(id AS INT) AS v")
        .select($"id", min($"v").over(
          Window.partitionBy($"pk").orderBy($"id").rowsBetween(-3, 3)).as("mn"))
      val m = windowMetricValues(df)
      assert(m("number of segment-tree frames") === 3L,
        s"expected 3 segtree frames (one per window partition), got $m")
      assert(m("number of segment-tree fallback frames") === 0L, s"got $m")
    }
  }

  test("T2 (G2) identical-length partitions across keys") {
    withSQLConf(
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "64",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // Two distinct partition keys, both exactly 200 rows (> 64 => segtree).
      // The old guard would dedupe the second because `rows.length` matches.
      val df = spark.range(0, 400)
        .selectExpr("id", "CASE WHEN id < 200 THEN 'a' ELSE 'b' END AS pk",
          "CAST(id AS INT) AS v")
        .select($"id", min($"v").over(
          Window.partitionBy($"pk").orderBy($"id").rowsBetween(-3, 3)).as("mn"))
      val m = windowMetricValues(df)
      assert(m("number of segment-tree frames") === 2L,
        s"expected 2 segtree frames (one per key, both length 200), got $m")
      assert(m("number of segment-tree fallback frames") === 0L, s"got $m")
    }
  }

  test("T3 (G3) all-length-1 unique keys, fallback path: every partition counted") {
    withSQLConf(
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "64",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // 100 unique keys, 1 row each, all < 64 => fallback. The old guard
      // would collapse this to 1 because every partition has length 1.
      val df = spark.range(0, 100).selectExpr("id AS pk", "CAST(id AS INT) AS v")
        .select($"pk", min($"v").over(
          Window.partitionBy($"pk").orderBy($"pk").rowsBetween(-3, 3)).as("mn"))
      val m = windowMetricValues(df)
      assert(m("number of segment-tree fallback frames") === 100L,
        s"expected 100 fallback frames (one per unique-key partition), got $m")
      assert(m("number of segment-tree frames") === 0L, s"got $m")
    }
  }

  test("T4 (G4) mixed segtree + fallback, non-aliasing order") {
    withSQLConf(
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "64",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      // 4 window partitions, lengths (50, 50, 100, 100) in sort order.
      // 50 < 64 => fallback; 100 >= 64 => segtree. Non-aliasing ordering
      // (50,50,100,100) exposes the old length-only-dedup bug: with the
      // guard, consecutive same-length partitions collapsed to 1 bump
      // each => (fb=1, seg=1). Under the fix => (fb=2, seg=2). Note that
      // an alternating (50,100,50,100) ordering would coincidentally give
      // (2,2) under both code paths (the key changes every step), so it
      // is a test false-positive and must not be used.
      val df = spark.range(0, 300)
        .selectExpr(
          "id",
          // key ordering k1<k2<k3<k4 matches size-monotone (50,50,100,100)
          "CASE WHEN id < 50 THEN 'k1' " +
            "WHEN id < 100 THEN 'k2' " +
            "WHEN id < 200 THEN 'k3' " +
            "ELSE 'k4' END AS pk",
          "CAST(id AS INT) AS v")
        .select($"id", min($"v").over(
          Window.partitionBy($"pk").orderBy($"id").rowsBetween(-3, 3)).as("mn"))
      val m = windowMetricValues(df)
      assert(m("number of segment-tree frames") === 2L,
        s"expected 2 segtree frames (k3, k4 @ 100 rows each), got $m")
      assert(m("number of segment-tree fallback frames") === 2L,
        s"expected 2 fallback frames (k1, k2 @ 50 rows each), got $m")
    }
  }
}
