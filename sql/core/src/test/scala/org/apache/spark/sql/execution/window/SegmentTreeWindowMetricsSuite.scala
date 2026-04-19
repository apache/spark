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
}
