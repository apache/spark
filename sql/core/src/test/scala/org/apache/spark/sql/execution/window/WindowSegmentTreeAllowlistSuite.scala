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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Coverage for the explicit segment-tree aggregate allowlist
 * ([[WindowSegmentTree.EligibleAggregates]]):
 *   - allowlisted aggregates route to segtree (`numSegmentTreeFrames` bumps).
 *   - non-allowlisted aggregates fall through to the sliding path without
 *     crashing or producing wrong results (segtree counters stay at 0).
 * Eligibility gating only; exhaustive equivalence lives in
 * [[SegmentTreeWindowFunctionSuite]].
 */
class WindowSegmentTreeAllowlistSuite
    extends SharedSparkSession with SQLMetricsTestUtils {

  import testImplicits._

  private val enableSegTree: Map[String, String] = Map(
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
    SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1")

  private def baseDF = spark.range(0, 120).selectExpr(
    "id", "(id % 3) AS pk", "CAST(id AS INT) AS v", "CAST(id AS DOUBLE) AS vd")

  private val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-3, 3)

  private def segTreeCounters(df: DataFrame): (Long, Long) = {
    val previousExecutionIds = currentExecutionIds()
    df.collect()
    sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionId = currentExecutionIds().diff(previousExecutionIds).head
    val metricValues = statusStore.executionMetrics(executionId)
    val graph = SparkPlanGraph(SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan))
    val windowNode = graph.allNodes.find(_.name == "Window").getOrElse {
      fail(s"No Window node in plan:\n${df.queryExecution.executedPlan}")
    }
    def total(name: String): Long = {
      val mOpt = windowNode.metrics.find(_.name == name)
      mOpt.map { m =>
        val raw = metricValues(m.accumulatorId)
        "-?\\d+".r.findFirstIn(raw).map(_.toLong).getOrElse(0L)
      }.getOrElse(0L)
    }
    (total("number of segment-tree frames prepared"),
      total("number of segment-tree fallback frames prepared"))
  }

  // Positive: allowlisted aggregates route to segtree

  Seq(
    ("min", (c: org.apache.spark.sql.Column) => min(c)),
    ("max", (c: org.apache.spark.sql.Column) => max(c)),
    ("sum", (c: org.apache.spark.sql.Column) => sum(c)),
    ("count", (c: org.apache.spark.sql.Column) => count(c)),
    ("avg", (c: org.apache.spark.sql.Column) => avg(c)),
    ("stddev_pop", (c: org.apache.spark.sql.Column) => stddev_pop(c)),
    ("stddev_samp", (c: org.apache.spark.sql.Column) => stddev_samp(c)),
    ("var_pop", (c: org.apache.spark.sql.Column) => var_pop(c)),
    ("var_samp", (c: org.apache.spark.sql.Column) => var_samp(c))
  ).foreach { case (name, fn) =>
    test(s"$name routes to the segment-tree path") {
      withSQLConf(enableSegTree.toSeq: _*) {
        val df = baseDF.withColumn("agg", fn($"vd").over(winSpec))
        val (seg, fallback) = segTreeCounters(df)
        assert(seg > 0, s"$name should bump numSegmentTreeFrames (got $seg)")
        assert(fallback == 0,
          s"$name should not fall back (fallback counter: $fallback)")
      }
    }
  }

  // Negative: non-allowlisted aggregates fall through

  test("first_value falls through (order-dependent aggregate)") {
    withSQLConf(enableSegTree.toSeq: _*) {
      val df = baseDF.withColumn("agg", first($"v").over(winSpec))
      val (seg, _) = segTreeCounters(df)
      assert(seg == 0, s"first_value should not use segment tree (got $seg frames)")
    }
  }

  test("last_value falls through (order-dependent aggregate)") {
    withSQLConf(enableSegTree.toSeq: _*) {
      val df = baseDF.withColumn("agg", last($"v").over(winSpec))
      val (seg, _) = segTreeCounters(df)
      assert(seg == 0, s"last_value should not use segment tree (got $seg frames)")
    }
  }

  test("collect_list falls through (unbounded buffer)") {
    withSQLConf(enableSegTree.toSeq: _*) {
      val df = baseDF.withColumn("agg", collect_list($"v").over(winSpec))
      val (seg, _) = segTreeCounters(df)
      assert(seg == 0, s"collect_list should not use segment tree (got $seg frames)")
    }
  }

  test("collect_set falls through (unbounded buffer)") {
    withSQLConf(enableSegTree.toSeq: _*) {
      val df = baseDF.withColumn("agg", collect_set($"v").over(winSpec))
      val (seg, _) = segTreeCounters(df)
      assert(seg == 0, s"collect_set should not use segment tree (got $seg frames)")
    }
  }

  test("approx_count_distinct (HyperLogLog++) falls through (fail-closed)") {
    withSQLConf(enableSegTree.toSeq: _*) {
      val df = baseDF.withColumn("agg", approx_count_distinct($"v").over(winSpec))
      val (seg, _) = segTreeCounters(df)
      assert(seg == 0,
        s"approx_count_distinct is intentionally not on the allowlist (got $seg frames)")
    }
  }

  test("percentile_approx falls through (sketch buffer not auditable)") {
    withSQLConf(enableSegTree.toSeq: _*) {
      val df = baseDF.withColumn(
        "agg", percentile_approx($"vd", lit(0.5), lit(100)).over(winSpec))
      val (seg, _) = segTreeCounters(df)
      assert(seg == 0, s"percentile_approx should not use segment tree (got $seg frames)")
    }
  }

  // Gate: aggregates carrying a FILTER (WHERE ...) clause fall through.
  // `eligibleForSegTree` requires `filters.forall(_.isEmpty)` because the
  // segment-tree combine contract is defined over the unfiltered partial
  // buffer. A defensive regression: if any future analyzer rule ever rewrites
  // `AGG(x) FILTER (WHERE p)` in a way that strips `AggregateExpression.filter`
  // (e.g., pushing the predicate into the aggregate function), this test
  // fails and forces an explicit eligibility review.
  test("FILTER (WHERE ...) disables segment-tree path") {
    withSQLConf(enableSegTree.toSeq: _*) {
      withTempView("t") {
        baseDF.createOrReplaceTempView("t")
        val df = spark.sql(
          """SELECT id, pk, v,
            |  sum(v) FILTER (WHERE v % 2 = 0)
            |    OVER (PARTITION BY pk ORDER BY id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)
            |    AS filtered_sum
            |FROM t""".stripMargin)
        val (seg, _) = segTreeCounters(df)
        assert(seg == 0,
          s"filtered aggregate must not take segment-tree path (got $seg segtree frames)")
      }
    }
  }

  // Mixed: ANY non-eligible aggregate disqualifies the group

  test("mix of allowlisted + non-allowlisted aggregates falls through entirely") {
    withSQLConf(enableSegTree.toSeq: _*) {
      val df = baseDF
        .withColumn("s", sum($"v").over(winSpec))
        .withColumn("fv", first($"v").over(winSpec))
      val (seg, _) = segTreeCounters(df)
      // Both aggregates share the same Window node; gating is forall(isEligible),
      // so `first_value` drops the whole group.
      assert(seg == 0,
        s"Window group containing a non-allowlisted agg must fall through (got $seg)")
    }
  }
}
