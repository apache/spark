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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for the block-chunked segment-tree moving window frame.
 *
 * Coverage by section:
 *   - Coverage: various cases (basic aggregates), various cases (frame boundaries),
 *                min-partition-rows fallback, AggregateWindowFunction
 *                regression.
 *   - Coverage: various cases (NULL, NaN/Infinity), various cases
 *                (numeric / string / date-timestamp types), various cases
 *                (unsupported-merge / DISTINCT / feature-flag fallback).
 *
  */
class SegmentTreeWindowFunctionSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  // Common config: force the segment-tree path regardless of partition size
  // (we exercise the fallback explicitly below).
  private val enableSegTree: Map[String, String] = Map(
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
    SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1")

  private val disableSegTree: Map[String, String] = Map(
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false")

  /** Build `f(conf)` twice (enabled / disabled) and assert equal results. */
  private def checkEquivalence(build: () => DataFrame): Unit = {
    val baseline: Array[Row] = withSQLConf(disableSegTree.toSeq: _*) {
      build().collect().sortBy(_.toString)
    }
    withSQLConf(enableSegTree.toSeq: _*) {
      val actual = build().collect().sortBy(_.toString)
      assert(actual.toSeq === baseline.toSeq,
        s"segment-tree output differs from baseline.\nExpected: ${baseline.toSeq}\n" +
          s"Actual:   ${actual.toSeq}")
    }
  }

  /** Standard fixture: 3 partitions, sizes 40/40/40, values = row index. */
  private def baseDF: DataFrame = {
    spark.range(0, 120).selectExpr(
      "id",
      "(id % 3) AS pk",
      "CAST(id AS INT) AS v")
  }

  private def winSpec(lo: Int, hi: Int) =
    Window.partitionBy($"pk").orderBy($"id").rowsBetween(lo, hi)

  // ---------------- A1: basic aggregate equivalence ----------------

  test("MIN over ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", min($"v").over(winSpec(-3, 3)).as("agg")))
  }

  test("MAX over ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", max($"v").over(winSpec(-3, 3)).as("agg")))
  }

  test("SUM over ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", sum($"v").over(winSpec(-3, 3)).as("agg")))
  }

  test("COUNT over ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", count($"v").over(winSpec(-3, 3)).as("agg")))
  }

  test("AVG over ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", avg($"v").over(winSpec(-3, 3)).as("agg")))
  }

  test("MIN + MAX + SUM share a single window frame") {
    checkEquivalence(() =>
      baseDF.select(
        $"id",
        $"pk",
        min($"v").over(winSpec(-3, 3)).as("mn"),
        max($"v").over(winSpec(-3, 3)).as("mx"),
        sum($"v").over(winSpec(-3, 3)).as("sm")))
  }

  // ---------------- A2: frame-size boundaries ----------------

  test("frame size = 1 (CURRENT ROW only)") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", sum($"v").over(winSpec(0, 0)).as("agg")))
  }

  test("frame spans full partition") {
    // 40 rows per partition; use a wide symmetric window covering it.
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", sum($"v").over(winSpec(-100, 100)).as("agg")))
  }

  test("frame extends past both partition edges") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk",
        sum($"v").over(winSpec(-50, 50)).as("agg"),
        min($"v").over(winSpec(-50, 50)).as("mn"),
        max($"v").over(winSpec(-50, 50)).as("mx")))
  }

  
  test("partition below minPartitionRows falls back to SlidingWindowFunctionFrame") {
    // 5-row partition, min threshold = 10 -> must fall back.
    val df = spark.range(0, 5).selectExpr(
      "id", "0 AS pk", "CAST(id AS INT) AS v")
    val enabledConf = Map(
      SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
      SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "10")

    // 1. Result correctness against baseline.
    val baseline = withSQLConf(disableSegTree.toSeq: _*) {
      df.select($"id", sum($"v").over(winSpec(-1, 1)).as("s"))
        .collect().sortBy(_.toString)
    }
    val actual = withSQLConf(enabledConf.toSeq: _*) {
      df.select($"id", sum($"v").over(winSpec(-1, 1)).as("s"))
        .collect().sortBy(_.toString)
    }
    assert(actual.toSeq === baseline.toSeq)

    // 2. Directly exercise the frame to confirm the fallback flag flips.
    withSQLConf(enabledConf.toSeq: _*) {
      SegmentTreeWindowTestHelpers.withSmallPartitionFrame(
        SQLConf.get, rows = 5) { frame =>
        assert(frame.fallbackUsed,
          "expected fallbackUsed=true for partition smaller than minPartitionRows")
      }
    }
  }

  
  test("NTH_VALUE over ROWS frame falls back cleanly (no mergeExpressions crash)") {
    // NthValue extends DeclarativeAggregate but its mergeExpressions throws
    // mergeUnsupportedByWindowFunctionError. eligibleForSegTree must exclude it.
    val df = baseDF
    val withSegTree = withSQLConf(enableSegTree.toSeq: _*) {
      df.selectExpr(
        "id", "pk",
        "nth_value(v, 3) OVER (PARTITION BY pk ORDER BY id " +
          "ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS n3")
        .collect().sortBy(_.toString)
    }
    val baseline = withSQLConf(disableSegTree.toSeq: _*) {
      df.selectExpr(
        "id", "pk",
        "nth_value(v, 3) OVER (PARTITION BY pk ORDER BY id " +
          "ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS n3")
        .collect().sortBy(_.toString)
    }
    assert(withSegTree.toSeq === baseline.toSeq)
  }

  test("ROW_NUMBER over ROWS frame falls back cleanly (no mergeExpressions crash)") {
    val df = baseDF
    val withSegTree = withSQLConf(enableSegTree.toSeq: _*) {
      df.selectExpr(
        "id", "pk",
        "row_number() OVER (PARTITION BY pk ORDER BY id) AS rn")
        .collect().sortBy(_.toString)
    }
    val baseline = withSQLConf(disableSegTree.toSeq: _*) {
      df.selectExpr(
        "id", "pk",
        "row_number() OVER (PARTITION BY pk ORDER BY id) AS rn")
        .collect().sortBy(_.toString)
    }
    assert(withSegTree.toSeq === baseline.toSeq)
  }

    // A3.*: NULL / NaN / Infinity handling
  // A4.*: numeric / string / date-timestamp types
  // various cases: unsupported-merge / DISTINCT / feature-flag fallback
  //
  // All tests use the same oracle strategy as the frame integration: run with
  // `segmentTree.enabled=true` (forced via min-rows=1) and with `=false`,
  // then assert bit-for-bit equal Row sequences. That gives us:
  //   - Correctness: seg-tree output matches SlidingWindowFunctionFrame
  //     (the community-validated baseline).
  //   - Fallback paths: various cases exercise the `eligibleForSegTree` filter,
  //     which must decline to drive the seg-tree path and hand off to the
  //     sliding frame; equal rows prove the hand-off preserves semantics.

  // ---------------- A3: NULL / special values ----------------

  test("all-NULL column: MIN/MAX/SUM/AVG/COUNT") {
    val df = spark.range(0, 30).selectExpr(
      "id", "(id % 3) AS pk", "CAST(NULL AS INT) AS v")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        min($"v").over(winSpec(-3, 3)).as("mn"),
        max($"v").over(winSpec(-3, 3)).as("mx"),
        sum($"v").over(winSpec(-3, 3)).as("sm"),
        avg($"v").over(winSpec(-3, 3)).as("av"),
        count($"v").over(winSpec(-3, 3)).as("cn")))
  }

  test("mixed NULL and non-NULL: NULLs must not leak into MIN/MAX") {
    // Every 3rd value is NULL. Aggregates must skip them (NULL-agnostic merge).
    val df = spark.range(0, 60).selectExpr(
      "id",
      "(id % 3) AS pk",
      "CASE WHEN id % 3 = 0 THEN NULL ELSE CAST(id AS INT) END AS v")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        min($"v").over(winSpec(-4, 4)).as("mn"),
        max($"v").over(winSpec(-4, 4)).as("mx"),
        sum($"v").over(winSpec(-4, 4)).as("sm"),
        count($"v").over(winSpec(-4, 4)).as("cn")))
  }

  test("Double NaN and +/-Infinity propagate correctly through MIN/MAX/SUM") {
    // Spark's NaN ordering: NaN is treated as greater than +Inf for MIN/MAX.
    // +Inf + -Inf = NaN for SUM. The seg-tree path uses DeclarativeAggregate's
    // own merge, so behavior must match the baseline exactly.
    val df = spark.range(0, 30).selectExpr(
      "id",
      "(id % 2) AS pk",
      """CASE
           WHEN id % 7 = 0 THEN double('NaN')
           WHEN id % 7 = 1 THEN double('Infinity')
           WHEN id % 7 = 2 THEN double('-Infinity')
           ELSE CAST(id AS DOUBLE)
         END AS v""")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        min($"v").over(winSpec(-3, 3)).as("mn"),
        max($"v").over(winSpec(-3, 3)).as("mx"),
        sum($"v").over(winSpec(-3, 3)).as("sm")))
  }

  // ---------------- A4: data types ----------------

  test("numeric types: Int / Long / Double / Decimal") {
    val df = spark.range(0, 60).selectExpr(
      "id",
      "(id % 3) AS pk",
      "CAST(id AS INT)             AS vi",
      "CAST(id * 1000000L AS LONG) AS vl",
      "CAST(id AS DOUBLE) + 0.25   AS vd",
      "CAST(id AS DECIMAL(20,4))   AS vdec")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        sum($"vi").over(winSpec(-2, 2)).as("si"),
        min($"vl").over(winSpec(-2, 2)).as("ml"),
        max($"vd").over(winSpec(-2, 2)).as("xd"),
        sum($"vdec").over(winSpec(-2, 2)).as("sdec"),
        avg($"vdec").over(winSpec(-2, 2)).as("adec")))
  }

  test("String lexicographic MIN/MAX") {
    // Deliberately non-monotone string values so that MIN/MAX actually
    // exercise segment-tree merge rather than trivially matching the edge.
    val df = spark.range(0, 40).selectExpr(
      "id",
      "(id % 2) AS pk",
      "CONCAT('s', LPAD(CAST((id * 37) % 97 AS STRING), 3, '0')) AS v")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        min($"v").over(winSpec(-3, 3)).as("mn"),
        max($"v").over(winSpec(-3, 3)).as("mx")))
  }

  test("Date / Timestamp MIN/MAX") {
    val df = spark.range(0, 40).selectExpr(
      "id",
      "(id % 2) AS pk",
      "date_add(DATE'2020-01-01', CAST((id * 13) % 365 AS INT)) AS vd",
      "CAST(TIMESTAMP'2020-01-01 00:00:00' + " +
        "make_interval(0, 0, 0, 0, 0, 0, CAST(id AS DECIMAL(18,6))) AS TIMESTAMP) AS vt")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        min($"vd").over(winSpec(-3, 3)).as("mnd"),
        max($"vd").over(winSpec(-3, 3)).as("mxd"),
        min($"vt").over(winSpec(-3, 3)).as("mnt"),
        max($"vt").over(winSpec(-3, 3)).as("mxt")))
  }

  
  test("collect_list falls back cleanly (non-DeclarativeAggregate)") {
    // collect_list is a Collect(TypedImperativeAggregate) -- not a
    // DeclarativeAggregate, so eligibleForSegTree must decline and the
    // sliding frame must take over.
    checkEquivalence(() =>
      baseDF.select($"id", $"pk",
        collect_list($"v").over(winSpec(-2, 2)).as("lst")))
  }

  test("DISTINCT window aggregate is rejected by analyzer regardless of seg-tree flag") {
    // Spark does not support DISTINCT window aggregates at all -- the analyzer
    // throws DISTINCT_WINDOW_FUNCTION_UNSUPPORTED before we ever reach frame
    // construction. The seg-tree feature flag must not alter this behavior.
    def run(): Unit = {
      baseDF.select($"id", $"pk",
        count_distinct($"v").over(winSpec(-3, 3)).as("cd")).collect()
    }
    withSQLConf(disableSegTree.toSeq: _*) {
      val e = intercept[org.apache.spark.sql.AnalysisException](run())
      assert(e.getMessage.contains("DISTINCT_WINDOW_FUNCTION_UNSUPPORTED"))
    }
    withSQLConf(enableSegTree.toSeq: _*) {
      val e = intercept[org.apache.spark.sql.AnalysisException](run())
      assert(e.getMessage.contains("DISTINCT_WINDOW_FUNCTION_UNSUPPORTED"))
    }
  }

  test("feature flag off: segmentTree.enabled=false yields baseline semantics") {
    // Sanity check: disabling the flag on a workload the seg-tree path would
    // otherwise handle (MIN, wide frame, partitions above the min-rows
    // threshold) still produces the SlidingWindowFunctionFrame answer.
    val df = baseDF
    val expected = withSQLConf(disableSegTree.toSeq: _*) {
      df.select($"id", $"pk", min($"v").over(winSpec(-3, 3)).as("mn"))
        .collect().sortBy(_.toString).toSeq
    }
    // Explicit disable with the full-size partition config (no min-rows
    // override). This exercises the flag-off branch of eligibleForSegTree.
    withSQLConf(
      SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false",
      SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1024") {
      val actual = df.select($"id", $"pk", min($"v").over(winSpec(-3, 3)).as("mn"))
        .collect().sortBy(_.toString).toSeq
      assert(actual === expected)
    }
  }
}
