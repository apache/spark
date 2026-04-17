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
 * Covers test-plan.md the frame integration: various cases (basic aggregates), various cases
 * (frame-size boundaries), min-partition-rows fallback.
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
}
