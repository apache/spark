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
 * End-to-end correctness tests for the segment-tree shrinking-frame path
 * (`... ROWS/RANGE BETWEEN <lower> AND UNBOUNDED FOLLOWING`).
 *
 * Mirrors the structure of [[SegmentTreeWindowFunctionSuite]]: every test
 * runs the same SQL with `spark.sql.window.segmentTree.enabled` off and on
 * and asserts row-set equality. The "off" path runs through
 * [[UnboundedFollowingWindowFunctionFrame]] (the O(N^2) baseline); the "on"
 * path runs through the new shrinking branch in
 * [[SegmentTreeWindowFunctionFrame]] (`ubound = None`).
 */
class UnboundedFollowingSegmentTreeSuite extends SharedSparkSession {

  import testImplicits._

  private val enableSegTree: Map[String, String] = Map(
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
    SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1")

  private val disableSegTree: Map[String, String] = Map(
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false")

  /** Baseline (flag off) vs segtree (flag on); compare row-sets. */
  private def checkEquivalence(build: () => DataFrame): Unit = {
    val baseline: Seq[Row] = withSQLConf(disableSegTree.toSeq: _*) {
      build().collect().toSeq
    }
    withSQLConf(enableSegTree.toSeq: _*) {
      val actual = build().collect().toSeq
      QueryTest.sameRows(baseline, actual, isSorted = false).foreach { err =>
        fail(s"shrinking-frame segtree output differs from baseline.\n$err")
      }
    }
  }

  /** SQL-level variant that accepts a query string. */
  private def checkSqlEquivalence(df: DataFrame, query: String): Unit = {
    df.createOrReplaceTempView("t")
    try {
      val baseline = withSQLConf(disableSegTree.toSeq: _*) {
        spark.sql(query).collect().sortBy(_.toString)
      }
      withSQLConf(enableSegTree.toSeq: _*) {
        val actual = spark.sql(query).collect().sortBy(_.toString)
        assert(actual.toSeq === baseline.toSeq,
          s"shrinking-frame segtree output differs from baseline.\n" +
            s"Expected: ${baseline.toSeq}\nActual:   ${actual.toSeq}")
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

  /** 3 partitions, 40 rows each; values = row index. */
  private def baseDF: DataFrame =
    spark.range(0, 120).selectExpr(
      "id",
      "(id % 3) AS pk",
      "CAST(id AS INT) AS v")

  /** Shrinking ROWS frame: [lo, end-of-partition). */
  private def shrinkingRowsFrame(lo: Int) =
    Window.partitionBy($"pk").orderBy($"id")
      .rowsBetween(lo, Window.unboundedFollowing)

  // ============================================================
  // ROWS frame: basic aggregate equivalence (CURRENT ROW lower)
  // ============================================================

  test("MIN over ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", min($"v").over(shrinkingRowsFrame(0)).as("agg")))
  }

  test("MAX over ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", max($"v").over(shrinkingRowsFrame(0)).as("agg")))
  }

  test("SUM over ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", sum($"v").over(shrinkingRowsFrame(0)).as("agg")))
  }

  test("COUNT over ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", count($"v").over(shrinkingRowsFrame(0)).as("agg")))
  }

  test("AVG over ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", avg($"v").over(shrinkingRowsFrame(0)).as("agg")))
  }

  // ============================================================
  // ROWS frame: lower-bound variations
  // ============================================================

  test("ROWS BETWEEN 5 PRECEDING AND UNBOUNDED FOLLOWING (suffix + lookback)") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", sum($"v").over(shrinkingRowsFrame(-5)).as("agg")))
  }

  test("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING is NOT this path") {
    // Both-unbounded routes to UnboundedWindowFunctionFrame (different case
    // in the dispatcher) and is one-shot O(1). This test just verifies the
    // segtree flag doesn't break it.
    val frame = Window.partitionBy($"pk").orderBy($"id")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", sum($"v").over(frame).as("agg")))
  }

  test("ROWS BETWEEN 5 FOLLOWING AND UNBOUNDED FOLLOWING (lower bound is positive)") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", sum($"v").over(shrinkingRowsFrame(5)).as("agg")))
  }

  // ============================================================
  // Multi-aggregate: shared frame
  // ============================================================

  test("MIN + MAX + SUM share a single shrinking frame") {
    checkEquivalence(() =>
      baseDF.select(
        $"id", $"pk",
        min($"v").over(shrinkingRowsFrame(0)).as("mn"),
        max($"v").over(shrinkingRowsFrame(0)).as("mx"),
        sum($"v").over(shrinkingRowsFrame(0)).as("s")))
  }

  // ============================================================
  // Partition / boundary edge cases
  // ============================================================

  test("single-row partition") {
    val df = spark.range(0, 5).selectExpr("id", "id AS pk", "CAST(id AS INT) AS v")
    checkEquivalence(() =>
      df.select($"id", $"pk", sum($"v").over(shrinkingRowsFrame(0)).as("agg")))
  }

  test("empty result table (no rows)") {
    val df = spark.emptyDataFrame.selectExpr("CAST(NULL AS BIGINT) AS id",
      "CAST(NULL AS BIGINT) AS pk", "CAST(NULL AS INT) AS v")
      .where("id IS NOT NULL")
    checkEquivalence(() =>
      df.select($"id", $"pk", sum($"v").over(shrinkingRowsFrame(0)).as("agg")))
  }

  test("partition below minPartitionRows falls back to UnboundedFollowingWindowFunctionFrame") {
    // With minRows=1024 the segtree path forces fallback; baseline (off) and
    // forced-fallback (on, but min=1024) must match. The point is that the
    // small-partition path goes through the legacy frame, not segtree.
    val df = baseDF
    val baseline = withSQLConf(disableSegTree.toSeq: _*) {
      df.select($"id", $"pk", sum($"v").over(shrinkingRowsFrame(0)).as("s"))
        .collect().toSeq
    }
    withSQLConf(
      SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
      SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1024") {
      val actual = df.select($"id", $"pk", sum($"v").over(shrinkingRowsFrame(0)).as("s"))
        .collect().toSeq
      QueryTest.sameRows(baseline, actual, isSorted = false).foreach { err =>
        fail(s"forced-fallback path diverges from baseline.\n$err")
      }
    }
  }

  // ============================================================
  // NULL / NaN / numeric edge cases
  // ============================================================

  test("all-NULL column: SUM/MIN/MAX/AVG/COUNT") {
    val df = spark.range(0, 30).selectExpr("id", "(id % 3) AS pk",
      "CAST(NULL AS INT) AS v")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        sum($"v").over(shrinkingRowsFrame(0)).as("s"),
        min($"v").over(shrinkingRowsFrame(0)).as("mn"),
        max($"v").over(shrinkingRowsFrame(0)).as("mx"),
        avg($"v").over(shrinkingRowsFrame(0)).as("a"),
        count($"v").over(shrinkingRowsFrame(0)).as("c")))
  }

  test("mixed NULL and non-NULL: NULLs must not leak into MIN/MAX") {
    val df = (0 until 60).map { i =>
      val v: Option[Int] = if (i % 4 == 0) None else Some(i)
      (i.toLong, (i % 3).toLong, v)
    }.toDF("id", "pk", "v")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        min($"v").over(shrinkingRowsFrame(0)).as("mn"),
        max($"v").over(shrinkingRowsFrame(0)).as("mx"),
        sum($"v").over(shrinkingRowsFrame(0)).as("s"),
        count($"v").over(shrinkingRowsFrame(0)).as("c")))
  }

  test("Double NaN and +/-Infinity propagate correctly through MIN/MAX/SUM") {
    val df = Seq(
      (0L, 0L, 1.0d), (1L, 0L, Double.NaN), (2L, 0L, 3.0d),
      (3L, 0L, Double.PositiveInfinity), (4L, 0L, 5.0d),
      (5L, 0L, Double.NegativeInfinity), (6L, 0L, 7.0d), (7L, 0L, 9.0d)
    ).toDF("id", "pk", "v")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        min($"v").over(shrinkingRowsFrame(0)).as("mn"),
        max($"v").over(shrinkingRowsFrame(0)).as("mx"),
        sum($"v").over(shrinkingRowsFrame(0)).as("s")))
  }

  // ============================================================
  // Type coverage
  // ============================================================

  test("numeric types: Int / Long / Double / Decimal") {
    val df = spark.range(0, 60).selectExpr(
      "id",
      "(id % 3) AS pk",
      "CAST(id AS INT) AS vi",
      "id * 1000000000L AS vl",
      "CAST(id AS DOUBLE) * 1.5 AS vd",
      "CAST(id AS DECIMAL(20, 5)) AS vdec")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        sum($"vi").over(shrinkingRowsFrame(0)).as("si"),
        sum($"vl").over(shrinkingRowsFrame(0)).as("sl"),
        sum($"vd").over(shrinkingRowsFrame(0)).as("sd"),
        sum($"vdec").over(shrinkingRowsFrame(0)).as("sdec")))
  }

  test("String lexicographic MIN/MAX") {
    val df = spark.range(0, 30).selectExpr(
      "id",
      "(id % 3) AS pk",
      "CONCAT('s', LPAD(CAST((id * 7) % 31 AS STRING), 3, '0')) AS v")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        min($"v").over(shrinkingRowsFrame(0)).as("mn"),
        max($"v").over(shrinkingRowsFrame(0)).as("mx")))
  }

  test("Date / Timestamp MIN/MAX") {
    val df = spark.range(0, 24).selectExpr(
      "id",
      "(id % 3) AS pk",
      "DATE_ADD(DATE'2024-01-01', CAST(id AS INT)) AS d",
      "TIMESTAMPADD(HOUR, CAST(id AS INT), TIMESTAMP'2024-01-01 00:00:00') AS ts")
    checkEquivalence(() =>
      df.select($"id", $"pk",
        min($"d").over(shrinkingRowsFrame(0)).as("dmn"),
        max($"ts").over(shrinkingRowsFrame(0)).as("tsmx")))
  }

  // ============================================================
  // Allow-list: non-DeclarativeAggregate paths must fall back
  // ============================================================

  test("collect_list falls back cleanly (non-DeclarativeAggregate)") {
    // collect_list is ImperativeAggregate; segtree path must not engage.
    // The result should still be correct via the legacy frame.
    checkEquivalence(() =>
      baseDF.select($"id", $"pk",
        collect_list($"v").over(shrinkingRowsFrame(0)).as("lst")))
  }

  test("DISTINCT shrinking aggregate is rejected by analyzer regardless of seg-tree flag") {
    def run(): Unit = {
      baseDF.select($"id", $"pk",
        count_distinct($"v").over(shrinkingRowsFrame(0)).as("cd")).collect()
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

  // ============================================================
  // RANGE shrinking frame (single-order-expr)
  // ============================================================

  test("RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING with non-uniform gaps") {
    val df = spark.range(0, 40).selectExpr(
      "CAST(id AS INT) AS id",
      "(CAST(id AS INT) % 2) AS pk",
      "CAST(CASE CAST(id AS INT) % 7 " +
        "WHEN 0 THEN 1 WHEN 1 THEN 3 WHEN 2 THEN 4 WHEN 3 THEN 4 " +
        "WHEN 4 THEN 7 WHEN 5 THEN 10 ELSE 15 END + (CAST(id AS INT) / 7) * 20 AS INT) AS k",
      "CAST((id * 31) % 97 AS INT) AS v")
    checkSqlEquivalence(df,
      """SELECT id, pk,
        |  MIN(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS mn,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS mx
        |FROM t""".stripMargin)
  }

  test("RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING") {
    val df = spark.range(0, 40).selectExpr(
      "CAST(id AS INT) AS id",
      "(CAST(id AS INT) % 2) AS pk",
      "CAST((id * 7) % 31 AS INT) AS k",
      "CAST((id * 11) % 53 AS INT) AS v")
    checkSqlEquivalence(df,
      """SELECT id, pk, k,
        |  SUM(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS s
        |FROM t""".stripMargin)
  }

  test("RANGE with tie (duplicate order keys): full tie group at lower edge") {
    // Trap: at the lower edge, the FULL tie group at the lower offset must
    // be retained, not just the first row.
    val rows = (0 until 40).map { i =>
      val k = Seq(1, 2, 2, 2, 3, 4, 5)(i % 7)
      (i, i % 2, k, (i * 13) % 41)
    }
    val df = rows.toDF("id", "pk", "k", "v")
    checkSqlEquivalence(df,
      """SELECT id, pk, k,
        |  MIN(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS mn,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS mx
        |FROM t""".stripMargin)
  }

  test("RANGE with NULL order key (NULLS FIRST / NULLS LAST)") {
    val rows = (0 until 36).map { i =>
      val kOpt: Option[Int] = (i % 6) match {
        case 0 | 1 | 5 => None
        case 2 => Some(1)
        case 3 => Some(2)
        case _ => Some(3)
      }
      (i, i % 2, kOpt, (i * 11) % 37)
    }
    val df = rows.toDF("id", "pk", "k", "v")
    checkSqlEquivalence(df,
      """SELECT id, pk,
        |  MIN(v) OVER (PARTITION BY pk ORDER BY k ASC NULLS FIRST
        |    RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS mn_nf,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY k ASC NULLS LAST
        |    RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS mx_nl
        |FROM t""".stripMargin)
  }

  test("RANGE Timestamp with INTERVAL offset (MAX) and shrinking upper") {
    val df = spark.range(0, 30).selectExpr(
      "CAST(id AS INT) AS id",
      "(CAST(id AS INT) % 2) AS pk",
      "CAST(TIMESTAMP'2024-01-01 10:00:00' + " +
        "make_interval(0, 0, 0, 0, 0, 30 * CAST(id AS INT) * " +
        "(CASE CAST(id AS INT) % 3 WHEN 0 THEN 1 WHEN 1 THEN 3 ELSE 4 END), 0) " +
        "AS TIMESTAMP) AS ts",
      "CAST((id * 17) % 53 AS INT) AS v")
    checkSqlEquivalence(df,
      """SELECT id, pk,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY ts
        |    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND UNBOUNDED FOLLOWING) AS mx
        |FROM t""".stripMargin)
  }

  // ============================================================
  // Feature-flag off: legacy frame is used
  // ============================================================

  test("feature flag off: segmentTree.enabled=false yields baseline semantics") {
    val df = baseDF
    val expected = withSQLConf(disableSegTree.toSeq: _*) {
      df.select($"id", $"pk", min($"v").over(shrinkingRowsFrame(0)).as("mn"))
        .collect().sortBy(_.toString).toSeq
    }
    withSQLConf(
      SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false",
      SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1024") {
      val actual = df.select($"id", $"pk", min($"v").over(shrinkingRowsFrame(0)).as("mn"))
        .collect().sortBy(_.toString).toSeq
      assert(actual === expected)
    }
  }
}
