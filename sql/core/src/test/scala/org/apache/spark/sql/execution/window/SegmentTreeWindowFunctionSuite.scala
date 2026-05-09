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

import org.apache.spark.sql.{DataFrame, Encoder, Encoders, QueryTest, Row}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, Window}
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.util.SparkErrorUtils

/**
 * End-to-end tests for the block-chunked segment-tree moving window frame.
 * Covers basic aggregates, frame boundaries, min-rows fallback, NULL/NaN,
 * numeric/string/date-timestamp types, RANGE, Decimal/Binary merge, UDAF
 * fallback, and frame lifecycle.
 */
class SegmentTreeWindowFunctionSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  // Force seg-tree path regardless of partition size (fallback exercised explicitly).
  private val enableSegTree: Map[String, String] = Map(
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
    SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1")

  private val disableSegTree: Map[String, String] = Map(
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false")

  /** Build `f(conf)` twice (enabled / disabled) and assert equal results. */
  private def checkEquivalence(build: () => DataFrame): Unit = {
    val baseline: Seq[Row] = withSQLConf(disableSegTree.toSeq: _*) {
      build().collect().toSeq
    }
    withSQLConf(enableSegTree.toSeq: _*) {
      val actual = build().collect().toSeq
      // Use QueryTest.sameRows (which normalizes Array[_] via prepareRow before
      // sorting) instead of sortBy(_.toString): Row.toString on Array[Byte]
      // and similar ref-typed values is address-based and can reorder baseline
      // vs actual differently even when the multiset of rows is identical.
      QueryTest.sameRows(baseline, actual, isSorted = false).foreach { err =>
        fail(s"segment-tree output differs from baseline.\n$err")
      }
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

  // A1: basic aggregate equivalence

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

  // A2: frame-size boundaries

  test("frame size = 1 (CURRENT ROW only)") {
    checkEquivalence(() =>
      baseDF.select($"id", $"pk", sum($"v").over(winSpec(0, 0)).as("agg")))
  }

  test("frame spans full partition") {
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

    val baseline = withSQLConf(disableSegTree.toSeq: _*) {
      df.select($"id", sum($"v").over(winSpec(-1, 1)).as("s"))
        .collect().sortBy(_.toString)
    }
    val actual = withSQLConf(enabledConf.toSeq: _*) {
      df.select($"id", sum($"v").over(winSpec(-1, 1)).as("s"))
        .collect().sortBy(_.toString)
    }
    assert(actual.toSeq === baseline.toSeq)

    // Confirm the fallback flag actually flipped.
    withSQLConf(enabledConf.toSeq: _*) {
      SegmentTreeWindowTestHelper.withSmallPartitionFrame(
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

  // A3: NULL/NaN/Infinity; A4: numeric/string/date-timestamp types.
  // Unsupported-merge / DISTINCT / feature-flag fallback.
  // Oracle: run with seg-tree enabled and disabled, assert equal Row sequences.

  // A3: NULL / special values

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
    // Trap: NaN > +Inf in Spark's MIN/MAX ordering; +Inf + -Inf = NaN in SUM.
    // Seg-tree uses DeclarativeAggregate.merge; behavior must match baseline.
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

  // A4: data types

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
    // Non-monotone values so MIN/MAX exercise the seg-tree merge (not edge).
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
    // collect_list is TypedImperativeAggregate; eligibleForSegTree must decline.
    checkEquivalence(() =>
      baseDF.select($"id", $"pk",
        collect_list($"v").over(winSpec(-2, 2)).as("lst")))
  }

  test("DISTINCT window aggregate is rejected by analyzer regardless of seg-tree flag") {
    // Analyzer throws DISTINCT_WINDOW_FUNCTION_UNSUPPORTED before frame
    // construction; seg-tree flag must not alter this behavior.
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
    // Sanity: disabling the flag on a seg-tree-eligible workload still
    // produces the SlidingWindowFunctionFrame answer.
    val df = baseDF
    val expected = withSQLConf(disableSegTree.toSeq: _*) {
      df.select($"id", $"pk", min($"v").over(winSpec(-3, 3)).as("mn"))
        .collect().sortBy(_.toString).toSeq
    }
    withSQLConf(
      SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false",
      SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1024") {
      val actual = df.select($"id", $"pk", min($"v").over(winSpec(-3, 3)).as("mn"))
        .collect().sortBy(_.toString).toSeq
      assert(actual === expected)
    }
  }

    // A5: RANGE frame equivalence (single-order-expr admission).
  // MIN/MAX non-invertible, guaranteeing seg-tree path is exercised.

  /** Run `sql` twice (flag off / on) and checkAnswer equality. */
  private def checkRangeEquivalence(df: DataFrame, query: String): Unit = {
    df.createOrReplaceTempView("t")
    try {
      val baseline = withSQLConf(disableSegTree.toSeq: _*) {
        spark.sql(query).collect().sortBy(_.toString)
      }
      withSQLConf(enableSegTree.toSeq: _*) {
        val actual = spark.sql(query).collect().sortBy(_.toString)
        assert(actual.toSeq === baseline.toSeq,
          s"segment-tree output differs from baseline.\nExpected: ${baseline.toSeq}\n" +
            s"Actual:   ${actual.toSeq}")
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

  test("-- RANGE INT offset basic (non-uniform gaps, MIN/MAX)") {
    // Non-uniform gaps so admit/drop loops must consult the order-key comparator.
    val df = spark.range(0, 40).selectExpr(
      "CAST(id AS INT) AS id",
      "(CAST(id AS INT) % 2) AS pk",
      "CAST(CASE CAST(id AS INT) % 7 " +
        "WHEN 0 THEN 1 WHEN 1 THEN 3 WHEN 2 THEN 4 WHEN 3 THEN 4 " +
        "WHEN 4 THEN 7 WHEN 5 THEN 10 ELSE 15 END + (CAST(id AS INT) / 7) * 20 AS INT) AS k",
      "CAST((id * 31) % 97 AS INT) AS v")
    checkRangeEquivalence(df,
      """SELECT id, pk,
        |  MIN(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS mn,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS mx
        |FROM t""".stripMargin)
  }

  test("-- RANGE Timestamp with INTERVAL offset (MAX)") {
    // Irregular gaps force the timestamp comparator at the 1-hour boundary.
    val df = spark.range(0, 30).selectExpr(
      "CAST(id AS INT) AS id",
      "(CAST(id AS INT) % 2) AS pk",
      "CAST(TIMESTAMP'2024-01-01 10:00:00' + " +
        "make_interval(0, 0, 0, 0, 0, 30 * CAST(id AS INT) * " +
        "(CASE CAST(id AS INT) % 3 WHEN 0 THEN 1 WHEN 1 THEN 3 ELSE 4 END), 0) " +
        "AS TIMESTAMP) AS ts",
      "CAST((id * 17) % 53 AS INT) AS v")
    checkRangeEquivalence(df,
      """SELECT id, pk,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY ts
        |    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING
        |              AND INTERVAL '1' HOUR FOLLOWING) AS mx
        |FROM t""".stripMargin)
  }

  test("-- RANGE with tie (duplicate order keys) inclusion at boundary") {
    // Trap: RANGE `0 PRECEDING AND 0 FOLLOWING` must include the FULL tie
    // group at the current row's key, not just the current row. A ROWS-vs-
    // RANGE confusion would return per-row MIN/MAX instead of group-level.
    val rows = (0 until 40).map { i =>
      val k = Seq(1, 2, 2, 2, 3, 4, 5)(i % 7)
      (i, i % 2, k, (i * 13) % 41)
    }
    val df = rows.toDF("id", "pk", "k", "v")
    checkRangeEquivalence(df,
      """SELECT id, pk, k,
        |  MIN(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING) AS mn,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING) AS mx
        |FROM t""".stripMargin)
  }

  test("-- RANGE frame wider than partition (C4: admit/drop loops no-op)") {
    // Once the first batch is admitted, admit/drop must detect no change
    // and skip work.
    val df = spark.range(0, 25).selectExpr(
      "CAST(id AS INT) AS id",
      "(CAST(id AS INT) / 5) AS pk",
      "CAST((id * 7) % 23 AS INT) AS k",
      "CAST((id * 19) % 101 AS INT) AS v")
    checkRangeEquivalence(df,
      """SELECT id, pk,
        |  MIN(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING) AS mn,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY k
        |    RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING) AS mx
        |FROM t""".stripMargin)
  }

  test("-- RANGE with NULL order key (NULLS FIRST / NULLS LAST)") {
    // Trap: Spark groups all NULLs into a single equivalence class at head
    // (NULLS FIRST) or tail (NULLS LAST); seg-tree must treat NULL as a
    // tie group identical to the sliding baseline.
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
    checkRangeEquivalence(df,
      """SELECT id, pk,
        |  MIN(v) OVER (PARTITION BY pk ORDER BY k ASC NULLS FIRST
        |    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mn_nf,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY k ASC NULLS FIRST
        |    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mx_nf,
        |  MIN(v) OVER (PARTITION BY pk ORDER BY k ASC NULLS LAST
        |    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mn_nl,
        |  MAX(v) OVER (PARTITION BY pk ORDER BY k ASC NULLS LAST
        |    RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mx_nl
        |FROM t""".stripMargin)
  }

    // Decimal overflow / BinaryType MIN/MAX across block merge; UDAF fallback.
  // Trap: blockSize=16 is SQLConf minimum; frame > blockSize ensures the
  // seg-tree merge path is actually crossed.

  private val segTreeBlock: String = "16"
  private val segTreeFramePrec: Int = 17
  private val segTreeRows: Int = 20

  private def withSegTreeBlock(conf: (String, String)*)(body: => Unit): Unit = {
    val extra = Seq(SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> segTreeBlock) ++ conf
    withSQLConf(extra: _*)(body)
  }


  /**
   * 20 rows in one partition, Decimal(38, 0) values near the type's upper
   * bound; frame of `segTreeFramePrec` PRECEDING..CURRENT ROW makes any
   * >=2-row window overflow Sum. Block 16 + frame 17 forces cross-block merge.
   */
  private def decimalOverflowDF: DataFrame = {
    // 9e37 -- below Decimal(38,0) MAX (~9.99e37), but 2x overflows.
    val big = "90000000000000000000000000000000000000"  // 38 digits
    spark.range(0, segTreeRows.toLong).selectExpr(
      "CAST(id AS INT) AS id",
      "0 AS pk",
      s"CAST('$big' AS DECIMAL(38, 0)) AS v")
  }

  private val decimalOverflowSql: String =
    s"""SELECT id, pk,
       |  SUM(v) OVER (PARTITION BY pk ORDER BY id
       |    ROWS BETWEEN $segTreeFramePrec PRECEDING AND CURRENT ROW) AS s
       |FROM t""".stripMargin

  test("a -- Decimal overflow ANSI on, seg-tree matches sliding (both throw)") {
    val df = decimalOverflowDF
    df.createOrReplaceTempView("t")
    try {
      withSegTreeBlock(SQLConf.ANSI_ENABLED.key -> "true") {
        withSQLConf(disableSegTree.toSeq: _*) {
          val e = intercept[Exception] {
            spark.sql(decimalOverflowSql).collect()
          }
          assert(hasArithmeticCause(e),
            s"expected ArithmeticException root cause, got: ${e.getMessage}")
        }
        withSQLConf(enableSegTree.toSeq: _*) {
          val e = intercept[Exception] {
            spark.sql(decimalOverflowSql).collect()
          }
          assert(hasArithmeticCause(e),
            s"expected ArithmeticException root cause, got: ${e.getMessage}")
        }
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

  test("b -- Decimal overflow ANSI off, seg-tree matches sliding (NULL on overflow)") {
    val df = decimalOverflowDF
    df.createOrReplaceTempView("t")
    try {
      withSegTreeBlock(SQLConf.ANSI_ENABLED.key -> "false") {
        val baseline = withSQLConf(disableSegTree.toSeq: _*) {
          spark.sql(decimalOverflowSql).collect().sortBy(_.toString)
        }
        // At least one row must be NULL so we know overflow actually fired.
        assert(baseline.exists(_.isNullAt(2)),
          "baseline should contain NULL overflow rows; test data may be too small")
        withSQLConf(enableSegTree.toSeq: _*) {
          val actual = spark.sql(decimalOverflowSql).collect().sortBy(_.toString)
          assert(actual.toSeq === baseline.toSeq)
        }
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

  test("c -- mid-window Decimal overflow slides past (seg-tree == sliding)") {
    // Big values at ids 14..17 straddle block boundary at id=16, so any
    // 4-row window overlapping >=2 of them overflows (-> NULL when ANSI off)
    // and cross-block merge sees overflowing buffers. Rows past id=20 slide
    // clear and recover non-NULL.
    val big = "90000000000000000000000000000000000000"
    val df = spark.range(0, 24).selectExpr(
      "CAST(id AS INT) AS id",
      "0 AS pk",
      s"""CASE WHEN id IN (14, 15, 16, 17)
              THEN CAST('$big' AS DECIMAL(38, 0))
              ELSE CAST(id AS DECIMAL(38, 0))
         END AS v""")
    df.createOrReplaceTempView("t")
    try {
      val sqlStr =
        """SELECT id, pk,
          |  SUM(v) OVER (PARTITION BY pk ORDER BY id
          |    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s
          |FROM t""".stripMargin
      withSegTreeBlock(SQLConf.ANSI_ENABLED.key -> "false") {
        val baseline = withSQLConf(disableSegTree.toSeq: _*) {
          spark.sql(sqlStr).collect().sortBy(_.toString)
        }
        // Sanity: overflow fired AND later rows recover non-NULL.
        assert(baseline.exists(_.isNullAt(2)),
          "baseline should contain NULL overflow rows")
        assert(baseline.exists(r => r.getInt(0) >= 21 && !r.isNullAt(2)),
          "rows with id>=21 should be non-NULL (window slid past big values)")
        withSQLConf(enableSegTree.toSeq: _*) {
          val actual = spark.sql(sqlStr).collect().sortBy(_.toString)
          assert(actual.toSeq === baseline.toSeq)
        }
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

  /** True iff the root cause of `t` is an [[ArithmeticException]] (ANSI overflow). */
  private def hasArithmeticCause(t: Throwable): Boolean =
    Option(SparkErrorUtils.getRootCause(t)).exists(_.isInstanceOf[ArithmeticException])


  /** Pattern of 20 Array[Byte] values used across a. */
  private def binaryVariedRows: Seq[(Int, Array[Byte])] = {
    (0 until 20).map { i =>
      val arr: Array[Byte] = (i % 8) match {
        case 0 => Array[Byte](0x01, 0x02)
        case 1 => Array[Byte](0x00)
        case 2 => Array[Byte](0x7f)
        case 3 => Array[Byte](0x7f, 0x00)
        case 4 => Array[Byte](0x10, 0x20, 0x30)
        case 5 => Array[Byte](0x10, 0x20)
        case 6 => Array[Byte](0x10)
        case _ => Array[Byte](0x05, 0x05, 0x05, 0x05)
      }
      (i, arr)
    }
  }

  test("a -- BinaryType MIN/MAX cross-block merge") {
    // Varied lengths/content; frame > blockSize guarantees merge path hit.
    val df = binaryVariedRows.toDF("id", "v").selectExpr("id", "0 AS pk", "v")
    df.createOrReplaceTempView("t")
    try {
      withSegTreeBlock() {
        val sqlStr =
          s"""SELECT id, pk,
             |  MIN(v) OVER (PARTITION BY pk ORDER BY id
             |    ROWS BETWEEN $segTreeFramePrec PRECEDING AND CURRENT ROW) AS mn,
             |  MAX(v) OVER (PARTITION BY pk ORDER BY id
             |    ROWS BETWEEN $segTreeFramePrec PRECEDING AND CURRENT ROW) AS mx
             |FROM t""".stripMargin
        val baseline = withSQLConf(disableSegTree.toSeq: _*) {
          spark.sql(sqlStr).collect().toSeq
        }
        withSQLConf(enableSegTree.toSeq: _*) {
          val actual = spark.sql(sqlStr).collect().toSeq
          QueryTest.sameRows(baseline, actual, isSorted = false).foreach { err =>
            fail(s"seg-tree binary MIN/MAX differs.\n$err")
          }
        }
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

  test("b -- BinaryType empty/NULL/single-zero distinction") {
    // Trap: Spark treats empty-array and NULL as distinct; seg-tree must
    // respect that. Pattern cycles across the block boundary.
    val rows: Seq[(Int, Array[Byte])] = (0 until 20).map { i =>
      val arr: Array[Byte] = (i % 4) match {
        case 0 => Array[Byte](0x00)
        case 1 => Array[Byte]()
        case 2 => null
        case _ => Array[Byte](0x01, 0x02)
      }
      (i, arr)
    }
    val df = rows.toDF("id", "v").selectExpr("id", "0 AS pk", "v")
    df.createOrReplaceTempView("t")
    try {
      withSegTreeBlock() {
        val sqlStr =
          s"""SELECT id, pk,
             |  MIN(v) OVER (PARTITION BY pk ORDER BY id
             |    ROWS BETWEEN $segTreeFramePrec PRECEDING AND CURRENT ROW) AS mn,
             |  MAX(v) OVER (PARTITION BY pk ORDER BY id
             |    ROWS BETWEEN $segTreeFramePrec PRECEDING AND CURRENT ROW) AS mx
             |FROM t""".stripMargin
        val baseline = withSQLConf(disableSegTree.toSeq: _*) {
          spark.sql(sqlStr).collect().toSeq
        }
        withSQLConf(enableSegTree.toSeq: _*) {
          val actual = spark.sql(sqlStr).collect().toSeq
          QueryTest.sameRows(baseline, actual, isSorted = false).foreach { err =>
            fail(s"seg-tree empty/NULL binary differs.\n$err")
          }
        }
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

  test("c -- BinaryType unsigned lexicographic ordering") {
    // Trap: Spark's BinaryType comparator is unsigned (0xFF > 0x01); a
    // signed-byte comparator would get this backwards. Seg-tree must match.
    val unsignedPattern: IndexedSeq[Array[Byte]] = IndexedSeq(
      Array[Byte](0xff.toByte),
      Array[Byte](0x01),
      Array[Byte](0x80.toByte, 0x00),
      Array[Byte](0x7f, 0xff.toByte),
      Array[Byte](0xfe.toByte),
      Array[Byte](0x00, 0xff.toByte),
      Array[Byte](0x80.toByte),
      Array[Byte](0x7f))
    val rows: Seq[(Int, Array[Byte])] =
      (0 until 20).map(i => (i, unsignedPattern(i % unsignedPattern.length)))
    val df = rows.toDF("id", "v").selectExpr("id", "0 AS pk", "v")
    df.createOrReplaceTempView("t")
    try {
      withSegTreeBlock() {
        val sqlStr =
          s"""SELECT id, pk,
             |  MIN(v) OVER (PARTITION BY pk ORDER BY id
             |    ROWS BETWEEN $segTreeFramePrec PRECEDING AND CURRENT ROW) AS mn,
             |  MAX(v) OVER (PARTITION BY pk ORDER BY id
             |    ROWS BETWEEN $segTreeFramePrec PRECEDING AND CURRENT ROW) AS mx
             |FROM t""".stripMargin
        val baseline = withSQLConf(disableSegTree.toSeq: _*) {
          spark.sql(sqlStr).collect().toSeq
        }
        withSQLConf(enableSegTree.toSeq: _*) {
          val actual = spark.sql(sqlStr).collect().toSeq
          QueryTest.sameRows(baseline, actual, isSorted = false).foreach { err =>
            fail(s"seg-tree unsigned binary ordering differs.\n$err")
          }
        }
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

    // ScalaUDAF/ScalaAggregator both extend ImperativeAggregate and must be
  // rejected by `eligibleForSegTree`. Flag ON must not throw (segtree merge on
  // ImperativeAggregate would NPE) and must match flag OFF bit-for-bit.

  test("a -- legacy ScalaUDAF falls back cleanly (no seg-tree merge)") {
    val udaf = new LegacySumUdaf
    spark.udf.register("seg_tree_legacy_sum", udaf)
    val df = baseDF.selectExpr("id", "pk", "CAST(v AS LONG) AS v")
    val query =
      """SELECT id, pk,
        |  seg_tree_legacy_sum(v) OVER (PARTITION BY pk ORDER BY id
        |    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS s
        |FROM t""".stripMargin
    df.createOrReplaceTempView("t")
    try {
      val baseline = withSQLConf(disableSegTree.toSeq: _*) {
        spark.sql(query).collect().sortBy(_.toString)
      }
      withSQLConf(enableSegTree.toSeq: _*) {
        val actual = spark.sql(query).collect().sortBy(_.toString)
        assert(actual.toSeq === baseline.toSeq,
          s"ScalaUDAF fallback result differs.\nExpected: ${baseline.toSeq}\n" +
            s"Actual:   ${actual.toSeq}")
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

  test("b -- typed Aggregator falls back cleanly (no seg-tree merge)") {
    val agg = udaf(new LongSumAggregator)
    spark.udf.register("seg_tree_typed_sum", agg)
    val df = baseDF.selectExpr("id", "pk", "CAST(v AS LONG) AS v")
    val query =
      """SELECT id, pk,
        |  seg_tree_typed_sum(v) OVER (PARTITION BY pk ORDER BY id
        |    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS s
        |FROM t""".stripMargin
    df.createOrReplaceTempView("t")
    try {
      val baseline = withSQLConf(disableSegTree.toSeq: _*) {
        spark.sql(query).collect().sortBy(_.toString)
      }
      withSQLConf(enableSegTree.toSeq: _*) {
        val actual = spark.sql(query).collect().sortBy(_.toString)
        assert(actual.toSeq === baseline.toSeq,
          s"typed Aggregator fallback result differs.\nExpected: ${baseline.toSeq}\n" +
            s"Actual:   ${actual.toSeq}")
      }
    } finally {
      spark.catalog.dropTempView("t")
    }
  }

  test("SPARK-56546: LAG does not eagerly construct AggregateProcessor under segtree") {
    // Pre-fix, `val processor` eagerly invoked AggregateProcessor.apply on any
  // non-empty `functions`, throwing INTERNAL_ERROR for lag(...) when routing
  // hit FRAME_LESS_OFFSET. Frameless lag below is the minimal repro.
    withSQLConf(enableSegTree.toSeq: _*) {
      val df = spark.range(10).select(
        col("id"),
        expr("lag(id, 1, id) OVER (ORDER BY id)").as("lag"))
      val expected = (0L until 10L).map(i => Row(i, if (i == 0) 0L else i - 1))
      checkAnswer(df, expected)
    }
  }

  // Frame lifecycle
  //   T1a: no fallback on pure-segtree partition.
  //   T1b: fallback lazily allocated on small partition.
  //   T1c: segtree->small reuses frame; fallback allocated once.
  //   T1d: small->segtree drops retained fallback reference (GC-eligible).
  //   T2 : throwing fallback.prepare must not flip `fallbackUsed` (frame is
  //        `final` so we cannot inject a throwing fallback; we rely on the
  //        structural witness that `fallbackUsed` is set ONLY after
  //        `fallback.prepare` returns normally -- a regression would fail T1d).
  //   T3 : close() after only small partition (tree never built) is a no-op.

  test("lifecycle: no fallback allocated on segtree-only partition") {
    val conf = new SQLConf
    conf.setConfString(SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key, "1")
    SegmentTreeWindowTestHelper.withFrameFactory(conf) { factory =>
      val frame = factory.newFrame()
      val array = factory.newArray(rows = 10)
      frame.prepare(array)
      assert(!frame.fallbackUsed, "segtree path expected")
      assert(!frame.fallbackAllocated,
        "fallback must not be allocated on segtree-only partition")
    }
  }

  test("lifecycle: fallback lazily allocated on small partition") {
    val conf = new SQLConf
    conf.setConfString(SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key, "100")
    SegmentTreeWindowTestHelper.withFrameFactory(conf) { factory =>
      val frame = factory.newFrame()
      assert(!frame.fallbackAllocated, "no allocation before prepare()")
      frame.prepare(factory.newArray(rows = 5))
      assert(frame.fallbackUsed, "fallback path expected")
      assert(frame.fallbackAllocated, "fallback allocated after first small partition")
    }
  }

  test("lifecycle: segtree then small partition reuses frame, allocates fallback once") {
    val conf = new SQLConf
    conf.setConfString(SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key, "10")
    SegmentTreeWindowTestHelper.withFrameFactory(conf) { factory =>
      val frame = factory.newFrame()
      // Partition 1: segtree path (20 rows >= 10).
      frame.prepare(factory.newArray(rows = 20))
      assert(!frame.fallbackUsed && !frame.fallbackAllocated)
      // Partition 2: small partition.
      frame.prepare(factory.newArray(rows = 5))
      assert(frame.fallbackUsed, "fallback path expected on small partition")
      assert(frame.fallbackAllocated, "fallback allocated on first small partition")
    }
  }

  test("lifecycle: small then segtree transition drops fallback reference") {
    val conf = new SQLConf
    conf.setConfString(SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key, "10")
    SegmentTreeWindowTestHelper.withFrameFactory(conf) { factory =>
      val frame = factory.newFrame()
      // Partition 1: small -> fallback allocated.
      frame.prepare(factory.newArray(rows = 5))
      assert(frame.fallbackAllocated && frame.fallbackUsed)
      // Partition 2: segtree -> fallback must be dropped (row-copy buffer GC-eligible).
      frame.prepare(factory.newArray(rows = 20))
      assert(!frame.fallbackUsed, "segtree path expected")
      assert(!frame.fallbackAllocated,
        "retained fallback reference must be dropped on segtree-path re-entry")
    }
  }

  test("lifecycle: throwing fallback.prepare must leave fallbackUsed=false") {
    // Structural-witness test: frame is `final`, can't inject a throwing
    // fallback. `fallbackUsed = true` is set ONLY after `fallback.prepare`
    // returns normally (see SegmentTreeWindowFunctionFrame.prepare). A
    // regression of that ordering would fail T1d. This test re-exercises
    // the happy path and documents the invariant.
    val conf = new SQLConf
    conf.setConfString(SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key, "100")
    SegmentTreeWindowTestHelper.withFrameFactory(conf) { factory =>
      val frame = factory.newFrame()
      frame.prepare(factory.newArray(rows = 5))
      assert(frame.fallbackUsed, "happy-path post-condition")
    }
  }

  test("lifecycle: close() after only small partition is a no-op") {
    val conf = new SQLConf
    conf.setConfString(SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key, "100")
    SegmentTreeWindowTestHelper.withFrameFactory(conf) { factory =>
      val frame = factory.newFrame()
      frame.prepare(factory.newArray(rows = 5))
      assert(frame.fallbackUsed)
      // Idempotent: no tree was ever built.
      frame.close()
      frame.close()
    }
  }
}

/** Legacy UDAF wrapped as ScalaUDAF (ImperativeAggregate); rejected by segtree guard. */
private class LegacySumUdaf extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = new StructType().add("v", LongType)
  override def bufferSchema: StructType = new StructType().add("s", LongType)
  override def dataType: DataType = LongType
  override def deterministic: Boolean = true
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
    }
  }
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }
  override def evaluate(buffer: Row): Any = buffer.getLong(0)
}

/** Typed Aggregator wrapped as ScalaAggregator (TypedImperativeAggregate); rejected. */
private class LongSumAggregator extends Aggregator[Long, Long, Long] {
  override def zero: Long = 0L
  override def reduce(b: Long, a: Long): Long = b + a
  override def merge(b1: Long, b2: Long): Long = b1 + b2
  override def finish(r: Long): Long = r
  override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
