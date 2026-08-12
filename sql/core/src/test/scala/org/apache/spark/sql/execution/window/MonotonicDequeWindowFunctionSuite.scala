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
 * Correctness tests verifying the monotonic deque-based sliding window frame optimization. Runs
 * differential testing to ensure equivalence between:
 *   1. Monotonic Deque (Enabled)
 *   2. Segment Tree (Deque disabled, SegTree enabled)
 *   3. Naive Baseline (Both disabled)
 */
class MonotonicDequeWindowFunctionSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  // Disable AQE so executedPlan.collect can descend into WindowExec without being
  // blocked by AdaptiveSparkPlanExec (a LeafExecNode). This matches SegmentTreeWindowMetricsSuite.
  private val enableDeque: Map[String, String] = Map(
    SQLConf.WINDOW_MONOTONIC_DEQUE_ENABLED.key -> "true",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")

  private val disableDequeSegTree: Map[String, String] = Map(
    SQLConf.WINDOW_MONOTONIC_DEQUE_ENABLED.key -> "false",
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
    SQLConf.WINDOW_SEGMENT_TREE_MIN_PARTITION_ROWS.key -> "1")

  private val disableDequeNaive: Map[String, String] = Map(
    SQLConf.WINDOW_MONOTONIC_DEQUE_ENABLED.key -> "false",
    SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "false")

  /** Build `df` thrice (Deque, SegTree, Naive) and assert equal results. */
  private def checkEquivalence(build: () => DataFrame, expectDeque: Boolean = true): Unit = {
    val naiveResult: Seq[Row] = withSQLConf(disableDequeNaive.toSeq: _*) {
      build().collect().toSeq
    }
    val segTreeResult: Seq[Row] = withSQLConf(disableDequeSegTree.toSeq: _*) {
      build().collect().toSeq
    }
    val dequeResult: Seq[Row] = withSQLConf(enableDeque.toSeq: _*) {
      val df = build()
      val res = df.collect().toSeq

      // Verify routing actually hit (or didn't hit) the deque.
      // Use the registered metric key "numMonotonicDequeFrames" (not the display name).
      val windowNodes = df.queryExecution.executedPlan.collect {
        case w: WindowExec => w
      }
      assert(windowNodes.nonEmpty, "No WindowExec found in the query plan")
      val dequeCount =
        windowNodes.flatMap(_.metrics.get("numMonotonicDequeFrames").map(_.value)).sum

      if (expectDeque) {
        assert(dequeCount > 0, "Monotonic deque was enabled but no frames were routed to it")
      } else {
        assert(dequeCount == 0, "Monotonic deque was used but expected to fallback")
      }
      res
    }

    QueryTest.sameRows(naiveResult, dequeResult, isSorted = false).foreach { err =>
      fail(s"Monotonic Deque output differs from Naive baseline.\n$err")
    }
    QueryTest.sameRows(segTreeResult, dequeResult, isSorted = false).foreach { err =>
      fail(s"Monotonic Deque output differs from Segment Tree baseline.\n$err")
    }
  }

  private def baseDF: DataFrame = {
    spark
      .range(0, 100)
      .selectExpr(
        "id",
        "(id % 3) AS pk",
        "CAST(id AS INT) AS v_int",
        "CAST(id AS LONG) AS v_long",
        "CAST(id AS DOUBLE) AS v_double",
        "CAST(id AS STRING) AS v_str")
  }

  test("SPARK-58201: Moving rows frame: MIN/MAX on primitives (Int/Long/Double)") {
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-3, 2)
    checkEquivalence(() =>
      baseDF.select(
        $"id",
        min($"v_int").over(winSpec),
        max($"v_int").over(winSpec),
        min($"v_long").over(winSpec),
        max($"v_long").over(winSpec),
        min($"v_double").over(winSpec),
        max($"v_double").over(winSpec)))
  }

  test("SPARK-58201: Fallback for mixed aggregates (SUM + MIN/MAX)") {
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-4, 2)
    checkEquivalence(() =>
      baseDF.select(
        $"id",
        min($"v_int").over(winSpec),
        sum($"v_int").over(winSpec)),
      expectDeque = false)
  }

  test("SPARK-58201: Fallback for FILTER clauses") {
    val df = spark.sql("""SELECT id,
        |  MIN(id) FILTER (WHERE id % 2 = 0) OVER (
        |    PARTITION BY (id % 3) ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        |  ) AS v
        |FROM RANGE(0, 20)""".stripMargin)
    // Deque shouldn't be used since FILTER is not supported. We can't use checkEquivalence
    // because checkEquivalence builds DF inside, so we'll just check metrics.
    withSQLConf(
      SQLConf.WINDOW_MONOTONIC_DEQUE_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val res = df.collect()
      val windowNodes = df.queryExecution.executedPlan.collect {
        case w: WindowExec => w
      }
      val dequeCount =
        windowNodes.flatMap(_.metrics.get("numMonotonicDequeFrames").map(_.value)).sum
      assert(dequeCount == 0, "Monotonic deque was used for FILTER clause")
    }
  }

  test("SPARK-58201: Moving rows frame: MIN/MAX on reference types (String)") {
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 3)
    checkEquivalence(() =>
      baseDF.select($"id", min($"v_str").over(winSpec), max($"v_str").over(winSpec)))
  }

  test("SPARK-58201: MIN/MAX on Date and Timestamp types") {
    val df = baseDF.selectExpr(
      "id",
      "pk",
      "CAST(id * 24 * 3600 AS TIMESTAMP) AS v_ts",
      "date_add(to_date('1970-01-01'), CAST(id AS INT)) AS v_date")
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 2)
    checkEquivalence(() =>
      df.select(
        $"id",
        min($"v_ts").over(winSpec),
        max($"v_ts").over(winSpec),
        min($"v_date").over(winSpec),
        max($"v_date").over(winSpec)))
  }

  test("SPARK-58201: MIN/MAX on Interval types (YearMonthIntervalType and DayTimeIntervalType)") {
    val df = baseDF.selectExpr(
      "id",
      "pk",
      "make_ym_interval(0, CAST(id AS INT)) AS v_ym",
      "make_dt_interval(CAST(id AS INT), 0, 0, 0) AS v_dt")
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-3, 3)
    checkEquivalence(() =>
      df.select(
        $"id",
        min($"v_ym").over(winSpec),
        max($"v_ym").over(winSpec),
        min($"v_dt").over(winSpec),
        max($"v_dt").over(winSpec)))
  }

  test("SPARK-58201: MIN/MAX with null values in partition") {
    val df = spark
      .range(0, 50)
      .selectExpr("id", "(id % 2) AS pk", "IF(id % 5 == 0, null, CAST(id AS INT)) AS v")
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 2)
    checkEquivalence(() => df.select($"id", min($"v").over(winSpec), max($"v").over(winSpec)))
  }

  test("SPARK-58201: MIN/MAX on all-null partition") {
    val df = spark.range(0, 20).selectExpr("id", "1 AS pk", "CAST(null AS INT) AS v")
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 2)
    checkEquivalence(() => df.select($"id", min($"v").over(winSpec), max($"v").over(winSpec)))
  }

  test("SPARK-58201: Range-based moving frame: MIN/MAX on primitive types") {
    val df = baseDF.selectExpr("id", "pk", "CAST(id / 2 AS INT) AS ord_val", "v_int")
    val winSpec = Window.partitionBy($"pk").orderBy($"ord_val").rangeBetween(-2, 2)
    checkEquivalence(() =>
      df.select($"id", min($"v_int").over(winSpec), max($"v_int").over(winSpec)))
  }

  // Finding 3: verify strict inequality preserves first-of-equals behavior
  // under collated strings and signed zero.

  test("SPARK-58201: MIN/MAX on collated strings (UTF8_LCASE) preserves first-of-equals") {
    // Under UTF8_LCASE, 'Bob' and 'bob' compare equal. MIN must keep the
    // first occurrence (lowest index), matching naive/segment-tree semantics.
    val df = spark.sql("""SELECT id, 1 AS pk,
        |  CASE WHEN id = 0 THEN COLLATE('Bob', 'UTF8_LCASE')
        |       WHEN id = 1 THEN COLLATE('bob', 'UTF8_LCASE')
        |       WHEN id = 2 THEN COLLATE('alice', 'UTF8_LCASE')
        |       WHEN id = 3 THEN COLLATE('BOB', 'UTF8_LCASE')
        |       ELSE COLLATE(CAST(id AS STRING), 'UTF8_LCASE')
        |  END AS v
        |FROM RANGE(0, 20)""".stripMargin)
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 2)
    checkEquivalence(() => df.select($"id", min($"v").over(winSpec), max($"v").over(winSpec)))
  }

  test("SPARK-58201: MIN/MAX on Double with signed zero (+0.0 / -0.0)") {
    // SQLOrderingUtil.compareDoubles treats -0.0 == +0.0, so they compare
    // equal. MIN must keep the first occurrence, matching naive semantics.
    val df = spark
      .range(0, 20)
      .selectExpr(
        "id",
        "1 AS pk",
        """CASE
           WHEN id % 4 = 0 THEN CAST(-0.0 AS DOUBLE)
           WHEN id % 4 = 1 THEN CAST(0.0 AS DOUBLE)
           WHEN id % 4 = 2 THEN CAST(id AS DOUBLE)
           ELSE CAST(-id AS DOUBLE)
         END AS v""")
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 2)
    checkEquivalence(() => df.select($"id", min($"v").over(winSpec), max($"v").over(winSpec)))
  }

  // Finding 6: spill coverage - lower thresholds to force ExternalAppendOnlyUnsafeRowArray
  // to use its SpillableArrayIterator, which recycles a single UnsafeRow.

  test("SPARK-58201: Moving rows frame: MIN/MAX on reference types (String) with spill") {
    withSQLConf(
      SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "8",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "16") {
      val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 3)
      checkEquivalence(() =>
        baseDF.select($"id", min($"v_str").over(winSpec), max($"v_str").over(winSpec)))
    }
  }

  test("SPARK-58201: MIN/MAX on ArrayType with spill") {
    val df = spark
      .range(0, 60)
      .selectExpr("id", "(id % 3) AS pk", "array(CAST(id AS INT), CAST(id * 2 AS INT)) AS v")
    withSQLConf(
      SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "8",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "16") {
      val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 2)
      checkEquivalence(() => df.select($"id", min($"v").over(winSpec), max($"v").over(winSpec)))
    }
  }

  test("SPARK-58201: MIN/MAX on StructType with spill") {
    val df = spark
      .range(0, 60)
      .selectExpr(
        "id",
        "(id % 3) AS pk",
        "named_struct('a', CAST(id AS INT), 'b', CAST(id * 3 AS INT)) AS v")
    withSQLConf(
      SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "8",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "16") {
      val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 2)
      checkEquivalence(() => df.select($"id", min($"v").over(winSpec), max($"v").over(winSpec)))
    }
  }

  // Finding 18: lowerBound-advances-without-admitting branch (both bounds on FOLLOWING side).
  test("SPARK-58201: both-FOLLOWING rows frame exercises lowerBound-advances branch") {
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(2, 4)
    checkEquivalence(() =>
      baseDF.select($"id", min($"v_int").over(winSpec), max($"v_int").over(winSpec)))
  }

  // Finding 18: wide ascending data forces MinMaxDeque.expand() (ring-buffer grow path).
  test("SPARK-58201: wide window on ascending data forces ring-buffer expand") {
    val df = spark.range(0, 300).selectExpr("id", "1 AS pk", "CAST(id AS INT) AS v")
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-70, 0)
    checkEquivalence(() => df.select($"id", min($"v").over(winSpec), max($"v").over(winSpec)))
  }

  // Finding 18: both-PRECEDING frame -- first rows have an empty window.
  test("SPARK-58201: both-PRECEDING rows frame (empty window for first rows)") {
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-4, -2)
    checkEquivalence(() =>
      baseDF.select($"id", min($"v_int").over(winSpec), max($"v_int").over(winSpec)))
  }

  // Finding 18: wide random data, exercises normal sliding path at scale.
  test("SPARK-58201: wide random rows frame") {
    val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-60, 40)
    checkEquivalence(() =>
      baseDF.select($"id", min($"v_int").over(winSpec), max($"v_int").over(winSpec)))
  }

  // Finding 18: range frame on tied order key.
  test("SPARK-58201: range frame on tied order key") {
    val df = baseDF.selectExpr("id", "pk", "CAST(id / 3 AS INT) AS ord", "v_int")
    val winSpec = Window.partitionBy($"pk").orderBy($"ord").rangeBetween(1, 3)
    checkEquivalence(() =>
      df.select($"id", min($"v_int").over(winSpec), max($"v_int").over(winSpec)))
  }

  // Finding 7 + 18: DECIMAL and BINARY types -- UnsafeRow.getDecimal/getBinary allocate fresh
  // objects per call (after ExtractWindowExpressions hoists args into the Project below), so
  // the deque never holds a stale reference regardless of the isPrimitive branch.
  test("SPARK-58201: MIN/MAX on DECIMAL and BINARY types with spill") {
    val df = spark
      .range(0, 60)
      .selectExpr(
        "id",
        "(id % 3) AS pk",
        "CAST(id AS DECIMAL(38, 10)) AS v_dec",
        "CAST(id AS BINARY) AS v_bin")
    withSQLConf(
      SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "8",
      SQLConf.WINDOW_EXEC_BUFFER_SPILL_THRESHOLD.key -> "16") {
      val winSpec = Window.partitionBy($"pk").orderBy($"id").rowsBetween(-2, 2)
      checkEquivalence(() =>
        df.select(
          $"id",
          min($"v_dec").over(winSpec),
          max($"v_dec").over(winSpec),
          min($"v_bin").over(winSpec),
          max($"v_bin").over(winSpec)))
    }
  }
}
