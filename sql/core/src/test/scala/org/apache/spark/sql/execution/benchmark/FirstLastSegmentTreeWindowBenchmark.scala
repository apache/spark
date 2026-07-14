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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark for FIRST/LAST window aggregates over sliding and shrinking
 * ROWS frames, comparing the legacy O(N x W) / O(N^2) frame paths against
 * the segment-tree path enabled by adding `classOf[First]` / `classOf[Last]`
 * to `WindowSegmentTree.EligibleAggregates`.
 *
 * Today's slow paths:
 *   - Sliding: `SlidingWindowFunctionFrame.write` rebuilds the per-row
 *     buffer aggregate by iterating `processor.update` over every row in
 *     the buffer (O(W) per output row, O(N*W) total).
 *   - Shrinking: `UnboundedFollowingWindowFunctionFrame.write` walks the
 *     remaining suffix on every output row (O(N^2) total; class scaladoc
 *     literally says O(n*(n-1)/2)).
 *
 * Sections:
 *   - A: FIRST/LAST per-mode at N=10K, sliding wide frame.
 *   - B: FIRST/LAST per-mode at N=10K, shrinking frame.
 *   - C: N-sweep for FIRST shrinking, naive vs segtree, demonstrating the
 *        algorithmic gap. Mirrors UnboundedFollowingWindowBenchmark layout.
 */
object FirstLastSegmentTreeWindowBenchmark extends SqlBasedBenchmark {

  // Section A/B: per-mode per-frame-shape at calibrated N
  private val AB_N: Long = 10L * 1024L

  // Section C: N-sweep for shrinking FIRST
  private val C_N_SMALL: Long = 5L * 1024L
  private val C_N_MID: Long = 25L * 1024L
  private val C_N_LARGE: Long = 50L * 1024L
  private val C_N_HUGE: Long = 100L * 1024L

  private val ITERS_NORMAL: Int = 5
  private val ITERS_STRESS: Int = 3

  // Sliding frame width tuned so the O(N*W) baseline is observable but not
  // catastrophic. With N=10K and W=2001 the legacy SlidingWindowFunctionFrame
  // performs ~20M update calls (10K rows x ~2K-row buffer rebuild on each
  // boundary change) which is enough to expose the gap without dominating
  // wall-clock at calibration time.
  private val SLIDING_FRAME =
    "OVER (ORDER BY id ROWS BETWEEN 1000 PRECEDING AND 1000 FOLLOWING)"
  private val SHRINKING_FRAME =
    "OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val smokeMode = mainArgs.nonEmpty
    val smokeRowCount = if (smokeMode) mainArgs(0).toLong else 0L

    def setupIntTable(n: Long): Unit = {
      // Sprinkle ~10% NULLs so IGNORE NULLS exercises the merge path
      // distinctly from respect-nulls; integer values otherwise.
      spark.range(n)
        .selectExpr("id",
          "CASE WHEN rand(7) < 0.1 THEN NULL " +
            "ELSE cast(rand(42) * 1000000 as int) END as v")
        .coalesce(1)
        .createOrReplaceTempView("t")
    }

    def rowsLabel(rows: Long): String = {
      if (rows >= 1000000) s"${rows / 1000000}M"
      else if (rows >= 1024) s"${rows / 1024}K"
      else rows.toString
    }

    /**
     * Equivalence digest. FIRST/LAST in respect-nulls mode are bit-exact
     * across naive and segtree paths; in IGNORE NULLS the per-block merge
     * also yields the same result row-for-row. We hash the result column
     * directly (not COALESCE'd) so a NULL row hashes to NULL and a NULL
     * sum is propagated, but the comparison still distinguishes shapes.
     */
    def digest(sql: String, sqlConfs: (String, String)*): Long = {
      withSQLConf(sqlConfs: _*) {
        val r = spark.sql(s"SELECT SUM(HASH(m)) FROM (SELECT $sql AS m FROM t)")
          .head().get(0)
        if (r == null) 0L else r.asInstanceOf[Long]
      }
    }

    def runCase(label: String, sql: String, iters: Int, rows: Long): Unit = {
      val dNaive = digest(sql)
      val dSeg = digest(sql, SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true")
      require(dNaive == dSeg,
        s"$label digest mismatch: naive=$dNaive seg=$dSeg")

      val benchmark = new Benchmark(
        s"$label, N=${rowsLabel(rows)} rows", rows, output = output)
      benchmark.addCase(s"$label naive", numIters = iters) { _ =>
        spark.sql(s"SELECT $sql FROM t").noop()
      }
      benchmark.addCase(s"$label segtree", numIters = iters) { _ =>
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT $sql FROM t").noop()
        }
      }
      benchmark.run()
    }

    def runSweepCase(rows: Long, includeNaive: Boolean, iters: Int): Unit = {
      val sql = s"FIRST(v) $SHRINKING_FRAME"
      if (includeNaive) {
        val dNaive = digest(sql)
        val dSeg = digest(sql, SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true")
        require(dNaive == dSeg,
          s"Section C N=${rowsLabel(rows)} digest mismatch: naive=$dNaive seg=$dSeg")
      }
      val benchmark = new Benchmark(
        s"FIRST shrinking frame, N=${rowsLabel(rows)} rows" +
          (if (!includeNaive) " (segtree-only)" else ""),
        rows, output = output)
      if (includeNaive) {
        benchmark.addCase(s"FIRST naive N=${rowsLabel(rows)}", numIters = iters) { _ =>
          spark.sql(s"SELECT $sql FROM t").noop()
        }
      }
      benchmark.addCase(s"FIRST segtree N=${rowsLabel(rows)}", numIters = iters) { _ =>
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT $sql FROM t").noop()
        }
      }
      benchmark.run()
    }

    if (smokeMode) {
      setupIntTable(smokeRowCount)
      runBenchmark("SMOKE Section A FIRST sliding") {
        runCase("FIRST sliding respect-nulls",
          s"FIRST(v) $SLIDING_FRAME", ITERS_STRESS, smokeRowCount)
      }
    } else {
      setupIntTable(AB_N)

      // Section A: sliding frame, all four mode/function combinations.
      runBenchmark("Section A - FIRST sliding respect-nulls") {
        runCase("FIRST sliding respect-nulls",
          s"FIRST(v) $SLIDING_FRAME", ITERS_NORMAL, AB_N)
      }
      runBenchmark("Section A - LAST sliding respect-nulls") {
        runCase("LAST sliding respect-nulls",
          s"LAST(v) $SLIDING_FRAME", ITERS_NORMAL, AB_N)
      }
      runBenchmark("Section A - FIRST sliding IGNORE NULLS") {
        runCase("FIRST sliding ignore-nulls",
          s"FIRST(v) IGNORE NULLS $SLIDING_FRAME", ITERS_NORMAL, AB_N)
      }
      runBenchmark("Section A - LAST sliding IGNORE NULLS") {
        runCase("LAST sliding ignore-nulls",
          s"LAST(v) IGNORE NULLS $SLIDING_FRAME", ITERS_NORMAL, AB_N)
      }

      // Section B: shrinking frame, all four mode/function combinations.
      runBenchmark("Section B - FIRST shrinking respect-nulls") {
        runCase("FIRST shrinking respect-nulls",
          s"FIRST(v) $SHRINKING_FRAME", ITERS_NORMAL, AB_N)
      }
      runBenchmark("Section B - LAST shrinking respect-nulls") {
        runCase("LAST shrinking respect-nulls",
          s"LAST(v) $SHRINKING_FRAME", ITERS_NORMAL, AB_N)
      }
      runBenchmark("Section B - FIRST shrinking IGNORE NULLS") {
        runCase("FIRST shrinking ignore-nulls",
          s"FIRST(v) IGNORE NULLS $SHRINKING_FRAME", ITERS_NORMAL, AB_N)
      }
      runBenchmark("Section B - LAST shrinking IGNORE NULLS") {
        runCase("LAST shrinking ignore-nulls",
          s"LAST(v) IGNORE NULLS $SHRINKING_FRAME", ITERS_NORMAL, AB_N)
      }

      // Section C: shrinking-frame N-sweep on FIRST, demonstrating O(N^2)
      // legacy vs O(N log N) segtree gap that widens with N.
      setupIntTable(C_N_SMALL)
      runBenchmark("Section C - N=5K") {
        runSweepCase(C_N_SMALL, includeNaive = true, ITERS_NORMAL)
      }
      setupIntTable(C_N_MID)
      runBenchmark("Section C - N=25K (stress)") {
        runSweepCase(C_N_MID, includeNaive = true, ITERS_STRESS)
      }
      setupIntTable(C_N_LARGE)
      runBenchmark("Section C - N=50K (stress, last naive run)") {
        runSweepCase(C_N_LARGE, includeNaive = true, ITERS_STRESS)
      }
      setupIntTable(C_N_HUGE)
      runBenchmark("Section C - N=100K (segtree-only, stress)") {
        runSweepCase(C_N_HUGE, includeNaive = false, ITERS_STRESS)
      }
    }
  }
}
