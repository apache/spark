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
 * Benchmark for shrinking ROWS frames (... BETWEEN <lower> AND UNBOUNDED FOLLOWING).
 *
 * Today's `UnboundedFollowingWindowFunctionFrame` runs the suffix aggregate
 * O(n * (n - 1) / 2) per partition (acknowledged inline at
 * `WindowFunctionFrame.scala:636`). The segtree path replaces this with
 * O(n log n) when `spark.sql.window.segmentTree.enabled=true`.
 *
 * Layout: single partition on a non-partitioned ORDER BY (the worst case;
 * with PARTITION BY the cost decomposes as sum-of-partition N^2 and is
 * dominated by the largest partition).
 *
 * Sections:
 *   - A: per-aggregate equivalence at N=10K (naive ~3-5s/iter target).
 *   - B: N-sweep for SUM, naive vs segtree, demonstrating the algorithmic
 *        gap. N=50K is the largest naive run (~60s/iter); N=100K and 200K
 *        are segtree-only because naive would take ~4-16 min/iter.
 */
object UnboundedFollowingWindowBenchmark extends SqlBasedBenchmark {

  // Section A: calibrated so naive baseline lands ~3s/iter at MAIN_N.
  private val A_N: Long = 10L * 1024L              // ~2.4s naive @ N=10K (smoke: 2391ms)

  // Section B: N-sweep
  private val B_N_SMALL: Long = 5L * 1024L         // ~1.2s naive
  private val B_N_MID: Long = 25L * 1024L          // ~14s naive
  private val B_N_LARGE: Long = 50L * 1024L        // ~57s naive (last naive run)
  private val B_N_HUGE: Long = 100L * 1024L        // segtree-only, naive would be ~4 min
  private val B_N_GIANT: Long = 200L * 1024L       // segtree-only, naive would be ~16 min

  private val ITERS_NORMAL: Int = 5
  private val ITERS_STRESS: Int = 3

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val smokeMode = mainArgs.nonEmpty
    val smokeRowCount = if (smokeMode) mainArgs(0).toLong else 0L

    def setupIntTable(n: Long): Unit = {
      spark.range(n)
        .selectExpr("id", "cast(rand(42) * 1000000 as int) as v")
        .coalesce(1)
        .createOrReplaceTempView("t")
    }

    // Shrinking frame: [current, end-of-partition).
    val frame = "OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"

    // Digest comparison ensuring naive and segtree produce identical results.
    // SUM/COUNT/MIN/MAX on integers are bit-exact across paths.
    def digest(aggFn: String, sqlConfs: (String, String)*): Long = {
      withSQLConf(sqlConfs: _*) {
        spark.sql(s"SELECT SUM(HASH(m)) FROM (SELECT $aggFn(v) $frame AS m FROM t)")
          .head().getLong(0)
      }
    }

    def rowsLabel(rows: Long): String = {
      if (rows >= 1000000) s"${rows / 1000000}M"
      else if (rows >= 1024) s"${rows / 1024}K"
      else rows.toString
    }

    /**
     * Section A: Run the same SQL with conf off and on. The naive case is the
     * baseline, so iterations must be cheap enough to fit ~3-5s/iter.
     */
    def runSectionA(aggFn: String, iters: Int, rows: Long): Unit = {
      val dNaive = digest(aggFn)
      val dSeg = digest(aggFn, SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true")
      require(dNaive == dSeg,
        s"$aggFn shrinking-frame digest mismatch: naive=$dNaive seg=$dSeg")

      val benchmark = new Benchmark(
        s"$aggFn shrinking frame, N=${rowsLabel(rows)} rows",
        rows, output = output)
      benchmark.addCase(s"$aggFn naive (master O(N^2))", numIters = iters) { _ =>
        spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
      }
      benchmark.addCase(s"$aggFn segtree", numIters = iters) { _ =>
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
        }
      }
      benchmark.run()
    }

    /**
     * Section B: SUM-only N-sweep. At N <= 50K we run both paths. At N >= 100K
     * we run segtree-only because naive would dominate the benchmark wall-clock.
     */
    def runSectionB(rows: Long, includeNaive: Boolean, iters: Int): Unit = {
      if (includeNaive) {
        val dNaive = digest("SUM")
        val dSeg = digest("SUM", SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true")
        require(dNaive == dSeg,
          s"Section B N=${rowsLabel(rows)} digest mismatch: naive=$dNaive seg=$dSeg")
      }
      val benchmark = new Benchmark(
        s"SUM shrinking frame, N=${rowsLabel(rows)} rows" +
          (if (!includeNaive) " (segtree-only)" else ""),
        rows, output = output)
      if (includeNaive) {
        benchmark.addCase(s"SUM naive (master O(N^2)) N=${rowsLabel(rows)}",
            numIters = iters) { _ =>
          spark.sql(s"SELECT SUM(v) $frame FROM t").noop()
        }
      }
      benchmark.addCase(s"SUM segtree N=${rowsLabel(rows)}", numIters = iters) { _ =>
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT SUM(v) $frame FROM t").noop()
        }
      }
      benchmark.run()
    }

    if (smokeMode) {
      setupIntTable(smokeRowCount)
      runBenchmark("SMOKE Section A SUM") {
        runSectionA("SUM", ITERS_STRESS, smokeRowCount)
      }
    } else {
      // Section A: per-aggregate (SUM, MIN, MAX, COUNT, AVG) at calibrated N=10K.
      // STDDEV omitted: shrinking frame doesn't widen multi-buffer aggregates'
      // win profile vs sliding (the gain is purely algorithmic, not buffer-pack).
      setupIntTable(A_N)
      runBenchmark("Section A - SUM (non-invertible suffix)") {
        runSectionA("SUM", ITERS_NORMAL, A_N)
      }
      runBenchmark("Section A - MIN") {
        runSectionA("MIN", ITERS_NORMAL, A_N)
      }
      runBenchmark("Section A - MAX") {
        runSectionA("MAX", ITERS_NORMAL, A_N)
      }
      runBenchmark("Section A - COUNT") {
        runSectionA("COUNT", ITERS_NORMAL, A_N)
      }
      runBenchmark("Section A - AVG (multi-buffer)") {
        runSectionA("AVG", ITERS_NORMAL, A_N)
      }

      // Section B: N-sweep showing the algorithmic gap widening with N.
      setupIntTable(B_N_SMALL)
      runBenchmark("Section B - N=5K") {
        runSectionB(B_N_SMALL, includeNaive = true, ITERS_NORMAL)
      }
      setupIntTable(B_N_MID)
      runBenchmark("Section B - N=25K (stress)") {
        runSectionB(B_N_MID, includeNaive = true, ITERS_STRESS)
      }
      setupIntTable(B_N_LARGE)
      runBenchmark("Section B - N=50K (stress, last naive run)") {
        runSectionB(B_N_LARGE, includeNaive = true, ITERS_STRESS)
      }
      setupIntTable(B_N_HUGE)
      runBenchmark("Section B - N=100K (segtree-only, stress)") {
        runSectionB(B_N_HUGE, includeNaive = false, ITERS_STRESS)
      }
      setupIntTable(B_N_GIANT)
      runBenchmark("Section B - N=200K (segtree-only, stress)") {
        runSectionB(B_N_GIANT, includeNaive = false, ITERS_STRESS)
      }
    }
  }
}
