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

import scala.collection.mutable

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure window function performance with bounded ROWS frames.
 *
 * Matrix (rev4; see the PR description):
 *   - Section A: 5 aggregates x 3 cells (naive / segtree default / segtree bs=256),
 *                per-case N calibrated so naive lands in 3-5s/iter. STDDEV_SAMP keeps
 *                N=2M (stress: multi-buffer noise demo).
 *   - Section B: SUM-over-INT, W sweep {10, 50, 201, 4001}; naive / segtree default.
 *                W=4001 also runs segtree bs=256. W=10 / W=50 are stress cases
 *                demonstrating Pareto-loss zone (segtree slower than naive).
 *   - Section F: Spill regression guard, 1M String rows x MAX x W=1001 (stress).
 *   - Section C: N-sweep {2M, 8M, 16M} segtree-only at W=1001 (stress): memory-pressure
 *                invariance / sub-linear growth demonstration.
 *
 * Reading guide: W-sweep uses different N per point so naive lands in 3-5s. Compare
 * `Per Row(ns)` column (not Best time) to read O(log W) scaling on segtree side.
 *
 * Invocation arguments (positional; dev smoke only, do NOT set with
 * SPARK_GENERATE_BENCHMARK_FILES=1):
 *   mainArgs(0) rowCount   if present, run SMOKE mode: Section A (MIN only) + one W sweep point.
 *   mainArgs(1) halfWindow (smoke mode only) default 100.
 *
 * {{{
 *   # full matrix (CI / committed results):
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *     "sql/Test/runMain org.apache.spark.sql.execution.benchmark.WindowBenchmark"
 *   # smoke (100k rows, W=201):
 *   build/sbt "sql/Test/runMain \
 *     org.apache.spark.sql.execution.benchmark.WindowBenchmark 100000 100"
 * }}}
 */
object WindowBenchmark extends SqlBasedBenchmark {

  // ---- Section A: per-case N (naive baseline ~3-5s/iter) ----
  private val A_N_INT: Long = 256L * 1024              // MIN/MAX/SUM/COUNT @ W=1001
  private val A_N_AVG: Long = 192L * 1024              // AVG  @ W=1001
  private val A_N_STDDEV: Long = 2L * 1000L * 1000L    // STDDEV stress

  // ---- Section B: W-sweep ----
  private val B_N_W10: Long = 2L * 1000L * 1000L       // stress (Pareto loss)
  private val B_N_W50: Long = 2L * 1000L * 1000L       // stress (Pareto loss)
  private val B_N_W201: Long = 1L * 1000L * 1000L
  private val B_N_W4001: Long = 2L * 1000L * 1000L     // stress O(W) cliff

  // ---- Section F: String spill ----
  private val SPILL_N: Long = 1L * 1000L * 1000L

  // ---- Section C: N-sweep segtree-only ----
  private val C_N_SMALL: Long = 2L * 1000L * 1000L
  private val C_N_MID: Long = 8L * 1000L * 1000L
  private val C_N_LARGE: Long = 16L * 1000L * 1000L
  private val C_HALF_W: Int = 500                      // W=1001

  private val MAIN_HALF_W: Int = 500                   // Section A W=1001

  private val ITERS_NORMAL: Int = 5
  private val ITERS_STRESS: Int = 3

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val smokeMode = mainArgs.nonEmpty
    val smokeRowCount = if (smokeMode) mainArgs(0).toLong else 0L
    val smokeHalfW = if (mainArgs.length > 1) mainArgs(1).toInt else 100

    if (smokeMode) {
      require(smokeRowCount >= 4096,
        s"rowCount=$smokeRowCount too small; segtree may fallback. Use >= 4096.")
    }

    // Shared metrics listener: per-case peak mem + disk spill
    val metrics = mutable.Map[String, (Long, Long)]()
    @volatile var currentCase: String = ""
    val listener = new SparkListener {
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
        val m = e.taskMetrics
        val cc = currentCase
        if (m != null && cc.nonEmpty) {
          metrics.synchronized {
            val (pm, ds) = metrics.getOrElse(cc, (0L, 0L))
            metrics(cc) = (math.max(pm, m.peakExecutionMemory), ds + m.diskBytesSpilled)
          }
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)
    val allCaseNames = mutable.ArrayBuffer[String]()

    // ---- helpers ----
    def setupIntTable(n: Long): Unit = {
      spark.range(n)
        .selectExpr("id", "cast(rand(42) * 1000000 as int) as v")
        .coalesce(1)
        .createOrReplaceTempView("t")
    }

    def setupStringTable(n: Long): Unit = {
      // value is ~20-char string; variable-length, exercises spill path
      spark.range(n)
        .selectExpr("id", "repeat(cast(id as string), 5) as v")
        .coalesce(1)
        .createOrReplaceTempView("t")
    }

    def frameFor(halfW: Int): String =
      s"OVER (ORDER BY id ROWS BETWEEN $halfW PRECEDING AND $halfW FOLLOWING)"

    // Aggregates that produce Double (AVG/STDDEV/VAR) accumulate in
    // floating-point. The segtree merge order differs from the row-by-row
    // order used by SlidingWindowFunctionFrame, so the result differs at
    // the bit (ULP) level even though it is mathematically equivalent.
    //
    // HASH(double) is bit-sensitive, and HASH(ROUND(m, k)) is a trap at
    // scale: any row whose value sits within ~1 ULP of a rounding bin
    // boundary can round to a different k-decimal value across backends.
    // Those rounded doubles then hash to unrelated values, and SUM of the
    // hashes turns tiny floating-point differences into large digest drift.
    // We hit this at N=2M on STDDEV_SAMP (0.2% digest diff even though
    // per-row relative error is <1e-10).
    //
    // Instead use SUM(CAST(ROUND(m, 3) * 1000 AS BIGINT)): a single
    // boundary-crossing row only contributes +/-1 to the aggregate (vs.
    // chaos for HASH), so identical-to-ULP implementations always agree,
    // and real bugs that exceed 1e-3 relative error are still caught.
    // Integer aggregates (MIN/MAX/SUM/COUNT over INT inputs) remain
    // bit-exact and are hashed directly.
    def digestExprFor(aggFn: String): String = {
      if (aggFn.startsWith("AVG") || aggFn.startsWith("STDDEV") ||
          aggFn.startsWith("VAR")) {
        "CAST(ROUND(m, 3) * 1000 AS BIGINT)"
      } else {
        "HASH(m)"
      }
    }

    def digest(aggFn: String, frame: String, sqlConfs: (String, String)*): Long = {
      val expr = digestExprFor(aggFn)
      withSQLConf(sqlConfs: _*) {
        spark.sql(s"SELECT SUM($expr) FROM (SELECT $aggFn(v) $frame AS m FROM t)")
          .head().getLong(0)
      }
    }

    def rowsLabel(rows: Long): String = {
      if (rows >= 1000000) s"${rows / 1000000}M"
      else if (rows >= 1024) s"${rows / 1024}K"
      else rows.toString
    }

    // Section A: one aggregate, 3 cells.
    def runSectionA(
        aggFn: String, iters: Int, rows: Long, halfW: Int, stressMark: String): Unit = {
      val frame = frameFor(halfW)
      val dNaive = digest(aggFn, frame)
      val dSeg = digest(aggFn, frame, SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true")
      val dSegBs = digest(aggFn, frame,
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> "256")
      require(dNaive == dSeg,
        s"$aggFn segtree digest mismatch: naive=$dNaive seg=$dSeg")
      require(dNaive == dSegBs,
        s"$aggFn segtree (bs=256) digest mismatch: naive=$dNaive seg=$dSegBs")

      val W = 2 * halfW + 1
      val benchmark = new Benchmark(
        s"$aggFn sliding window, W=$W, ${rowsLabel(rows)} rows$stressMark",
        rows, output = output)
      val nNaive = s"$aggFn naive (current, baseline)"
      val nSeg = s"$aggFn segtree (default)"
      val nSegBs = s"$aggFn segtree (blockSize=256)"
      allCaseNames ++= Seq(nNaive, nSeg, nSegBs)

      benchmark.addCase(nNaive, numIters = iters) { _ =>
        currentCase = nNaive
        spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
      }
      benchmark.addCase(nSeg, numIters = iters) { _ =>
        currentCase = nSeg
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
        }
      }
      benchmark.addCase(nSegBs, numIters = iters) { _ =>
        currentCase = nSegBs
        withSQLConf(
          SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
          SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> "256") {
          spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
        }
      }
      benchmark.run()
    }

    // Section B: SUM-over-INT, one W, naive+default(+bs=256 when stressBs true).
    def runSectionB(
        halfW: Int, stressBs: Boolean, rows: Long, iters: Int, stressMark: String): Unit = {
      val aggFn = "SUM"
      val frame = frameFor(halfW)
      val W = 2 * halfW + 1
      val dNaive = digest(aggFn, frame)
      val dSeg = digest(aggFn, frame, SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true")
      require(dNaive == dSeg, s"Section B W=$W digest mismatch: naive=$dNaive seg=$dSeg")

      val benchmark = new Benchmark(
        s"$aggFn scaling, W=$W, ${rowsLabel(rows)} rows$stressMark", rows, output = output)
      val nNaive = s"$aggFn naive W=$W"
      val nSeg = s"$aggFn segtree (default) W=$W"
      allCaseNames ++= Seq(nNaive, nSeg)
      benchmark.addCase(nNaive, numIters = iters) { _ =>
        currentCase = nNaive
        spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
      }
      benchmark.addCase(nSeg, numIters = iters) { _ =>
        currentCase = nSeg
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
        }
      }
      if (stressBs) {
        val dSegBs = digest(aggFn, frame,
          SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
          SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> "256")
        require(dNaive == dSegBs,
          s"Section B W=$W bs=256 digest mismatch: naive=$dNaive segBs=$dSegBs")
        val nSegBs = s"$aggFn segtree (blockSize=256) W=$W"
        allCaseNames += nSegBs
        benchmark.addCase(nSegBs, numIters = iters) { _ =>
          currentCase = nSegBs
          withSQLConf(
            SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
            SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> "256") {
            spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
          }
        }
      }
      benchmark.run()
    }

    // Section F: spill guard, MAX + String + W=1001.
    def runSpillGuard(): Unit = {
      val halfW = 500
      val frame = frameFor(halfW)
      // Digest parity is NOT checked here: the pre-check scan on 1M String x
      // W=1001 naive costs ~90s. Correctness is covered by unit tests
      // (SegmentTreeWindowFunctionSuite), so we only observe perf here.
      val benchmark = new Benchmark(
        "MAX String spill guard, W=1001, 1M rows (stress)", SPILL_N, output = output)
      val nNaive = "MAX naive (String)"
      val nSeg = "MAX segtree default (String)"
      allCaseNames ++= Seq(nNaive, nSeg)
      benchmark.addCase(nNaive, numIters = ITERS_STRESS) { _ =>
        currentCase = nNaive
        spark.sql(s"SELECT MAX(v) $frame FROM t").noop()
      }
      benchmark.addCase(nSeg, numIters = ITERS_STRESS) { _ =>
        currentCase = nSeg
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT MAX(v) $frame FROM t").noop()
        }
      }
      benchmark.run()
    }

    // Section C: N-sweep, segtree-only.
    // Digest check skipped: correctness covered by Section A (same aggregate SUM at N=2M
    // shares the same code path). Naive not run at 16M (would cost ~4min/iter with no
    // measurement value). The goal is memory-pressure invariance / sub-linear growth:
    // segtree per-row ns at 16M should be <= 2 x per-row ns at 2M.
    def runSectionC(rows: Long): Unit = {
      val aggFn = "SUM"
      val frame = frameFor(C_HALF_W)
      val W = 2 * C_HALF_W + 1
      val benchmark = new Benchmark(
        s"$aggFn N-sweep (segtree-only), W=$W, ${rowsLabel(rows)} rows (stress)",
        rows, output = output)
      val nSeg = s"$aggFn segtree (default) N=${rowsLabel(rows)}"
      allCaseNames += nSeg
      benchmark.addCase(nSeg, numIters = ITERS_STRESS) { _ =>
        currentCase = nSeg
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
        }
      }
      benchmark.run()
    }

    // ---- run suite ----
    try {
      if (smokeMode) {
        // Dev smoke: Section A MIN only + Section B at the given halfW.
        setupIntTable(smokeRowCount)
        runBenchmark("SMOKE: Section A MIN") {
          runSectionA("MIN", ITERS_STRESS, smokeRowCount, smokeHalfW, "")
        }
        runBenchmark("SMOKE: Section B SUM W sweep point") {
          runSectionB(
            smokeHalfW, stressBs = smokeHalfW >= 2000, smokeRowCount, ITERS_STRESS, "")
        }
      } else {
        // --- Section A: 5 aggregates @ per-case N; W=1001. ---
        // MIN/MAX/SUM/COUNT share A_N_INT; AVG uses A_N_AVG; STDDEV is stress @ 2M.
        setupIntTable(A_N_INT)
        runBenchmark("Section A - MIN (non-invertible)") {
          runSectionA("MIN", ITERS_NORMAL, A_N_INT, MAIN_HALF_W, "")
        }
        runBenchmark("Section A - MAX (non-invertible)") {
          runSectionA("MAX", ITERS_NORMAL, A_N_INT, MAIN_HALF_W, "")
        }
        runBenchmark("Section A - SUM (Spark has no inverse; full recompute)") {
          runSectionA("SUM", ITERS_NORMAL, A_N_INT, MAIN_HALF_W, "")
        }
        runBenchmark("Section A - COUNT") {
          runSectionA("COUNT", ITERS_NORMAL, A_N_INT, MAIN_HALF_W, "")
        }

        setupIntTable(A_N_AVG)
        runBenchmark("Section A - AVG (multi-buffer)") {
          runSectionA("AVG", ITERS_NORMAL, A_N_AVG, MAIN_HALF_W, "")
        }

        setupIntTable(A_N_STDDEV)
        runBenchmark("Section A - STDDEV_SAMP (multi-buffer, stress)") {
          runSectionA("STDDEV_SAMP", ITERS_STRESS, A_N_STDDEV, MAIN_HALF_W, " (stress)")
        }

        // --- Section B: SUM, W sweep. W=10/50 are stress (Pareto loss zone). ---
        setupIntTable(B_N_W10)
        runBenchmark("Section B - W=10 scaling (stress: Pareto loss zone)") {
          runSectionB(5, stressBs = false, B_N_W10, ITERS_STRESS, " (stress)")
        }
        setupIntTable(B_N_W50)
        runBenchmark("Section B - W=50 scaling (stress: Pareto loss zone)") {
          runSectionB(25, stressBs = false, B_N_W50, ITERS_STRESS, " (stress)")
        }
        setupIntTable(B_N_W201)
        runBenchmark("Section B - W=201 scaling") {
          runSectionB(100, stressBs = false, B_N_W201, ITERS_NORMAL, "")
        }
        setupIntTable(B_N_W4001)
        runBenchmark("Section B - W=4001 scaling (stress, + bs=256 cross-block)") {
          runSectionB(2000, stressBs = true, B_N_W4001, ITERS_STRESS, " (stress)")
        }

        // --- Section F: spill guard. ---
        setupStringTable(SPILL_N)
        runBenchmark("Section F - spill regression guard (String, stress)") {
          runSpillGuard()
        }

        // --- Section C: N-sweep segtree-only (memory-pressure invariance). ---
        setupIntTable(C_N_SMALL)
        runBenchmark("Section C - N-sweep small (stress)") {
          runSectionC(C_N_SMALL)
        }
        setupIntTable(C_N_MID)
        runBenchmark("Section C - N-sweep mid (stress)") {
          runSectionC(C_N_MID)
        }
        setupIntTable(C_N_LARGE)
        runBenchmark("Section C - N-sweep large (stress)") {
          runSectionC(C_N_LARGE)
        }
      }

      // Drain listener before reading metrics
      spark.sparkContext.listenerBus.waitUntilEmpty()

      // scalastyle:off println
      val out = System.out
      out.println()
      out.println("Memory/Spill (peak executor memory / total disk spilled):")
      if (allCaseNames.nonEmpty) {
        val width = allCaseNames.map(_.length).max + 2
        for (name <- allCaseNames) {
          val (pm, ds) = metrics.synchronized {
            metrics.getOrElse(name, (0L, 0L))
          }
          val peakMb = pm.toDouble / (1024.0 * 1024.0)
          val label = (name + ":").padTo(width, ' ')
          out.println(f"  $label%s peak=$peakMb%8.2f MB   spilled=$ds%d B")
        }
      }
      out.println()
      // scalastyle:on println
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
