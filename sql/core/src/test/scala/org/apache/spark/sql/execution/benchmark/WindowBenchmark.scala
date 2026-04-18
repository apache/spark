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
 * Matrix (rev3; see todos/features/window-segment-tree/docs/benchmark-design.md):
 *   - Section A: 6 aggregates x 3 cells (naive / segtree default / segtree bs=256),
 *                N=2M, W=1001. MAX / STDDEV_SAMP use numIters=5 (noisier); others 3.
 *   - Section B: MIN only, W sweep {201, 1001, 4001}; naive / segtree default.
 *                W=4001 also runs segtree bs=256 (cross-block stress).
 *   - Section F: Spill regression guard, 1M String(20)-like rows x MAX x W=1001,
 *                 naive / segtree default. Meaningful only under `-Xmx2g`.
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

  private val FULL_N: Long = 2L << 20          // 2,097,152
  private val SPILL_N: Long = 1L << 20         // 1,048,576
  private val MAIN_HALF_W: Int = 500           // Section A: W = 1001

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val smokeMode = mainArgs.nonEmpty
    val rowCount = if (smokeMode) mainArgs(0).toLong else FULL_N
    val smokeHalfW = if (mainArgs.length > 1) mainArgs(1).toInt else 100

    require(rowCount >= 4096,
      s"rowCount=$rowCount too small; segtree may fallback. Use >= 4096.")

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

    def digest(aggFn: String, frame: String, sqlConfs: (String, String)*): Long = {
      withSQLConf(sqlConfs: _*) {
        spark.sql(s"SELECT SUM(HASH(m)) FROM (SELECT $aggFn(v) $frame AS m FROM t)")
          .head().getLong(0)
      }
    }

    // Section A: one aggregate, 3 cells, numIters configurable.
    def runSectionA(aggFn: String, iters: Int, rows: Long, halfW: Int): Unit = {
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
      val rowsLabel = if (rows >= 1000000) s"${rows / 1000000}M" else rows.toString
      val benchmark = new Benchmark(
        s"$aggFn sliding window, W=$W, $rowsLabel rows", rows, output = output)
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

    // Section B: MIN-only, one W, naive+default(+bs=256 when stressBs true).
    def runSectionB(halfW: Int, stressBs: Boolean, rows: Long): Unit = {
      val aggFn = "MIN"
      val frame = frameFor(halfW)
      val W = 2 * halfW + 1
      val dNaive = digest(aggFn, frame)
      val dSeg = digest(aggFn, frame, SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true")
      require(dNaive == dSeg, s"Section B W=$W digest mismatch: naive=$dNaive seg=$dSeg")

      val rowsLabel = if (rows >= 1000000) s"${rows / 1000000}M" else rows.toString
      val benchmark = new Benchmark(
        s"MIN scaling, W=$W, $rowsLabel rows", rows, output = output)
      val nNaive = s"MIN naive W=$W"
      val nSeg = s"MIN segtree (default) W=$W"
      allCaseNames ++= Seq(nNaive, nSeg)
      benchmark.addCase(nNaive, numIters = 3) { _ =>
        currentCase = nNaive
        spark.sql(s"SELECT MIN(v) $frame FROM t").noop()
      }
      benchmark.addCase(nSeg, numIters = 3) { _ =>
        currentCase = nSeg
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT MIN(v) $frame FROM t").noop()
        }
      }
      if (stressBs) {
        val dSegBs = digest(aggFn, frame,
          SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
          SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> "256")
        require(dNaive == dSegBs,
          s"Section B W=$W bs=256 digest mismatch: naive=$dNaive segBs=$dSegBs")
        val nSegBs = s"MIN segtree (blockSize=256) W=$W"
        allCaseNames += nSegBs
        benchmark.addCase(nSegBs, numIters = 3) { _ =>
          currentCase = nSegBs
          withSQLConf(
            SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
            SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> "256") {
            spark.sql(s"SELECT MIN(v) $frame FROM t").noop()
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
        "MAX String spill guard, W=1001, 1M rows", SPILL_N, output = output)
      val nNaive = "MAX naive (String)"
      val nSeg = "MAX segtree default (String)"
      allCaseNames ++= Seq(nNaive, nSeg)
      benchmark.addCase(nNaive, numIters = 3) { _ =>
        currentCase = nNaive
        spark.sql(s"SELECT MAX(v) $frame FROM t").noop()
      }
      benchmark.addCase(nSeg, numIters = 3) { _ =>
        currentCase = nSeg
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT MAX(v) $frame FROM t").noop()
        }
      }
      benchmark.run()
    }

    // ---- run suite ----
    try {
      if (smokeMode) {
        // Dev smoke: Section A MIN only + Section B at the given halfW.
        setupIntTable(rowCount)
        runBenchmark("SMOKE: Section A MIN") {
          runSectionA("MIN", 3, rowCount, smokeHalfW)
        }
        runBenchmark("SMOKE: Section B MIN W sweep point") {
          runSectionB(smokeHalfW, stressBs = smokeHalfW >= 2000, rowCount)
        }
      } else {
        // Full matrix
        setupIntTable(FULL_N)

        // Section A: 6 aggregates x 3 cells; MAX/STDDEV use iters=5 per rev3 skeptic
        runBenchmark("Section A - MIN (non-invertible)") {
          runSectionA("MIN", 3, FULL_N, MAIN_HALF_W)
        }
        runBenchmark("Section A - MAX (non-invertible, noisy -> iters=5)") {
          runSectionA("MAX", 5, FULL_N, MAIN_HALF_W)
        }
        runBenchmark("Section A - SUM (Spark has no inverse; full recompute)") {
          runSectionA("SUM", 3, FULL_N, MAIN_HALF_W)
        }
        runBenchmark("Section A - COUNT") {
          runSectionA("COUNT", 3, FULL_N, MAIN_HALF_W)
        }
        runBenchmark("Section A - AVG (multi-buffer)") {
          runSectionA("AVG", 3, FULL_N, MAIN_HALF_W)
        }
        runBenchmark("Section A - STDDEV_SAMP (multi-buffer, noisy -> iters=5)") {
          runSectionA("STDDEV_SAMP", 5, FULL_N, MAIN_HALF_W)
        }

        // Section B: MIN, W sweep
        runBenchmark("Section B - W=201 scaling") {
          runSectionB(100, stressBs = false, FULL_N)
        }
        runBenchmark("Section B - W=1001 scaling") {
          runSectionB(500, stressBs = false, FULL_N)
        }
        runBenchmark("Section B - W=4001 scaling (+ bs=256 cross-block stress)") {
          runSectionB(2000, stressBs = true, FULL_N)
        }

        // Section F: spill regression guard -- only meaningful with tight heap.
        // Phase 0 baseline (C1/C2 unpatched): numbers are observational only,
        // no hard threshold. Will be gated after R1.
        setupStringTable(SPILL_N)
        runBenchmark("Section F - spill regression guard (String, baseline)") {
          runSpillGuard()
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
