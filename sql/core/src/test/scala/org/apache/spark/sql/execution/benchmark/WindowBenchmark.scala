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
 * Benchmark to measure window function performance with bounded frames.
 *
 * Invocation arguments (positional):
 *   mainArgs(0) rowCount   default 16777216 (16M), used for committed results
 *   mainArgs(1) halfWindow default 500 (frame is W = 2 * halfWindow + 1)
 *
 * Passing args is intended for dev smoke-tests; do NOT combine with
 * SPARK_GENERATE_BENCHMARK_FILES=1 (would overwrite committed baseline).
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/WindowBenchmark-results.txt".
 *   4. quick smoke run (4M rows, W=201):
 *      build/sbt "sql/Test/runMain <this class> 4000000 100"
 * }}}
 */
object WindowBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val rowCount = if (mainArgs.length > 0) mainArgs(0).toLong else 16L << 20
    val halfWindow = if (mainArgs.length > 1) mainArgs(1).toInt else 500
    val windowSize = 2 * halfWindow + 1

    require(rowCount >= 4096,
      s"rowCount=$rowCount too small; segtree may fallback. " +
        s"Use >= 4096 for meaningful measurement.")

    spark.range(rowCount)
      .selectExpr("id", "cast(rand(42) * 1000000 as int) as v")
      .coalesce(1)
      .createOrReplaceTempView("t")

    val frame =
      s"OVER (ORDER BY id ROWS BETWEEN $halfWindow PRECEDING AND $halfWindow FOLLOWING)"
    val rowsLabel = if (rowCount >= 1000000) s"${rowCount / 1000000}M" else rowCount.toString

    // -- SparkListener for memory/spill metrics, shared across sub-benchmarks --
    val metrics = mutable.Map[String, (Long, Long)]()  // caseName -> (peakMem, spilled)
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

    // -- Digest-based correctness check (runs before benchmark timings) --
    def digest(aggFn: String, sqlConfs: (String, String)*): Long = {
      withSQLConf(sqlConfs: _*) {
        spark.sql(s"SELECT SUM(HASH(m)) FROM (SELECT $aggFn(v) $frame AS m FROM t)")
          .head().getLong(0)
      }
    }

    val allCaseNames = mutable.ArrayBuffer[String]()

    def runOne(aggFn: String): Unit = {
      val dNaive = digest(aggFn)
      val dSeg = digest(aggFn, SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true")
      val dSegBs = digest(aggFn,
        SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
        SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> "256")
      require(dNaive == dSeg,
        s"$aggFn segtree digest mismatch: naive=$dNaive seg=$dSeg")
      require(dNaive == dSegBs,
        s"$aggFn segtree (bs=256) digest mismatch: naive=$dNaive seg=$dSegBs")

      val benchmark = new Benchmark(
        s"$aggFn sliding window, W=$windowSize, $rowsLabel rows",
        rowCount, output = output)
      val nNaive = s"$aggFn naive (current, baseline)"
      val nSeg = s"$aggFn segtree (default)"
      val nSegBs = s"$aggFn segtree (blockSize=256)"
      allCaseNames ++= Seq(nNaive, nSeg, nSegBs)

      benchmark.addCase(nNaive, numIters = 3) { _ =>
        currentCase = nNaive
        spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
      }
      benchmark.addCase(nSeg, numIters = 3) { _ =>
        currentCase = nSeg
        withSQLConf(SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true") {
          spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
        }
      }
      benchmark.addCase(nSegBs, numIters = 3) { _ =>
        currentCase = nSegBs
        withSQLConf(
          SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key -> "true",
          SQLConf.WINDOW_SEGMENT_TREE_BLOCK_SIZE.key -> "256") {
          spark.sql(s"SELECT $aggFn(v) $frame FROM t").noop()
        }
      }
      benchmark.run()
    }

    try {
      runBenchmark("Window Sliding Frame - MIN (non-invertible)") {
        runOne("MIN")
      }
      runBenchmark("Window Sliding Frame - MAX (non-invertible)") {
        runOne("MAX")
      }

      // Ensure all task end events are processed before reading metrics.
      spark.sparkContext.listenerBus.waitUntilEmpty()

      // -- Memory/spill side table --
      // scalastyle:off println
      val out = System.out
      out.println()
      out.println("Memory/Spill (peak executor memory / total disk spilled):")
      val width = allCaseNames.map(_.length).max + 2
      for (name <- allCaseNames) {
        val (pm, ds) = metrics.synchronized {
          metrics.getOrElse(name, (0L, 0L))
        }
        val peakMb = pm.toDouble / (1024.0 * 1024.0)
        val label = (name + ":").padTo(width, ' ')
        out.println(f"  $label%s peak=$peakMb%8.2f MB   spilled=$ds%d B")
      }
      out.println()
      // scalastyle:on println
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
