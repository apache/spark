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
 * Benchmark for `RewriteSlidingFramesAsPrefixDifferences`, mimicking the motivating
 * production pattern: a group of nested sliding `ROWS BETWEEN n PRECEDING AND CURRENT ROW`
 * window frames over one `(PARTITION BY user_id ORDER BY day)` key, with several measures
 * and frame widths 2/6/29/89/179 PRECEDING, plus unbounded-preceding running sums.
 *
 * The naive plan re-aggregates every row inside each frame on every output row, i.e. the sum
 * of the frame widths (= 3+7+30+90+180 = 310) aggregate updates per output row per measure;
 * the rewrite computes one running sum per measure and derives every frame from it as
 * `C - coalesce(lag(C, width), 0)`.
 *
 * A third variant runs the naive query under the executor-side block-chunked segment tree
 * (SPARK-56546, `spark.sql.window.segmentTree.enabled`): same logical plan, O(log W) frame
 * evaluation for the moving frames. This is the head-to-head comparison against the
 * prefix-difference rewrite, which is O(1) per frame but restricted to invertible aggregates
 * over integral, ANSI-off measures.
 *
 * Run via SBT:
 * {{{
 *   build/sbt "sql/test:runMain org.apache.spark.sql.execution.benchmark."
 *     WindowPrefixRewriteBenchmark
 * }}}
 * Dev smoke via positional mainArgs: (0)=rowCount. Set SPARK_GENERATE_BENCHMARK_FILES=1 to
 * regenerate the results file; do not combine with positional mainArgs.
 */
object WindowPrefixRewriteBenchmark extends SqlBasedBenchmark {

  private val confKey = SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key
  private val segTreeKey = SQLConf.WINDOW_SEGMENT_TREE_ENABLED.key

  // The motivating query's frame family: five horizons per measure.
  private val widths = Seq(2, 6, 29, 89, 179)

  // Section A: production-like shape, six measures. N calibrated so the naive case lands
  // in the ~20s/iter range, and 2048 days per partition (close to the motivating node's
  // ~2.5k-row partitions).
  private val A_N: Long = 2L * 1024 * 1024
  private val A_USERS: Long = 1024

  // Section B: single measure at the cost-gate boundary (widths just above 32).
  private val B_N: Long = 2L * 1024 * 1024
  private val B_USERS: Long = 1024

  // Section C: same row count as A, but 16x fewer users, i.e. 32768 days per window
  // partition. The segment tree needs wide partitions before its per-tree build cost
  // pays off (break-even around ~1000 rows per partition on this shape), so this is
  // the section where the executor-side alternative gets its best shot.
  private val C_N: Long = 2L * 1024 * 1024
  private val C_USERS: Long = 64

  // Section D: gate sweep row count.
  private val D_N: Long = 2L * 1024 * 1024
  private val D_USERS: Long = 1024

  private val measures = Seq("cnt", "good_cnt", "bad_cnt", "risk_cnt", "warn_cnt", "diff_cnt")

  private def setupTable(n: Long, users: Long = A_USERS): Unit = {
    // The motivating node's measures arrive via nvl(..., 0), i.e. non-nullable.
    spark.range(n)
      .selectExpr(
        s"id % $users as user_id",
        s"cast(id / $users as int) as day",
        "nvl(cast(rand(42) * 100 as bigint), 0) as cnt",
        "nvl(cast(rand(43) * 100 as bigint), 0) as good_cnt",
        "nvl(cast(rand(44) * 100 as bigint), 0) as bad_cnt",
        "nvl(cast(rand(45) * 100 as bigint), 0) as risk_cnt",
        "nvl(cast(rand(46) * 100 as bigint), 0) as warn_cnt",
        "nvl(cast(rand(47) * 100 as bigint), 0) as diff_cnt")
      .coalesce(1)
      .createOrReplaceTempView("seller_day")
  }

  private def slidingSums(ms: Seq[String]): String =
    ms.flatMap { m =>
      widths.map { w =>
        s"sum($m) OVER (PARTITION BY user_id ORDER BY day " +
          s"ROWS BETWEEN $w PRECEDING AND CURRENT ROW) AS ${m}_${w}d"
      }
    }.mkString(",\n  ")

  private def growingSums(ms: Seq[String]): String =
    ms.map { m =>
      s"sum($m) OVER (PARTITION BY user_id ORDER BY day " +
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS " + s"${m}_total"
    }.mkString(",\n  ")

  private def queryText(ms: Seq[String]): String =
    s"""SELECT user_id, day, ${ms.mkString(", ")},
       |  ${slidingSums(ms)},
       |  ${growingSums(ms)}
       |FROM seller_day""".stripMargin

  /** Bit-level digest of the full projection, to catch any silent result change. */
  private def digest(ms: Seq[String]): Long = {
    val windowCols = ms.flatMap(m => widths.map(w => s"${m}_${w}d")) ++ ms.map(m => s"${m}_total")
    val cols = Seq("user_id", "day", ms.mkString(", "), windowCols.mkString(", ")).mkString(", ")
    spark.sql(s"SELECT SUM(hash($cols)) FROM (${queryText(ms)})").head().getLong(0)
  }

  // Gate-calibration sweep: widths to test for the naive-vs-rewrite crossover. Each entry
  // is `n PRECEDING`, so the frame holds n+1 rows (the width the gate sums).
  private val D_PRECEDINGS = Seq(1, 3, 7, 15, 31)

  private def naiveQuery(n: Int): String =
    s"""SELECT user_id, day, cnt,
       |  sum(cnt) OVER (PARTITION BY user_id ORDER BY day
       |    ROWS BETWEEN $n PRECEDING AND CURRENT ROW) AS s
       |FROM seller_day""".stripMargin

  /**
   * Hand-written Form A - exactly the plan shape the rule emits for a non-nullable
   * measure: one running sum in an inner window, lag differences in an outer window.
   */
  private def rewrittenQuery(n: Int): String = {
    val w = n + 1
    s"""SELECT user_id, day,
       |  c - coalesce(lag(c, $w) OVER (PARTITION BY user_id ORDER BY day), 0) AS s
       |FROM (
       |  SELECT user_id, day,
       |    sum(cnt) OVER (PARTITION BY user_id ORDER BY day
       |      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c
       |  FROM (SELECT user_id, day, cnt FROM seller_day)
       |)""".stripMargin
  }

  private def valueDigest(q: String): Long =
    spark.sql(s"SELECT SUM(hash(user_id, day, s)) FROM ($q)").head().getLong(0)

  /**
   * Section D: for each width, time the naive single-window plan against the hand-written
   * Form-A plan, independent of the rule's cost gate, to locate the cost crossover.
   */
  private def runGateSweep(rows: Long, users: Long, iters: Int): Unit = {
    setupTable(rows, users)
    D_PRECEDINGS.foreach { n =>
      val dNaive = valueDigest(naiveQuery(n))
      val dRewritten = valueDigest(rewrittenQuery(n))
      require(dNaive == dRewritten,
        s"gate sweep digest mismatch at $n PRECEDING: naive=$dNaive rewritten=$dRewritten")
    }
    runBenchmark("Section D - gate sweep: naive vs Form-A rewrite, widths 2/4/8/16/32") {
      val benchmark = new Benchmark(
        "Section D - gate sweep (single measure)", rows, output = output)
      D_PRECEDINGS.foreach { n =>
        val w = n + 1
        benchmark.addCase(s"naive, width $w", numIters = iters) { _ =>
          withSQLConf(confKey -> "false") {
            spark.sql(naiveQuery(n)).noop()
          }
        }
        benchmark.addCase(s"Form-A rewrite, width $w", numIters = iters) { _ =>
          withSQLConf(confKey -> "true") {
            spark.sql(rewrittenQuery(n)).noop()
          }
        }
      }
      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // The rule only applies in a legacy (ANSI-off) session; the sums are analyzed with the
    // session's ANSI setting, so the whole suite runs with ANSI off.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      runBenchmarkSuiteInternal(mainArgs)
    }
  }

  private def runBenchmarkSuiteInternal(mainArgs: Array[String]): Unit = {
    val sweepOnly = mainArgs.headOption.contains("sweep")
    val smokeMode = mainArgs.nonEmpty && !sweepOnly
    val smokeRowCount = if (smokeMode) mainArgs(0).toLong else 0L

    def runSection(name: String, rows: Long, users: Long, ms: Seq[String], iters: Int): Unit = {
      setupTable(rows, users)
      val dNaive = digest(ms)
      val dRewrite = withSQLConf(confKey -> "true") { digest(ms) }
      require(dNaive == dRewrite,
        s"digest mismatch: naive=$dNaive rewritten=$dRewrite")
      // The segment tree must be bit-identical too: for integral sums it re-folds exact
      // integer partials, so no floating-point-style drift is acceptable.
      val dSegTree = withSQLConf(confKey -> "false", segTreeKey -> "true") { digest(ms) }
      require(dNaive == dSegTree,
        s"digest mismatch: naive=$dNaive segment-tree=$dSegTree")

      runBenchmark(name) {
        val benchmark = new Benchmark(name, rows, output = output)
        benchmark.addCase("sliding frames (rewrite off, segtree off)", numIters = iters) { _ =>
          withSQLConf(confKey -> "false", segTreeKey -> "false") {
            spark.sql(queryText(ms)).noop()
          }
        }
        benchmark.addCase("segment tree (rewrite off, segtree on)", numIters = iters) { _ =>
          withSQLConf(confKey -> "false", segTreeKey -> "true") {
            spark.sql(queryText(ms)).noop()
          }
        }
        benchmark.addCase("prefix differences (rewrite on)", numIters = iters) { _ =>
          withSQLConf(confKey -> "true") {
            spark.sql(queryText(ms)).noop()
          }
        }
        benchmark.run()
      }
    }

    if (smokeMode) {
      runSection(s"Smoke, $smokeRowCount rows", smokeRowCount, A_USERS, measures, iters = 1)
    } else if (sweepOnly) {
      runGateSweep(D_N, D_USERS, iters = 5)
    } else {
      runSection("Section A - production-like shape: 6 measures x widths 2/6/29/89/179",
        A_N, A_USERS, measures, iters = 3)
      // Single measure over the production frame family (total width 310).
      runSection("Section B - single measure, widths 2/6/29/89/179 (total width 310)",
        B_N, B_USERS, Seq("cnt"), iters = 5)
      // Wide window partitions: the segment tree's best case.
      runSection("Section C - wide partitions (32768 days/user), 6 measures x widths 2/6/29/89/179",
        C_N, C_USERS, measures, iters = 3)
      // Cost crossover calibration for the rule's gate (16, 2x the crossover at ~8).
      runGateSweep(D_N, D_USERS, iters = 3)
    }
  }
}
