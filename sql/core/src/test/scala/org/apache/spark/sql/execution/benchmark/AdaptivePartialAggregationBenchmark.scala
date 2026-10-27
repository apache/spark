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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark comparing runtime adaptive partial aggregation (see
 * [[SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED]]) against the static pre-shuffle partial
 * aggregation. When the partial aggregation is not reducing rows, the operator streams the
 * remaining rows through as single-row partial buffers instead of maintaining (and possibly
 * spilling) a large aggregation map.
 *
 * The first scenarios run the query across the full matrix of whole-stage codegen on/off and the
 * feature disabled (`adaptive = F`, the pre-change baseline) vs enabled (`adaptive = T`), over a
 * {high, low}-cardinality x {no-spill, on-spill} grid:
 *   - high-cardinality, no spill: the periodic check bypasses, which should win.
 *   - low-cardinality, no spill: nothing bypasses, and the per-row guard the feature still adds
 *     shows up as a small overhead to quantify.
 *   - high-cardinality, forced regular-map spill: the spill check bypasses instead of spilling,
 *     which should win.
 *   - low-cardinality, forced regular-map spill: the compaction ratio is far above the threshold,
 *     so the spill check keeps aggregating and both runs spill identically (no regression).
 *
 * The spill-check scenarios read four partitions, so an `Exchange` separates the Partial from the
 * Final, force the map to fill every few hundred thousand rows, and compare the feature disabled
 * with `minCompactionToSpill` at 1.05 and at its default, 2.0, over inputs whose keys arrive
 * together, recur across the task, or concentrate on a few hot keys.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain
 *        org.apache.spark.sql.execution.benchmark.AdaptivePartialAggregationBenchmark"
 *   2. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain
 *        org.apache.spark.sql.execution.benchmark.AdaptivePartialAggregationBenchmark"
 *      Results will be written to "benchmarks/AdaptivePartialAggregationBenchmark-results.txt".
 * }}}
 */
object AdaptivePartialAggregationBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // The upstream `CombineAdjacentAggregation` and `ReplaceHashWithSortAgg` rules would collapse
    // or convert these single-partition hash aggregates, so both are disabled to keep the
    // Partial+Final `HashAggregateExec` structure the adaptive feature governs.
    val fixedPlanConfs = Seq(
      SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false",
      SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "false")

    // Adds the (whole-stage codegen, adaptive switch) matrix for `query`. `extraConf` is applied
    // to all four cases so the only differences are the two axes.
    def addCodegenAdaptiveCases(
        benchmark: Benchmark,
        query: () => DataFrame,
        extraConf: Seq[(String, String)] = Nil): Unit = {
      for {
        wholeStage <- Seq(true, false)
        adaptive <- Seq(false, true)
      } {
        val adaptiveLabel = if (adaptive) "T" else "F"
        val label = s"codegen = $wholeStage, adaptive = $adaptiveLabel"
        benchmark.addCase(label) { _ =>
          withSQLConf(
            (Seq(
              SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
              SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> adaptive.toString) ++
              fixedPlanConfs ++ extraConf): _*) {
            query().noop()
          }
        }
      }
    }

    // Fully distinct keys make partial aggregation useless, so the periodic check bypasses: the
    // feature should be faster than the baseline that maintains a map entry per row.
    runBenchmark("high-cardinality input, pass-through at the periodic check") {
      val N = 8L << 20
      val benchmark = new Benchmark("adaptive partial agg, high card, no spill", N,
        output = output)
      addCodegenAdaptiveCases(benchmark, () => distinctKeyedDf(N))
      benchmark.run()
    }

    // 1000 distinct keys over a large input: partial aggregation reduces a lot, so the periodic
    // check never activates pass-through. The two runs still differ: the adaptive path pays a
    // small per-row overhead (the stop check, the bypass counter and the check-point compare)
    // even when nothing bypasses. The measured difference is a few percent, not a functional
    // regression.
    runBenchmark("low-cardinality input, pass-through at the periodic check") {
      val N = 16L << 20
      val benchmark = new Benchmark("adaptive partial agg, low card, no spill", N,
        output = output)
      addCodegenAdaptiveCases(benchmark, () =>
        spark.range(N).selectExpr("id % 1000 as k", "id as v").groupBy("k").agg("v" -> "sum"))
      benchmark.run()
    }

    // Force the regular map to spill quickly and disable the periodic check (minRows = 0, which
    // also leaves the spill check without a row floor). With fully distinct keys the compaction
    // ratio is 1.0, so at the spill boundary the spill check bypasses instead of spilling; the
    // baseline spills repeatedly and falls back to sort-based aggregation.
    runBenchmark("high-cardinality input, pass-through at the spill check") {
      val N = 8L << 20
      val benchmark = new Benchmark("adaptive partial agg, high card, spill", N, output = output)
      val spillCheckConf = Seq(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "0",
        "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 1048576")
      addCodegenAdaptiveCases(benchmark, () => distinctKeyedDf(N), extraConf = spillCheckConf)
      benchmark.run()
    }

    // Force the regular map to spill quickly on low-cardinality input. With only 1000 distinct
    // keys the compaction ratio is far above the threshold, so even at the spill boundary the
    // spill check correctly keeps aggregating: both runs spill and fall back to sort-based
    // aggregation identically (no regression).
    runBenchmark("low-cardinality input, pass-through at the spill check") {
      val N = 16L << 20
      val benchmark = new Benchmark("adaptive partial agg, low card, spill", N, output = output)
      val spillCheckConf = Seq(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "0",
        "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 1048576")
      addCodegenAdaptiveCases(benchmark, () =>
        spark.range(N).selectExpr("id % 1000 as k", "id as v").groupBy("k").agg("v" -> "sum"),
        extraConf = spillCheckConf)
      benchmark.run()
    }

    // Inputs that compact by about 1.5 within each map epoch, read from four partitions so an
    // `Exchange` separates the Partial from the Final, and with a map forced to fill every few
    // hundred thousand rows. Whether keeping the aggregation through the spills pays off depends
    // on whether the rest of the input keeps repeating keys:
    //   - each key's rows arrive together, so later epochs share no keys and passing the rest
    //     through at the first fill avoids spilling every epoch;
    //   - keys recur uniformly across the task (10 rows per key), so the final merge collapses
    //     them and keeping the aggregation shuffles a tenth of the rows;
    //   - the same over twice the rows and keys, so the maps fill when they hold 40% of a task's
    //     keys: the recent rows compact by about 1.5 and the default passes the rest through,
    //     although each key repeats about 10 times. It runs with the fast map off, and with it on,
    //     where the fast map's keys never spill. The interpreted path has no fast map, so the
    //     latter runs with codegen only;
    //   - 10 hot keys hold 30% of the rows and the rest are unique, so passing through avoids the
    //     spills but sends every hot-key row to the reducer of its key.
    // `minCompactionToSpill = 1.05` judges the spill check by `minCompaction`; the default, 2.0,
    // demands that the recent rows compact by 2.0.
    case class SpillCheckCase(name: String, key: String, fillRows: Int,
        numRows: Long = 8L << 20, twoLevelMap: Boolean = true, codegenOnly: Boolean = false)
    val spillCheckCases = Seq(
      SpillCheckCase("keys arrive together", "cast(id * 2 / 3 as bigint)", 300000),
      SpillCheckCase("keys recur", "pmod(xxhash64(id), 200000)", 220000),
      SpillCheckCase("keys recur, map holds 40% of them, fast map off",
        "pmod(xxhash64(id), 400000)", 204330, numRows = 16L << 20, twoLevelMap = false),
      SpillCheckCase("keys recur, maps hold 40% of them, fast map on",
        "pmod(xxhash64(id), 400000)", 204330, numRows = 16L << 20, codegenOnly = true),
      SpillCheckCase("hot keys", "if(id % 100 < 30, id % 10, id + 1000)", 300000))
    spillCheckCases.foreach { c =>
      runBenchmark(s"spill check with an Exchange, ${c.name}") {
        val N = c.numRows
        val benchmark = new Benchmark(s"adaptive partial agg, spill check, ${c.name}", N,
          output = output)
        val query = () => spark.range(0, N, 1, 4).selectExpr(s"${c.key} as k", "id as v")
          .groupBy("k").agg("v" -> "sum")
        val spillConf = Seq(
          SQLConf.SHUFFLE_PARTITIONS.key -> "8",
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> c.twoLevelMap.toString,
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> s"65536, ${c.fillRows}")
        for {
          wholeStage <- if (c.codegenOnly) Seq(true) else Seq(true, false)
          minCompactionToSpill <- Seq(None, Some("1.05"), Some("2.0"))
        } {
          val label = minCompactionToSpill.map(v => s"minCompactionToSpill = $v")
            .getOrElse("adaptive = F")
          benchmark.addCase(s"codegen = $wholeStage, $label") { _ =>
            withSQLConf(
              (Seq(
                SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
                SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key ->
                  minCompactionToSpill.isDefined.toString) ++
                minCompactionToSpill.map(
                  SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key -> _) ++
                fixedPlanConfs ++ spillConf): _*) {
              query().noop()
            }
          }
        }
        benchmark.run()
      }
    }
  }

  private def distinctKeyedDf(N: Long): DataFrame =
    spark.range(N).selectExpr("id as k", "id as v").groupBy("k").agg("v" -> "sum")
}
