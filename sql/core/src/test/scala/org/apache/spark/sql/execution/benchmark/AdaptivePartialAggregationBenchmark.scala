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
 * Each scenario runs the query across the full matrix of whole-stage codegen on/off and the
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

    // Force the regular map to spill quickly and disable the periodic check (huge minRows). With
    // fully distinct keys the compaction ratio is 1.0, so at the spill boundary the spill check
    // bypasses instead of spilling; the baseline spills repeatedly and falls back to sort-based
    // aggregation.
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
  }

  private def distinctKeyedDf(N: Long): DataFrame =
    spark.range(N).selectExpr("id as k", "id as v").groupBy("k").agg("v" -> "sum")
}
