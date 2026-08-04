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
 *   - high-cardinality, no spill: the no-spill tier bypasses, which should win.
 *   - low-cardinality, no spill: nothing bypasses, which must not regress.
 *   - high-cardinality, forced regular-map spill: the on-spill tier bypasses instead of spilling,
 *     which should win.
 *   - low-cardinality, forced regular-map spill: the ratio is too low for the on-spill tier to
 *     bypass, so both runs spill identically (no regression).
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

    // Fully distinct keys make partial aggregation useless, so the no-spill (Tier 1) sampling tier
    // bypasses: the feature should be faster than the baseline that maintains a map entry per row.
    runBenchmark("high-cardinality input, no-spill pass-through (Tier 1)") {
      val N = 8L << 20
      val benchmark = new Benchmark("adaptive partial agg, high card, no spill", N,
        output = output)
      addCodegenAdaptiveCases(benchmark, () => distinctKeyedDf(N))
      benchmark.run()
    }

    // 1000 distinct keys over a large input: partial aggregation reduces a lot, the no-spill tier
    // never fires, and the two runs must match (no regression).
    runBenchmark("low-cardinality input, no-spill pass-through (Tier 1)") {
      val N = 16L << 20
      val benchmark = new Benchmark("adaptive partial agg, low card, no spill", N,
        output = output)
      addCodegenAdaptiveCases(benchmark, () =>
        spark.range(N).selectExpr("id % 1000 as k", "id as v").groupBy("k").agg("v" -> "sum"))
      benchmark.run()
    }

    // Force the regular map to spill quickly and disable the no-spill tier (huge sample). With
    // fully distinct keys the reduction ratio is 1.0, so at the spill boundary the on-spill
    // (Tier 2) tier bypasses instead of spilling; the baseline spills repeatedly and falls back
    // to sort-based aggregation.
    runBenchmark("high-cardinality input, on-spill pass-through (Tier 2)") {
      val N = 8L << 20
      val benchmark = new Benchmark("adaptive partial agg, high card, spill", N, output = output)
      val tier2Conf = Seq(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_SAMPLE_ROWS.key -> Int.MaxValue.toString,
        "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 1048576")
      addCodegenAdaptiveCases(benchmark, () => distinctKeyedDf(N), extraConf = tier2Conf)
      benchmark.run()
    }

    // Force the regular map to spill quickly on low-cardinality input. The reduction ratio is tiny
    // (1000 distinct keys), so even at the spill boundary the on-spill tier correctly does not
    // bypass: both runs spill and fall back to sort-based aggregation identically (no regression).
    runBenchmark("low-cardinality input, on-spill pass-through (Tier 2)") {
      val N = 16L << 20
      val benchmark = new Benchmark("adaptive partial agg, low card, spill", N, output = output)
      val tier2Conf = Seq(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_SAMPLE_ROWS.key -> Int.MaxValue.toString,
        "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 1048576")
      addCodegenAdaptiveCases(benchmark, () =>
        spark.range(N).selectExpr("id % 1000 as k", "id as v").groupBy("k").agg("v" -> "sum"),
        extraConf = tier2Conf)
      benchmark.run()
    }
  }

  private def distinctKeyedDf(N: Long): DataFrame =
    spark.range(N).selectExpr("id as k", "id as v").groupBy("k").agg("v" -> "sum")
}
