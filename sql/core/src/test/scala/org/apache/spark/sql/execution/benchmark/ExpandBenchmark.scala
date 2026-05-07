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
 * Benchmark to measure performance for queries with multiple COUNT(DISTINCT)
 * on different columns, which trigger the Expand operator via
 * [[org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates]].
 *
 * The Expand operator duplicates each input row N times (one per distinct
 * group + optional non-distinct group). This data amplification becomes a
 * bottleneck for large N.
 *
 * This benchmark compares baseline (current Expand behavior) against the
 * OptimizeExpand rule that pre-aggregates data before the Expand. Controlled by
 * spark.sql.optimizer.optimizeExpandRatio (default -1 = disabled).
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ExpandBenchmark-results.txt".
 * }}}
 */
object ExpandBenchmark extends SqlBasedBenchmark {

  private val OPTIMIZE_EXPAND_RATIO =
    SQLConf.OPTIMIZE_EXPAND_RATIO.key

  private def prepareTable(name: String, N: Long,
      keyMod: Int, colMods: Seq[Int]): Unit = {
    val colExprs = colMods.zipWithIndex.map { case (mod, i) =>
      s"cast(id % $mod as int) as col${i + 1}"
    }
    spark.range(N)
      .selectExpr(
        (Seq(s"cast(id % $keyMod as string) as key") ++
          colExprs :+ "cast(id as double) as value"): _*
      )
      .createOrReplaceTempView(name)
  }

  private def countDistinctQuery(table: String,
      numDistinct: Int, hasGroupBy: Boolean,
      pureDistinct: Boolean = false): String = {
    val distinctCols = (1 to numDistinct).map(i =>
      s"count(distinct col$i) as cd$i").mkString(",\n  ")
    val nonDistinct = if (pureDistinct) "" else ",\n  sum(value) as total"
    val groupBy = if (hasGroupBy) "GROUP BY key" else ""
    s"""SELECT ${if (hasGroupBy) "key," else ""}
       |  $distinctCols$nonDistinct
       |FROM $table
       |$groupBy""".stripMargin
  }

  private def expandRatioBenchmark(
      title: String, N: Long, numDistinct: Int,
      table: String): Unit = {
    val benchmark = new Benchmark(title, N, output = output)

    val sqlWithSum = countDistinctQuery(
      table, numDistinct, hasGroupBy = true)
    val sqlPure = countDistinctQuery(
      table, numDistinct, hasGroupBy = true,
      pureDistinct = true)
    // With sum(value): Expand ratio = numDistinct + 1 (includes gid=0)
    // Pure distinct: Expand ratio = numDistinct (no gid=0)
    val ratioWithSum = numDistinct + 1
    val ratioPure = numDistinct

    benchmark.addCase(
        s"with sum - baseline (ratio $ratioWithSum)",
        numIters = 5) { _ =>
      withSQLConf(OPTIMIZE_EXPAND_RATIO -> "-1") {
        spark.sql(sqlWithSum).noop()
      }
    }
    benchmark.addCase(
        s"with sum - optimized (ratio $ratioWithSum)",
        numIters = 5) { _ =>
      withSQLConf(OPTIMIZE_EXPAND_RATIO -> "2") {
        spark.sql(sqlWithSum).noop()
      }
    }
    benchmark.addCase(
        s"pure distinct - baseline (ratio $ratioPure)",
        numIters = 5) { _ =>
      withSQLConf(OPTIMIZE_EXPAND_RATIO -> "-1") {
        spark.sql(sqlPure).noop()
      }
    }
    benchmark.addCase(
        s"pure distinct - optimized (ratio $ratioPure)",
        numIters = 5) { _ =>
      withSQLConf(OPTIMIZE_EXPAND_RATIO -> "2") {
        spark.sql(sqlPure).noop()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 10L << 20 // ~10M rows

    runBenchmark("Expand: varying number of COUNT(DISTINCT)") {
      prepareTable("expand_bench", N, keyMod = 1000,
        colMods = Seq(100, 200, 300, 400, 500, 600, 700, 800))

      expandRatioBenchmark(
        "2 distinct aggregates", N, 2, "expand_bench")
      expandRatioBenchmark(
        "4 distinct aggregates", N, 4, "expand_bench")
      expandRatioBenchmark(
        "6 distinct aggregates", N, 6, "expand_bench")
      expandRatioBenchmark(
        "8 distinct aggregates", N, 8, "expand_bench")
    }

    runBenchmark("Expand: varying data characteristics (pure distinct)") {
      // Default: 1K groups, moderate distinct cardinality
      prepareTable("expand_default", N, keyMod = 1000,
        colMods = Seq(100, 200, 300, 400, 500, 600))

      // High cardinality grouping key (100K groups, ~100 rows/group)
      prepareTable("expand_highkey", N, keyMod = 100000,
        colMods = Seq(100, 200, 300, 400, 500, 600))

      // Low cardinality distinct columns (only 5 distinct values each)
      prepareTable("expand_lowcard", N, keyMod = 1000,
        colMods = Seq(5, 5, 5, 5, 5, 5))

      val sql6d = countDistinctQuery(
        "expand_default", 6, hasGroupBy = true, pureDistinct = true)
      val sql6dHighKey = countDistinctQuery(
        "expand_highkey", 6, hasGroupBy = true, pureDistinct = true)
      val sql6dLowCard = countDistinctQuery(
        "expand_lowcard", 6, hasGroupBy = true, pureDistinct = true)
      val sql6dGlobal = countDistinctQuery(
        "expand_default", 6, hasGroupBy = false, pureDistinct = true)

      val benchDataChar = new Benchmark(
        "6 pure distinct aggs with varying data", N, output = output)

      benchDataChar.addCase("1K groups, moderate card - baseline",
          numIters = 5) { _ =>
        withSQLConf(OPTIMIZE_EXPAND_RATIO -> "-1") {
          spark.sql(sql6d).noop()
        }
      }
      benchDataChar.addCase("1K groups, moderate card - optimized",
          numIters = 5) { _ =>
        withSQLConf(OPTIMIZE_EXPAND_RATIO -> "2") {
          spark.sql(sql6d).noop()
        }
      }
      benchDataChar.addCase("100K groups, moderate card - baseline",
          numIters = 5) { _ =>
        withSQLConf(OPTIMIZE_EXPAND_RATIO -> "-1") {
          spark.sql(sql6dHighKey).noop()
        }
      }
      benchDataChar.addCase("100K groups, moderate card - optimized",
          numIters = 5) { _ =>
        withSQLConf(OPTIMIZE_EXPAND_RATIO -> "2") {
          spark.sql(sql6dHighKey).noop()
        }
      }
      benchDataChar.addCase("1K groups, low card (5 vals) - baseline",
          numIters = 5) { _ =>
        withSQLConf(OPTIMIZE_EXPAND_RATIO -> "-1") {
          spark.sql(sql6dLowCard).noop()
        }
      }
      benchDataChar.addCase("1K groups, low card (5 vals) - optimized",
          numIters = 5) { _ =>
        withSQLConf(OPTIMIZE_EXPAND_RATIO -> "2") {
          spark.sql(sql6dLowCard).noop()
        }
      }
      benchDataChar.addCase("no grouping key - baseline",
          numIters = 5) { _ =>
        withSQLConf(OPTIMIZE_EXPAND_RATIO -> "-1") {
          spark.sql(sql6dGlobal).noop()
        }
      }
      benchDataChar.addCase("no grouping key - optimized",
          numIters = 5) { _ =>
        withSQLConf(OPTIMIZE_EXPAND_RATIO -> "2") {
          spark.sql(sql6dGlobal).noop()
        }
      }

      benchDataChar.run()
    }
  }
}
