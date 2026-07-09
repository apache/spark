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
 * Benchmark to measure performance for sort-based aggregate, focusing on the whole-stage
 * code-gen path. Hash-map based aggregation is intentionally excluded; both
 * `spark.sql.execution.useHashAggregateExec` and `spark.sql.execution.useObjectHashAggregateExec`
 * are disabled so the planner always picks
 * [[org.apache.spark.sql.execution.aggregate.SortAggregateExec]].
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/SortAggregateBenchmark-results.txt".
 * }}}
 */
object SortAggregateBenchmark extends SqlBasedBenchmark {

  // Force the planner to pick SortAggregateExec by disabling both hash-based aggregate operators.
  private val forceSortAggregate: Map[String, String] = Map(
    SQLConf.USE_HASH_AGG.key -> "false",
    SQLConf.USE_OBJECT_HASH_AGG.key -> "false")

  /**
   * Adds the two cases we care about for a sort aggregate. Whole-stage code-gen stays enabled in
   * both so the child pipeline (scan, sort) is code-gen'd either way; only the sort aggregate's own
   * code-gen is toggled via `ENABLE_SORT_AGGREGATE_CODEGEN`, which isolates its contribution:
   *  - code-gen off: sort aggregate falls back to the interpreted `SortBasedAggregationIterator`;
   *  - code-gen on: code-gen'd `SortAggregateExec`.
   */
  private def addGroupingKeyCases(benchmark: Benchmark)(f: () => Unit): Unit = {
    benchmark.addCase("codegen = F", numIters = 2) { _ =>
      withSQLConf(
        (forceSortAggregate ++ Map(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_SORT_AGGREGATE_CODEGEN.key -> "false")).toSeq: _*) {
        f()
      }
    }

    benchmark.addCase("codegen = T", numIters = 5) { _ =>
      withSQLConf(
        (forceSortAggregate ++ Map(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.ENABLE_SORT_AGGREGATE_CODEGEN.key -> "true")).toSeq: _*) {
        f()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("sort aggregate without grouping") {
      val N = 500L << 20
      val benchmark = new Benchmark("sort agg w/o group", N, output = output)
      addGroupingKeyCases(benchmark) { () =>
        spark.range(N).selectExpr("sum(id)").noop()
      }
      benchmark.run()
    }

    runBenchmark("sort aggregate with linear keys") {
      val N = 20 << 22
      val benchmark = new Benchmark("sort agg w linear keys", N, output = output)
      // The child of a sort aggregate must be sorted by the grouping keys. Sorting the whole input
      // dominates, so pre-sort once into a temp view and aggregate over the already-ordered rows.
      spark.range(N).selectExpr("id", "(id & 65535) as k")
        .sortWithinPartitions("k").createOrReplaceTempView("linear")
      addGroupingKeyCases(benchmark) { () =>
        spark.sql("select k, count(*), sum(id), max(id) from linear group by k").noop()
      }
      benchmark.run()
    }

    runBenchmark("sort aggregate with randomized keys") {
      val N = 20 << 22
      val benchmark = new Benchmark("sort agg w randomized keys", N, output = output)
      spark.range(N).selectExpr("id", "floor(rand() * 10000) as k")
        .sortWithinPartitions("k").createOrReplaceTempView("rand_keys")
      addGroupingKeyCases(benchmark) { () =>
        spark.sql("select k, count(*), sum(id), max(id) from rand_keys group by k").noop()
      }
      benchmark.run()
    }

    runBenchmark("sort aggregate with string key") {
      val N = 20 << 20
      val benchmark = new Benchmark("sort agg w string key", N, output = output)
      spark.range(N).selectExpr("id", "cast(id & 1023 as string) as k")
        .sortWithinPartitions("k").createOrReplaceTempView("string_key")
      addGroupingKeyCases(benchmark) { () =>
        spark.sql("select k, count(*), sum(id), max(id) from string_key group by k").noop()
      }
      benchmark.run()
    }

    runBenchmark("sort aggregate with decimal key") {
      val N = 20 << 20
      val benchmark = new Benchmark("sort agg w decimal key", N, output = output)
      spark.range(N).selectExpr("id", "cast(id & 65535 as decimal(18, 0)) as k")
        .sortWithinPartitions("k").createOrReplaceTempView("decimal_key")
      addGroupingKeyCases(benchmark) { () =>
        spark.sql("select k, count(*), sum(id), max(id) from decimal_key group by k").noop()
      }
      benchmark.run()
    }

    runBenchmark("sort aggregate with multiple key types") {
      val N = 20 << 20
      val benchmark = new Benchmark("sort agg w multiple keys", N, output = output)
      spark.range(N)
        .selectExpr(
          "id",
          "(id & 1023) as k1",
          "cast(id & 1023 as string) as k2",
          "cast(id & 1023 as int) as k3",
          "id > 1023 as k4")
        .sortWithinPartitions("k1", "k2", "k3", "k4")
        .createOrReplaceTempView("multi_keys")
      addGroupingKeyCases(benchmark) { () =>
        spark.sql("select k1, k2, k3, k4, count(*), sum(id), max(id) " +
          "from multi_keys group by k1, k2, k3, k4").noop()
      }
      benchmark.run()
    }
  }
}
