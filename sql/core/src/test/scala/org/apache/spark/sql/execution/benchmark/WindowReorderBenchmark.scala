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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure the `TransposeWindow` stack reordering behind
 * `spark.sql.optimizer.windowReorder.enabled` (default false). The query
 * mirrors the window-function shape of a typical LLM-serving ETL: one wide
 * SELECT that annotates every inference request with a batch of ROW_NUMBER /
 * LAG ranking columns, keyed on a single user dimension. The windows are
 * partitioned over three specs that share the user key: the nested pair
 * (user_id, is_primary) and (user_id, tier, is_primary), plus (user_id,
 * model, priority), so the analyzer stacks them into a single chain of
 * adjacent Window operators.
 * Several windows use `ORDER BY create_ts` and others `ORDER BY create_ts DESC`;
 * apart from rn_user_tier and prev_create_ts, which share one spec and which the
 * analyzer therefore fuses into a single Window in both configurations, every
 * complete required child ordering is distinct and reordering cannot share sorts
 * here; the measured win is from grouping by partition spec and removing
 * redundant shuffles.
 *
 * A second case mixes a COLLECT_LIST window (an unbounded array output) with narrow
 * ranking windows over incomparable key groups. The collect_list rides one minimal
 * group's exchange, and first-appearance group order would move it below the other
 * group's exchange, shipping every partition's array through that shuffle and its
 * sort. The rule ranks the wide group last among exchange-optimal orders, so the
 * optimized run keeps the wide output above the surviving exchanges.
 *
 * This benchmark compares the baseline (default, reordering off) against the
 * optimized behavior (reordering on), and measures the wall-clock runtime only:
 * the exchanges that the reordering removes are observable as fewer, larger
 * shuffles, so they translate into lower total runtime.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/WindowReorderBenchmark-results.txt".
 * }}}
 *
 * Optional arguments:
 * {{{
 *   <this class> smoke   # fast run with tiny data and a single iteration
 * }}}
 */
object WindowReorderBenchmark extends SqlBasedBenchmark {

  private val WINDOW_REORDER_ENABLED =
    SQLConf.WINDOW_REORDER_ENABLED.key

  // ~16M rows.
  private val N = 16L << 20

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // The base session runs local[1] with a single shuffle partition (and AQE on),
      // which coalesces every exchange and would hide the very shuffles this benchmark
      // measures. Several cores and many partitions with AQE off keep the inserted
      // exchanges observable.
      .set("spark.master", "local[4]")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "16")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    SparkSession.builder().config(conf).getOrCreate()
  }

  /**
   * Synthetic inference-request rows keyed by user. The user modulus is prime, so none of
   * the other `id % k` columns is a function of `user_id`: with a 1000000 modulus, `id % 2`,
   * `id % 4` and `id % 5` would all be, and the three specs below would collapse back onto
   * the user key instead of grouping rows differently. The user count is small enough that
   * the widest spec `(user_id, model, priority)` still groups several rows per partition
   * rather than degenerating into singleton groups.
   */
  private def prepareServingTable(name: String, n: Long): Unit = {
    spark.range(n)
      .selectExpr(
        "id as req_id",
        "cast(id % 99989 as long) as user_id",
        "cast(id % 2 as int) as is_primary",
        "cast(id % 3 as int) as tier",
        "cast(id % 5 as int) as model",
        "cast(id % 4 as int) as priority",
        "cast(id as long) as create_ts")
      .createOrReplaceTempView(name)
  }

  /** A SELECT with the ranking/lag window batch over the two user key groups. */
  private def windowedSelect(table: String): String =
    s"""SELECT
       |  ROW_NUMBER() OVER (PARTITION BY user_id, is_primary
       |      ORDER BY create_ts) AS rn_user,
       |  ROW_NUMBER() OVER (PARTITION BY user_id, tier, is_primary
       |      ORDER BY create_ts) AS rn_user_tier,
       |  ROW_NUMBER() OVER (PARTITION BY user_id, model, priority
       |      ORDER BY create_ts) AS rn_user_model,
       |  ROW_NUMBER() OVER (PARTITION BY user_id, is_primary
       |      ORDER BY create_ts DESC) AS rn_user_desc,
       |  ROW_NUMBER() OVER (PARTITION BY user_id, tier, is_primary
       |      ORDER BY create_ts DESC) AS rn_user_tier_desc,
       |  ROW_NUMBER() OVER (PARTITION BY user_id, model, priority
       |      ORDER BY create_ts DESC) AS rn_user_model_desc,
       |  LAG(create_ts) OVER (PARTITION BY user_id, tier, is_primary
       |      ORDER BY create_ts) AS prev_create_ts
       |FROM $table""".stripMargin

  /**
   * Fewer users than the serving table, so each COLLECT_LIST partition holds enough
   * events for the wide-output shuffle cost to be observable. The user modulus is prime
   * for the same reason as in `prepareServingTable`.
   */
  private def prepareSessionTable(name: String, n: Long): Unit = {
    spark.range(n)
      .selectExpr(
        "id as req_id",
        "cast(id % 49999 as long) as user_id",
        "cast(id % 2 as int) as is_primary",
        "cast(id % 3 as int) as model",
        "cast(id % 4 as int) as priority",
        "cast(id as long) as create_ts")
      .createOrReplaceTempView(name)
  }

  /**
   * Narrow ranking windows beside a wide collect_list: (user_id, is_primary) and
   * (model) are the minimal specs, and the collect_list on (user_id, is_primary,
   * priority) rides the first one's exchange. With the flag off the collect_list
   * stays on top over three exchanges; with it on, the wide group must rank last
   * over two exchanges rather than crossing the (model) shuffle.
   */
  private def wideWindowedSelect(table: String): String =
    s"""SELECT
       |  ROW_NUMBER() OVER (PARTITION BY user_id, is_primary
       |      ORDER BY create_ts) AS rn_user,
       |  COUNT(1) OVER (PARTITION BY model) AS model_reqs,
       |  COLLECT_LIST(create_ts) OVER (PARTITION BY user_id, is_primary, priority
       |      ORDER BY create_ts) AS user_events
       |FROM $table""".stripMargin

  private def windowReorderBenchmark(
      title: String, n: Long, query: String, numIters: Int): Unit = {
    val benchmark = new Benchmark(title, n, output = output)

    benchmark.addCase("baseline (window reorder off)", numIters = numIters) { _ =>
      withSQLConf(WINDOW_REORDER_ENABLED -> "false") {
        spark.sql(query).noop()
      }
    }
    benchmark.addCase("optimized (window reorder on)", numIters = numIters) { _ =>
      withSQLConf(WINDOW_REORDER_ENABLED -> "true") {
        spark.sql(query).noop()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = if (mainArgs.contains("smoke")) 1 else 5
    val n = if (mainArgs.contains("smoke")) 1000L else N

    prepareServingTable("serving_log", n)
    runBenchmark("WindowReorder: stack reordering of the window chain") {
      windowReorderBenchmark("window batch over two user key groups", n,
        windowedSelect("serving_log"), numIters)
    }

    prepareSessionTable("session_log", n)
    runBenchmark("WindowReorder: wide collect_list output in the window chain") {
      windowReorderBenchmark("ranking windows with a wide event list", n,
        wideWindowedSelect("session_log"), numIters)
    }
  }
}
