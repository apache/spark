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
 * LAG ranking columns, keyed on a single user dimension. The window functions
 * share one timestamp ORDER BY but are partitioned over two overlapping key
 * groups (user id plus a core flag and plus a service-tier flag, and user id
 * plus a model and a priority flag), so the analyzer stacks them into a single
 * chain of adjacent Window operators. Reordering the stack groups windows by
 * their partition spec, collapsing the number of inserted shuffles and sorts.
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

  private val N = 16L << 20 // ~16M rows

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

  /** Synthetic inference-request rows keyed by user. */
  private def prepareServingTable(name: String, n: Long): Unit = {
    spark.range(n)
      .selectExpr(
        "id as req_id",
        "cast(id % 1000000 as long) as user_id",
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

  private def windowReorderBenchmark(
      title: String, n: Long, table: String, numIters: Int): Unit = {
    val benchmark = new Benchmark(title, n, output = output)
    val sql = windowedSelect(table)

    benchmark.addCase("baseline (window reorder off)", numIters = numIters) { _ =>
      withSQLConf(WINDOW_REORDER_ENABLED -> "false") {
        spark.sql(sql).noop()
      }
    }
    benchmark.addCase("optimized (window reorder on)", numIters = numIters) { _ =>
      withSQLConf(WINDOW_REORDER_ENABLED -> "true") {
        spark.sql(sql).noop()
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
        "serving_log", numIters)
    }
  }
}
