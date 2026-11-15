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
import org.apache.spark.sql.internal.SQLConf.{WHOLESTAGE_CODEGEN_ENABLED, WINDOW_GROUP_LIMIT_THRESHOLD}

/**
 * Benchmark to measure performance for top-k computation.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/TopKBenchmark-results.txt".
 * }}}
 */
object TopKBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Top-K Computation") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath + "/topk_benchmark_table"
        val N = 1024 * 1024 * 20
        spark.range(0, N, 1, 11)
          .selectExpr("id as a", "id % 1024 as b")
          .write.mode("overwrite")
          .parquet(path)

        def f(rankLikeFunc: String, partition: String): Unit = {
          spark.read.parquet(path)
            .selectExpr(s"$rankLikeFunc() OVER($partition ORDER BY a) AS rn", "a", "b")
            .where("rn > 100 and rn <= 200")
            .noop()
        }

        val benchmark = new Benchmark("Benchmark Top-K", N, minNumIters = 10, output = output)

        Seq("ROW_NUMBER", "RANK", "DENSE_RANK").foreach { function =>
          Seq("", "PARTITION BY b").foreach { partition =>
            benchmark.addCase(
              s"$function (PARTITION: $partition, WindowGroupLimit: false)") { _ =>
              withSQLConf(WINDOW_GROUP_LIMIT_THRESHOLD.key -> "-1") {
                f(function, partition)
              }
            }

            benchmark.addCase(
              s"$function (PARTITION: $partition, WindowGroupLimit: true)") { _ =>
              f(function, partition)
            }
          }
        }

        benchmark.run()
      }
    }

    // Guards against a specific regression in `WindowGroupLimitExec`'s codegen for `RANK`/
    // `DENSE_RANK`: once a group's rank has passed `limit`, it must stop projecting the order key
    // and comparing it against the previous row for the group's remaining rows (mirroring the
    // interpreted `GroupedLimitIterator.skipRemainingRows`'s short-circuit), rather than
    // continuing to do that work on every already-rejected row. `ROW_NUMBER`'s codegen does no
    // order-key work at all, admitted or not, so it is unaffected either way and serves as the
    // reference point: the RANK/DENSE_RANK-to-ROW_NUMBER time ratio here should stay close to
    // 1.0x with `WholeStageCodegen: true`. A single essentially-unbounded group with `limit=1`
    // maximizes the number of already-rejected rows, making a reintroduced "keep doing the work
    // anyway" regression as visible as possible. The `WholeStageCodegen: false` cases take the
    // interpreted `RankLimitIterator`/`DenseRankLimitIterator` path, which already short-circuits
    // correctly; they are the pre-codegen baseline, not expected to match `ROW_NUMBER` as closely
    // (that comparison belongs to the non-skewed "Top-K Computation" benchmark above).
    runBenchmark("Top-K with Skewed Partition") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath + "/topk_skew_benchmark_table"
        val N = 1024 * 1024 * 5
        // `a` is the partition key: all but a trickle of rows share the single value 0, so
        // `PARTITION BY a` sees one group of nearly N rows.
        spark.range(0, N, 1, 8)
          .selectExpr(s"if(id < ${N - 16}, 0L, id) AS a", "id AS b")
          .write.mode("overwrite")
          .parquet(path)

        def f(rankLikeFunc: String): Unit = {
          spark.read.parquet(path)
            .selectExpr(s"$rankLikeFunc() OVER(PARTITION BY a ORDER BY b) AS rn", "a", "b")
            .where("rn <= 1")
            .noop()
        }

        val benchmark = new Benchmark("Skewed Top-1 per key", N, minNumIters = 10, output = output)

        Seq("ROW_NUMBER", "RANK", "DENSE_RANK").foreach { function =>
          benchmark.addCase(s"$function (WholeStageCodegen: false)") { _ =>
            withSQLConf(WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
              f(function)
            }
          }

          benchmark.addCase(s"$function (WholeStageCodegen: true)") { _ =>
            f(function)
          }
        }

        benchmark.run()
      }
    }
  }
}
