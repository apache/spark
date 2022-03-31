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

/**
 * Benchmark to measure performance for top-k computation.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
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

        def f(rankFunc: String, partition: String): Unit = {
          spark.read.parquet(path)
            .selectExpr(s"$rankFunc() OVER($partition ORDER BY a) AS rn", "a", "b")
            .where("rn > 100 and rn <= 200")
            .noop()
        }

        val benchmark = new Benchmark("Benchmark Top-K", N, minNumIters = 10)

        benchmark.addCase("ROW_NUMBER WITHOUT PARTITION") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "false") {
            f("ROW_NUMBER", "")
          }
        }

        benchmark.addCase("ROW_NUMBER WITHOUT PARTITION (RANKLIMIT Sorting)") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "true",
            "spark.sql.execution.topKSortFallbackThreshold" -> "0") {
            f("ROW_NUMBER", "")
          }
        }

        benchmark.addCase("ROW_NUMBER WITHOUT PARTITION (RANKLIMIT TakeOrdered)") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "true") {
            f("ROW_NUMBER", "")
          }
        }

        benchmark.addCase("RANK WITHOUT PARTITION") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "false") {
            f("RANK", "")
          }
        }

        benchmark.addCase("RANK WITHOUT PARTITION (RANKLIMIT)") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "true") {
            f("RANK", "")
          }
        }

        benchmark.addCase("DENSE_RANK WITHOUT PARTITION") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "false") {
            f("DENSE_RANK", "")
          }
        }

        benchmark.addCase("DENSE_RANK WITHOUT PARTITION (RANKLIMIT)") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "true") {
            f("DENSE_RANK", "")
          }
        }

        benchmark.addCase("ROW_NUMBER WITH PARTITION") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "false") {
            f("ROW_NUMBER", "PARTITION BY b")
          }
        }

        benchmark.addCase("ROW_NUMBER WITH PARTITION (RANKLIMIT Sorting)") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "true",
            "spark.sql.execution.topKSortFallbackThreshold" -> "0") {
            f("ROW_NUMBER", "PARTITION BY b")
          }
        }

        benchmark.addCase("ROW_NUMBER WITH PARTITION (RANKLIMIT TakeOrdered)") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "true") {
            f("ROW_NUMBER", "PARTITION BY b")
          }
        }

        benchmark.addCase("RANK WITH PARTITION") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "false") {
            f("RANK", "PARTITION BY b")
          }
        }

        benchmark.addCase("RANK WITH PARTITION (RANKLIMIT)") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "true") {
            f("RANK", "PARTITION BY b")
          }
        }

        benchmark.addCase("DENSE_RANK WITH PARTITION") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "false") {
            f("DENSE_RANK", "PARTITION BY b")
          }
        }

        benchmark.addCase("DENSE_RANK WITH PARTITION (RANKLIMIT)") { _ =>
          withSQLConf("spark.sql.rankLimit.enabled" -> "true") {
            f("DENSE_RANK", "PARTITION BY b")
          }
        }

        benchmark.run()
      }
    }
  }
}
