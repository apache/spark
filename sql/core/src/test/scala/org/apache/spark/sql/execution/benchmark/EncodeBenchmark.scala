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
 * Benchmark for measuring perf of different Base64 implementations
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/EncodeBenchmark-results.txt".
 * }}}
 */
object EncodeBenchmark extends SqlBasedBenchmark {
  import spark.implicits._
  private val N = 20L * 1000 * 1000

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withTempPath { path =>
      // scalastyle:off nonascii
      val exprs = Seq(
        "",
        "Spark",
        "白日依山尽，黄河入海流。欲穷千里目，更上一层楼。",
        "το Spark είναι το πιο δημοφιλές πλαίσιο επεξεργασίας μεγάλων δεδομένων παγκοσμίως",
        "세계에서 가장 인기 있는 빅데이터 처리 프레임워크인 Spark",
        "Sparkは世界で最も人気のあるビッグデータ処理フレームワークである。")
      // scalastyle:off nonascii

      spark.range(N).map { i =>
        val idx = (i % 6).toInt
        val str = exprs(idx)
        (str, str * 3, str * 5, str * 9, "")
      }.write.parquet(path.getCanonicalPath)

      val utf8 = new Benchmark("encode", N, output = output)
      utf8.addCase("UTF-8", 3) { _ =>
        spark.read.parquet(path.getCanonicalPath).selectExpr(
          "encode(_1, 'UTF-8')",
          "encode(_2, 'UTF-8')",
          "encode(_3, 'UTF-8')",
          "encode(_4, 'UTF-8')",
          "encode(_5, 'UTF-8')").noop()
      }
      utf8.run()
    }
  }
}
