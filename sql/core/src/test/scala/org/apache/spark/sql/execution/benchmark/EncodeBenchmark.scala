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
 * Benchmark for encode
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
  private val N = 10L * 1000 * 1000

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

      val benchmark = new Benchmark("encode", N, output = output)
      def addBenchmarkCase(charset: String): Unit = {
        benchmark.addCase(charset) { _ =>
          spark.read.parquet(path.getCanonicalPath).selectExpr(
            s"encode(_1, '$charset')",
            s"encode(_2, '$charset')",
            s"encode(_3, '$charset')",
            s"encode(_4, '$charset')",
            s"encode(_5, '$charset')").noop()
        }
      }
      addBenchmarkCase("UTF-32")
      addBenchmarkCase("UTF-16")
      addBenchmarkCase("UTF-8")
      benchmark.run()
    }
  }
}
