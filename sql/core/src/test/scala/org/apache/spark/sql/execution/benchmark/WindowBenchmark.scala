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
 * Benchmark to measure window function performance with bounded frames.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/WindowBenchmark-results.txt".
 * }}}
 */
object WindowBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Window Sliding Frame - MIN/MAX (non-invertible aggregates)") {
      val N = 16L << 20
      spark.range(N)
        .selectExpr("id", "cast(rand(42) * 1000000 as int) as v")
        .coalesce(1)
        .createOrReplaceTempView("t")

      val frame = "OVER (ORDER BY id ROWS BETWEEN 500 PRECEDING AND 500 FOLLOWING)"
      val benchmark = new Benchmark("sliding window, W=1001, 16M rows", N, output = output)

      benchmark.addCase("MIN naive (current)", numIters = 3) { _ =>
        spark.sql(s"SELECT MIN(v) $frame FROM t").noop()
      }
      benchmark.addCase("MAX naive (current)", numIters = 3) { _ =>
        spark.sql(s"SELECT MAX(v) $frame FROM t").noop()
      }
      benchmark.run()
    }
  }
}
