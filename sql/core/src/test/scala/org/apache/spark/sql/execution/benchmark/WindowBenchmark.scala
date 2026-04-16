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
 * Invocation arguments (positional):
 *   mainArgs(0) rowCount   default 16777216 (16M), used for committed results
 *   mainArgs(1) halfWindow default 500 (frame is W = 2 * halfWindow + 1)
 *
 * Passing args is intended for dev smoke-tests; do NOT combine with
 * SPARK_GENERATE_BENCHMARK_FILES=1 (would overwrite committed baseline).
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/WindowBenchmark-results.txt".
 *   4. quick smoke run (4M rows, W=201):
 *      build/sbt "sql/Test/runMain <this class> 4000000 100"
 * }}}
 */
object WindowBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val rowCount = if (mainArgs.length > 0) mainArgs(0).toLong else 16L << 20
    val halfWindow = if (mainArgs.length > 1) mainArgs(1).toInt else 500
    val windowSize = 2 * halfWindow + 1

    runBenchmark("Window Sliding Frame - MIN/MAX (non-invertible aggregates)") {
      spark.range(rowCount)
        .selectExpr("id", "cast(rand(42) * 1000000 as int) as v")
        .coalesce(1)
        .createOrReplaceTempView("t")

      val frame =
        s"OVER (ORDER BY id ROWS BETWEEN $halfWindow PRECEDING AND $halfWindow FOLLOWING)"
      val rowsLabel = if (rowCount >= 1000000) s"${rowCount / 1000000}M" else rowCount.toString
      val benchmark = new Benchmark(
        s"sliding window, W=$windowSize, $rowsLabel rows", rowCount, output = output)

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
