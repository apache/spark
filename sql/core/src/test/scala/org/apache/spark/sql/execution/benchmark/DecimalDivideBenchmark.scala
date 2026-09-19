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
 * A benchmark that measures the performance of per-row Decimal division (`a / b`).
 *
 * It covers a compact case (decimal(7,2) / decimal(7,2), result decimal(17,10)) and an
 * already-inflated case (decimal(38,18) / decimal(38,18), result decimal(38,6)), each with
 * ANSI mode off and on. Each case also measures division by an integer literal (`a / 2`),
 * the per-row shape used by TPC-DS q4's year_total. Divisors are generated non-null and
 * >= 1 so the divide-by-zero branch is never taken.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/DecimalDivideBenchmark-results.txt".
 * }}}
 */
object DecimalDivideBenchmark extends SqlBasedBenchmark {

  /**
   * Division cases: (label, p, s, integral-part bound).
   *
   * `bound` caps the generated integral part so `rand() * bound + 1`
   * stays within `decimal(p, s)`; the `+ 1` floor keeps every divisor
   * non-zero.
   */
  private val DivideCases: Seq[(String, Int, Int, Long)] = Seq(
    ("decimal(7,2) / decimal(7,2)", 7, 2, 99998L),
    ("decimal(38,18) / decimal(38,18)", 38, 18, 99999998L)
  )

  private def setupDivideTable(n: Long, p: Int, s: Int, bound: Long): Unit = {
    spark.range(n)
      .selectExpr(
        s"cast(rand(1) * $bound + 1 as decimal($p, $s)) as a",
        s"cast(rand(2) * $bound + 1 as decimal($p, $s)) as b")
      .coalesce(1)
      .createOrReplaceTempView("t")
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numRows: Long = if (mainArgs.length > 0) mainArgs(0).toLong else 10L * 1000L * 1000L
    val iters: Int = if (mainArgs.length > 1) mainArgs(1).toInt else 5

    runBenchmark("Decimal division") {
      DivideCases.foreach { case (label, p, s, bound) =>
        setupDivideTable(numRows, p, s, bound)
        val bench = new Benchmark(label, numRows, output = output)
        Seq("a / b", "a / 2").foreach { expr =>
          Seq("false", "true").foreach { ansi =>
            bench.addCase(s"$expr (ansi=$ansi)", numIters = iters) { _ =>
              withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
                spark.sql(s"select $expr from t").noop()
              }
            }
          }
        }
        bench.run()
      }
    }
  }
}
