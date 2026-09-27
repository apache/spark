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
 * A benchmark that measures grouped decimal aggregates whose buffers have a precision above
 * `Decimal.MAX_LONG_DIGITS`, so the buffer lives in the variable-length region of an UnsafeRow.
 *
 * It covers `sum` and `avg` of a decimal(15,2) column (buffer decimal(25,2)), the TPC-H q1
 * expression `sum(p * (1 - d) * (1 + t))` over decimal(15,2) columns, and a sum over a
 * decimal(28,0) column whose values are all >= 10^18, so the unscaled values never fit the
 * compact (Long) representation. Each case runs with ANSI mode off and on. The input table is
 * cached up front so that the measured time is dominated by the aggregation.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/DecimalArithmeticBenchmark-results.txt".
 * }}}
 */
object DecimalArithmeticBenchmark extends SqlBasedBenchmark {

  private val NumGroups = 1000

  /** Aggregate cases: (label, aggregate expression). */
  private val AggCases: Seq[(String, String)] = Seq(
    ("sum(decimal(15,2))", "sum(p)"),
    ("avg(decimal(15,2))", "avg(p)"),
    ("TPC-H q1 sum(p * (1 - d) * (1 + t))", "sum(p * (1 - d) * (1 + t))"),
    ("sum(decimal(28,0)), all values >= 10^18", "sum(big)")
  )

  private def setupTable(n: Long): Unit = {
    spark.range(n)
      .selectExpr(
        s"id % $NumGroups as k",
        "cast(rand(1) * 99999 + 900 as decimal(15, 2)) as p",
        "cast(rand(2) * 0.1 as decimal(15, 2)) as d",
        "cast(rand(3) * 0.08 as decimal(15, 2)) as t",
        "cast(rand(4) * 1e18 + 1e18 as decimal(28, 0)) as big")
      .coalesce(1)
      .createOrReplaceTempView("t")
    spark.catalog.cacheTable("t")
    spark.table("t").noop()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numRows: Long = if (mainArgs.length > 0) mainArgs(0).toLong else 10L * 1000L * 1000L
    val iters: Int = if (mainArgs.length > 1) mainArgs(1).toInt else 5

    runBenchmark("Grouped decimal aggregates with a wide (precision > 18) buffer") {
      setupTable(numRows)
      try {
        AggCases.foreach { case (label, agg) =>
          val bench = new Benchmark(label, numRows, output = output)
          Seq("false", "true").foreach { ansi =>
            bench.addCase(s"ansi=$ansi", numIters = iters) { _ =>
              withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
                spark.sql(s"select k, $agg from t group by k").noop()
              }
            }
          }
          bench.run()
        }
      } finally {
        spark.catalog.uncacheTable("t")
      }
    }
  }
}
