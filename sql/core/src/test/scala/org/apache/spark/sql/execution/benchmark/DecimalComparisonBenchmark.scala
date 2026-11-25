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
 * A benchmark that measures the performance of comparing Decimal values.
 *
 * The filters compare every row of a cached column with literals, and few rows match. Literals
 * and decimal(20,2) values hold a BigDecimal, decimal(15,2) values hold a long. The aggregate
 * compares decimal(20,2) values with each other.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/DecimalComparisonBenchmark-results.txt".
 * }}}
 */
object DecimalComparisonBenchmark extends SqlBasedBenchmark {

  // Literals outside the range of the column would let the cache skip every batch, and a list of
  // more than ten would become an InSet.
  private val narrowFilter =
    "n IN (1010.01, 2020.02, 3030.03, 4040.04, 5050.05, 6060.06, 7070.07, 8080.08, 9090.09, " +
      "9910.10) OR n BETWEEN 4210.10 AND 4290.19"
  private val wideFilter =
    "w IN (10.01, 20.02, 30.03, 40.04, 50.05, 60.06, 70.07, 80.08, 90.09, 99.10) " +
      "OR w BETWEEN 42.10 AND 42.19"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numRows: Long = if (mainArgs.length > 0) mainArgs(0).toLong else 10L * 1000L * 1000L
    val iters: Int = if (mainArgs.length > 1) mainArgs(1).toInt else 5

    runBenchmark("Decimal comparison") {
      val df = spark.range(0, numRows, 1, 1).selectExpr(
        "cast(rand(1) * 99999 + 900 as decimal(15, 2)) as n",
        "cast(rand(2) * 100 as decimal(20, 2)) as w").cache()
      df.count()

      val bench = new Benchmark("decimal comparison", numRows, output = output)
      Seq(
        "filter on decimal(15,2)" -> df.where(narrowFilter).select("n"),
        "filter on decimal(20,2)" -> df.where(wideFilter).select("w"),
        "max of decimal(20,2)" -> df.selectExpr("max(w)")).foreach { case (name, query) =>
        bench.addCase(name, numIters = iters) { _ => query.noop() }
      }
      bench.run()
    }
  }
}
