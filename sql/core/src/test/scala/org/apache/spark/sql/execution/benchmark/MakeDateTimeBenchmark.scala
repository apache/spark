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
 * Synthetic benchmark for the make_date() and make_timestamp() functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/MakeDateTimeBenchmark-results.txt".
 * }}}
 */
object MakeDateTimeBenchmark extends SqlBasedBenchmark {
  private def doBenchmark(cardinality: Long, exprs: String*): Unit = {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      spark
        .range(0, cardinality, 1, 1)
        .selectExpr(exprs: _*)
        .write
        .format("noop")
        .save()
    }
  }

  private def run(
      benchmark: Benchmark,
      cardinality: Long,
      name: String,
      exprs: String*): Unit = {
    benchmark.addCase(name, numIters = 3) { _ =>
      doBenchmark(cardinality, exprs: _*)
    }
  }

  private def benchmarkMakeDate(cardinality: Long): Unit = {
    val benchmark = new Benchmark(s"make_date()", cardinality, output = output)
    val ymdExprs = Seq("(1900 + (id % 200))", "((id % 12) + 1)", "((id % 27) + 1)")

    run(benchmark, cardinality, "prepare make_date()", ymdExprs: _*)
    val foldableExpr = "make_date(2019, 9, 16)"
    run(benchmark, cardinality, foldableExpr, foldableExpr)
    run(
      benchmark,
      cardinality,
      "make_date(1900..2099, 1..12, 1..28)",
      "make_date" + ymdExprs.mkString("(", ",", ")"))

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 100000000L

    benchmarkMakeDate(N)
  }
}
