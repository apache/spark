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
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
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
        .noop()
    }
  }

  private def run(benchmark: Benchmark, cardinality: Long, name: String, exprs: String*): Unit = {
    benchmark.addCase(name, numIters = 3) { _ => doBenchmark(cardinality, exprs: _*) }
  }

  private val ymdExprs = Seq("(2000 + (id % 30))", "((id % 12) + 1)", "((id % 27) + 1)")

  private def benchmarkMakeDate(cardinality: Long): Unit = {
    val benchmark = new Benchmark("make_date()", cardinality, output = output)
    val args = ymdExprs

    run(benchmark, cardinality, "prepare make_date()", args: _*)
    val foldableExpr = "make_date(2019, 9, 16)"
    run(benchmark, cardinality, foldableExpr, foldableExpr)
    run(
      benchmark,
      cardinality,
      "make_date(*, *, *)",
      "make_date" + args.mkString("(", ",", ")"))

    benchmark.run()
  }

  private def benchmarkMakeTimestamp(cardinality: Long): Unit = {
    val benchmark = new Benchmark("make_timestamp()", cardinality, output = output)
    val hmExprs = Seq("id % 24", "id % 60")
    val hmsExprs = hmExprs ++ Seq("cast((id % 60000000) / 1000000.0 as decimal(8, 6))")
    val args = ymdExprs ++ hmsExprs

    run(
      benchmark,
      cardinality,
      "prepare make_timestamp()",
      args: _*)
    var foldableExpr = "make_timestamp(2019, 1, 2, 3, 4, 50.123456)"
    run(benchmark, cardinality, foldableExpr, foldableExpr)
    foldableExpr = "make_timestamp(2019, 1, 2, 3, 4, 60.000000)"
    run(benchmark, cardinality, foldableExpr, foldableExpr)
    foldableExpr = "make_timestamp(2019, 12, 31, 23, 59, 60.00)"
    run(benchmark, cardinality, foldableExpr, foldableExpr)
    run(
      benchmark,
      cardinality,
      "make_timestamp(*, *, *, 3, 4, 50.123456)",
      s"make_timestamp(${ymdExprs.mkString(",")}, 3, 4, 50.123456)")
    run(
      benchmark,
      cardinality,
      "make_timestamp(*, *, *, *, *, 0)",
      s"make_timestamp(" + (ymdExprs ++ hmExprs).mkString(", ") + ", 0)")
    run(
      benchmark,
      cardinality,
      "make_timestamp(*, *, *, *, *, 60.0)",
      s"make_timestamp(" + (ymdExprs ++ hmExprs).mkString(", ") + ", 60.0)")
    run(
      benchmark,
      cardinality,
      "make_timestamp(2019, 1, 2, *, *, *)",
      s"make_timestamp(2019, 1, 2, ${hmsExprs.mkString(",")})")
    run(
      benchmark,
      cardinality,
      "make_timestamp(*, *, *, *, *, *)",
      s"make_timestamp" + args.mkString("(", ", ", ")"))

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    benchmarkMakeDate(100000000L)
    benchmarkMakeTimestamp(1000000L)
  }
}
