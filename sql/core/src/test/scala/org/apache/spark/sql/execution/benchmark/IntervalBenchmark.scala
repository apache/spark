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

import scala.collection.mutable.ListBuffer

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for interval functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/IntervalBenchmark-results.txt".
 * }}}
 */
object IntervalBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private def doBenchmark(cardinality: Long, columns: Column*): Unit = {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      spark
        .range(0, cardinality, 1, 1)
        .select(columns: _*)
        .queryExecution
        .toRdd
        .foreach(_ => ())
    }
  }

  private def addCase(
      benchmark: Benchmark,
      cardinality: Long,
      name: String,
      exprs: Column*): Unit = {
    benchmark.addCase(name, numIters = 3) { _ =>
      doBenchmark(cardinality, exprs: _*)
    }
  }

  private def doBenchmarkExpr(cardinality: Long, exprs: String*): Unit = {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      spark
        .range(0, cardinality, 1, 1)
        .selectExpr(exprs: _*)
        .queryExecution
        .toRdd
        .foreach(_ => ())
    }
  }

  private def addCaseExpr(
      benchmark: Benchmark,
      cardinality: Long,
      name: String,
      exprs: String*): Unit = {
    benchmark.addCase(name, numIters = 3) { _ => doBenchmarkExpr(cardinality, exprs: _*) }
  }


  private def buildString(withPrefix: Boolean, units: Seq[String] = Seq.empty): Column = {
    val init = lit(if (withPrefix) "interval" else "") ::
      ($"id" % 10000).cast("string") ::
      lit("years") :: Nil

    concat_ws(" ", (init ++ units.map(lit)): _*)
  }

  private def addCase(benchmark: Benchmark, cardinality: Long, units: Seq[String]): Unit = {
    Seq(true, false).foreach { withPrefix =>
      val expr = buildString(withPrefix, units).cast("interval")
      val note = if (withPrefix) "w/ interval" else "w/o interval"
      benchmark.addCase(s"${units.length + 1} units $note", numIters = 3) { _ =>
        doBenchmark(cardinality, expr)
      }
    }
  }

  private def benchmarkIntervalStringParsing(cardinality: Long): Unit = {
    val timeUnits = Seq(
      "13 months", "                      1                     months",
      "100 weeks", "9 days", "12 hours", "-                    3 hours",
      "5 minutes", "45 seconds", "123 milliseconds", "567 microseconds")
    val intervalToTest = ListBuffer[String]()

    val benchmark = new Benchmark("cast strings to intervals", cardinality, output = output)
    // The first 2 cases are used to show the overhead of preparing the interval string.
    addCase(benchmark, cardinality, "prepare string w/ interval", buildString(true, timeUnits))
    addCase(benchmark, cardinality, "prepare string w/o interval", buildString(false, timeUnits))
    addCase(benchmark, cardinality, intervalToTest.toSeq) // Only years

    for (unit <- timeUnits) {
      intervalToTest.append(unit)
      addCase(benchmark, cardinality, intervalToTest.toSeq)
    }

    benchmark.run()
  }

  private def benchmarkMakeInterval(cardinality: Long): Unit = {
    val benchmark = new Benchmark("make_interval()", cardinality, output = output)
    val hmExprs = Seq("id % 24", "id % 60")
    val hmsExprs = hmExprs ++ Seq("cast((id % 500000000) / 1000000.0 as decimal(18, 6))")
    val ymExprs = Seq("(2000 + (id % 30))", "((id % 12) + 1)")
    val wdExpr = Seq("((id % 54) + 1)", "((id % 1000) + 1)")
    val args = ymExprs ++ wdExpr ++ hmsExprs

    addCaseExpr(
      benchmark,
      cardinality,
      "prepare make_interval()",
      args: _*)
    val foldableExpr = "make_interval(0, 1, 2, 3, 4, 5, 50.123456)"
    addCaseExpr(benchmark, cardinality, foldableExpr, foldableExpr)
    addCaseExpr(
      benchmark,
      cardinality,
      "make_interval(*, *, 2, 3, 4, 5, 50.123456)",
      s"make_interval(${ymExprs.mkString(",")}, 2, 3, 4, 5, 50.123456)")
    addCaseExpr(
      benchmark,
      cardinality,
      "make_interval(0, 1, *, *, 4, 5, 50.123456)",
      s"make_interval(0, 1, ${wdExpr.mkString(",")}, 4, 5, 50.123456)")
    addCaseExpr(
      benchmark,
      cardinality,
      "make_interval(0, 1, 2, 3, *, *, *)",
      s"make_interval(0, 1, 2, 3, ${hmsExprs.mkString(",")})")
    addCaseExpr(
      benchmark,
      cardinality,
      "make_interval(*, *, *, *, *, *, *)",
      s"make_interval(${args.mkString(",")})")

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    benchmarkIntervalStringParsing(1000000)
    benchmarkMakeInterval(1000000)
  }
}
