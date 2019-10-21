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
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for interval functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/IntervalBenchmark-results.txt".
 * }}}
 */
object IntervalBenchmark extends SqlBasedBenchmark {

  private def doBenchmark(cardinality: Long, exprs: String*): Unit = {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      spark
        .range(0, cardinality, 1, 1)
        .selectExpr(exprs: _*)
        .write
        .format("noop")
        .mode(Overwrite)
        .save()
    }
  }

  private def addCase(
      benchmark: Benchmark,
      cardinality: Long,
      name: String,
      exprs: String*): Unit = {
    benchmark.addCase(name, numIters = 3) { _ =>
      doBenchmark(cardinality, exprs: _*)
    }
  }

  private def buildString(withPrefix: Boolean, units: Seq[String] = Seq.empty): String = {
    val sep = if (units.length > 0) ", " else ""
    val otherUnits = s"$sep'${units.mkString(" ")}'"
    val prefix = if (withPrefix) "'interval'" else "''"
    s"concat_ws(' ', ${prefix}, cast(id % 10000 AS string), 'years'${otherUnits})"
  }

  private def addCase(benchmark: Benchmark, cardinality: Long, units: Seq[String]): Unit = {
    Seq(true, false).foreach { withPrefix =>
      val expr = s"CAST(${buildString(withPrefix, units)} AS interval)"
      val note = if (withPrefix) "w/ interval" else "w/o interval"
      benchmark.addCase(s"${units.length + 1} units $note", numIters = 3) { _ =>
        doBenchmark(cardinality, expr)
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 1000000
    val timeUnits = Seq(
      "13 months", "100 weeks", "9 days", "12 hours",
      "5 minutes", "45 seconds", "123 milliseconds", "567 microseconds")
    val intervalToTest = ListBuffer[String]()

    val benchmark = new Benchmark(s"cast strings to intervals", N, output = output)
    addCase(benchmark, N, s"string w/ interval", buildString(true, timeUnits))
    addCase(benchmark, N, s"string w/o interval", buildString(false, timeUnits))
    addCase(benchmark, N, intervalToTest) // Only years

    for (unit <- timeUnits) {
      intervalToTest.append(unit)
      addCase(benchmark, N, intervalToTest)
    }

    benchmark.run()
  }
}
