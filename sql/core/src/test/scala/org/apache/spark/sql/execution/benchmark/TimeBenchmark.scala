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

import java.time.LocalTime

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for TIME data type functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/TimeBenchmark-results.txt".
 * }}}
 */
object TimeBenchmark extends SqlBasedBenchmark {
  private def doBenchmark(cardinality: Int, exprs: String*): Unit = {
    spark.range(cardinality)
      .selectExpr(exprs: _*)
      .noop()
  }

  private def run(cardinality: Int, name: String, exprs: String*): Unit = {
    codegenBenchmark(name, cardinality) {
      doBenchmark(cardinality, exprs: _*)
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    spark.conf.set(SQLConf.TIME_TYPE_ENABLED.key, "true")
    val N = 10000000
    // Generate TIME values using make_time(hour, minute, decimal_seconds)
    val timeExpr = "make_time(cast(mod(id, 24) as int), cast(mod(id, 60) as int), " +
      "cast(mod(id, 60) as decimal(8,6)))"

    runBenchmark("Current time") {
      run(N, "current_time", "current_time()")
    }

    runBenchmark("make_time") {
      run(N, "make_time", timeExpr)
    }

    runBenchmark("Parsing time") {
      val timeStrExpr = "concat(lpad(cast(mod(id, 24) as string), 2, '0'), ':', " +
        "lpad(cast(mod(id, 60) as string), 2, '0'), ':', " +
        "lpad(cast(mod(id, 60) as string), 2, '0'))"
      run(N, "to_time", s"to_time($timeStrExpr, 'HH:mm:ss')")
    }

    runBenchmark("Extract components from TIME") {
      run(N, "hour of time", s"hour($timeExpr)")
      run(N, "minute of time", s"minute($timeExpr)")
      run(N, "second of time", s"second($timeExpr)")
    }

    runBenchmark("time_trunc") {
      Seq("HOUR", "MINUTE", "SECOND").foreach { level =>
        run(N, s"time_trunc $level", s"time_trunc('$level', $timeExpr)")
      }
    }

    runBenchmark("time_diff") {
      val timeExpr2 = "make_time(cast(mod(id + 1, 24) as int), cast(mod(id + 2, 60) as int), " +
        "cast(mod(id + 3, 60) as decimal(8,6)))"
      run(N, "time_diff", s"time_diff('SECOND', $timeExpr, $timeExpr2)")
    }

    runBenchmark("TIME +/- interval") {
      // Use make_time with hour < 22 to avoid overflow when adding intervals
      val safeTimeExpr = "make_time(cast(mod(id, 20) as int), cast(mod(id, 60) as int), " +
        "cast(mod(id, 60) as decimal(8,6)))"
      val benchmark = new Benchmark("TIME +/- interval", N, output = output)
      benchmark.addCase("time + interval hour") { _ =>
        doBenchmark(N, s"$safeTimeExpr + interval 1 hour")
      }
      benchmark.addCase("time + interval minute") { _ =>
        doBenchmark(N, s"$safeTimeExpr + interval 30 minute")
      }
      benchmark.addCase("time + interval second") { _ =>
        doBenchmark(N, s"$safeTimeExpr + interval 45 second")
      }
      benchmark.addCase("time - interval hour") { _ =>
        // Use hours >= 1 to avoid underflow
        val subTimeExpr = "make_time(cast(mod(id, 20) + 2 as int), cast(mod(id, 60) as int), " +
          "cast(mod(id, 60) as decimal(8,6)))"
        doBenchmark(N, s"$subTimeExpr - interval 1 hour")
      }
      benchmark.run()
    }

    runBenchmark("Conversion from/to external types") {
      import spark.implicits._
      val rowsNum = 5000000
      val numIters = 3
      val benchmark = new Benchmark("To/from java.time.LocalTime", rowsNum, output = output)
      benchmark.addCase("From java.time.LocalTime", numIters) { _ =>
        spark.range(rowsNum)
          .map(nanos => LocalTime.ofNanoOfDay(nanos % 86400000000000L))
          .noop()
      }
      def localTimes = {
        spark.range(0, rowsNum, 1, 1)
          .map(nanos => LocalTime.ofNanoOfDay(nanos % 86400000000000L))
      }
      benchmark.addCase("Collect java.time.LocalTime", numIters) { _ =>
        localTimes.collect()
      }
      benchmark.run()
    }
  }
}
