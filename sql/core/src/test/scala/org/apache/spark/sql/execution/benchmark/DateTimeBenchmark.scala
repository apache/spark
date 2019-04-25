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

import java.sql.Timestamp

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for date and timestamp functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/DateTimeBenchmark-results.txt".
 * }}}
 */
object DateTimeBenchmark extends SqlBasedBenchmark {
  private def doBenchmark(cardinality: Int, exprs: String*): Unit = {
    spark.range(cardinality).selectExpr(exprs: _*).write.format("noop").save()
  }

  private def run(cardinality: Int, name: String, exprs: String*): Unit = {
    codegenBenchmark(name, cardinality) {
      doBenchmark(cardinality, exprs: _*)
    }
  }

  private def run(cardinality: Int, func: String): Unit = {
    codegenBenchmark(s"$func of timestamp", cardinality) {
      doBenchmark(cardinality, s"$func(cast(id as timestamp))")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 10000000
    runBenchmark("Extract components") {
      run(N, "cast to timestamp", "cast(id as timestamp)")
      run(N, "year")
      run(N, "quarter")
      run(N, "month")
      run(N, "weekofyear")
      run(N, "day")
      run(N, "dayofyear")
      run(N, "dayofmonth")
      run(N, "dayofweek")
      run(N, "weekday")
      run(N, "hour")
      run(N, "minute")
      run(N, "second")
    }
    runBenchmark("Current date and time") {
      run(N, "current_date", "current_date")
      run(N, "current_timestamp", "current_timestamp")
    }
    runBenchmark("Date arithmetic") {
      val dateExpr = "cast(cast(id as timestamp) as date)"
      run(N, "cast to date", dateExpr)
      run(N, "last_day", s"last_day($dateExpr)")
      run(N, "next_day", s"next_day($dateExpr, 'TU')")
      run(N, "date_add", s"date_add($dateExpr, 10)")
      run(N, "date_sub", s"date_sub($dateExpr, 10)")
      run(N, "add_months", s"add_months($dateExpr, 10)")
    }
    runBenchmark("Formatting dates") {
      val dateExpr = "cast(cast(id as timestamp) as date)"
      run(N, "format date", s"date_format($dateExpr, 'MMM yyyy')")
    }
    runBenchmark("Formatting timestamps") {
      run(N, "from_unixtime", "from_unixtime(id, 'yyyy-MM-dd HH:mm:ss.SSSSSS')")
    }
    runBenchmark("Convert timestamps") {
      withSQLConf(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key -> "true") {
        val timestampExpr = "cast(id as timestamp)"
        run(N, "from_utc_timestamp", s"from_utc_timestamp($timestampExpr, 'CET')")
        run(N, "to_utc_timestamp", s"to_utc_timestamp($timestampExpr, 'CET')")
      }
    }
    runBenchmark("Intervals") {
      val (start, end) = ("cast(id as timestamp)", "cast((id+8640000) as timestamp)")
      run(N, "cast interval", start, end)
      run(N, "datediff", s"datediff($start, $end)")
      run(N, "months_between", s"months_between($start, $end)")
      run(1000000, "window", s"window($start, 100, 10, 1)")
    }
    runBenchmark("Truncation") {
      val timestampExpr = "cast(id as timestamp)"
      Seq("YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "DAY", "DD", "HOUR", "MINUTE",
          "SECOND", "WEEK", "QUARTER").foreach { level =>
        run(N, s"date_trunc $level", s"date_trunc('$level', $timestampExpr)")
      }
      val dateExpr = "cast(cast(id as timestamp) as date)"
      Seq("year", "yyyy", "yy", "mon", "month", "mm").foreach { level =>
        run(N, s"trunc $level", s"trunc('$level', $dateExpr)")
      }
    }
    runBenchmark("Parsing") {
      val n = 1000000
      val timestampStrExpr = "concat('2019-01-27 11:02:01.', cast(mod(id, 1000) as string))"
      val pattern = "'yyyy-MM-dd HH:mm:ss.SSS'"
      run(n, "to timestamp str", timestampStrExpr)
      run(n, "to_timestamp", s"to_timestamp($timestampStrExpr, $pattern)")
      run(n, "to_unix_timestamp", s"to_unix_timestamp($timestampStrExpr, $pattern)")
      val dateStrExpr = "concat('2019-01-', cast(mod(id, 25) as string))"
      run(n, "to date str", dateStrExpr)
      run(n, "to_date", s"to_date($dateStrExpr, 'yyyy-MM-dd')")
    }
    runBenchmark("Conversion from/to external types") {
      import spark.implicits._
      val rowsNum = 5000000
      val numIters = 3
      val benchmark = new Benchmark("To/from java.sql.Timestamp", rowsNum, output = output)
      benchmark.addCase("From java.sql.Timestamp", numIters) { _ =>
        spark.range(rowsNum)
          .map(millis => new Timestamp(millis))
          .write.format("noop").save()
      }
      benchmark.addCase("Collect longs", numIters) { _ =>
        spark.range(0, rowsNum, 1, 1)
          .collect()
      }
      benchmark.addCase("Collect timestamps", numIters) { _ =>
        spark.range(0, rowsNum, 1, 1)
          .map(millis => new Timestamp(millis))
          .collect()
      }
      benchmark.run()
    }
  }
}
