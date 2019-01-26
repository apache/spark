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

package org.apache.spark.sql

import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark

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
  private def doBenchmark(cardinality: Int, expr: String): Unit = {
    spark.range(cardinality).selectExpr(expr).write.format("noop").save()
  }

  private def run(cardinality: Int, name: String, expr: String): Unit = {
    codegenBenchmark(name, cardinality) {
      doBenchmark(cardinality, expr)
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
      val timestampExpr = "cast(id as timestamp)"
      run(N, "from_utc_timestamp", s"from_utc_timestamp($timestampExpr, 'CET')")
      run(N, "to_utc_timestamp", s"to_utc_timestamp($timestampExpr, 'CET')")
    }
  }
}
