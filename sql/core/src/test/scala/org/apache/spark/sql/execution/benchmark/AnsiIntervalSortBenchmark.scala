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
 * Benchmark to measure performance for interval sort.
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
object AnsiIntervalSortBenchmark extends SqlBasedBenchmark {
  private val numRows = 100 * 1000 * 1000

  private def radixBenchmark(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)
    benchmark.addCase(s"$name enable radix", 3) { _ =>
      withSQLConf(SQLConf.RADIX_SORT_ENABLED.key -> "true") {
        f
      }
    }

    benchmark.addCase(s"$name disable radix", 3) { _ =>
      withSQLConf(SQLConf.RADIX_SORT_ENABLED.key -> "false") {
        f
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val dt = spark.range(numRows).selectExpr("make_dt_interval(id % 24) as c1", "id as c2")
    radixBenchmark("year month interval one column", numRows) {
      dt.sortWithinPartitions("c1").select("c2").noop()
    }

    radixBenchmark("year month interval two columns", numRows) {
      dt.sortWithinPartitions("c1", "c2").select("c2").noop()
    }

    val ym = spark.range(numRows).selectExpr("make_ym_interval(id % 2000) as c1", "id as c2")
    radixBenchmark("day time interval one columns", numRows) {
      ym.sortWithinPartitions("c1").select("c2").noop()
    }

    radixBenchmark("day time interval two columns", numRows) {
      ym.sortWithinPartitions("c1", "c2").select("c2").noop()
    }
  }
}
