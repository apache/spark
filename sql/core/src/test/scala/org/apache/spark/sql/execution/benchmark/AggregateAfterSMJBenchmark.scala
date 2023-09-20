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
 * Benchmark to measure performance for aggregate primitives.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/AggregateAfterSMJBenchmark-results.txt".
 * }}}
 */
object AggregateAfterSMJBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Aggregate after SMJ") {
      val N = 10 << 21

      val benchmark = new Benchmark("Aggregate after SMJ", N, output = output)
      spark
        .range(N)
        .selectExpr(
          "cast(id as decimal) as id1",
          "cast(id as decimal) as id2",
          "cast(id + 1 as decimal) as id3")
        .createOrReplaceTempView("t1")
      spark
        .range(N)
        .selectExpr(
          "cast(id as decimal) as id1",
          "cast(id as decimal) as id2",
          "cast(id as decimal) as id3")
        .createOrReplaceTempView("t2")

      def f(): Unit = {
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          spark.sql(
            s"""SELECT t1.id1, t1.id2, count(t1.id3) as cnt
               |FROM t1
               |JOIN t2
               |ON t1.id2 = t2.id2 AND t1.id3 > t2.id3
               |GROUP BY t1.id1, t1.id2
               |""".stripMargin).noop()
        }
      }

      benchmark.addCase("Hash aggregate after SMJ") { _ =>
        withSQLConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "false") {
          f()
        }
      }
      benchmark.addCase("Sort aggregate after SMJ") { _ =>
        withSQLConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "true") {
          f()
        }
      }

      benchmark.run()
    }
  }
}
