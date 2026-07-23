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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure performance for NearestByJoin.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/NearestByJoinBenchmark-results.txt".
 * }}}
 */
object NearestByJoinBenchmark extends SqlBasedBenchmark {

  private val k = 5

  private def streamingHeapVsRewrite(): Unit = {
    val size = 30000
    val left = spark.range(0, size).toDF("id").withColumn("x", rand(42) * 1000.0)
    val right = spark.range(0, size).toDF("rid").withColumn("y", rand(43) * 1000.0)
    left.cache().count()
    right.cache().count()

    val benchmark = new org.apache.spark.benchmark.Benchmark(
      s"NearestByJoin ${size}x${size} k=$k", size, output = output)

    benchmark.addCase("streaming heap (broadcast)") { _ =>
      withSQLConf(
        SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> "true",
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        left.nearestByJoin(right, abs(col("x") - col("y")),
          numResults = k, mode = "exact", direction = "distance").noop()
      }
    }

    benchmark.addCase("cross-product rewrite") { _ =>
      withSQLConf(
        SQLConf.NEAREST_BY_BROADCAST_ENABLED.key -> "false",
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        left.nearestByJoin(right, abs(col("x") - col("y")),
          numResults = k, mode = "exact", direction = "distance").noop()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("NearestByJoin Benchmark") {
      streamingHeapVsRewrite()
    }
  }
}
