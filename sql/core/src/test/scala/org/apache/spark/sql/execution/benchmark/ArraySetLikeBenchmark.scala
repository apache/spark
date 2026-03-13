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
import org.apache.spark.sql.functions._

/**
 * Benchmark for measuring perf of array set-like operations.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ArraySetLikeBenchmark-results.txt".
 * }}}
 */
object ArraySetLikeBenchmark extends SqlBasedBenchmark {
  private val N = 1000L

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val arrayDistinctBenchmark = new Benchmark("Array Distinct", 1000, output = output)
    arrayDistinctBenchmark.addCase("array_distinct") { _ =>
      spark.range(N)
        .select(concat(sequence(lit(1), lit(10000)), sequence(lit(1), lit(10000))).alias("arr"))
        .select(array_distinct(col("arr")))
        .collect()
    }
    arrayDistinctBenchmark.run()

    val arrayUnionBenchmark = new Benchmark("Array Union", 1000, output = output)
    arrayUnionBenchmark.addCase("array_union") { _ =>
      spark.range(N)
        .select(
          sequence(lit(1), lit(10000)).alias("arr1"),
          sequence(lit(5001), lit(15000)).alias("arr2"))
        .select(array_union(col("arr1"), col("arr2")))
        .collect()
    }
    arrayUnionBenchmark.run()

    val arrayExceptBenchmark = new Benchmark("Array Except", 1000, output = output)
    arrayExceptBenchmark.addCase("array_except") { _ =>
      spark.range(N)
        .select(
          sequence(lit(1), lit(10000)).alias("arr1"),
          sequence(lit(5001), lit(15000)).alias("arr2"))
        .select(array_except(col("arr1"), col("arr2")))
        .collect()
    }
    arrayExceptBenchmark.run()

    val arrayIntersectBenchmark = new Benchmark("Array Intersect", 1000, output = output)
    arrayIntersectBenchmark.addCase("array_intersect") { _ =>
      spark.range(N)
        .select(
          sequence(lit(1), lit(10000)).alias("arr1"),
          sequence(lit(5001), lit(15000)).alias("arr2"))
        .select(array_intersect(col("arr1"), col("arr2")))
        .collect()
    }
    arrayIntersectBenchmark.run()
  }
}
