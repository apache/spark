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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.SparkSession

/**
 * Benchmark primitive arrays via DataFrame and Dataset program using primitive arrays
 * To run this benchmark:
 * 1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 * 2. build/sbt "sql/test:runMain <this class>"
 * 3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *    Results will be written to "benchmarks/PrimitiveArrayBenchmark-results.txt".
 */
object PrimitiveArrayBenchmark extends BenchmarkBase {
  lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .getOrCreate()

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Write primitive arrays in dataset") {
      writeDatasetArray(4)
    }
  }

  def writeDatasetArray(iters: Int): Unit = {
    import sparkSession.implicits._

    val count = 1024 * 1024 * 2

    val sc = sparkSession.sparkContext
    val primitiveIntArray = Array.fill[Int](count)(65535)
    val dsInt = sc.parallelize(Seq(primitiveIntArray), 1).toDS
    dsInt.count  // force to build dataset
    val intArray = { i: Int =>
      var n = 0
      var len = 0
      while (n < iters) {
        len += dsInt.map(e => e).queryExecution.toRdd.collect.length
        n += 1
      }
    }
    val primitiveDoubleArray = Array.fill[Double](count)(65535.0)
    val dsDouble = sc.parallelize(Seq(primitiveDoubleArray), 1).toDS
    dsDouble.count  // force to build dataset
    val doubleArray = { i: Int =>
      var n = 0
      var len = 0
      while (n < iters) {
        len += dsDouble.map(e => e).queryExecution.toRdd.collect.length
        n += 1
      }
    }

    val benchmark = new Benchmark("Write an array in Dataset", count * iters, output = output)
    benchmark.addCase("Int   ")(intArray)
    benchmark.addCase("Double")(doubleArray)
    benchmark.run
  }
}
