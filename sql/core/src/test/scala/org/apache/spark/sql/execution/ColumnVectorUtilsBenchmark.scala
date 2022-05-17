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

package org.apache.spark.sql.execution

import org.apache.commons.lang3.RandomStringUtils

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ColumnVectorUtils, OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.StringType


/**
 * Benchmark for ColumnVectorUtils.populate use OnHeapColumnVector with OffHeapColumnVector
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/ColumnVectorUtilsBenchmark-results.txt".
 * }}}
 */
object ColumnVectorUtilsBenchmark extends BenchmarkBase {

  def testPopulate(valuesPerIteration: Int, length: Int): Unit = {
    import org.apache.spark.unsafe.UTF8StringBuilder

    val batchSize = 4096
    val onHeapColumnVector = new OnHeapColumnVector(batchSize, StringType)
    val offHeapColumnVector = new OffHeapColumnVector(batchSize, StringType)

    val benchmark = new Benchmark(
      s"Test ColumnVectorUtils.populate, row length = $length",
      valuesPerIteration * batchSize,
      output = output)

    val builder = new UTF8StringBuilder()
    builder.append(RandomStringUtils.random(length))
    val row = InternalRow(builder.build())

    benchmark.addCase("OnHeapColumnVector") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        onHeapColumnVector.reset()
        ColumnVectorUtils.populate(onHeapColumnVector, row, 0)
      }
    }

    benchmark.addCase("OffHeapColumnVector") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        offHeapColumnVector.reset()
        ColumnVectorUtils.populate(offHeapColumnVector, row, 0)
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000
    Seq(1, 5, 10, 15, 20).foreach { length =>
      testPopulate(valuesPerIteration, length)
    }
  }
}
