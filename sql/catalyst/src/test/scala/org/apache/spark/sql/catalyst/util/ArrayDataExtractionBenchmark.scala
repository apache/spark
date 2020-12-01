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

package org.apache.spark.sql.catalyst.util

import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for [[ArrayData]] array extraction methods.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/test:runMain <this class>"
 *      Results will be written to "benchmarks/ArrayDataExtractionBenchmark-results.txt".
 * }}}
 */
object ArrayDataExtractionBenchmark extends BenchmarkBase {

  def benchmark(): Unit = {
    val valuesPerIteration: Long = 1000 * 1000 * 10
    val arraySize = 25
    val randomIntArray: Array[Int] = new Array[Int](arraySize).map(_ => Random.nextInt())

    val benchmark = new Benchmark(
      "ArrayData - arrays extraction",
      valuesPerIteration, output = output)

    benchmark.addCase("GenericArrayData (int[] constructor): array() call") { _ =>
      val arr: Array[Int] = randomIntArray
      var n = 0
      while (n < valuesPerIteration) {
        new GenericArrayData(arr).array
        n += 1
      }
    }

    benchmark.addCase("GenericArrayData (int[] constructor): toIntArray() call") { _ =>
      val arr: Array[Int] = randomIntArray
      var n = 0
      while (n < valuesPerIteration) {
        new GenericArrayData(arr).toIntArray()
        n += 1
      }
    }

    benchmark.addCase("UnsafeArrayData: toIntArray() call") { _ =>
      val arr: Array[Int] = randomIntArray
      var n = 0
      while (n < valuesPerIteration) {
        ArrayData.toArrayData(arr).toIntArray()
        n += 1
      }
    }

    benchmark.addCase("PrimitiveArrayData: toIntArray() call") { _ =>
      val arr: Array[Int] = randomIntArray
      var n = 0
      while (n < valuesPerIteration) {
        PrimitiveArrayData.create(arr).toIntArray()
        n += 1
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    benchmark()
  }
}
