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
package org.apache.spark.util

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for Utils.getIteratorSize vs Iterator.size.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/IteratorSizeBenchmark-results.txt".
 * }}}
 */
object IteratorSizeBenchmark extends BenchmarkBase {

  private def testRangeIteratorSize(valuesPerIteration: Int, range: Range): Unit = {

    val benchmark = new Benchmark(s"Test Range iterator size ${range.size}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Iterator.size") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        range.iterator.size
      }
    }

    benchmark.addCase("Use Utils.getIteratorSize") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Utils.getIteratorSize(range.iterator)
      }
    }

    benchmark.run()
  }

  private def testSeqIteratorSize(valuesPerIteration: Int, seq: Seq[Int]): Unit = {

    val benchmark = new Benchmark(s"Test Seq iterator size ${seq.size}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Iterator.size") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        seq.iterator.size
      }
    }

    benchmark.addCase("Use Utils.getIteratorSize") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Utils.getIteratorSize(seq.iterator)
      }
    }

    benchmark.run()
  }

  private def testArrayIteratorSize(valuesPerIteration: Int, array: Array[Int]): Unit = {

    val benchmark = new Benchmark(s"Test Array iterator size ${array.length}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Iterator.size") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        array.iterator.size
      }
    }

    benchmark.addCase("Use Utils.getIteratorSize") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Utils.getIteratorSize(array.iterator)
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    // Test Range
    testRangeIteratorSize(valuesPerIteration, Range(0, 10))
    testRangeIteratorSize(valuesPerIteration, Range(0, 100))
    testRangeIteratorSize(valuesPerIteration, Range(0, 1000))
    testRangeIteratorSize(valuesPerIteration, Range(0, 10000))
    testRangeIteratorSize(valuesPerIteration, Range(0, 30000))

    // Test Seq
    testSeqIteratorSize(valuesPerIteration, Seq.range(0, 10))
    testSeqIteratorSize(valuesPerIteration, Seq.range(0, 100))
    testSeqIteratorSize(valuesPerIteration, Seq.range(0, 1000))
    testSeqIteratorSize(valuesPerIteration, Seq.range(0, 10000))
    testSeqIteratorSize(valuesPerIteration, Seq.range(0, 30000))

    // Test Array
    testArrayIteratorSize(valuesPerIteration, Array.range(0, 10))
    testArrayIteratorSize(valuesPerIteration, Array.range(0, 100))
    testArrayIteratorSize(valuesPerIteration, Array.range(0, 1000))
    testArrayIteratorSize(valuesPerIteration, Array.range(0, 10000))
    testArrayIteratorSize(valuesPerIteration, Array.range(0, 30000))
  }
}
