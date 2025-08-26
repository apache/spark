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

package org.apache.spark.util.collection

import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for PercentileHeap performance.
 * Measures heap insertion and percentile calculation performance
 * under various heap sizes and percentile values.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/PercentileHeapBenchmark-results.txt".
 * }}}
 */
object PercentileHeapBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("PercentileHeap Operations") {
      percentileHeapBenchmark()
    }
  }

  private def percentileHeapBenchmark(): Unit = {

    for (inputSize <- Seq(10000, 50000, 100000, 200000)) {
      val benchmark = new Benchmark(s"PercentileHeap Operations - Input Size: $inputSize",
        inputSize, output = output)
      for (percentile <- Seq(0.5, 0.9, 0.95, 0.99)) {
        benchmark.addTimerCase(s"Percentile: $percentile", 3) { timer =>
          performPercentileHeapOperations(inputSize, percentile, timer)
        }
      }
      benchmark.run()
    }
  }

  private def performPercentileHeapOperations(
       inputSize: Int, percentile: Double, timer: Benchmark.Timer): Unit = {
    val input: Seq[Int] = 0 until inputSize
    val shuffled = Random.shuffle(input).toArray
    val h = new PercentileHeap(percentile)

    timer.startTiming()
    shuffled.foreach { x =>
      h.insert(x)
      for (_ <- 0 until h.size()) {
        h.percentile()
      }
    }
    timer.stopTiming()
  }
}
