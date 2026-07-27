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

package org.apache.spark

import org.mockito.Mockito.{doReturn, mock}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config
import org.apache.spark.scheduler.HighlyCompressedMapStatus
import org.apache.spark.storage.BlockManagerId

/**
 * Benchmark for building a HighlyCompressedMapStatus, which every map task of a shuffle with more
 * than spark.shuffle.minNumPartitionsToHighlyCompress partitions has to pay for. It covers both
 * the case where skewed block sizes are recorded accurately and the case where they are not.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/HighlyCompressedMapStatusBenchmark-results.txt".
 * }}}
 */
object HighlyCompressedMapStatusBenchmark extends BenchmarkBase {

  private val loc = BlockManagerId("a", "host", 1000)

  private def envWithSkewedFactor(skewedFactor: Double): SparkEnv = {
    val conf = new SparkConf()
      .set(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR.key, skewedFactor.toString)
    val env = mock(classOf[SparkEnv])
    doReturn(conf, Seq.empty: _*).when(env).conf
    env
  }

  private def skewedBlockSizes(numPartitions: Int): Array[Long] = {
    val r = new scala.util.Random(912)
    Array.tabulate(numPartitions) { i =>
      // A few blocks are orders of magnitude larger than the rest, as in a skewed shuffle.
      if (i % 500 == 0) (r.nextDouble() * 512 * 1024 * 1024).toLong
      else (r.nextDouble() * 64 * 1024).toLong
    }
  }

  /**
   * Sizes that rise and then fall. This is the worst case for selecting an order statistic with a
   * fixed pivot choice, so it covers the cost of the fallback to sorting.
   */
  private def organPipeBlockSizes(numPartitions: Int): Array[Long] = {
    Array.tabulate(numPartitions)(i => Math.min(i, numPartitions - 1 - i).toLong)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val envWithoutSkewedSizes = envWithSkewedFactor(-1.0)
    val envWithSkewedSizes = envWithSkewedFactor(5.0)
    Seq("skewed" -> skewedBlockSizes _, "organ pipe" -> organPipeBlockSizes _).foreach {
      case (distribution, blockSizes) =>
        runBenchmark(s"Build HighlyCompressedMapStatus, $distribution block sizes") {
          Seq(2048, 10000, 50000).foreach { numPartitions =>
            val benchmark = new Benchmark(s"$numPartitions shuffle partitions", 1, output = output)
            val sizes = blockSizes(numPartitions)

            benchmark.addCase("skewed block sizes not recorded", 10) { _ =>
              SparkEnv.set(envWithoutSkewedSizes)
              HighlyCompressedMapStatus(loc, sizes, 0)
            }

            benchmark.addCase("skewed block sizes recorded accurately", 10) { _ =>
              SparkEnv.set(envWithSkewedSizes)
              HighlyCompressedMapStatus(loc, sizes, 0)
            }

            benchmark.run()
          }
        }
    }
    SparkEnv.set(null)
  }
}
