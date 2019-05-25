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

package org.apache.spark.sql

import java.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.expressions.{HiveHasher, XXH64}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * Synthetic benchmark for MurMurHash 3 and xxHash64.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/test:runMain <this class>"
 *      Results will be written to "benchmarks/HashByteArrayBenchmark-results.txt".
 * }}}
 */
object HashByteArrayBenchmark extends BenchmarkBase {
  def test(length: Int, seed: Long, numArrays: Int, iters: Int): Unit = {
    val random = new Random(seed)
    val arrays = Array.fill[Array[Byte]](numArrays) {
      val bytes = new Array[Byte](length)
      random.nextBytes(bytes)
      bytes
    }

    val benchmark = new Benchmark(
      "Hash byte arrays with length " + length, iters * numArrays.toLong, output = output)
    benchmark.addCase("Murmur3_x86_32") { _: Int =>
      var sum = 0L
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numArrays) {
          sum += Murmur3_x86_32.hashUnsafeBytes(arrays(i), Platform.BYTE_ARRAY_OFFSET, length, 42)
          i += 1
        }
      }
    }

    benchmark.addCase("xxHash 64-bit") { _: Int =>
      var sum = 0L
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numArrays) {
          sum += XXH64.hashUnsafeBytes(arrays(i), Platform.BYTE_ARRAY_OFFSET, length, 42)
          i += 1
        }
      }
    }

    benchmark.addCase("HiveHasher") { _: Int =>
      var sum = 0L
      for (_ <- 0L until iters) {
        var i = 0
        while (i < numArrays) {
          sum += HiveHasher.hashUnsafeBytes(arrays(i), Platform.BYTE_ARRAY_OFFSET, length)
          i += 1
        }
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Benchmark for MurMurHash 3 and xxHash64") {
      test(8, 42L, 1 << 10, 1 << 11)
      test(16, 42L, 1 << 10, 1 << 11)
      test(24, 42L, 1 << 10, 1 << 11)
      test(31, 42L, 1 << 10, 1 << 11)
      test(64 + 31, 42L, 1 << 10, 1 << 11)
      test(256 + 31, 42L, 1 << 10, 1 << 11)
      test(1024 + 31, 42L, 1 << 10, 1 << 11)
      test(2048 + 31, 42L, 1 << 10, 1 << 11)
      test(8192 + 31, 42L, 1 << 10, 1 << 11)
    }
  }
}
