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

import org.apache.spark.sql.catalyst.expressions.XXH64
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.util.Benchmark

/**
 * Synthetic benchmark for MurMurHash 3 and xxHash64.
 */
object HashByteArrayBenchmark {
  def test(length: Int, seed: Long, numArrays: Int, iters: Int): Unit = {
    val random = new Random(seed)
    val arrays = Array.fill[Array[Byte]](numArrays) {
      val bytes = new Array[Byte](length)
      random.nextBytes(bytes)
      bytes
    }

    val benchmark = new Benchmark("Hash byte arrays with length " + length, iters * numArrays)
    benchmark.addCase("Murmur3_x86_32") { _: Int =>
      for (_ <- 0L until iters) {
        var sum = 0
        var i = 0
        while (i < numArrays) {
          sum += Murmur3_x86_32.hashUnsafeBytes(arrays(i), Platform.BYTE_ARRAY_OFFSET, length, 42)
          i += 1
        }
      }
    }

    benchmark.addCase("xxHash 64-bit") { _: Int =>
      for (_ <- 0L until iters) {
        var sum = 0L
        var i = 0
        while (i < numArrays) {
          sum += XXH64.hashUnsafeBytes(arrays(i), Platform.BYTE_ARRAY_OFFSET, length, 42)
          i += 1
        }
      }
    }

    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    // Add 31 to all arrays to create worse case alignment for xxHash.
    /*
    Running benchmark: Hash byte arrays with length 31
      Running case: Murmur3_x86_32
      Running case: xxHash 64-bit

    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 31:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                             41 /   41         51.7          19.3       1.0X
    xxHash 64-bit                              33 /   34         63.7          15.7       1.2X
    */
    test(31, 42L, 1 << 10, 1 << 11)

    /*
    Running benchmark: Hash byte arrays with length 95
      Running case: Murmur3_x86_32
      Running case: xxHash 64-bit

    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 95:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                             97 /   98         21.6          46.3       1.0X
    xxHash 64-bit                              73 /   74         28.8          34.7       1.3X
    */
    test(64 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Running benchmark: Hash byte arrays with length 287
      Running case: Murmur3_x86_32
      Running case: xxHash 64-bit

    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 287:   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                            282 /  284          7.4         134.7       1.0X
    xxHash 64-bit                             113 /  114         18.5          54.0       2.5X
    */
    test(256 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Running benchmark: Hash byte arrays with length 1055
      Running case: Murmur3_x86_32
      Running case: xxHash 64-bit

    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 1055:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                           1050 / 1051          2.0         500.8       1.0X
    xxHash 64-bit                             283 /  284          7.4         135.1       3.7X
    */
    test(1024 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Running benchmark: Hash byte arrays with length 2079
      Running case: Murmur3_x86_32
      Running case: xxHash 64-bit

    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 2079:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                           2065 / 2070          1.0         984.7       1.0X
    xxHash 64-bit                             472 /  473          4.4         225.3       4.4X
    */
    test(2048 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Running benchmark: Hash byte arrays with length 8223
      Running case: Murmur3_x86_32
      Running case: xxHash 64-bit

    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 8223:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                           8184 / 8198          0.3        3902.3       1.0X
    xxHash 64-bit                            1563 / 1569          1.3         745.4       5.2X
     */
    test(8192 + 31, 42L, 1 << 10, 1 << 11)
  }
}
