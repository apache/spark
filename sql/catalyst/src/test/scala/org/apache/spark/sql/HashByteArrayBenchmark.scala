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

import org.apache.spark.sql.catalyst.expressions.{HiveHasher, XXH64}
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

  def main(args: Array[String]): Unit = {
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 8:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                  12 /   16        174.3           5.7       1.0X
    xxHash 64-bit                                   17 /   22        120.0           8.3       0.7X
    HiveHasher                                      13 /   15        162.1           6.2       0.9X
    */
    test(8, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 16:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                  19 /   22        107.6           9.3       1.0X
    xxHash 64-bit                                   20 /   24        104.6           9.6       1.0X
    HiveHasher                                      24 /   28         87.0          11.5       0.8X
    */
    test(16, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 24:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                  28 /   32         74.8          13.4       1.0X
    xxHash 64-bit                                   24 /   29         87.3          11.5       1.2X
    HiveHasher                                      36 /   41         57.7          17.3       0.8X
    */
    test(24, 42L, 1 << 10, 1 << 11)

    // Add 31 to all arrays to create worse case alignment for xxHash.
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 31:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                  41 /   45         51.1          19.6       1.0X
    xxHash 64-bit                                   36 /   44         58.8          17.0       1.2X
    HiveHasher                                      49 /   54         42.6          23.5       0.8X
    */
    test(31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 95:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                 100 /  110         21.0          47.7       1.0X
    xxHash 64-bit                                   74 /   78         28.2          35.5       1.3X
    HiveHasher                                     189 /  196         11.1          90.3       0.5X
    */
    test(64 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 287:        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                 299 /  311          7.0         142.4       1.0X
    xxHash 64-bit                                  113 /  122         18.5          54.1       2.6X
    HiveHasher                                     620 /  624          3.4         295.5       0.5X
    */
    test(256 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 1055:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                1068 / 1070          2.0         509.1       1.0X
    xxHash 64-bit                                  306 /  315          6.9         145.9       3.5X
    HiveHasher                                    2316 / 2369          0.9        1104.3       0.5X
    */
    test(1024 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 2079:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                2252 / 2274          0.9        1074.1       1.0X
    xxHash 64-bit                                  534 /  580          3.9         254.6       4.2X
    HiveHasher                                    4739 / 4786          0.4        2259.8       0.5X
    */
    test(2048 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 8223:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                9249 / 9586          0.2        4410.5       1.0X
    xxHash 64-bit                                 2897 / 3241          0.7        1381.6       3.2X
    HiveHasher                                  19392 / 20211          0.1        9246.6       0.5X
    */
    test(8192 + 31, 42L, 1 << 10, 1 << 11)
  }
}
