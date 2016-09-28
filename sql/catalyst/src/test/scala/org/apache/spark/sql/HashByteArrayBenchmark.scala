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

    benchmark.addCase("HiveHasher") { _: Int =>
      for (_ <- 0L until iters) {
        var sum = 0L
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
    Murmur3_x86_32                                  11 /   12        198.9           5.0       1.0X
    xxHash 64-bit                                   16 /   19        130.1           7.7       0.7X
    HiveHasher                                       0 /    0     282254.6           0.0    1419.0X
    */
    test(8, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 16:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                  18 /   19        119.7           8.4       1.0X
    xxHash 64-bit                                   19 /   21        109.9           9.1       0.9X
    HiveHasher                                       0 /    0     281308.1           0.0    2349.8X
    */
    test(16, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 24:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                  25 /   26         83.5          12.0       1.0X
    xxHash 64-bit                                   22 /   23         95.9          10.4       1.1X
    HiveHasher                                       0 /    0     281345.9           0.0    3367.5X
    */
    test(24, 42L, 1 << 10, 1 << 11)

    // Add 31 to all arrays to create worse case alignment for xxHash.
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 31:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                  37 /   38         57.0          17.5       1.0X
    xxHash 64-bit                                   32 /   33         65.8          15.2       1.2X
    HiveHasher                                       0 /    0     281761.7           0.0    4941.4X
    */
    test(31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 95:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                  91 /  100         23.0          43.5       1.0X
    xxHash 64-bit                                   68 /   71         31.0          32.3       1.3X
    HiveHasher                                       0 /    0     281761.7           0.0   12256.3X
    */
    test(64 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 287:        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                 265 /  272          7.9         126.3       1.0X
    xxHash 64-bit                                  107 /  114         19.7          50.8       2.5X
    HiveHasher                                       0 /    0     281837.4           0.0   35592.6X
    */
    test(256 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 1055:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                 941 /  959          2.2         448.7       1.0X
    xxHash 64-bit                                  266 /  278          7.9         126.8       3.5X
    HiveHasher                                       0 /    0     282292.6           0.0  126654.5X
    */
    test(1024 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 2079:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                                1912 / 1918          1.1         911.6       1.0X
    xxHash 64-bit                                  463 /  503          4.5         220.7       4.1X
    HiveHasher                                       0 /    0     281610.3           0.0  256709.1X
    */
    test(2048 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash byte arrays with length 8223:       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Murmur3_x86_32                              10061 / 10102          0.2        4797.5       1.0X
    xxHash 64-bit                                 2115 / 2221          1.0        1008.4       4.8X
    HiveHasher                                       0 /    0     281044.2           0.0 1348297.1X
    */
    test(8192 + 31, 42L, 1 << 10, 1 << 11)
  }
}
