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
    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 8:     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                             11 /   12        185.1           5.4       1.0X
    xxHash 64-bit                              17 /   18        120.0           8.3       0.6X
    */
    test(8, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 16:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                             18 /   18        118.6           8.4       1.0X
    xxHash 64-bit                              20 /   21        102.5           9.8       0.9X
    */
    test(16, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 24:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                             24 /   24         86.6          11.5       1.0X
    xxHash 64-bit                              23 /   23         93.2          10.7       1.1X
    */
    test(24, 42L, 1 << 10, 1 << 11)

    // Add 31 to all arrays to create worse case alignment for xxHash.
    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 31:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                             38 /   39         54.7          18.3       1.0X
    xxHash 64-bit                              33 /   33         64.4          15.5       1.2X
    */
    test(31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 95:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                             91 /   94         22.9          43.6       1.0X
    xxHash 64-bit                              68 /   69         30.6          32.7       1.3X
    */
    test(64 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 287:   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                            268 /  268          7.8         127.6       1.0X
    xxHash 64-bit                             108 /  109         19.4          51.6       2.5X
    */
    test(256 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 1055:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                            942 /  945          2.2         449.4       1.0X
    xxHash 64-bit                             276 /  276          7.6         131.4       3.4X
    */
    test(1024 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 2079:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                           1839 / 1843          1.1         876.8       1.0X
    xxHash 64-bit                             445 /  448          4.7         212.1       4.1X
    */
    test(2048 + 31, 42L, 1 << 10, 1 << 11)

    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash byte arrays with length 8223:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Murmur3_x86_32                           7307 / 7310          0.3        3484.4       1.0X
    xxHash 64-bit                            1487 / 1488          1.4         709.1       4.9X
     */
    test(8192 + 31, 42L, 1 << 10, 1 << 11)
  }
}
