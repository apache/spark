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

import java.lang.{Long => JLong}
import java.util.{Arrays, Comparator}

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.Benchmark
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparator, PrefixComparators}
import org.apache.spark.util.random.XORShiftRandom

class RadixSortSuite extends SparkFunSuite with Logging {
  private val N = 100  // default size of test data
  private val NUM_FUZZ_ROUNDS = 10

  private def generateTestData(size: Int, rand: => Long): (Array[JLong], LongArray) = {
    val ref = Array.tabulate[Long](size) { i => rand }
    val extended = ref ++ Array.fill[Long](size)(0)
    (ref.map(i => new JLong(i)), new LongArray(MemoryBlock.fromLongArray(extended)))
  }

  private def collectToArray(array: LongArray, offset: Int, length: Int): Array[Long] = {
    var i = 0
    val out = new Array[Long](length)
    while (i < length) {
      out(i) = array.get(offset + i)
      i += 1
    }
    out
  }

  private def toJavaComparator(p: PrefixComparator): Comparator[JLong] = {
    new Comparator[JLong] {
      override def compare(a: JLong, b: JLong): Int = {
        p.compare(a, b)
      }
      override def equals(other: Any): Boolean = {
        other == this
      }
    }
  }

  test("sort binary data") {
    val rand = new XORShiftRandom(123)
    val (ref, buffer) = generateTestData(N, rand.nextLong)
    Arrays.sort(ref, toJavaComparator(PrefixComparators.BINARY))
    val outOffset = RadixSort.sort(buffer, N, 0, 7, false, false)
    val result = collectToArray(buffer, outOffset, N)
    assert(ref.view == result.view)
  }

  test("sort binary data descending") {
    val rand = new XORShiftRandom(123)
    val (ref, buffer) = generateTestData(N, rand.nextLong)
    Arrays.sort(ref, toJavaComparator(PrefixComparators.BINARY_DESC))
    val outOffset = RadixSort.sort(buffer, N, 0, 7, true, false)
    val result = collectToArray(buffer, outOffset, N)
    assert(ref.view == result.view)
  }

  test("partial sort binary data") {
    val rand = new XORShiftRandom(123)
    val (ref, buffer) = generateTestData(N, rand.nextLong)
    Arrays.sort(ref, toJavaComparator(new PrefixComparator {
      override def compare(a: Long, b: Long): Int = {
        return PrefixComparators.BINARY.compare(a & 0xffffff0000L, b & 0xffffff0000L)
      }
    }))
    val outOffset = RadixSort.sort(buffer, N, 2, 4, false, false)
    val result = collectToArray(buffer, outOffset, N)
    assert(ref.view == result.view)
  }

  test("sort twos complement integers") {
    val rand = new XORShiftRandom(123)
    val (ref, buffer) = generateTestData(N, rand.nextLong)
    Arrays.sort(ref, toJavaComparator(PrefixComparators.LONG))
    val outOffset = RadixSort.sort(buffer, N, 0, 7, false, true)
    val result = collectToArray(buffer, outOffset, N)
    assert(ref.view == result.view)
  }

  test("sort twos complement integers descending") {
    val rand = new XORShiftRandom(123)
    val (ref, buffer) = generateTestData(N, rand.nextLong)
    Arrays.sort(ref, toJavaComparator(PrefixComparators.LONG_DESC))
    val outOffset = RadixSort.sort(buffer, N, 0, 7, true, true)
    val result = collectToArray(buffer, outOffset, N)
    assert(ref.view == result.view)
  }

  test("sort key prefix array") {
  }

  test("sort fuzz test") {
    var seed = 0L
    try {
      for (i <- 0 to NUM_FUZZ_ROUNDS) {
        seed = System.nanoTime
        val rand = new XORShiftRandom(seed)
        // Generate a random mask to test RadixSort's behavior with different bit fields missing
        val mask = {
          var tmp = ~0L
          for (i <- 0 to rand.nextInt(5)) {
            tmp &= rand.nextLong
          }
          tmp
        }
        val (ref, buffer) = generateTestData(N, rand.nextLong & mask)
        Arrays.sort(ref, toJavaComparator(PrefixComparators.BINARY))
        val outOffset = RadixSort.sort(buffer, N, 0, 7, false, false)
        val result = collectToArray(buffer, outOffset, N)
        assert(ref.view == result.view)
      }
    } catch {
      case t: Throwable =>
        throw new Exception("Failed with seed: " + seed, t)
    }
  }

  test("benchmark sort") {
    val size = 1000000
    val rand = new XORShiftRandom(123)
    val benchmark = new Benchmark("radix sort", size)
    benchmark.addTimerCase("reference Arrays.sort") { timer =>
      val (ref, _) = generateTestData(size, rand.nextLong)
      timer.startTiming()
      Arrays.sort(ref, toJavaComparator(PrefixComparators.BINARY))
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort trivial") { timer =>
      val (_, buffer) = generateTestData(size, 0xffffffffL)
      timer.startTiming()
      RadixSort.sort(buffer, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort one byte") { timer =>
      val (_, buffer) = generateTestData(size, rand.nextLong & 0xff)
      timer.startTiming()
      RadixSort.sort(buffer, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort two bytes") { timer =>
      val (_, buffer) = generateTestData(size, rand.nextLong & 0xffff)
      timer.startTiming()
      RadixSort.sort(buffer, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort eight bytes") { timer =>
      val (_, buffer) = generateTestData(size, rand.nextLong)
      timer.startTiming()
      RadixSort.sort(buffer, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.run
  }
}
