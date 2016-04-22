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

package org.apache.spark.util.collection.unsafe.sort

import java.lang.{Long => JLong}
import java.util.{Arrays, Comparator}

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.Benchmark
import org.apache.spark.util.collection.Sorter
import org.apache.spark.util.random.XORShiftRandom

class RadixSortSuite extends SparkFunSuite with Logging {
  private val N = 10000  // scale this down for more readable results

  /**
   * Describes a type of sort to test, e.g. two's complement descending. Each sort type has
   * a defined reference ordering as well as radix sort parameters that can be used to
   * reproduce the given ordering.
   */
  case class RadixSortType(
    name: String,
    referenceComparator: PrefixComparator,
    startByteIdx: Int, endByteIdx: Int, descending: Boolean, signed: Boolean)

  val SORT_TYPES_TO_TEST = Seq(
    RadixSortType("unsigned binary data asc", PrefixComparators.BINARY, 0, 7, false, false),
    RadixSortType("unsigned binary data desc", PrefixComparators.BINARY_DESC, 0, 7, true, false),
    RadixSortType("twos complement asc", PrefixComparators.LONG, 0, 7, false, true),
    RadixSortType("twos complement desc", PrefixComparators.LONG_DESC, 0, 7, true, true),
    RadixSortType(
      "binary data partial",
      new PrefixComparators.RadixSortSupport {
        override def sortDescending = false
        override def sortSigned = false
        override def compare(a: Long, b: Long): Int = {
          return PrefixComparators.BINARY.compare(a & 0xffffff0000L, b & 0xffffff0000L)
        }
      },
      2, 4, false, false))

  private def generateTestData(size: Int, rand: => Long): (Array[JLong], LongArray) = {
    val ref = Array.tabulate[Long](size) { i => rand }
    val extended = ref ++ Array.fill[Long](size)(0)
    (ref.map(i => new JLong(i)), new LongArray(MemoryBlock.fromLongArray(extended)))
  }

  private def generateKeyPrefixTestData(size: Int, rand: => Long): (LongArray, LongArray) = {
    val ref = Array.tabulate[Long](size * 2) { i => rand }
    val extended = ref ++ Array.fill[Long](size * 2)(0)
    (new LongArray(MemoryBlock.fromLongArray(ref)),
     new LongArray(MemoryBlock.fromLongArray(extended)))
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

  private def referenceKeyPrefixSort(buf: LongArray, lo: Int, hi: Int, refCmp: PrefixComparator) {
    new Sorter(UnsafeSortDataFormat.INSTANCE).sort(
      buf, lo, hi, new Comparator[RecordPointerAndKeyPrefix] {
        override def compare(
            r1: RecordPointerAndKeyPrefix,
            r2: RecordPointerAndKeyPrefix): Int = {
          refCmp.compare(r1.keyPrefix, r2.keyPrefix)
        }
      })
  }

  private def fuzzTest(name: String)(testFn: Long => Unit): Unit = {
    test(name) {
      var seed = 0L
      try {
        for (i <- 0 to 10) {
          seed = System.nanoTime
          testFn(seed)
        }
      } catch {
        case t: Throwable =>
          throw new Exception("Failed with seed: " + seed, t)
      }
    }
  }

  // Radix sort is sensitive to the value distribution at different bit indices (e.g., we may
  // omit a sort on a byte if all values are equal). This generates random good test masks.
  def randomBitMask(rand: Random): Long = {
    var tmp = ~0L
    for (i <- 0 to rand.nextInt(5)) {
      tmp &= rand.nextLong
    }
    tmp
  }

  for (sortType <- SORT_TYPES_TO_TEST) {
    test("radix support for " + sortType.name) {
      val s = sortType.referenceComparator.asInstanceOf[PrefixComparators.RadixSortSupport]
      assert(s.sortDescending() == sortType.descending)
      assert(s.sortSigned() == sortType.signed)
    }

    test("sort " + sortType.name) {
      val rand = new XORShiftRandom(123)
      val (ref, buffer) = generateTestData(N, rand.nextLong)
      Arrays.sort(ref, toJavaComparator(sortType.referenceComparator))
      val outOffset = RadixSort.sort(
        buffer, N, sortType.startByteIdx, sortType.endByteIdx,
        sortType.descending, sortType.signed)
      val result = collectToArray(buffer, outOffset, N)
      assert(ref.view == result.view)
    }

    test("sort key prefix " + sortType.name) {
      val rand = new XORShiftRandom(123)
      val (buf1, buf2) = generateKeyPrefixTestData(N, rand.nextLong & 0xff)
      referenceKeyPrefixSort(buf1, 0, N, sortType.referenceComparator)
      val outOffset = RadixSort.sortKeyPrefixArray(
        buf2, N, sortType.startByteIdx, sortType.endByteIdx,
        sortType.descending, sortType.signed)
      val res1 = collectToArray(buf1, 0, N * 2)
      val res2 = collectToArray(buf2, outOffset, N * 2)
      assert(res1.view == res2.view)
    }

    fuzzTest(s"fuzz test ${sortType.name} with random bitmasks") { seed =>
      val rand = new XORShiftRandom(seed)
      val mask = randomBitMask(rand)
      val (ref, buffer) = generateTestData(N, rand.nextLong & mask)
      Arrays.sort(ref, toJavaComparator(sortType.referenceComparator))
      val outOffset = RadixSort.sort(
        buffer, N, sortType.startByteIdx, sortType.endByteIdx,
        sortType.descending, sortType.signed)
      val result = collectToArray(buffer, outOffset, N)
      assert(ref.view == result.view)
    }

    fuzzTest(s"fuzz test key prefix ${sortType.name} with random bitmasks") { seed =>
      val rand = new XORShiftRandom(seed)
      val mask = randomBitMask(rand)
      val (buf1, buf2) = generateKeyPrefixTestData(N, rand.nextLong & mask)
      referenceKeyPrefixSort(buf1, 0, N, sortType.referenceComparator)
      val outOffset = RadixSort.sortKeyPrefixArray(
        buf2, N, sortType.startByteIdx, sortType.endByteIdx,
        sortType.descending, sortType.signed)
      val res1 = collectToArray(buf1, 0, N * 2)
      val res2 = collectToArray(buf2, outOffset, N * 2)
      assert(res1.view == res2.view)
    }
  }

  ignore("microbenchmarks") {
    val size = 25000000
    val rand = new XORShiftRandom(123)
    val benchmark = new Benchmark("radix sort " + size, size)
    benchmark.addTimerCase("reference TimSort key prefix array") { timer =>
      val array = Array.tabulate[Long](size * 2) { i => rand.nextLong }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      referenceKeyPrefixSort(buf, 0, size, PrefixComparators.BINARY)
      timer.stopTiming()
    }
    benchmark.addTimerCase("reference Arrays.sort") { timer =>
      val ref = Array.tabulate[Long](size) { i => rand.nextLong }
      timer.startTiming()
      Arrays.sort(ref)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort one byte") { timer =>
      val array = new Array[Long](size * 2)
      var i = 0
      while (i < size) {
        array(i) = rand.nextLong & 0xff
        i += 1
      }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      RadixSort.sort(buf, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort two bytes") { timer =>
      val array = new Array[Long](size * 2)
      var i = 0
      while (i < size) {
        array(i) = rand.nextLong & 0xffff
        i += 1
      }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      RadixSort.sort(buf, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort eight bytes") { timer =>
      val array = new Array[Long](size * 2)
      var i = 0
      while (i < size) {
        array(i) = rand.nextLong
        i += 1
      }
      val buf = new LongArray(MemoryBlock.fromLongArray(array))
      timer.startTiming()
      RadixSort.sort(buf, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.addTimerCase("radix sort key prefix array") { timer =>
      val (_, buf2) = generateKeyPrefixTestData(size, rand.nextLong)
      timer.startTiming()
      RadixSort.sortKeyPrefixArray(buf2, size, 0, 7, false, false)
      timer.stopTiming()
    }
    benchmark.run

    /**
      Running benchmark: radix sort 25000000
      Java HotSpot(TM) 64-Bit Server VM 1.8.0_66-b17 on Linux 3.13.0-44-generic
      Intel(R) Core(TM) i7-4600U CPU @ 2.10GHz

      radix sort 25000000:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      -------------------------------------------------------------------------------------------
      reference TimSort key prefix array     15546 / 15859          1.6         621.9       1.0X
      reference Arrays.sort                    2416 / 2446         10.3          96.6       6.4X
      radix sort one byte                       133 /  137        188.4           5.3     117.2X
      radix sort two bytes                      255 /  258         98.2          10.2      61.1X
      radix sort eight bytes                    991 /  997         25.2          39.6      15.7X
      radix sort key prefix array              1540 / 1563         16.2          61.6      10.1X
    */
  }
}
