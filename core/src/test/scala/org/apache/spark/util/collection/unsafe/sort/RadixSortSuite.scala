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

import com.google.common.primitives.Ints

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.collection.Sorter
import org.apache.spark.util.random.XORShiftRandom

class RadixSortSuite extends SparkFunSuite with Logging {
  private val N = 10000L  // scale this down for more readable results

  /**
   * Describes a type of sort to test, e.g. two's complement descending. Each sort type has
   * a defined reference ordering as well as radix sort parameters that can be used to
   * reproduce the given ordering.
   */
  case class RadixSortType(
    name: String,
    referenceComparator: PrefixComparator,
    startByteIdx: Int, endByteIdx: Int, descending: Boolean, signed: Boolean, nullsFirst: Boolean)

  val SORT_TYPES_TO_TEST = Seq(
    RadixSortType("unsigned binary data asc nulls first",
      PrefixComparators.BINARY, 0, 7, false, false, true),
    RadixSortType("unsigned binary data asc nulls last",
      PrefixComparators.BINARY_NULLS_LAST, 0, 7, false, false, false),
    RadixSortType("unsigned binary data desc nulls last",
      PrefixComparators.BINARY_DESC_NULLS_FIRST, 0, 7, true, false, false),
    RadixSortType("unsigned binary data desc nulls first",
      PrefixComparators.BINARY_DESC, 0, 7, true, false, true),

    RadixSortType("twos complement asc nulls first",
      PrefixComparators.LONG, 0, 7, false, true, true),
    RadixSortType("twos complement asc nulls last",
      PrefixComparators.LONG_NULLS_LAST, 0, 7, false, true, false),
    RadixSortType("twos complement desc nulls last",
      PrefixComparators.LONG_DESC, 0, 7, true, true, false),
    RadixSortType("twos complement desc nulls first",
      PrefixComparators.LONG_DESC_NULLS_FIRST, 0, 7, true, true, true),

    RadixSortType(
      "binary data partial",
      new PrefixComparators.RadixSortSupport {
        override def sortDescending = false
        override def sortSigned = false
        override def nullsFirst = true
        override def compare(a: Long, b: Long): Int =
          PrefixComparators.BINARY.compare(a & 0xffffff0000L, b & 0xffffff0000L)
      },
      2, 4, false, false, true))

  private def generateTestData(size: Long, rand: => Long): (Array[JLong], LongArray) = {
    val ref = Array.tabulate[Long](Ints.checkedCast(size)) { i => rand }
    val extended = ref ++ Array.fill[Long](Ints.checkedCast(size))(0)
    (ref.map(i => JLong.valueOf(i)), new LongArray(MemoryBlock.fromLongArray(extended)))
  }

  private def generateKeyPrefixTestData(size: Long, rand: => Long): (LongArray, LongArray) = {
    val ref = Array.tabulate[Long](Ints.checkedCast(size * 2)) { i => rand }
    val extended = ref ++ Array.fill[Long](Ints.checkedCast(size * 2))(0)
    (new LongArray(MemoryBlock.fromLongArray(ref)),
     new LongArray(MemoryBlock.fromLongArray(extended)))
  }

  private def collectToArray(array: LongArray, offset: Int, length: Long): Array[Long] = {
    var i = 0
    val out = new Array[Long](Ints.checkedCast(length))
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

  private def referenceKeyPrefixSort(buf: LongArray, lo: Long, hi: Long, refCmp: PrefixComparator) {
    val sortBuffer = new LongArray(MemoryBlock.fromLongArray(new Array[Long](buf.size().toInt)))
    new Sorter(new UnsafeSortDataFormat(sortBuffer)).sort(
      buf, Ints.checkedCast(lo), Ints.checkedCast(hi),
      (r1: RecordPointerAndKeyPrefix, r2: RecordPointerAndKeyPrefix) =>
        refCmp.compare(r1.keyPrefix, r2.keyPrefix))
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
        buf2, 0, N, sortType.startByteIdx, sortType.endByteIdx,
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
        buf2, 0, N, sortType.startByteIdx, sortType.endByteIdx,
        sortType.descending, sortType.signed)
      val res1 = collectToArray(buf1, 0, N * 2)
      val res2 = collectToArray(buf2, outOffset, N * 2)
      assert(res1.view == res2.view)
    }
  }
}
