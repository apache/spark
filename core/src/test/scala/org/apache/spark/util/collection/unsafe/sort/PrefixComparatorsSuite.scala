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

import java.nio.charset.StandardCharsets

import com.google.common.primitives.UnsignedBytes
import org.scalatest.prop.PropertyChecks

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.types.UTF8String

class PrefixComparatorsSuite extends SparkFunSuite with PropertyChecks {

  test("String prefix comparator") {

    def testPrefixComparison(s1: String, s2: String): Unit = {
      val utf8string1 = UTF8String.fromString(s1)
      val utf8string2 = UTF8String.fromString(s2)
      val s1Prefix = PrefixComparators.StringPrefixComparator.computePrefix(utf8string1)
      val s2Prefix = PrefixComparators.StringPrefixComparator.computePrefix(utf8string2)
      val prefixComparisonResult = PrefixComparators.STRING.compare(s1Prefix, s2Prefix)

      val cmp = UnsignedBytes.lexicographicalComparator().compare(
        utf8string1.getBytes.take(8), utf8string2.getBytes.take(8))

      assert(
        (prefixComparisonResult == 0 && cmp == 0) ||
        (prefixComparisonResult < 0 && s1.compareTo(s2) < 0) ||
        (prefixComparisonResult > 0 && s1.compareTo(s2) > 0))
    }

    // scalastyle:off
    val regressionTests = Table(
      ("s1", "s2"),
      ("abc", "世界"),
      ("你好", "世界"),
      ("你好123", "你好122"),
      ("", "")
    )
    // scalastyle:on

    forAll (regressionTests) { (s1: String, s2: String) => testPrefixComparison(s1, s2) }
    forAll { (s1: String, s2: String) => testPrefixComparison(s1, s2) }
  }

  test("Binary prefix comparator") {

     def compareBinary(x: Array[Byte], y: Array[Byte]): Int = {
      for (i <- 0 until x.length; if i < y.length) {
        val v1 = x(i) & 0xff
        val v2 = y(i) & 0xff
        val res = v1 - v2
        if (res != 0) return res
      }
      x.length - y.length
    }

    def testPrefixComparison(x: Array[Byte], y: Array[Byte]): Unit = {
      val s1Prefix = PrefixComparators.BinaryPrefixComparator.computePrefix(x)
      val s2Prefix = PrefixComparators.BinaryPrefixComparator.computePrefix(y)
      val prefixComparisonResult =
        PrefixComparators.BINARY.compare(s1Prefix, s2Prefix)
      assert(
        (prefixComparisonResult == 0) ||
        (prefixComparisonResult < 0 && compareBinary(x, y) < 0) ||
        (prefixComparisonResult > 0 && compareBinary(x, y) > 0))
    }

    val binaryRegressionTests = Seq(
      (Array[Byte](1), Array[Byte](-1)),
      (Array[Byte](1, 1, 1, 1, 1), Array[Byte](1, 1, 1, 1, -1)),
      (Array[Byte](1, 1, 1, 1, 1, 1, 1, 1, 1), Array[Byte](1, 1, 1, 1, 1, 1, 1, 1, -1)),
      (Array[Byte](1), Array[Byte](1, 1, 1, 1)),
      (Array[Byte](1, 1, 1, 1, 1), Array[Byte](1, 1, 1, 1, 1, 1, 1, 1, 1)),
      (Array[Byte](-1), Array[Byte](-1, -1, -1, -1)),
      (Array[Byte](-1, -1, -1, -1, -1), Array[Byte](-1, -1, -1, -1, -1, -1, -1, -1, -1))
    )
    binaryRegressionTests.foreach { case (b1, b2) =>
      testPrefixComparison(b1, b2)
    }

    // scalastyle:off
    val regressionTests = Table(
      ("s1", "s2"),
      ("abc", "世界"),
      ("你好", "世界"),
      ("你好123", "你好122")
    )
    // scalastyle:on

    forAll (regressionTests) { (s1: String, s2: String) =>
      testPrefixComparison(
        s1.getBytes(StandardCharsets.UTF_8), s2.getBytes(StandardCharsets.UTF_8))
    }
    forAll { (s1: String, s2: String) =>
      testPrefixComparison(
        s1.getBytes(StandardCharsets.UTF_8), s2.getBytes(StandardCharsets.UTF_8))
    }
  }

  test("double prefix comparator handles NaNs properly") {
    val nan1: Double = java.lang.Double.longBitsToDouble(0x7ff0000000000001L)
    val nan2: Double = java.lang.Double.longBitsToDouble(0x7fffffffffffffffL)
    assert(
      java.lang.Double.doubleToRawLongBits(nan1) != java.lang.Double.doubleToRawLongBits(nan2))
    assert(nan1.isNaN)
    assert(nan2.isNaN)
    val nan1Prefix = PrefixComparators.DoublePrefixComparator.computePrefix(nan1)
    val nan2Prefix = PrefixComparators.DoublePrefixComparator.computePrefix(nan2)
    assert(nan1Prefix === nan2Prefix)
    val doubleMaxPrefix = PrefixComparators.DoublePrefixComparator.computePrefix(Double.MaxValue)
    // NaN is greater than the max double value.
    assert(PrefixComparators.DOUBLE.compare(nan1Prefix, doubleMaxPrefix) === 1)
  }

  test("double prefix comparator handles negative NaNs properly") {
    val negativeNan: Double = java.lang.Double.longBitsToDouble(0xfff0000000000001L)
    assert(negativeNan.isNaN)
    assert(java.lang.Double.doubleToRawLongBits(negativeNan) < 0)
    val prefix = PrefixComparators.DoublePrefixComparator.computePrefix(negativeNan)
    val doubleMaxPrefix = PrefixComparators.DoublePrefixComparator.computePrefix(Double.MaxValue)
    // -NaN is greater than the max double value.
    assert(PrefixComparators.DOUBLE.compare(prefix, doubleMaxPrefix) === 1)
  }

  test("double prefix comparator handles other special values properly") {
    // See `SortPrefix.nullValue` for how we deal with nulls for float/double type
    val smallestNullPrefix = 0L
    val largestNullPrefix = -1L
    val nan = PrefixComparators.DoublePrefixComparator.computePrefix(Double.NaN)
    val posInf = PrefixComparators.DoublePrefixComparator.computePrefix(Double.PositiveInfinity)
    val negInf = PrefixComparators.DoublePrefixComparator.computePrefix(Double.NegativeInfinity)
    val minValue = PrefixComparators.DoublePrefixComparator.computePrefix(Double.MinValue)
    val maxValue = PrefixComparators.DoublePrefixComparator.computePrefix(Double.MaxValue)
    val zero = PrefixComparators.DoublePrefixComparator.computePrefix(0.0)
    val minusZero = PrefixComparators.DoublePrefixComparator.computePrefix(-0.0)

    // null is greater than everything including NaN, when we need to treat it as the largest value.
    assert(PrefixComparators.DOUBLE.compare(largestNullPrefix, nan) === 1)
    // NaN is greater than the positive infinity.
    assert(PrefixComparators.DOUBLE.compare(nan, posInf) === 1)
    assert(PrefixComparators.DOUBLE.compare(posInf, maxValue) === 1)
    assert(PrefixComparators.DOUBLE.compare(maxValue, zero) === 1)
    assert(PrefixComparators.DOUBLE.compare(zero, minValue) === 1)
    assert(PrefixComparators.DOUBLE.compare(minValue, negInf) === 1)
    // null is smaller than everything including negative infinity, when we need to treat it as
    // the smallest value.
    assert(PrefixComparators.DOUBLE.compare(negInf, smallestNullPrefix) === 1)
    // 0.0 should be equal to -0.0.
    assert(PrefixComparators.DOUBLE.compare(zero, minusZero) === 0)
  }
}
