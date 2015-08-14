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
      ("你好123", "你好122")
    )
    // scalastyle:on

    forAll (regressionTests) { (s1: String, s2: String) => testPrefixComparison(s1, s2) }
    forAll { (s1: String, s2: String) => testPrefixComparison(s1, s2) }
  }

  test("Binary prefix comparator") {

     def compareBinary(x: Array[Byte], y: Array[Byte]): Int = {
      for (i <- 0 until x.length; if i < y.length) {
        val res = x(i).compare(y(i))
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

    // scalastyle:off
    val regressionTests = Table(
      ("s1", "s2"),
      ("abc", "世界"),
      ("你好", "世界"),
      ("你好123", "你好122")
    )
    // scalastyle:on

    forAll (regressionTests) { (s1: String, s2: String) =>
      testPrefixComparison(s1.getBytes("UTF-8"), s2.getBytes("UTF-8"))
    }
    forAll { (s1: String, s2: String) =>
      testPrefixComparison(s1.getBytes("UTF-8"), s2.getBytes("UTF-8"))
    }
  }

  test("double prefix comparator handles NaNs properly") {
    val nan1: Double = java.lang.Double.longBitsToDouble(0x7ff0000000000001L)
    val nan2: Double = java.lang.Double.longBitsToDouble(0x7fffffffffffffffL)
    assert(nan1.isNaN)
    assert(nan2.isNaN)
    val nan1Prefix = PrefixComparators.DoublePrefixComparator.computePrefix(nan1)
    val nan2Prefix = PrefixComparators.DoublePrefixComparator.computePrefix(nan2)
    assert(nan1Prefix === nan2Prefix)
    val doubleMaxPrefix = PrefixComparators.DoublePrefixComparator.computePrefix(Double.MaxValue)
    assert(PrefixComparators.DOUBLE.compare(nan1Prefix, doubleMaxPrefix) === 1)
  }

}
