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

package org.apache.spark.util

import scala.collection.mutable.ArrayBuffer

import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.Tests.{TEST_USE_COMPACT_OBJECT_HEADERS_KEY, TEST_USE_COMPRESSED_OOPS_KEY}
import org.apache.spark.util.collection.Utils.createArray

class DummyClass1 {}

class DummyClass2 {
  val x: Int = 0
}

class DummyClass3 {
  val x: Int = 0
  val y: Double = 0.0
}

class DummyClass4(val d: DummyClass3) {
  val x: Int = 0
}

// dummy class to show class field blocks alignment.
class DummyClass5 extends DummyClass1 {
  val x: Boolean = true
}

class DummyClass6 extends DummyClass5 {
  val y: Boolean = true
}

class DummyClass7 {
  val x: DummyClass1 = new DummyClass1
}

object DummyString {
  def apply(str: String) : DummyString = new DummyString(str.toArray)
}
class DummyString(val arr: Array[Char]) {
  override val hashCode: Int = 0
  // JDK-7 has an extra hash32 field http://hg.openjdk.java.net/jdk7u/jdk7u6/jdk/rev/11987e85555f
  @transient val hash32: Int = 0
}

class DummyClass8 extends KnownSizeEstimation {
  val x: Int = 0

  override def estimatedSize: Long = 2015
}

class SizeEstimatorSuite
  extends SparkFunSuite
  with PrivateMethodTester
  with ResetSystemProperties {

  // Save modified system properties so that we can restore them after tests.
  val originalArch = Utils.osArch
  val originalCompressedOops = System.getProperty(TEST_USE_COMPRESSED_OOPS_KEY)
  val originalCompactObjectHeaders = System.getProperty(TEST_USE_COMPACT_OBJECT_HEADERS_KEY)

  def reinitializeSizeEstimator(
      arch: String,
      useCompressedOops: String,
      useCompactObjectHeaders: String): Unit = {
    def set(k: String, v: String): Unit = {
      if (v == null) {
        System.clearProperty(k)
      } else {
        System.setProperty(k, v)
      }
    }
    set("os.arch", arch)
    set(TEST_USE_COMPRESSED_OOPS_KEY, useCompressedOops)
    set(TEST_USE_COMPACT_OBJECT_HEADERS_KEY, useCompactObjectHeaders)
    val initialize = PrivateMethod[Unit](Symbol("initialize"))
    SizeEstimator invokePrivate initialize()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Set the arch to 64-bit and compressedOops to true so that SizeEstimator
    // provides identical results across all systems in these tests.
    reinitializeSizeEstimator("amd64", "true", "false")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    // Restore system properties and SizeEstimator to their original states.
    reinitializeSizeEstimator(originalArch, originalCompressedOops, originalCompactObjectHeaders)
  }

  test("simple classes") {
    assertResult(16)(SizeEstimator.estimate(new DummyClass1))
    assertResult(16)(SizeEstimator.estimate(new DummyClass2))
    assertResult(24)(SizeEstimator.estimate(new DummyClass3))
    assertResult(24)(SizeEstimator.estimate(new DummyClass4(null)))
    assertResult(48)(SizeEstimator.estimate(new DummyClass4(new DummyClass3)))
  }

  test("primitive wrapper objects") {
    assertResult(16)(SizeEstimator.estimate(java.lang.Boolean.TRUE))
    assertResult(16)(SizeEstimator.estimate(java.lang.Byte.valueOf("1")))
    assertResult(16)(SizeEstimator.estimate(java.lang.Character.valueOf('1')))
    assertResult(16)(SizeEstimator.estimate(java.lang.Short.valueOf("1")))
    assertResult(16)(SizeEstimator.estimate(java.lang.Integer.valueOf(1)))
    assertResult(24)(SizeEstimator.estimate(java.lang.Long.valueOf(1)))
    assertResult(16)(SizeEstimator.estimate(java.lang.Float.valueOf(1.0f)))
    assertResult(24)(SizeEstimator.estimate(java.lang.Double.valueOf(1.0)))
  }

  test("class field blocks rounding") {
    assertResult(16)(SizeEstimator.estimate(new DummyClass5))
    assertResult(24)(SizeEstimator.estimate(new DummyClass6))
  }

  // NOTE: The String class definition varies across JDK versions (1.6 vs. 1.7) and vendors
  // (Sun vs IBM). Use a DummyString class to make tests deterministic.
  test("strings") {
    assertResult(40)(SizeEstimator.estimate(DummyString("")))
    assertResult(48)(SizeEstimator.estimate(DummyString("a")))
    assertResult(48)(SizeEstimator.estimate(DummyString("ab")))
    assertResult(56)(SizeEstimator.estimate(DummyString("abcdefgh")))
  }

  test("primitive arrays") {
    assertResult(32)(SizeEstimator.estimate(new Array[Byte](10)))
    assertResult(40)(SizeEstimator.estimate(new Array[Char](10)))
    assertResult(40)(SizeEstimator.estimate(new Array[Short](10)))
    assertResult(56)(SizeEstimator.estimate(new Array[Int](10)))
    assertResult(96)(SizeEstimator.estimate(new Array[Long](10)))
    assertResult(56)(SizeEstimator.estimate(new Array[Float](10)))
    assertResult(96)(SizeEstimator.estimate(new Array[Double](10)))
    assertResult(4016)(SizeEstimator.estimate(new Array[Int](1000)))
    assertResult(8016)(SizeEstimator.estimate(new Array[Long](1000)))
  }

  test("object arrays") {
    // Arrays containing nulls should just have one pointer per element
    assertResult(56)(SizeEstimator.estimate(new Array[String](10)))
    assertResult(56)(SizeEstimator.estimate(new Array[AnyRef](10)))
    // For object arrays with non-null elements, each object should take one pointer plus
    // however many bytes that class takes. (Note that Array.fill calls the code in its
    // second parameter separately for each object, so we get distinct objects.)
    assertResult(216)(SizeEstimator.estimate(Array.fill(10)(new DummyClass1)))
    assertResult(216)(SizeEstimator.estimate(Array.fill(10)(new DummyClass2)))
    assertResult(296)(SizeEstimator.estimate(Array.fill(10)(new DummyClass3)))
    assertResult(56)(SizeEstimator.estimate(Array(new DummyClass1, new DummyClass2)))

    // Past size 100, our samples 100 elements, but we should still get the right size.
    assertResult(28016)(SizeEstimator.estimate(Array.fill(1000)(new DummyClass3)))


    val arr = new Array[Char](100000)
    assertResult(200016)(SizeEstimator.estimate(arr))
    assertResult(480032)(SizeEstimator.estimate(Array.fill(10000)(new DummyString(arr))))

    val buf = new ArrayBuffer[DummyString]()
    for (i <- 0 until 5000) {
      buf += new DummyString(new Array[Char](10))
    }
    assertResult(340016)(SizeEstimator.estimate(buf.toArray))

    for (i <- 0 until 5000) {
      buf += new DummyString(arr)
    }
    assertResult(683912)(SizeEstimator.estimate(buf.toArray))

    // If an array contains the *same* element many times, we should only count it once.
    val d1 = new DummyClass1
    // 10 pointers plus 8-byte object
    assertResult(72)(SizeEstimator.estimate(createArray(10, d1)))
    // 100 pointers plus 8-byte object
    assertResult(432)(SizeEstimator.estimate(createArray(100, d1)))

    // Same thing with huge array containing the same element many times. Note that this won't
    // return exactly 4032 because it can't tell that *all* the elements will equal the first
    // one it samples, but it should be close to that.

    // TODO: If we sample 100 elements, this should always be 4176 ?
    val estimatedSize = SizeEstimator.estimate(createArray(1000, d1))
    assert(estimatedSize >= 4000, "Estimated size " + estimatedSize + " should be more than 4000")
    assert(estimatedSize <= 4200, "Estimated size " + estimatedSize + " should be less than 4200")
  }

  test("32-bit arch") {
    reinitializeSizeEstimator("x86", "true", "false")
    assertResult(40)(SizeEstimator.estimate(DummyString("")))
    assertResult(48)(SizeEstimator.estimate(DummyString("a")))
    assertResult(48)(SizeEstimator.estimate(DummyString("ab")))
    assertResult(56)(SizeEstimator.estimate(DummyString("abcdefgh")))
  }

  // NOTE: The String class definition varies across JDK versions (1.6 vs. 1.7) and vendors
  // (Sun vs IBM). Use a DummyString class to make tests deterministic.
  test("64-bit arch with no compressed oops") {
    reinitializeSizeEstimator("amd64", "false", "false")
    assertResult(56)(SizeEstimator.estimate(DummyString("")))
    assertResult(64)(SizeEstimator.estimate(DummyString("a")))
    assertResult(64)(SizeEstimator.estimate(DummyString("ab")))
    assertResult(72)(SizeEstimator.estimate(DummyString("abcdefgh")))

    // primitive wrapper classes
    assertResult(24)(SizeEstimator.estimate(java.lang.Boolean.TRUE))
    assertResult(24)(SizeEstimator.estimate(java.lang.Byte.valueOf("1")))
    assertResult(24)(SizeEstimator.estimate(java.lang.Character.valueOf('1')))
    assertResult(24)(SizeEstimator.estimate(java.lang.Short.valueOf("1")))
    assertResult(24)(SizeEstimator.estimate(java.lang.Integer.valueOf(1)))
    assertResult(24)(SizeEstimator.estimate(java.lang.Long.valueOf(1)))
    assertResult(24)(SizeEstimator.estimate(java.lang.Float.valueOf(1.0f)))
    assertResult(24)(SizeEstimator.estimate(java.lang.Double.valueOf(1.0)))
  }

  test("class field blocks rounding on 64-bit VM without useCompressedOops") {
    reinitializeSizeEstimator("amd64", "false", "false")
    assertResult(24)(SizeEstimator.estimate(new DummyClass5))
    assertResult(32)(SizeEstimator.estimate(new DummyClass6))
  }

  test("check 64-bit detection for s390x arch") {
    reinitializeSizeEstimator("s390x", "true", "false")
    // Class should be 32 bytes on s390x if recognised as 64 bit platform
    assertResult(32)(SizeEstimator.estimate(new DummyClass7))
  }

  test("SizeEstimation can provide the estimated size") {
    // DummyClass8 provides its size estimation.
    assertResult(2015)(SizeEstimator.estimate(new DummyClass8))
    assertResult(20206)(SizeEstimator.estimate(Array.fill(10)(new DummyClass8)))
  }

  // Tests for JEP 450/519: Compact Object Headers
  // With Compact Object Headers, the object header is 8 bytes (vs. 12 with Compressed Oops,
  // or 16 without) because the class pointer is encoded inside the mark word.
  // Object reference pointers are 4 bytes with Compressed Oops, or 8 bytes without.

  test("64-bit arch with compact object headers: simple classes") {
    reinitializeSizeEstimator("amd64", "true", "true")
    // objectSize = 8 (compact header), pointerSize = 4
    // DummyClass1: 8-byte header, no fields => 8 bytes
    assertResult(8)(SizeEstimator.estimate(new DummyClass1))
    // DummyClass2: 8-byte header + Int(4) => 12, aligned to 16
    assertResult(16)(SizeEstimator.estimate(new DummyClass2))
    // DummyClass3: 8-byte header + Int(4) + Double(8) => 20, aligned to 24
    assertResult(24)(SizeEstimator.estimate(new DummyClass3))
    // DummyClass4: 8-byte header + Int(4) + pointer(4) => 16, aligned to 16
    assertResult(16)(SizeEstimator.estimate(new DummyClass4(null)))
    // DummyClass4 with DummyClass3: 16 + 24 = 40
    assertResult(40)(SizeEstimator.estimate(new DummyClass4(new DummyClass3)))
  }

  test("64-bit arch with compact object headers: primitive wrapper objects") {
    reinitializeSizeEstimator("amd64", "true", "true")
    // Boolean/Byte/Char/Short/Int/Float wrappers: 8-byte header + primitive up to 4 bytes => 16
    assertResult(16)(SizeEstimator.estimate(java.lang.Boolean.TRUE))
    assertResult(16)(SizeEstimator.estimate(java.lang.Byte.valueOf("1")))
    assertResult(16)(SizeEstimator.estimate(java.lang.Character.valueOf('1')))
    assertResult(16)(SizeEstimator.estimate(java.lang.Short.valueOf("1")))
    assertResult(16)(SizeEstimator.estimate(java.lang.Integer.valueOf(1)))
    assertResult(16)(SizeEstimator.estimate(java.lang.Float.valueOf(1.0f)))
    // Long/Double wrappers: 8-byte header + 8-byte primitive => 16
    assertResult(16)(SizeEstimator.estimate(java.lang.Long.valueOf(1)))
    assertResult(16)(SizeEstimator.estimate(java.lang.Double.valueOf(1.0)))
  }

  test("64-bit arch with compact object headers: primitive arrays") {
    reinitializeSizeEstimator("amd64", "true", "true")
    // Array header = objectSize(8) + length Int(4) = 12, aligned to 16
    // Array[Byte](10): 16 + alignSize(10*1=10) = 16 + 16 = 32
    assertResult(32)(SizeEstimator.estimate(new Array[Byte](10)))
    // Array[Char](10): 16 + alignSize(10*2=20) = 16 + 24 = 40
    assertResult(40)(SizeEstimator.estimate(new Array[Char](10)))
    // Array[Int](10): 16 + alignSize(10*4=40) = 16 + 40 = 56
    assertResult(56)(SizeEstimator.estimate(new Array[Int](10)))
    // Array[Long](10): 16 + alignSize(10*8=80) = 16 + 80 = 96
    assertResult(96)(SizeEstimator.estimate(new Array[Long](10)))
  }

  test("64-bit arch with compact object headers: strings") {
    reinitializeSizeEstimator("amd64", "true", "true")
    // DummyString has: pointer(arr,4) + Int(hashCode,4) + Int(hash32,4) = 12 bytes of fields
    // objectSize=8, fields=12 => shellSize=20, aligned to 24
    // DummyString("") => DummyString(24) + Array[Char](0)(16) = 40
    assertResult(40)(SizeEstimator.estimate(DummyString("")))
    // DummyString("a") => 24 + Array[Char](1): 16 + alignSize(2) = 16+8=24 => 24+24=48
    assertResult(48)(SizeEstimator.estimate(DummyString("a")))
  }

  test("64-bit arch with compact object headers: class field blocks rounding") {
    reinitializeSizeEstimator("amd64", "true", "true")
    // DummyClass1: 8-byte header, no fields => shellSize=8, alignSize(8)=8
    // DummyClass5 extends DummyClass1: parent.shellSize=8, adds Boolean(1)
    //   alignedSize = max(8, alignSizeUp(8,1)+1) = 9, shellSize=9
    //   shellSize = alignSizeUp(9, pointerSize=4) = 12
    //   alignSize(12) = 16
    assertResult(16)(SizeEstimator.estimate(new DummyClass5))
    // DummyClass6 extends DummyClass5: parent.shellSize=12, adds Boolean(1)
    //   alignedSize = max(12, alignSizeUp(12,1)+1) = 13, shellSize=13
    //   shellSize = alignSizeUp(13, pointerSize=4) = 16
    //   alignSize(16) = 16
    assertResult(16)(SizeEstimator.estimate(new DummyClass6))
  }

  test("64-bit arch with compact object headers and no compressed oops") {
    reinitializeSizeEstimator("amd64", "false", "true")
    // objectSize = 8, pointerSize = 8
    // DummyClass1: 8-byte header, no fields => 8 bytes
    assertResult(8)(SizeEstimator.estimate(new DummyClass1))
    assertResult(16)(SizeEstimator.estimate(new DummyClass2))
    // DummyClass3: 8-byte header + Int(4) + Double(8) => 20, aligned to 24
    assertResult(24)(SizeEstimator.estimate(new DummyClass3))
    // DummyClass4: 8-byte header + Int(4) + pointer(8) => 20, aligned to 24
    assertResult(24)(SizeEstimator.estimate(new DummyClass4(null)))
    // DummyClass4 with DummyClass3: 24 + 24 = 48
    assertResult(48)(SizeEstimator.estimate(new DummyClass4(new DummyClass3)))

    // Primitive wrapper objects
    // Boolean/Byte/Char/Short/Int/Float wrappers:
    // 8-byte header + primitive up to 4 bytes => 12, aligned to 16
    assertResult(16)(SizeEstimator.estimate(java.lang.Boolean.TRUE))
    assertResult(16)(SizeEstimator.estimate(java.lang.Byte.valueOf("1")))
    assertResult(16)(SizeEstimator.estimate(java.lang.Character.valueOf('1')))
    assertResult(16)(SizeEstimator.estimate(java.lang.Short.valueOf("1")))
    assertResult(16)(SizeEstimator.estimate(java.lang.Integer.valueOf(1)))
    assertResult(16)(SizeEstimator.estimate(java.lang.Float.valueOf(1.0f)))
    // Long/Double wrappers: 8-byte header + 8-byte primitive => 16, aligned to 16
    assertResult(16)(SizeEstimator.estimate(java.lang.Long.valueOf(1)))
    assertResult(16)(SizeEstimator.estimate(java.lang.Double.valueOf(1.0)))

    // Primitive arrays
    // Array header = objectSize(8) + length Int(4) = 12, aligned to 16
    // Array[Byte](10): 16 + alignSize(10*1=10) = 16 + 16 = 32
    assertResult(32)(SizeEstimator.estimate(new Array[Byte](10)))
    // Array[Char](10): 16 + alignSize(10*2=20) = 16 + 24 = 40
    assertResult(40)(SizeEstimator.estimate(new Array[Char](10)))
    // Array[Int](10): 16 + alignSize(10*4=40) = 16 + 40 = 56
    assertResult(56)(SizeEstimator.estimate(new Array[Int](10)))
    // Array[Long](10): 16 + alignSize(10*8=80) = 16 + 80 = 96
    assertResult(96)(SizeEstimator.estimate(new Array[Long](10)))

    // Strings (DummyString)
    // DummyString has: pointer(arr,8) + Int(hashCode,4) + Int(hash32,4) = 16 bytes of fields
    // objectSize=8, fields=16 => shellSize=24, aligned to 24
    // DummyString("") => DummyString(24) + Array[Char](0)(16) = 40
    assertResult(40)(SizeEstimator.estimate(DummyString("")))
    // DummyString("a") => 24 + Array[Char](1): 16 + alignSize(2) = 16+8=24 => 24+24=48
    assertResult(48)(SizeEstimator.estimate(DummyString("a")))

    // Class field blocks rounding
    // DummyClass5 extends DummyClass1: parent.shellSize=8, adds Boolean(1)
    //   alignedSize = max(8, alignSizeUp(8,1)+1) = 9, shellSize=9
    //   shellSize = alignSizeUp(9, pointerSize=8) = 16
    //   alignSize(16) = 16
    assertResult(16)(SizeEstimator.estimate(new DummyClass5))
    // DummyClass6 extends DummyClass5: parent.shellSize=16, adds Boolean(1)
    //   alignedSize = max(16, alignSizeUp(16,1)+1) = 17, shellSize=17
    //   shellSize = alignSizeUp(17, pointerSize=8) = 24
    //   alignSize(24) = 24
    assertResult(24)(SizeEstimator.estimate(new DummyClass6))
  }
}
