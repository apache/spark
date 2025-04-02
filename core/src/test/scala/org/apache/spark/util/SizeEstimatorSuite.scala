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

import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.Tests.TEST_USE_COMPRESSED_OOPS_KEY

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
  with BeforeAndAfterEach
  with PrivateMethodTester
  with ResetSystemProperties {

  // Save modified system properties so that we can restore them after tests.
  val originalArch = System.getProperty("os.arch")
  val originalCompressedOops = System.getProperty(TEST_USE_COMPRESSED_OOPS_KEY)

  def reinitializeSizeEstimator(arch: String, useCompressedOops: String): Unit = {
    def set(k: String, v: String): Unit = {
      if (v == null) {
        System.clearProperty(k)
      } else {
        System.setProperty(k, v)
      }
    }
    set("os.arch", arch)
    set(TEST_USE_COMPRESSED_OOPS_KEY, useCompressedOops)
    val initialize = PrivateMethod[Unit](Symbol("initialize"))
    SizeEstimator invokePrivate initialize()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Set the arch to 64-bit and compressedOops to true so that SizeEstimator
    // provides identical results across all systems in these tests.
    reinitializeSizeEstimator("amd64", "true")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    // Restore system properties and SizeEstimator to their original states.
    reinitializeSizeEstimator(originalArch, originalCompressedOops)
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
    assertResult(72)(SizeEstimator.estimate(Array.fill(10)(d1)))
    // 100 pointers plus 8-byte object
    assertResult(432)(SizeEstimator.estimate(Array.fill(100)(d1)))

    // Same thing with huge array containing the same element many times. Note that this won't
    // return exactly 4032 because it can't tell that *all* the elements will equal the first
    // one it samples, but it should be close to that.

    // TODO: If we sample 100 elements, this should always be 4176 ?
    val estimatedSize = SizeEstimator.estimate(Array.fill(1000)(d1))
    assert(estimatedSize >= 4000, "Estimated size " + estimatedSize + " should be more than 4000")
    assert(estimatedSize <= 4200, "Estimated size " + estimatedSize + " should be less than 4200")
  }

  test("32-bit arch") {
    reinitializeSizeEstimator("x86", "true")
    assertResult(40)(SizeEstimator.estimate(DummyString("")))
    assertResult(48)(SizeEstimator.estimate(DummyString("a")))
    assertResult(48)(SizeEstimator.estimate(DummyString("ab")))
    assertResult(56)(SizeEstimator.estimate(DummyString("abcdefgh")))
  }

  // NOTE: The String class definition varies across JDK versions (1.6 vs. 1.7) and vendors
  // (Sun vs IBM). Use a DummyString class to make tests deterministic.
  test("64-bit arch with no compressed oops") {
    reinitializeSizeEstimator("amd64", "false")
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
    reinitializeSizeEstimator("amd64", "false")
    assertResult(24)(SizeEstimator.estimate(new DummyClass5))
    assertResult(32)(SizeEstimator.estimate(new DummyClass6))
  }

  test("check 64-bit detection for s390x arch") {
    reinitializeSizeEstimator("s390x", "true")
    // Class should be 32 bytes on s390x if recognised as 64 bit platform
    assertResult(32)(SizeEstimator.estimate(new DummyClass7))
  }

  test("SizeEstimation can provide the estimated size") {
    // DummyClass8 provides its size estimation.
    assertResult(2015)(SizeEstimator.estimate(new DummyClass8))
    assertResult(20206)(SizeEstimator.estimate(Array.fill(10)(new DummyClass8)))
  }
}
