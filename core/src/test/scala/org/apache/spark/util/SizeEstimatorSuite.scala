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

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.PrivateMethodTester

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

object DummyString {
  def apply(str: String) : DummyString = new DummyString(str.toArray)
}
class DummyString(val arr: Array[Char]) {
  override val hashCode: Int = 0
  // JDK-7 has an extra hash32 field http://hg.openjdk.java.net/jdk7u/jdk7u6/jdk/rev/11987e85555f
  @transient val hash32: Int = 0
}

class SizeEstimatorSuite
  extends FunSuite with BeforeAndAfterAll with PrivateMethodTester {

  var oldArch: String = _
  var oldOops: String = _

  override def beforeAll() {
    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case
    oldArch = System.setProperty("os.arch", "amd64")
    oldOops = System.setProperty("spark.test.useCompressedOops", "true")
  }

  override def afterAll() {
    resetOrClear("os.arch", oldArch)
    resetOrClear("spark.test.useCompressedOops", oldOops)
  }

  test("simple classes") {
    expectResult(16)(SizeEstimator.estimate(new DummyClass1))
    expectResult(16)(SizeEstimator.estimate(new DummyClass2))
    expectResult(24)(SizeEstimator.estimate(new DummyClass3))
    expectResult(24)(SizeEstimator.estimate(new DummyClass4(null)))
    expectResult(48)(SizeEstimator.estimate(new DummyClass4(new DummyClass3)))
  }

  // NOTE: The String class definition varies across JDK versions (1.6 vs. 1.7) and vendors
  // (Sun vs IBM). Use a DummyString class to make tests deterministic.
  test("strings") {
    expectResult(40)(SizeEstimator.estimate(DummyString("")))
    expectResult(48)(SizeEstimator.estimate(DummyString("a")))
    expectResult(48)(SizeEstimator.estimate(DummyString("ab")))
    expectResult(56)(SizeEstimator.estimate(DummyString("abcdefgh")))
  }

  test("primitive arrays") {
    expectResult(32)(SizeEstimator.estimate(new Array[Byte](10)))
    expectResult(40)(SizeEstimator.estimate(new Array[Char](10)))
    expectResult(40)(SizeEstimator.estimate(new Array[Short](10)))
    expectResult(56)(SizeEstimator.estimate(new Array[Int](10)))
    expectResult(96)(SizeEstimator.estimate(new Array[Long](10)))
    expectResult(56)(SizeEstimator.estimate(new Array[Float](10)))
    expectResult(96)(SizeEstimator.estimate(new Array[Double](10)))
    expectResult(4016)(SizeEstimator.estimate(new Array[Int](1000)))
    expectResult(8016)(SizeEstimator.estimate(new Array[Long](1000)))
  }

  test("object arrays") {
    // Arrays containing nulls should just have one pointer per element
    expectResult(56)(SizeEstimator.estimate(new Array[String](10)))
    expectResult(56)(SizeEstimator.estimate(new Array[AnyRef](10)))
    // For object arrays with non-null elements, each object should take one pointer plus
    // however many bytes that class takes. (Note that Array.fill calls the code in its
    // second parameter separately for each object, so we get distinct objects.)
    expectResult(216)(SizeEstimator.estimate(Array.fill(10)(new DummyClass1)))
    expectResult(216)(SizeEstimator.estimate(Array.fill(10)(new DummyClass2)))
    expectResult(296)(SizeEstimator.estimate(Array.fill(10)(new DummyClass3)))
    expectResult(56)(SizeEstimator.estimate(Array(new DummyClass1, new DummyClass2)))

    // Past size 100, our samples 100 elements, but we should still get the right size.
    expectResult(28016)(SizeEstimator.estimate(Array.fill(1000)(new DummyClass3)))

    // If an array contains the *same* element many times, we should only count it once.
    val d1 = new DummyClass1
    expectResult(72)(SizeEstimator.estimate(Array.fill(10)(d1))) // 10 pointers plus 8-byte object
    expectResult(432)(SizeEstimator.estimate(Array.fill(100)(d1))) // 100 pointers plus 8-byte object

    // Same thing with huge array containing the same element many times. Note that this won't
    // return exactly 4032 because it can't tell that *all* the elements will equal the first
    // one it samples, but it should be close to that.

    // TODO: If we sample 100 elements, this should always be 4176 ?
    val estimatedSize = SizeEstimator.estimate(Array.fill(1000)(d1))
    assert(estimatedSize >= 4000, "Estimated size " + estimatedSize + " should be more than 4000")
    assert(estimatedSize <= 4200, "Estimated size " + estimatedSize + " should be less than 4100")
  }

  test("32-bit arch") {
    val arch = System.setProperty("os.arch", "x86")

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()

    expectResult(40)(SizeEstimator.estimate(DummyString("")))
    expectResult(48)(SizeEstimator.estimate(DummyString("a")))
    expectResult(48)(SizeEstimator.estimate(DummyString("ab")))
    expectResult(56)(SizeEstimator.estimate(DummyString("abcdefgh")))
    resetOrClear("os.arch", arch)
  }

  // NOTE: The String class definition varies across JDK versions (1.6 vs. 1.7) and vendors
  // (Sun vs IBM). Use a DummyString class to make tests deterministic.
  test("64-bit arch with no compressed oops") {
    val arch = System.setProperty("os.arch", "amd64")
    val oops = System.setProperty("spark.test.useCompressedOops", "false")

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()

    expectResult(56)(SizeEstimator.estimate(DummyString("")))
    expectResult(64)(SizeEstimator.estimate(DummyString("a")))
    expectResult(64)(SizeEstimator.estimate(DummyString("ab")))
    expectResult(72)(SizeEstimator.estimate(DummyString("abcdefgh")))

    resetOrClear("os.arch", arch)
    resetOrClear("spark.test.useCompressedOops", oops)
  }

  def resetOrClear(prop: String, oldValue: String) {
    if (oldValue != null) {
      System.setProperty(prop, oldValue)
    } else {
      System.clearProperty(prop)
    }
  }
}
