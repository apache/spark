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

import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.SizeEstimator

class OpenHashSetSuite extends SparkFunSuite with Matchers {

  test("size for specialized, primitive int") {
    val loadFactor = 0.7
    val set = new OpenHashSet[Int](64, loadFactor)
    for (i <- 0 until 1024) {
      set.add(i)
    }
    assert(set.size === 1024)
    assert(set.capacity > 1024)
    val actualSize = SizeEstimator.estimate(set)
    // 32 bits for the ints + 1 bit for the bitset
    val expectedSize = set.capacity * (32 + 1) / 8
    // Make sure we are not allocating a significant amount of memory beyond our expected.
    actualSize should be <= (expectedSize * 1.1).toLong
  }

  test("primitive int") {
    val set = new OpenHashSet[Int]
    assert(set.size === 0)
    assert(!set.contains(10))
    assert(!set.contains(50))
    assert(!set.contains(999))
    assert(!set.contains(10000))

    set.add(10)
    assert(set.contains(10))
    assert(!set.contains(50))
    assert(!set.contains(999))
    assert(!set.contains(10000))

    set.add(50)
    assert(set.size === 2)
    assert(set.contains(10))
    assert(set.contains(50))
    assert(!set.contains(999))
    assert(!set.contains(10000))

    set.add(999)
    assert(set.size === 3)
    assert(set.contains(10))
    assert(set.contains(50))
    assert(set.contains(999))
    assert(!set.contains(10000))

    set.add(50)
    assert(set.size === 3)
    assert(set.contains(10))
    assert(set.contains(50))
    assert(set.contains(999))
    assert(!set.contains(10000))
  }

  test("primitive long") {
    val set = new OpenHashSet[Long]
    assert(set.size === 0)
    assert(!set.contains(10L))
    assert(!set.contains(50L))
    assert(!set.contains(999L))
    assert(!set.contains(10000L))

    set.add(10L)
    assert(set.size === 1)
    assert(set.contains(10L))
    assert(!set.contains(50L))
    assert(!set.contains(999L))
    assert(!set.contains(10000L))

    set.add(50L)
    assert(set.size === 2)
    assert(set.contains(10L))
    assert(set.contains(50L))
    assert(!set.contains(999L))
    assert(!set.contains(10000L))

    set.add(999L)
    assert(set.size === 3)
    assert(set.contains(10L))
    assert(set.contains(50L))
    assert(set.contains(999L))
    assert(!set.contains(10000L))

    set.add(50L)
    assert(set.size === 3)
    assert(set.contains(10L))
    assert(set.contains(50L))
    assert(set.contains(999L))
    assert(!set.contains(10000L))
  }

  test("primitive float") {
    val set = new OpenHashSet[Float]
    assert(set.size === 0)
    assert(!set.contains(10.1F))
    assert(!set.contains(50.5F))
    assert(!set.contains(999.9F))
    assert(!set.contains(10000.1F))

    set.add(10.1F)
    assert(set.size === 1)
    assert(set.contains(10.1F))
    assert(!set.contains(50.5F))
    assert(!set.contains(999.9F))
    assert(!set.contains(10000.1F))

    set.add(50.5F)
    assert(set.size === 2)
    assert(set.contains(10.1F))
    assert(set.contains(50.5F))
    assert(!set.contains(999.9F))
    assert(!set.contains(10000.1F))

    set.add(999.9F)
    assert(set.size === 3)
    assert(set.contains(10.1F))
    assert(set.contains(50.5F))
    assert(set.contains(999.9F))
    assert(!set.contains(10000.1F))

    set.add(50.5F)
    assert(set.size === 3)
    assert(set.contains(10.1F))
    assert(set.contains(50.5F))
    assert(set.contains(999.9F))
    assert(!set.contains(10000.1F))
  }

  test("primitive double") {
    val set = new OpenHashSet[Double]
    assert(set.size === 0)
    assert(!set.contains(10.1D))
    assert(!set.contains(50.5D))
    assert(!set.contains(999.9D))
    assert(!set.contains(10000.1D))

    set.add(10.1D)
    assert(set.size === 1)
    assert(set.contains(10.1D))
    assert(!set.contains(50.5D))
    assert(!set.contains(999.9D))
    assert(!set.contains(10000.1D))

    set.add(50.5D)
    assert(set.size === 2)
    assert(set.contains(10.1D))
    assert(set.contains(50.5D))
    assert(!set.contains(999.9D))
    assert(!set.contains(10000.1D))

    set.add(999.9D)
    assert(set.size === 3)
    assert(set.contains(10.1D))
    assert(set.contains(50.5D))
    assert(set.contains(999.9D))
    assert(!set.contains(10000.1D))

    set.add(50.5D)
    assert(set.size === 3)
    assert(set.contains(10.1D))
    assert(set.contains(50.5D))
    assert(set.contains(999.9D))
    assert(!set.contains(10000.1D))
  }

  test("non-primitive") {
    val set = new OpenHashSet[String]
    assert(set.size === 0)
    assert(!set.contains(10.toString))
    assert(!set.contains(50.toString))
    assert(!set.contains(999.toString))
    assert(!set.contains(10000.toString))

    set.add(10.toString)
    assert(set.size === 1)
    assert(set.contains(10.toString))
    assert(!set.contains(50.toString))
    assert(!set.contains(999.toString))
    assert(!set.contains(10000.toString))

    set.add(50.toString)
    assert(set.size === 2)
    assert(set.contains(10.toString))
    assert(set.contains(50.toString))
    assert(!set.contains(999.toString))
    assert(!set.contains(10000.toString))

    set.add(999.toString)
    assert(set.size === 3)
    assert(set.contains(10.toString))
    assert(set.contains(50.toString))
    assert(set.contains(999.toString))
    assert(!set.contains(10000.toString))

    set.add(50.toString)
    assert(set.size === 3)
    assert(set.contains(10.toString))
    assert(set.contains(50.toString))
    assert(set.contains(999.toString))
    assert(!set.contains(10000.toString))
  }

  test("non-primitive set growth") {
    val set = new OpenHashSet[String]
    for (i <- 1 to 1000) {
      set.add(i.toString)
    }
    assert(set.size === 1000)
    assert(set.capacity > 1000)
    for (i <- 1 to 100) {
      set.add(i.toString)
    }
    assert(set.size === 1000)
    assert(set.capacity > 1000)
  }

  test("primitive set growth") {
    val set = new OpenHashSet[Long]
    for (i <- 1 to 1000) {
      set.add(i.toLong)
    }
    assert(set.size === 1000)
    assert(set.capacity > 1000)
    for (i <- 1 to 100) {
      set.add(i.toLong)
    }
    assert(set.size === 1000)
    assert(set.capacity > 1000)
  }

  test("SPARK-18200 Support zero as an initial set size") {
    val set = new OpenHashSet[Long](0)
    assert(set.size === 0)
  }

  test("support for more than 12M items") {
    val cnt = 12000000 // 12M
    val set = new OpenHashSet[Int](cnt)
    for (i <- 0 until cnt) {
      set.add(i)
      assert(set.contains(i))

      val pos1 = set.getPos(i)
      val pos2 = set.addWithoutResize(i) & OpenHashSet.POSITION_MASK
      assert(pos1 == pos2)
    }
  }

  test("SPARK-45599: 0.0 and -0.0 are equal but not the same") {
    // Therefore, 0.0 and -0.0 should get separate entries in the hash set.
    //
    // Exactly these elements provided in roughly this order will trigger the following scenario:
    // When probing the bitset in `getPos(-0.0)`, the loop will happen upon the entry for 0.0.
    // In the old logic pre-SPARK-45599, the loop will find that the bit is set and, because
    // -0.0 == 0.0, it will think that's the position of -0.0. But in reality this is the position
    // of 0.0. So -0.0 and 0.0 will be stored at different positions, but `getPos()` will return
    // the same position for them. This can cause users of OpenHashSet, like OpenHashMap, to
    // return the wrong value for a key based on whether or not this bitset lookup collision
    // happens.
    val spark45599Repro = Seq(
      Double.NaN,
      2.0,
      168.0,
      Double.NaN,
      Double.NaN,
      -0.0,
      153.0,
      0.0
    )
    val set = new OpenHashSet[Double]()
    spark45599Repro.foreach(set.add)
    assert(set.size == 6)
    val zeroPos = set.getPos(0.0)
    val negZeroPos = set.getPos(-0.0)
    assert(zeroPos != negZeroPos)
  }

  test("SPARK-45599: NaN and NaN are the same but not equal") {
    // Any mathematical comparison to NaN will return false, but when we place it in
    // a hash set we want the lookup to work like a "normal" value.
    val set = new OpenHashSet[Double]()
    set.add(Double.NaN)
    set.add(Double.NaN)
    assert(set.contains(Double.NaN))
    assert(set.size == 1)
  }
}
