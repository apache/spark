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

import scala.collection.mutable.HashSet

import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.apache.spark.util.SizeEstimator

class PrimitiveKeyOpenHashMapSuite extends FunSuite with Matchers {

  test("size for specialized, primitive key, value (int, int)") {
    val capacity = 1024
    val map = new PrimitiveKeyOpenHashMap[Int, Int](capacity)
    val actualSize = SizeEstimator.estimate(map)
    // 32 bit for keys, 32 bit for values, and 1 bit for the bitset.
    val expectedSize = capacity * (32 + 32 + 1) / 8
    // Make sure we are not allocating a significant amount of memory beyond our expected.
    actualSize should be <= (expectedSize * 1.1).toLong
  }

  test("initialization") {
    val goodMap1 = new PrimitiveKeyOpenHashMap[Int, Int](1)
    assert(goodMap1.size === 0)
    val goodMap2 = new PrimitiveKeyOpenHashMap[Int, Int](255)
    assert(goodMap2.size === 0)
    val goodMap3 = new PrimitiveKeyOpenHashMap[Int, Int](256)
    assert(goodMap3.size === 0)
    intercept[IllegalArgumentException] {
      new PrimitiveKeyOpenHashMap[Int, Int](1 << 30) // Invalid map size: bigger than 2^29
    }
    intercept[IllegalArgumentException] {
      new PrimitiveKeyOpenHashMap[Int, Int](-1)
    }
    intercept[IllegalArgumentException] {
      new PrimitiveKeyOpenHashMap[Int, Int](0)
    }
  }

  test("basic operations") {
    val longBase = 1000000L
    val map = new PrimitiveKeyOpenHashMap[Long, Int]

    for (i <- 1 to 1000) {
      map(i + longBase) = i
      assert(map(i + longBase) === i)
    }

    assert(map.size === 1000)

    for (i <- 1 to 1000) {
      assert(map(i + longBase) === i)
    }

    // Test iterator
    val set = new HashSet[(Long, Int)]
    for ((k, v) <- map) {
      set.add((k, v))
    }
    assert(set === (1 to 1000).map(x => (x + longBase, x)).toSet)
  }

  test("null values") {
    val map = new PrimitiveKeyOpenHashMap[Long, String]()
    for (i <- 1 to 100) {
      map(i.toLong) = null
    }
    assert(map.size === 100)
    assert(map(1.toLong) === null)
  }

  test("changeValue") {
    val map = new PrimitiveKeyOpenHashMap[Long, String]()
    for (i <- 1 to 100) {
      map(i.toLong) = i.toString
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      val res = map.changeValue(i.toLong, { assert(false); "" }, v => {
        assert(v === i.toString)
        v + "!"
      })
      assert(res === i + "!")
    }
    // Iterate from 101 to 400 to make sure the map grows a couple of times, because we had a
    // bug where changeValue would return the wrong result when the map grew on that insert
    for (i <- 101 to 400) {
      val res = map.changeValue(i.toLong, { i + "!" }, v => { assert(false); v })
      assert(res === i + "!")
    }
    assert(map.size === 400)
  }

  test("inserting in capacity-1 map") {
    val map = new PrimitiveKeyOpenHashMap[Long, String](1)
    for (i <- 1 to 100) {
      map(i.toLong) = i.toString
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map(i.toLong) === i.toString)
    }
  }

  test("contains") {
    val map = new PrimitiveKeyOpenHashMap[Int, Int](1)
    map(0) = 0
    assert(map.contains(0))
    assert(!map.contains(1))
  }
}
