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

class OpenHashMapSuite extends FunSuite with Matchers {

  test("size for specialized, primitive value (int)") {
    val capacity = 1024
    val map = new OpenHashMap[String, Int](capacity)
    val actualSize = SizeEstimator.estimate(map)
    // 64 bit for pointers, 32 bit for ints, and 1 bit for the bitset.
    val expectedSize = capacity * (64 + 32 + 1) / 8
    // Make sure we are not allocating a significant amount of memory beyond our expected.
    actualSize should be <= (expectedSize * 1.1).toLong
  }

  test("initialization") {
    val goodMap1 = new OpenHashMap[String, Int](1)
    assert(goodMap1.size === 0)
    val goodMap2 = new OpenHashMap[String, Int](255)
    assert(goodMap2.size === 0)
    val goodMap3 = new OpenHashMap[String, String](256)
    assert(goodMap3.size === 0)
    intercept[IllegalArgumentException] {
      new OpenHashMap[String, Int](1 << 30) // Invalid map size: bigger than 2^29
    }
    intercept[IllegalArgumentException] {
      new OpenHashMap[String, Int](-1)
    }
    intercept[IllegalArgumentException] {
      new OpenHashMap[String, String](0)
    }
  }

  test("primitive value") {
    val map = new OpenHashMap[String, Int]

    for (i <- 1 to 1000) {
      map(i.toString) = i
      assert(map(i.toString) === i)
    }

    assert(map.size === 1000)
    assert(map(null) === 0)

    map(null) = -1
    assert(map.size === 1001)
    assert(map(null) === -1)

    for (i <- 1 to 1000) {
      assert(map(i.toString) === i)
    }

    // Test iterator
    val set = new HashSet[(String, Int)]
    for ((k, v) <- map) {
      set.add((k, v))
    }
    val expected = (1 to 1000).map(x => (x.toString, x)) :+ (null.asInstanceOf[String], -1)
    assert(set === expected.toSet)
  }

  test("non-primitive value") {
    val map = new OpenHashMap[String, String]

    for (i <- 1 to 1000) {
      map(i.toString) = i.toString
      assert(map(i.toString) === i.toString)
    }

    assert(map.size === 1000)
    assert(map(null) === null)

    map(null) = "-1"
    assert(map.size === 1001)
    assert(map(null) === "-1")

    for (i <- 1 to 1000) {
      assert(map(i.toString) === i.toString)
    }

    // Test iterator
    val set = new HashSet[(String, String)]
    for ((k, v) <- map) {
      set.add((k, v))
    }
    val expected = (1 to 1000).map(_.toString).map(x => (x, x)) :+ (null.asInstanceOf[String], "-1")
    assert(set === expected.toSet)
  }

  test("null keys") {
    val map = new OpenHashMap[String, String]()
    for (i <- 1 to 100) {
      map(i.toString) = i.toString
    }
    assert(map.size === 100)
    assert(map(null) === null)
    map(null) = "hello"
    assert(map.size === 101)
    assert(map(null) === "hello")
  }

  test("null values") {
    val map = new OpenHashMap[String, String]()
    for (i <- 1 to 100) {
      map(i.toString) = null
    }
    assert(map.size === 100)
    assert(map("1") === null)
    assert(map(null) === null)
    assert(map.size === 100)
    map(null) = null
    assert(map.size === 101)
    assert(map(null) === null)
  }

  test("changeValue") {
    val map = new OpenHashMap[String, String]()
    for (i <- 1 to 100) {
      map(i.toString) = i.toString
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      val res = map.changeValue(i.toString, { assert(false); "" }, v => {
        assert(v === i.toString)
        v + "!"
      })
      assert(res === i + "!")
    }
    // Iterate from 101 to 400 to make sure the map grows a couple of times, because we had a
    // bug where changeValue would return the wrong result when the map grew on that insert
    for (i <- 101 to 400) {
      val res = map.changeValue(i.toString, { i + "!" }, v => { assert(false); v })
      assert(res === i + "!")
    }
    assert(map.size === 400)
    assert(map(null) === null)
    map.changeValue(null, { "null!" }, v => { assert(false); v })
    assert(map.size === 401)
    map.changeValue(null, { assert(false); "" }, v => {
      assert(v === "null!")
      "null!!"
    })
    assert(map.size === 401)
  }

  test("inserting in capacity-1 map") {
    val map = new OpenHashMap[String, String](1)
    for (i <- 1 to 100) {
      map(i.toString) = i.toString
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map(i.toString) === i.toString)
    }
  }
}
