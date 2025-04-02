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

import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.SizeEstimator

class OpenHashMapSuite extends SparkFunSuite with Matchers {

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
      new OpenHashMap[String, Int](1 << 30 + 1) // Invalid map size: bigger than 2^30
    }
    intercept[IllegalArgumentException] {
      new OpenHashMap[String, Int](-1)
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
    val expected = (1 to 1000).map(x => (x.toString, x)) :+ ((null.asInstanceOf[String], -1))
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
    val expected =
      (1 to 1000).map(_.toString).map(x => (x, x)) :+ ((null.asInstanceOf[String], "-1"))
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
      assert(res === s"$i!")
    }
    // Iterate from 101 to 400 to make sure the map grows a couple of times, because we had a
    // bug where changeValue would return the wrong result when the map grew on that insert
    for (i <- 101 to 400) {
      val res = map.changeValue(i.toString, { s"$i!" }, v => { assert(false); v })
      assert(res === s"$i!")
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

  test("contains") {
    val map = new OpenHashMap[String, Int](2)
    map("a") = 1
    assert(map.contains("a"))
    assert(!map.contains("b"))
    assert(!map.contains(null))
    map(null) = 0
    assert(map.contains(null))
  }

  test("distinguish between the 0/0.0/0L and null") {
    val specializedMap1 = new OpenHashMap[String, Long]
    specializedMap1("a") = null.asInstanceOf[Long]
    specializedMap1("b") = 0L
    assert(specializedMap1.contains("a"))
    assert(!specializedMap1.contains("c"))
    // null.asInstance[Long] will return 0L
    assert(specializedMap1("a") === 0L)
    assert(specializedMap1("b") === 0L)
    // If the data type is in @specialized annotation, and
    // the `key` is not be contained, the `map(key)` will return 0
    assert(specializedMap1("c") === 0L)

    val specializedMap2 = new OpenHashMap[String, Double]
    specializedMap2("a") = null.asInstanceOf[Double]
    specializedMap2("b") = 0.toDouble
    assert(specializedMap2.contains("a"))
    assert(!specializedMap2.contains("c"))
    // null.asInstance[Double] will return 0.0
    assert(specializedMap2("a") === 0.0)
    assert(specializedMap2("b") === 0.0)
    assert(specializedMap2("c") === 0.0)

    val map1 = new OpenHashMap[String, Short]
    map1("a") = null.asInstanceOf[Short]
    map1("b") = 0.toShort
    assert(map1.contains("a"))
    assert(!map1.contains("c"))
    // null.asInstance[Short] will return 0
    assert(map1("a") === 0)
    assert(map1("b") === 0)
    // If the data type is not in @specialized annotation, and
    // the `key` is not be contained, the `map(key)` will return null
    assert(map1("c") === null)

    val map2 = new OpenHashMap[String, Float]
    map2("a") = null.asInstanceOf[Float]
    map2("b") = 0.toFloat
    assert(map2.contains("a"))
    assert(!map2.contains("c"))
    // null.asInstance[Float] will return 0.0
    assert(map2("a") === 0.0)
    assert(map2("b") === 0.0)
    assert(map2("c") === null)
  }

  test("get") {
    val map = new OpenHashMap[String, String]()

    // Get with normal/null keys.
    map("1") = "1"
    assert(map.get("1") === Some("1"))
    assert(map.get("2") === None)
    assert(map.get(null) === None)
    map(null) = "hello"
    assert(map.get(null) === Some("hello"))

    // Get with null values.
    map("1") = null
    assert(map.get("1") === Some(null))
    map(null) = null
    assert(map.get(null) === Some(null))
  }

  test("SPARK-45599: 0.0 and -0.0 should count distinctly; NaNs should count together") {
    // Exactly these elements provided in roughly this order trigger a condition where lookups of
    // 0.0 and -0.0 in the bitset happen to collide, causing their counts to be merged incorrectly
    // and inconsistently if `==` is used to check for key equality.
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

    val map1 = new OpenHashMap[Double, Int]()
    spark45599Repro.foreach(map1.changeValue(_, 1, {_ + 1}))
    assert(map1(0.0) == 1)
    assert(map1(-0.0) == 1)
    assert(map1(Double.NaN) == 3)

    val map2 = new OpenHashMap[Double, Int]()
    // Simply changing the order in which the elements are added to the map should not change the
    // counts for 0.0 and -0.0.
    spark45599Repro.reverse.foreach(map2.changeValue(_, 1, {_ + 1}))
    assert(map2(0.0) == 1)
    assert(map2(-0.0) == 1)
    assert(map2(Double.NaN) == 3)
  }
}
