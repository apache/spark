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

import java.util.Comparator

import scala.collection.mutable.HashSet

import org.apache.spark.SparkFunSuite

class AppendOnlyMapSuite extends SparkFunSuite {
  test("initialization") {
    val goodMap1 = new AppendOnlyMap[Int, Int](1)
    assert(goodMap1.size === 0)
    val goodMap2 = new AppendOnlyMap[Int, Int](255)
    assert(goodMap2.size === 0)
    val goodMap3 = new AppendOnlyMap[Int, Int](256)
    assert(goodMap3.size === 0)
    intercept[IllegalArgumentException] {
      new AppendOnlyMap[Int, Int](1 << 30) // Invalid map size: bigger than 2^29
    }
    intercept[IllegalArgumentException] {
      new AppendOnlyMap[Int, Int](-1)
    }
    intercept[IllegalArgumentException] {
      new AppendOnlyMap[Int, Int](0)
    }
  }

  test("object keys and values") {
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map("" + i) === "" + i)
    }
    assert(map("0") === null)
    assert(map("101") === null)
    assert(map(null) === null)
    val set = new HashSet[(String, String)]
    for ((k, v) <- map) {   // Test the foreach method
      set += ((k, v))
    }
    assert(set === (1 to 100).map(_.toString).map(x => (x, x)).toSet)
  }

  test("primitive keys and values") {
    val map = new AppendOnlyMap[Int, Int]()
    for (i <- 1 to 100) {
      map(i) = i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map(i) === i)
    }
    assert(map(0) === null)
    assert(map(101) === null)
    val set = new HashSet[(Int, Int)]
    for ((k, v) <- map) {   // Test the foreach method
      set += ((k, v))
    }
    assert(set === (1 to 100).map(x => (x, x)).toSet)
  }

  test("null keys") {
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    assert(map(null) === null)
    map(null) = "hello"
    assert(map.size === 101)
    assert(map(null) === "hello")
  }

  test("null values") {
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = null
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
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      val res = map.changeValue("" + i, (hadValue, oldValue) => {
        assert(hadValue === true)
        assert(oldValue === "" + i)
        oldValue + "!"
      })
      assert(res === i + "!")
    }
    // Iterate from 101 to 400 to make sure the map grows a couple of times, because we had a
    // bug where changeValue would return the wrong result when the map grew on that insert
    for (i <- 101 to 400) {
      val res = map.changeValue("" + i, (hadValue, oldValue) => {
        assert(hadValue === false)
        i + "!"
      })
      assert(res === i + "!")
    }
    assert(map.size === 400)
    assert(map(null) === null)
    map.changeValue(null, (hadValue, oldValue) => {
      assert(hadValue === false)
      "null!"
    })
    assert(map.size === 401)
    map.changeValue(null, (hadValue, oldValue) => {
      assert(hadValue === true)
      assert(oldValue === "null!")
      "null!!"
    })
    assert(map.size === 401)
  }

  test("inserting in capacity-1 map") {
    val map = new AppendOnlyMap[String, String](1)
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map("" + i) === "" + i)
    }
  }

  test("destructive sort") {
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    map.update(null, "happy new year!")

    try {
      map.apply("1")
      map.update("1", "2013")
      map.changeValue("1", (hadValue, oldValue) => "2014")
      map.iterator
    } catch {
      case e: IllegalStateException => fail()
    }

    val it = map.destructiveSortedIterator(new Comparator[String] {
      def compare(key1: String, key2: String): Int = {
        val x = if (key1 != null) key1.toInt else Int.MinValue
        val y = if (key2 != null) key2.toInt else Int.MinValue
        x.compareTo(y)
      }
    })

    // Should be sorted by key
    assert(it.hasNext)
    var previous = it.next()
    assert(previous == (null, "happy new year!"))
    previous = it.next()
    assert(previous == ("1", "2014"))
    while (it.hasNext) {
      val kv = it.next()
      assert(kv._1.toInt > previous._1.toInt)
      previous = kv
    }

    // All subsequent calls to apply, update, changeValue and iterator should throw exception
    intercept[AssertionError] { map.apply("1") }
    intercept[AssertionError] { map.update("1", "2013") }
    intercept[AssertionError] { map.changeValue("1", (hadValue, oldValue) => "2014") }
    intercept[AssertionError] { map.iterator }
  }
}
