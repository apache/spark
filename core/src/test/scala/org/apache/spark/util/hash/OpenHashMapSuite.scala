package org.apache.spark.util.hash

import scala.collection.mutable.HashSet
import org.scalatest.FunSuite

class OpenHashMapSuite extends FunSuite {

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
      map("" + i) = "" + i
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
    val map = new OpenHashMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      val res = map.changeValue("" + i, { assert(false); "" }, v => {
        assert(v === "" + i)
        v + "!"
      })
      assert(res === i + "!")
    }
    // Iterate from 101 to 400 to make sure the map grows a couple of times, because we had a
    // bug where changeValue would return the wrong result when the map grew on that insert
    for (i <- 101 to 400) {
      val res = map.changeValue("" + i, { i + "!" }, v => { assert(false); v })
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
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map("" + i) === "" + i)
    }
  }
}
