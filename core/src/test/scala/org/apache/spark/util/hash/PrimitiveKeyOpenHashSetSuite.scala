package org.apache.spark.util.hash

import scala.collection.mutable.HashSet
import org.scalatest.FunSuite

class PrimitiveKeyOpenHashSetSuite extends FunSuite {

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
      map(i.toLong) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      val res = map.changeValue(i.toLong, { assert(false); "" }, v => {
        assert(v === "" + i)
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
      map(i.toLong) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map(i.toLong) === "" + i)
    }
  }
}
