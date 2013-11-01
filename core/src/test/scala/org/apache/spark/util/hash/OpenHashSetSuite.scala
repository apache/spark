package org.apache.spark.util.hash

import org.scalatest.FunSuite


class OpenHashSetSuite extends FunSuite {

  test("primitive int") {
    val set = new OpenHashSet[Int]
    assert(set.size === 0)
    set.add(10)
    assert(set.size === 1)
    set.add(50)
    assert(set.size === 2)
    set.add(999)
    assert(set.size === 3)
    set.add(50)
    assert(set.size === 3)
  }

  test("primitive long") {
    val set = new OpenHashSet[Long]
    assert(set.size === 0)
    set.add(10L)
    assert(set.size === 1)
    set.add(50L)
    assert(set.size === 2)
    set.add(999L)
    assert(set.size === 3)
    set.add(50L)
    assert(set.size === 3)
  }

  test("non-primitive") {
    val set = new OpenHashSet[String]
    assert(set.size === 0)
    set.add(10.toString)
    assert(set.size === 1)
    set.add(50.toString)
    assert(set.size === 2)
    set.add(999.toString)
    assert(set.size === 3)
    set.add(50.toString)
    assert(set.size === 3)
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
}
