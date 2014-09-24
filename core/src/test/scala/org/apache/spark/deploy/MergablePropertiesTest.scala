package org.apache.spark.deploy

/**
 * Created by dale on 22/09/2014.
 */

import org.apache.hadoop.util.hash.Hash
import org.scalatest.FunSuite
import org.scalatest.Matchers

import scala.collection.mutable

class MergablePropertiesTest extends FunSuite with Matchers {
  val test1 = "1"->"one"
  val test2 = "2"->"two"
  val test3a = "3"->"three a"
  val test3b = "3"->"three b"
  val test3c = "3"->"three c"
  val test4 = "4"->"four"

  test("merge one Map by itself") {
    val result = MergedPropertyMap.mergePropertyMaps(Vector(Map(test1, test2)))
    result should contain (test1)
    result should contain (test2)
  }

  test("merge two Maps no overlap") {
    val result = MergedPropertyMap.mergePropertyMaps(Vector(Map(test1), Map(test2)))
    result should contain (test1)
    result should contain (test2)
  }

  test("merge two maps with one level of overlap") {
    val result = MergedPropertyMap.mergePropertyMaps(Vector(Map(test1, test3a), Map(test2, test3b)))
    result should contain (test1)
    result should contain (test2)
    result should contain (test3b)
  }

  test("merge three maps with one level of overlap") {
    val result = MergedPropertyMap.mergePropertyMaps(Vector(Map(test1, test3a), Map(test2, test3b), Map(test4)))
    result should contain (test1)
    result should contain (test2)
    result should contain (test4)
  }

  test("merge three maps with two levels of overlap") {
    val result = MergedPropertyMap.mergePropertyMaps(Vector(Map(test1, test3a), Map(test2, test3b), Map(test4, test3c)))
    result should contain (test1)
    result should contain (test2)
    result should contain (test3c)
    result should contain (test4)
  }

}
