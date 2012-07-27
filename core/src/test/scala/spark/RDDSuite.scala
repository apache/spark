package spark

import scala.collection.mutable.HashMap
import org.scalatest.FunSuite
import SparkContext._

class RDDSuite extends FunSuite {
  test("basic operations") {
    val sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.collect().toList === List(1, 2, 3, 4))
    assert(nums.reduce(_ + _) === 10)
    assert(nums.fold(0)(_ + _) === 10)
    assert(nums.map(_.toString).collect().toList === List("1", "2", "3", "4"))
    assert(nums.filter(_ > 2).collect().toList === List(3, 4))
    assert(nums.flatMap(x => 1 to x).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    assert(nums.union(nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(nums.glom().map(_.toList).collect().toList === List(List(1, 2), List(3, 4)))
    val partitionSums = nums.mapPartitions(iter => Iterator(iter.reduceLeft(_ + _)))
    assert(partitionSums.collect().toList === List(3, 7))
    sc.stop()
  }

  test("aggregate") {
    val sc = new SparkContext("local", "test")
    val pairs = sc.makeRDD(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)))
    type StringMap = HashMap[String, Int]
    val emptyMap = new StringMap {
      override def default(key: String): Int = 0
    }
    val mergeElement: (StringMap, (String, Int)) => StringMap = (map, pair) => {
      map(pair._1) += pair._2
      map
    }
    val mergeMaps: (StringMap, StringMap) => StringMap = (map1, map2) => {
      for ((key, value) <- map2) {
        map1(key) += value
      }
      map1
    }
    val result = pairs.aggregate(emptyMap)(mergeElement, mergeMaps)
    assert(result.toSet === Set(("a", 6), ("b", 2), ("c", 5)))
    sc.stop()
  }

  test("checkpointing") {
    val sc = new SparkContext("local", "test")
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).flatMap(x => 1 to x).checkpoint()
    assert(rdd.collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    sc.stop()
  }
}
