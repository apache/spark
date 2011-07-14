package spark

import org.scalatest.FunSuite
import SparkContext._

class RDDSuite extends FunSuite {
  test("basic operations") {
    val sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.collect().toList === List(1, 2, 3, 4))
    assert(nums.reduce(_ + _) === 10)
    assert(nums.map(_.toString).collect().toList === List("1", "2", "3", "4"))
    assert(nums.filter(_ > 2).collect().toList === List(3, 4))
    assert(nums.flatMap(x => 1 to x).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    assert(nums.union(nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(nums.glom().map(_.toList).collect().toList === List(List(1, 2), List(3, 4)))
    val partitionSums = nums.mapPartitions(iter => Iterator(iter.reduceLeft(_ + _)))
    assert(partitionSums.collect().toList === List(3, 7))
    sc.stop()
  }
}
