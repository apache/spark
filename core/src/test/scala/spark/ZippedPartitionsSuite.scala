package spark

import scala.collection.immutable.NumericRange

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import SparkContext._


object ZippedPartitionsSuite {
  def procZippedData(i: Iterator[Int], s: Iterator[String], d: Iterator[Double]) : Iterator[Int] = {
    Iterator(i.toArray.size, s.toArray.size, d.toArray.size)
  }
}

class ZippedPartitionsSuite extends FunSuite with LocalSparkContext {
  test("print sizes") {
    sc = new SparkContext("local", "test")
    val data1 = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val data2 = sc.makeRDD(Array("1", "2", "3", "4", "5", "6"), 2)
    val data3 = sc.makeRDD(Array(1.0, 2.0), 2)

    val zippedRDD = data1.zipPartitions(ZippedPartitionsSuite.procZippedData, data2, data3)

    val obtainedSizes = zippedRDD.collect()
    val expectedSizes = Array(2, 3, 1, 2, 3, 1)
    assert(obtainedSizes.size == 6)
    assert(obtainedSizes.zip(expectedSizes).forall(x => x._1 == x._2))
  }
}
