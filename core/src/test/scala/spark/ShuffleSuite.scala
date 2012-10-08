package spark

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import com.google.common.io.Files

import spark.rdd.ShuffledAggregatedRDD
import SparkContext._

class ShuffleSuite extends FunSuite with ShouldMatchers with BeforeAndAfter {

  var sc: SparkContext = _

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }

  test("groupByKey") {
    sc = new SparkContext("local", "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)))
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with duplicates") {
    sc = new SparkContext("local", "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with negative key hash codes") {
    sc = new SparkContext("local", "test")
    val pairs = sc.parallelize(Array((-1, 1), (-1, 2), (-1, 3), (2, 1)))
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesForMinus1 = groups.find(_._1 == -1).get._2
    assert(valuesForMinus1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with many output partitions") {
    sc = new SparkContext("local", "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)))
    val groups = pairs.groupByKey(10).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with compression") {
    try {
      System.setProperty("spark.blockManager.compress", "true")
      sc = new SparkContext("local", "test")
      val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 4)
      val groups = pairs.groupByKey(4).collect()
      assert(groups.size === 2)
      val valuesFor1 = groups.find(_._1 == 1).get._2
      assert(valuesFor1.toList.sorted === List(1, 2, 3))
      val valuesFor2 = groups.find(_._1 == 2).get._2
      assert(valuesFor2.toList.sorted === List(1))
    } finally {
      System.setProperty("spark.blockManager.compress", "false")
    }
  }

  test("reduceByKey") {
    sc = new SparkContext("local", "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.reduceByKey(_+_).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))
  }

  test("reduceByKey with collectAsMap") {
    sc = new SparkContext("local", "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.reduceByKey(_+_).collectAsMap()
    assert(sums.size === 2)
    assert(sums(1) === 7)
    assert(sums(2) === 1)
  }

  test("reduceByKey with many output partitons") {
    sc = new SparkContext("local", "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.reduceByKey(_+_, 10).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))
  }

  test("join") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }

  test("join all-to-all") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (1, 3)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (1, 'y')))
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 6)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (1, 'y')),
      (1, (2, 'x')),
      (1, (2, 'y')),
      (1, (3, 'x')),
      (1, (3, 'y'))
    ))
  }

  test("leftOuterJoin") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.leftOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (1, Some('x'))),
      (1, (2, Some('x'))),
      (2, (1, Some('y'))),
      (2, (1, Some('z'))),
      (3, (1, None))
    ))
  }

  test("rightOuterJoin") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.rightOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (Some(1), 'x')),
      (1, (Some(2), 'x')),
      (2, (Some(1), 'y')),
      (2, (Some(1), 'z')),
      (4, (None, 'w'))
    ))
  }

  test("join with no matches") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((4, 'x'), (5, 'y'), (5, 'z'), (6, 'w')))
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 0)
  }

  test("join with many output partitions") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.join(rdd2, 10).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }

  test("groupWith") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.groupWith(rdd2).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (ArrayBuffer(1, 2), ArrayBuffer('x'))),
      (2, (ArrayBuffer(1), ArrayBuffer('y', 'z'))),
      (3, (ArrayBuffer(1), ArrayBuffer())),
      (4, (ArrayBuffer(), ArrayBuffer('w')))
    ))
  }

  test("zero-partition RDD") {
    sc = new SparkContext("local", "test")
    val emptyDir = Files.createTempDir()
    val file = sc.textFile(emptyDir.getAbsolutePath)
    assert(file.splits.size == 0)
    assert(file.collect().toList === Nil)
    // Test that a shuffle on the file works, because this used to be a bug
    assert(file.map(line => (line, 1)).reduceByKey(_ + _).collect().toList === Nil)
  }

  test("map-side combine") {
    sc = new SparkContext("local", "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1), (1, 1)), 2)

    // Test with map-side combine on.
    val sums = pairs.reduceByKey(_+_).collect()
    assert(sums.toSet === Set((1, 8), (2, 1)))

    // Turn off map-side combine and test the results.
    val aggregator = new Aggregator[Int, Int, Int](
      (v: Int) => v,
      _+_,
      _+_,
      false)
    val shuffledRdd = new ShuffledAggregatedRDD(
      pairs, aggregator, new HashPartitioner(2))
    assert(shuffledRdd.collect().toSet === Set((1, 8), (2, 1)))

    // Turn map-side combine off and pass a wrong mergeCombine function. Should
    // not see an exception because mergeCombine should not have been called.
    val aggregatorWithException = new Aggregator[Int, Int, Int](
      (v: Int) => v, _+_, ShuffleSuite.mergeCombineException, false)
    val shuffledRdd1 = new ShuffledAggregatedRDD(
      pairs, aggregatorWithException, new HashPartitioner(2))
    assert(shuffledRdd1.collect().toSet === Set((1, 8), (2, 1)))

    // Now run the same mergeCombine function with map-side combine on. We
    // expect to see an exception thrown.
    val aggregatorWithException1 = new Aggregator[Int, Int, Int](
      (v: Int) => v, _+_, ShuffleSuite.mergeCombineException)
    val shuffledRdd2 = new ShuffledAggregatedRDD(
      pairs, aggregatorWithException1, new HashPartitioner(2))
    evaluating { shuffledRdd2.collect() } should produce [SparkException]
  }
}

object ShuffleSuite {
  def mergeCombineException(x: Int, y: Int): Int = {
    throw new SparkException("Exception for map-side combine.")
    x + y
  }
}
