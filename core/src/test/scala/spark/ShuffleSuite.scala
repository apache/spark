package spark

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._

import com.google.common.io.Files

import spark.rdd.ShuffledRDD
import spark.SparkContext._

class ShuffleSuite extends FunSuite with ShouldMatchers with LocalSparkContext {

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
  
  test("reduceByKey with partitioner") {
    sc = new SparkContext("local", "test")
    val p = new Partitioner() {
      def numPartitions = 2
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 1), (0, 1))).partitionBy(p)
    val sums = pairs.reduceByKey(_+_)
    assert(sums.collect().toSet === Set((1, 4), (0, 1)))
    assert(sums.partitioner === Some(p))
    // count the dependencies to make sure there is only 1 ShuffledRDD
    val deps = new HashSet[RDD[_]]()
    def visit(r: RDD[_]) {
      for (dep <- r.dependencies) {
        deps += dep.rdd
        visit(dep.rdd)
      }
    }
    visit(sums)
    assert(deps.size === 2) // ShuffledRDD, ParallelCollection
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
    assert(file.partitions.size == 0)
    assert(file.collect().toList === Nil)
    // Test that a shuffle on the file works, because this used to be a bug
    assert(file.map(line => (line, 1)).reduceByKey(_ + _).collect().toList === Nil)
  }

  test("keys and values") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(Array((1, "a"), (2, "b")))
    assert(rdd.keys.collect().toList === List(1, 2))
    assert(rdd.values.collect().toList === List("a", "b"))
  }

  test("default partitioner uses partition size") {
    sc = new SparkContext("local", "test")
    // specify 2000 partitions
    val a = sc.makeRDD(Array(1, 2, 3, 4), 2000)
    // do a map, which loses the partitioner
    val b = a.map(a => (a, (a * 2).toString))
    // then a group by, and see we didn't revert to 2 partitions
    val c = b.groupByKey()
    assert(c.partitions.size === 2000)
  }

  test("default partitioner uses largest partitioner") {
    sc = new SparkContext("local", "test")
    val a = sc.makeRDD(Array((1, "a"), (2, "b")), 2)
    val b = sc.makeRDD(Array((1, "a"), (2, "b")), 2000)
    val c = a.join(b)
    assert(c.partitions.size === 2000)
  }

  test("subtract") {
    sc = new SparkContext("local", "test")
    val a = sc.parallelize(Array(1, 2, 3), 2)
    val b = sc.parallelize(Array(2, 3, 4), 4)
    val c = a.subtract(b)
    assert(c.collect().toSet === Set(1))
    assert(c.partitions.size === a.partitions.size)
  }

  test("subtract with narrow dependency") {
    sc = new SparkContext("local", "test")
    // use a deterministic partitioner
    val p = new Partitioner() {
      def numPartitions = 5
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    // partitionBy so we have a narrow dependency
    val a = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c"))).partitionBy(p)
    // more partitions/no partitioner so a shuffle dependency 
    val b = sc.parallelize(Array((2, "b"), (3, "cc"), (4, "d")), 4)
    val c = a.subtract(b)
    assert(c.collect().toSet === Set((1, "a"), (3, "c")))
    // Ideally we could keep the original partitioner...
    assert(c.partitioner === None)
  }

  test("subtractByKey") {
    sc = new SparkContext("local", "test")
    val a = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c")), 2)
    val b = sc.parallelize(Array((2, 20), (3, 30), (4, 40)), 4)
    val c = a.subtractByKey(b)
    assert(c.collect().toSet === Set((1, "a"), (1, "a")))
    assert(c.partitions.size === a.partitions.size)
  }

  test("subtractByKey with narrow dependency") {
    sc = new SparkContext("local", "test")
    // use a deterministic partitioner
    val p = new Partitioner() {
      def numPartitions = 5
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    // partitionBy so we have a narrow dependency
    val a = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c"))).partitionBy(p)
    // more partitions/no partitioner so a shuffle dependency 
    val b = sc.parallelize(Array((2, "b"), (3, "cc"), (4, "d")), 4)
    val c = a.subtractByKey(b)
    assert(c.collect().toSet === Set((1, "a"), (1, "a")))
    assert(c.partitioner.get === p)
  }

}

object ShuffleSuite {
  def mergeCombineException(x: Int, y: Int): Int = {
    throw new SparkException("Exception for map-side combine.")
    x + y
  }
}
