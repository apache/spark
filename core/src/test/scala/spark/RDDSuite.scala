package spark

import scala.collection.mutable.HashMap
import org.scalatest.{BeforeAndAfter, FunSuite}
import spark.SparkContext._
import spark.rdd.CoalescedRDD

class RDDSuite extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = _

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }

  test("basic operations") {
    sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.collect().toList === List(1, 2, 3, 4))
    val dups = sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 2)
    assert(dups.distinct.count === 4)
    assert(dups.distinct().collect === dups.distinct.collect)
    assert(dups.distinct(2).collect === dups.distinct.collect)
    assert(nums.reduce(_ + _) === 10)
    assert(nums.fold(0)(_ + _) === 10)
    assert(nums.map(_.toString).collect().toList === List("1", "2", "3", "4"))
    assert(nums.filter(_ > 2).collect().toList === List(3, 4))
    assert(nums.flatMap(x => 1 to x).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    assert(nums.union(nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(nums.glom().map(_.toList).collect().toList === List(List(1, 2), List(3, 4)))
    assert(nums.collect({ case i if i >= 3 => i.toString }).collect().toList === List("3", "4"))
    assert(nums.keyBy(_.toString).collect().toList === List(("1", 1), ("2", 2), ("3", 3), ("4", 4)))
    val partitionSums = nums.mapPartitions(iter => Iterator(iter.reduceLeft(_ + _)))
    assert(partitionSums.collect().toList === List(3, 7))

    val partitionSumsWithSplit = nums.mapPartitionsWithSplit {
      case(split, iter) => Iterator((split, iter.reduceLeft(_ + _)))
    }
    assert(partitionSumsWithSplit.collect().toList === List((0, 3), (1, 7)))
  }

  test("SparkContext.union") {
    sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(sc.union(nums).collect().toList === List(1, 2, 3, 4))
    assert(sc.union(nums, nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(sc.union(Seq(nums)).collect().toList === List(1, 2, 3, 4))
    assert(sc.union(Seq(nums, nums)).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
  }

  test("aggregate") {
    sc = new SparkContext("local", "test")
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
  }

  test("basic checkpointing") {
    import java.io.File
    val checkpointDir = File.createTempFile("temp", "")
    checkpointDir.delete()

    sc = new SparkContext("local", "test")
    sc.setCheckpointDir(checkpointDir.toString)
    val parCollection = sc.makeRDD(1 to 4)
    val flatMappedRDD = parCollection.flatMap(x => 1 to x)
    flatMappedRDD.checkpoint()
    assert(flatMappedRDD.dependencies.head.rdd == parCollection)
    val result = flatMappedRDD.collect()
    Thread.sleep(1000)
    assert(flatMappedRDD.dependencies.head.rdd != parCollection)
    assert(flatMappedRDD.collect() === result)

    checkpointDir.deleteOnExit()
  }

  test("basic caching") {
    sc = new SparkContext("local", "test")
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    assert(rdd.collect().toList === List(1, 2, 3, 4))
    assert(rdd.collect().toList === List(1, 2, 3, 4))
    assert(rdd.collect().toList === List(1, 2, 3, 4))
  }

  test("caching with failures") {
    sc = new SparkContext("local", "test")
    val onlySplit = new Split { override def index: Int = 0 }
    var shouldFail = true
    val rdd = new RDD[Int](sc, Nil) {
      override def getSplits: Array[Split] = Array(onlySplit)
      override val getDependencies = List[Dependency[_]]()
      override def compute(split: Split, context: TaskContext): Iterator[Int] = {
        if (shouldFail) {
          throw new Exception("injected failure")
        } else {
          return Array(1, 2, 3, 4).iterator
        }
      }
    }.cache()
    val thrown = intercept[Exception]{
      rdd.collect()
    }
    assert(thrown.getMessage.contains("injected failure"))
    shouldFail = false
    assert(rdd.collect().toList === List(1, 2, 3, 4))
  }

  test("coalesced RDDs") {
    sc = new SparkContext("local", "test")
    val data = sc.parallelize(1 to 10, 10)

    val coalesced1 = new CoalescedRDD(data, 2)
    assert(coalesced1.collect().toList === (1 to 10).toList)
    assert(coalesced1.glom().collect().map(_.toList).toList ===
      List(List(1, 2, 3, 4, 5), List(6, 7, 8, 9, 10)))

    // Check that the narrow dependency is also specified correctly
    assert(coalesced1.dependencies.head.asInstanceOf[NarrowDependency[_]].getParents(0).toList ===
      List(0, 1, 2, 3, 4))
    assert(coalesced1.dependencies.head.asInstanceOf[NarrowDependency[_]].getParents(1).toList ===
      List(5, 6, 7, 8, 9))

    val coalesced2 = new CoalescedRDD(data, 3)
    assert(coalesced2.collect().toList === (1 to 10).toList)
    assert(coalesced2.glom().collect().map(_.toList).toList ===
      List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9, 10)))

    val coalesced3 = new CoalescedRDD(data, 10)
    assert(coalesced3.collect().toList === (1 to 10).toList)
    assert(coalesced3.glom().collect().map(_.toList).toList ===
      (1 to 10).map(x => List(x)).toList)

    // If we try to coalesce into more partitions than the original RDD, it should just
    // keep the original number of partitions.
    val coalesced4 = new CoalescedRDD(data, 20)
    assert(coalesced4.collect().toList === (1 to 10).toList)
    assert(coalesced4.glom().collect().map(_.toList).toList ===
      (1 to 10).map(x => List(x)).toList)
  }

  test("zipped RDDs") {
    sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val zipped = nums.zip(nums.map(_ + 1.0))
    assert(zipped.glom().map(_.toList).collect().toList ===
      List(List((1, 2.0), (2, 3.0)), List((3, 4.0), (4, 5.0))))

    intercept[IllegalArgumentException] {
      nums.zip(sc.parallelize(1 to 4, 1)).collect()
    }
  }

  test("split pruning") {
    sc = new SparkContext("local", "test")
    val data = sc.parallelize(1 to 10, 10)
    // Note that split number starts from 0, so > 8 means only 10th partition left.
    val prunedRdd = data.pruneSplits(splitNum => splitNum > 8)
    assert(prunedRdd.splits.size === 1)
    val prunedData = prunedRdd.collect
    assert(prunedData.size === 1)
    assert(prunedData(0) === 10)
  }
}
