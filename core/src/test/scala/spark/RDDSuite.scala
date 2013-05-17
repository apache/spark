package spark

import scala.collection.mutable.HashMap
import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts._
import org.scalatest.time.{Span, Millis}
import spark.SparkContext._
import spark.rdd.{CoalescedRDD, CoGroupedRDD, EmptyRDD, PartitionPruningRDD, ShuffledRDD}

class RDDSuite extends FunSuite with LocalSparkContext {

  test("basic operations") {
    sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(nums.collect().toList === List(1, 2, 3, 4))
    val dups = sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 2)
    assert(dups.distinct().count() === 4)
    assert(dups.distinct.count === 4)  // Can distinct and count be called without parentheses?
    assert(dups.distinct.collect === dups.distinct().collect)
    assert(dups.distinct(2).collect === dups.distinct().collect)
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

    val partitionSumsWithIndex = nums.mapPartitionsWithIndex {
      case(split, iter) => Iterator((split, iter.reduceLeft(_ + _)))
    }
    assert(partitionSumsWithIndex.collect().toList === List((0, 3), (1, 7)))

    intercept[UnsupportedOperationException] {
      nums.filter(_ > 5).reduce(_ + _)
    }
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

  test("unpersist RDD") {
    sc = new SparkContext("local", "test")
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    rdd.count
    assert(sc.persistentRdds.isEmpty == false)
    rdd.unpersist()
    assert(sc.persistentRdds.isEmpty == true)

    failAfter(Span(3000, Millis)) {
      try {
        while (! sc.getRDDStorageInfo.isEmpty) {
          Thread.sleep(200)
        }
      } catch {
        case e: Exception =>
          // Do nothing. We might see exceptions because block manager
          // is racing this thread to remove entries from the driver.
      }
    }
    assert(sc.getRDDStorageInfo.isEmpty == true)
  }

  test("caching with failures") {
    sc = new SparkContext("local", "test")
    val onlySplit = new Partition { override def index: Int = 0 }
    var shouldFail = true
    val rdd = new RDD[Int](sc, Nil) {
      override def getPartitions: Array[Partition] = Array(onlySplit)
      override val getDependencies = List[Dependency[_]]()
      override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
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

  test("empty RDD") {
    sc = new SparkContext("local", "test")
    val empty = new EmptyRDD[Int](sc)
    assert(empty.count === 0)
    assert(empty.collect().size === 0)

    val thrown = intercept[UnsupportedOperationException]{
      empty.reduce(_+_)
    }
    assert(thrown.getMessage.contains("empty"))

    val emptyKv = new EmptyRDD[(Int, Int)](sc)
    val rdd = sc.parallelize(1 to 2, 2).map(x => (x, x))
    assert(rdd.join(emptyKv).collect().size === 0)
    assert(rdd.rightOuterJoin(emptyKv).collect().size === 0)
    assert(rdd.leftOuterJoin(emptyKv).collect().size === 2)
    assert(rdd.cogroup(emptyKv).collect().size === 2)
    assert(rdd.union(emptyKv).collect().size === 2)
  }

  test("cogrouped RDDs") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.makeRDD(Array((1, "one"), (1, "another one"), (2, "two"), (3, "three")), 2)
    val rdd2 = sc.makeRDD(Array((1, "one1"), (1, "another one1"), (2, "two1")), 2)

    // Use cogroup function
    val cogrouped = rdd1.cogroup(rdd2).collectAsMap()
    assert(cogrouped(1) === (Seq("one", "another one"), Seq("one1", "another one1")))
    assert(cogrouped(2) === (Seq("two"), Seq("two1")))
    assert(cogrouped(3) === (Seq("three"), Seq()))

    // Construct CoGroupedRDD directly, with map side combine enabled
    val cogrouped1 = new CoGroupedRDD[Int](
      Seq(rdd1.asInstanceOf[RDD[(Int, Any)]], rdd2.asInstanceOf[RDD[(Int, Any)]]),
      new HashPartitioner(3),
      true).collectAsMap()
    assert(cogrouped1(1).toSeq === Seq(Seq("one", "another one"), Seq("one1", "another one1")))
    assert(cogrouped1(2).toSeq === Seq(Seq("two"), Seq("two1")))
    assert(cogrouped1(3).toSeq === Seq(Seq("three"), Seq()))

    // Construct CoGroupedRDD directly, with map side combine disabled
    val cogrouped2 = new CoGroupedRDD[Int](
      Seq(rdd1.asInstanceOf[RDD[(Int, Any)]], rdd2.asInstanceOf[RDD[(Int, Any)]]),
      new HashPartitioner(3),
      false).collectAsMap()
    assert(cogrouped2(1).toSeq === Seq(Seq("one", "another one"), Seq("one1", "another one1")))
    assert(cogrouped2(2).toSeq === Seq(Seq("two"), Seq("two1")))
    assert(cogrouped2(3).toSeq === Seq(Seq("three"), Seq()))
  }

  test("coalesced RDDs") {
    sc = new SparkContext("local", "test")
    val data = sc.parallelize(1 to 10, 10)

    val coalesced1 = data.coalesce(2)
    assert(coalesced1.collect().toList === (1 to 10).toList)
    assert(coalesced1.glom().collect().map(_.toList).toList ===
      List(List(1, 2, 3, 4, 5), List(6, 7, 8, 9, 10)))

    // Check that the narrow dependency is also specified correctly
    assert(coalesced1.dependencies.head.asInstanceOf[NarrowDependency[_]].getParents(0).toList ===
      List(0, 1, 2, 3, 4))
    assert(coalesced1.dependencies.head.asInstanceOf[NarrowDependency[_]].getParents(1).toList ===
      List(5, 6, 7, 8, 9))

    val coalesced2 = data.coalesce(3)
    assert(coalesced2.collect().toList === (1 to 10).toList)
    assert(coalesced2.glom().collect().map(_.toList).toList ===
      List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9, 10)))

    val coalesced3 = data.coalesce(10)
    assert(coalesced3.collect().toList === (1 to 10).toList)
    assert(coalesced3.glom().collect().map(_.toList).toList ===
      (1 to 10).map(x => List(x)).toList)

    // If we try to coalesce into more partitions than the original RDD, it should just
    // keep the original number of partitions.
    val coalesced4 = data.coalesce(20)
    assert(coalesced4.collect().toList === (1 to 10).toList)
    assert(coalesced4.glom().collect().map(_.toList).toList ===
      (1 to 10).map(x => List(x)).toList)

    // we can optionally shuffle to keep the upstream parallel
    val coalesced5 = data.coalesce(1, shuffle = true)
    assert(coalesced5.dependencies.head.rdd.dependencies.head.rdd.asInstanceOf[ShuffledRDD[_, _]] !=
      null)
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

  test("partition pruning") {
    sc = new SparkContext("local", "test")
    val data = sc.parallelize(1 to 10, 10)
    // Note that split number starts from 0, so > 8 means only 10th partition left.
    val prunedRdd = new PartitionPruningRDD(data, splitNum => splitNum > 8)
    assert(prunedRdd.partitions.size === 1)
    val prunedData = prunedRdd.collect()
    assert(prunedData.size === 1)
    assert(prunedData(0) === 10)
  }

  test("mapWith") {
    import java.util.Random
    sc = new SparkContext("local", "test")
    val ones = sc.makeRDD(Array(1, 1, 1, 1, 1, 1), 2)
    val randoms = ones.mapWith(
      (index: Int) => new Random(index + 42))
      {(t: Int, prng: Random) => prng.nextDouble * t}.collect()
    val prn42_3 = {
      val prng42 = new Random(42)
      prng42.nextDouble(); prng42.nextDouble(); prng42.nextDouble()
    }
    val prn43_3 = {
      val prng43 = new Random(43)
      prng43.nextDouble(); prng43.nextDouble(); prng43.nextDouble()
    }
    assert(randoms(2) === prn42_3)
    assert(randoms(5) === prn43_3)
  }

  test("flatMapWith") {
    import java.util.Random
    sc = new SparkContext("local", "test")
    val ones = sc.makeRDD(Array(1, 1, 1, 1, 1, 1), 2)
    val randoms = ones.flatMapWith(
      (index: Int) => new Random(index + 42))
      {(t: Int, prng: Random) =>
        val random = prng.nextDouble()
        Seq(random * t, random * t * 10)}.
      collect()
    val prn42_3 = {
      val prng42 = new Random(42)
      prng42.nextDouble(); prng42.nextDouble(); prng42.nextDouble()
    }
    val prn43_3 = {
      val prng43 = new Random(43)
      prng43.nextDouble(); prng43.nextDouble(); prng43.nextDouble()
    }
    assert(randoms(5) === prn42_3 * 10)
    assert(randoms(11) === prn43_3 * 10)
  }

  test("filterWith") {
    import java.util.Random
    sc = new SparkContext("local", "test")
    val ints = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)
    val sample = ints.filterWith(
      (index: Int) => new Random(index + 42))
      {(t: Int, prng: Random) => prng.nextInt(3) == 0}.
      collect()
    val checkSample = {
      val prng42 = new Random(42)
      val prng43 = new Random(43)
      Array(1, 2, 3, 4, 5, 6).filter{i =>
	      if (i < 4) 0 == prng42.nextInt(3)
	      else 0 == prng43.nextInt(3)}
    }
    assert(sample.size === checkSample.size)
    for (i <- 0 until sample.size) assert(sample(i) === checkSample(i))
  }
}
