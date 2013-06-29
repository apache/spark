package spark

import scala.collection.mutable.HashMap
import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts._
import org.scalatest.time.{Span, Millis}
import spark.SparkContext._
import spark.rdd.{CoalescedRDD, CoGroupedRDD, EmptyRDD, PartitionPruningRDD, ShuffledRDD}

class RDDSuite extends FunSuite with SharedSparkContext {

  test("basic operations") {
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
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(sc.union(nums).collect().toList === List(1, 2, 3, 4))
    assert(sc.union(nums, nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    assert(sc.union(Seq(nums)).collect().toList === List(1, 2, 3, 4))
    assert(sc.union(Seq(nums, nums)).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
  }

  test("aggregate") {
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

  test("basic caching") {
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    assert(rdd.collect().toList === List(1, 2, 3, 4))
    assert(rdd.collect().toList === List(1, 2, 3, 4))
    assert(rdd.collect().toList === List(1, 2, 3, 4))
  }

  test("caching with failures") {
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
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val zipped = nums.zip(nums.map(_ + 1.0))
    assert(zipped.glom().map(_.toList).collect().toList ===
      List(List((1, 2.0), (2, 3.0)), List((3, 4.0), (4, 5.0))))

    intercept[IllegalArgumentException] {
      nums.zip(sc.parallelize(1 to 4, 1)).collect()
    }
  }

  test("partition pruning") {
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

  test("top with predefined ordering") {
    val nums = Array.range(1, 100000)
    val ints = sc.makeRDD(scala.util.Random.shuffle(nums), 2)
    val topK = ints.top(5)
    assert(topK.size === 5)
    assert(topK.sorted === nums.sorted.takeRight(5))
  }

  test("top with custom ordering") {
    val words = Vector("a", "b", "c", "d")
    implicit val ord = implicitly[Ordering[String]].reverse
    val rdd = sc.makeRDD(words, 2)
    val topK = rdd.top(2)
    assert(topK.size === 2)
    assert(topK.sorted === Array("b", "a"))
  }
}
