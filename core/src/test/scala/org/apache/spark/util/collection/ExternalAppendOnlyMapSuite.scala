package org.apache.spark.util.collection

import scala.collection.mutable.ArrayBuffer

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark._
import org.apache.spark.SparkContext.rddToPairRDDFunctions

class ExternalAppendOnlyMapSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  override def beforeEach() {
    val conf = new SparkConf(false)
    conf.set("spark.shuffle.externalSorting", "true")
    conf.set("spark.shuffle.buffer.mb", "1024")
    conf.set("spark.shuffle.buffer.fraction", "0.8")
    sc = new SparkContext("local", "test", conf)
  }

  val createCombiner: (Int => ArrayBuffer[Int]) = i => ArrayBuffer[Int](i)
  val mergeValue: (ArrayBuffer[Int], Int) => ArrayBuffer[Int] = (buffer, i) => {
    buffer += i
  }
  val mergeCombiners: (ArrayBuffer[Int], ArrayBuffer[Int]) => ArrayBuffer[Int] =
    (buf1, buf2) => {
      buf1 ++= buf2
    }

  test("simple insert") {
    var map = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](createCombiner,
      mergeValue, mergeCombiners)

    // Single insert
    map.insert(1, 10)
    var it = map.iterator
    assert(it.hasNext)
    var kv = it.next()
    assert(kv._1 == 1 && kv._2 == ArrayBuffer[Int](10))
    assert(!it.hasNext)

    // Multiple insert
    map.insert(2, 20)
    map.insert(3, 30)
    it = map.iterator
    assert(it.hasNext)
    assert(it.toSet == Set[(Int, ArrayBuffer[Int])](
      (1, ArrayBuffer[Int](10)),
      (2, ArrayBuffer[Int](20)),
      (3, ArrayBuffer[Int](30))))
  }

  test("insert with collision") {
    val map = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](createCombiner,
      mergeValue, mergeCombiners)

    map.insert(1, 10)
    map.insert(2, 20)
    map.insert(3, 30)
    map.insert(1, 100)
    map.insert(2, 200)
    map.insert(1, 1000)
    var it = map.iterator
    assert(it.hasNext)
    val result = it.toSet[(Int, ArrayBuffer[Int])].map(kv => (kv._1, kv._2.toSet))
    assert(result == Set[(Int, Set[Int])](
      (1, Set[Int](10, 100, 1000)),
      (2, Set[Int](20, 200)),
      (3, Set[Int](30))))
  }

  test("ordering") {
    val map1 = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](createCombiner,
      mergeValue, mergeCombiners)
    map1.insert(1, 10)
    map1.insert(2, 20)
    map1.insert(3, 30)

    val map2 = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](createCombiner,
      mergeValue, mergeCombiners)
    map2.insert(2, 20)
    map2.insert(3, 30)
    map2.insert(1, 10)

    val map3 = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](createCombiner,
      mergeValue, mergeCombiners)
    map3.insert(3, 30)
    map3.insert(1, 10)
    map3.insert(2, 20)

    val it1 = map1.iterator
    val it2 = map2.iterator
    val it3 = map3.iterator

    var kv1 = it1.next()
    var kv2 = it2.next()
    var kv3 = it3.next()
    assert(kv1._1 == kv2._1 && kv2._1 == kv3._1)
    assert(kv1._2 == kv2._2 && kv2._2 == kv3._2)

    kv1 = it1.next()
    kv2 = it2.next()
    kv3 = it3.next()
    assert(kv1._1 == kv2._1 && kv2._1 == kv3._1)
    assert(kv1._2 == kv2._2 && kv2._2 == kv3._2)

    kv1 = it1.next()
    kv2 = it2.next()
    kv3 = it3.next()
    assert(kv1._1 == kv2._1 && kv2._1 == kv3._1)
    assert(kv1._2 == kv2._2 && kv2._2 == kv3._2)
  }

  test("null keys and values") {
    val map = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](createCombiner,
      mergeValue, mergeCombiners)
    map.insert(1, 5)
    map.insert(2, 6)
    map.insert(3, 7)
    assert(map.size === 3)
    assert(map.iterator.toSet == Set[(Int, Seq[Int])](
      (1, Seq[Int](5)),
      (2, Seq[Int](6)),
      (3, Seq[Int](7))
    ))

    // Null keys
    val nullInt = null.asInstanceOf[Int]
    map.insert(nullInt, 8)
    assert(map.size === 4)
    assert(map.iterator.toSet == Set[(Int, Seq[Int])](
      (1, Seq[Int](5)),
      (2, Seq[Int](6)),
      (3, Seq[Int](7)),
      (nullInt, Seq[Int](8))
    ))

    // Null values
    map.insert(4, nullInt)
    map.insert(nullInt, nullInt)
    assert(map.size === 5)
    val result = map.iterator.toSet[(Int, ArrayBuffer[Int])].map(kv => (kv._1, kv._2.toSet))
    assert(result == Set[(Int, Set[Int])](
      (1, Set[Int](5)),
      (2, Set[Int](6)),
      (3, Set[Int](7)),
      (4, Set[Int](nullInt)),
      (nullInt, Set[Int](nullInt, 8))
    ))
  }

  test("simple aggregator") {
    // reduceByKey
    val rdd = sc.parallelize(1 to 10).map(i => (i%2, 1))
    val result1 = rdd.reduceByKey(_+_).collect()
    assert(result1.toSet == Set[(Int, Int)]((0, 5), (1, 5)))

    // groupByKey
    val result2 = rdd.groupByKey().collect()
    assert(result2.toSet == Set[(Int, Seq[Int])]
      ((0, ArrayBuffer[Int](1, 1, 1, 1, 1)), (1, ArrayBuffer[Int](1, 1, 1, 1, 1))))
  }

  test("simple cogroup") {
    val rdd1 = sc.parallelize(1 to 4).map(i => (i, i))
    val rdd2 = sc.parallelize(1 to 4).map(i => (i%2, i))
    val result = rdd1.cogroup(rdd2).collect()

    result.foreach { case (i, (seq1, seq2)) =>
      i match {
        case 0 => assert(seq1.toSet == Set[Int]() && seq2.toSet == Set[Int](2, 4))
        case 1 => assert(seq1.toSet == Set[Int](1) && seq2.toSet == Set[Int](1, 3))
        case 2 => assert(seq1.toSet == Set[Int](2) && seq2.toSet == Set[Int]())
        case 3 => assert(seq1.toSet == Set[Int](3) && seq2.toSet == Set[Int]())
        case 4 => assert(seq1.toSet == Set[Int](4) && seq2.toSet == Set[Int]())
      }
    }
  }

  test("spilling") {
    System.setProperty("spark.shuffle.buffer.mb", "1")
    System.setProperty("spark.shuffle.buffer.fraction", "0.05")

    // reduceByKey - should spill exactly 6 times
    val rddA = sc.parallelize(0 until 10000).map(i => (i/2, i))
    val resultA = rddA.reduceByKey(math.max(_, _)).collect()
    assert(resultA.length == 5000)
    resultA.foreach { case(k, v) =>
      k match {
        case 0 => assert(v == 1)
        case 2500 => assert(v == 5001)
        case 4999 => assert(v == 9999)
        case _ =>
      }
    }

    // groupByKey - should spill exactly 11 times
    val rddB = sc.parallelize(0 until 10000).map(i => (i/4, i))
    val resultB = rddB.groupByKey().collect()
    assert(resultB.length == 2500)
    resultB.foreach { case(i, seq) =>
      i match {
        case 0 => assert(seq.toSet == Set[Int](0, 1, 2, 3))
        case 1250 => assert(seq.toSet == Set[Int](5000, 5001, 5002, 5003))
        case 2499 => assert(seq.toSet == Set[Int](9996, 9997, 9998, 9999))
        case _ =>
      }
    }

    // cogroup - should spill exactly 7 times
    val rddC1 = sc.parallelize(0 until 1000).map(i => (i, i))
    val rddC2 = sc.parallelize(0 until 1000).map(i => (i%100, i))
    val resultC = rddC1.cogroup(rddC2).collect()
    assert(resultC.length == 1000)
    resultC.foreach { case(i, (seq1, seq2)) =>
      i match {
        case 0 =>
          assert(seq1.toSet == Set[Int](0))
          assert(seq2.toSet == Set[Int](0, 100, 200, 300, 400, 500, 600, 700, 800, 900))
        case 500 =>
          assert(seq1.toSet == Set[Int](500))
          assert(seq2.toSet == Set[Int]())
        case 999 =>
          assert(seq1.toSet == Set[Int](999))
          assert(seq2.toSet == Set[Int]())
        case _ =>
      }
    }
  }
}
