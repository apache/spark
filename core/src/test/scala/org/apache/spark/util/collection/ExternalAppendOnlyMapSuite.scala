/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.SparkContext._

class ExternalAppendOnlyMapSuite extends FunSuite with LocalSparkContext {

  private def createCombiner(i: Int) = ArrayBuffer[Int](i)
  private def mergeValue(buffer: ArrayBuffer[Int], i: Int) = buffer += i
  private def mergeCombiners(buf1: ArrayBuffer[Int], buf2: ArrayBuffer[Int]) = buf1 ++= buf2

  test("simple insert") {
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    val map = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](createCombiner,
      mergeValue, mergeCombiners)

    // Single insert
    map.insert(1, 10)
    var it = map.iterator
    assert(it.hasNext)
    val kv = it.next()
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
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    val map = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](createCombiner,
      mergeValue, mergeCombiners)

    map.insertAll(Seq(
      (1, 10),
      (2, 20),
      (3, 30),
      (1, 100),
      (2, 200),
      (1, 1000)))
    val it = map.iterator
    assert(it.hasNext)
    val result = it.toSet[(Int, ArrayBuffer[Int])].map(kv => (kv._1, kv._2.toSet))
    assert(result == Set[(Int, Set[Int])](
      (1, Set[Int](10, 100, 1000)),
      (2, Set[Int](20, 200)),
      (3, Set[Int](30))))
  }

  test("ordering") {
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

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
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

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
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)

    // reduceByKey
    val rdd = sc.parallelize(1 to 10).map(i => (i%2, 1))
    val result1 = rdd.reduceByKey(_+_).collect()
    assert(result1.toSet == Set[(Int, Int)]((0, 5), (1, 5)))

    // groupByKey
    val result2 = rdd.groupByKey().collect().map(x => (x._1, x._2.toList)).toSet
    assert(result2.toSet == Set[(Int, Seq[Int])]
      ((0, List[Int](1, 1, 1, 1, 1)), (1, List[Int](1, 1, 1, 1, 1))))
  }

  test("simple cogroup") {
    val conf = new SparkConf(false)
    sc = new SparkContext("local", "test", conf)
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
    val conf = new SparkConf(true)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    // reduceByKey - should spill ~8 times
    val rddA = sc.parallelize(0 until 100000).map(i => (i/2, i))
    val resultA = rddA.reduceByKey(math.max).collect()
    assert(resultA.length == 50000)
    resultA.foreach { case(k, v) =>
      if (v != k * 2 + 1) {
        fail(s"Value for ${k} was wrong: expected ${k * 2 + 1}, got ${v}")
      }
    }

    // groupByKey - should spill ~17 times
    val rddB = sc.parallelize(0 until 100000).map(i => (i/4, i))
    val resultB = rddB.groupByKey().collect()
    assert(resultB.length == 25000)
    resultB.foreach { case(i, seq) =>
      val expected = Set(i * 4, i * 4 + 1, i * 4 + 2, i * 4 + 3)
      if (seq.toSet != expected) {
        fail(s"Value for ${i} was wrong: expected ${expected}, got ${seq.toSet}")
      }
    }

    // cogroup - should spill ~7 times
    val rddC1 = sc.parallelize(0 until 10000).map(i => (i, i))
    val rddC2 = sc.parallelize(0 until 10000).map(i => (i%1000, i))
    val resultC = rddC1.cogroup(rddC2).collect()
    assert(resultC.length == 10000)
    resultC.foreach { case(i, (seq1, seq2)) =>
      i match {
        case 0 =>
          assert(seq1.toSet == Set[Int](0))
          assert(seq2.toSet == Set[Int](0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000))
        case 1 =>
          assert(seq1.toSet == Set[Int](1))
          assert(seq2.toSet == Set[Int](1, 1001, 2001, 3001, 4001, 5001, 6001, 7001, 8001, 9001))
        case 5000 =>
          assert(seq1.toSet == Set[Int](5000))
          assert(seq2.toSet == Set[Int]())
        case 9999 =>
          assert(seq1.toSet == Set[Int](9999))
          assert(seq2.toSet == Set[Int]())
        case _ =>
      }
    }
  }

  test("spilling with hash collisions") {
    val conf = new SparkConf(true)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    def createCombiner(i: String) = ArrayBuffer[String](i)
    def mergeValue(buffer: ArrayBuffer[String], i: String) = buffer += i
    def mergeCombiners(buffer1: ArrayBuffer[String], buffer2: ArrayBuffer[String]) =
      buffer1 ++= buffer2

    val map = new ExternalAppendOnlyMap[String, String, ArrayBuffer[String]](
      createCombiner, mergeValue, mergeCombiners)

    val collisionPairs = Seq(
      ("Aa", "BB"),                   // 2112
      ("to", "v1"),                   // 3707
      ("variants", "gelato"),         // -1249574770
      ("Teheran", "Siblings"),        // 231609873
      ("misused", "horsemints"),      // 1069518484
      ("isohel", "epistolaries"),     // -1179291542
      ("righto", "buzzards"),         // -931102253
      ("hierarch", "crinolines"),     // -1732884796
      ("inwork", "hypercatalexes"),   // -1183663690
      ("wainages", "presentencing"),  // 240183619
      ("trichothecenes", "locular"),  // 339006536
      ("pomatoes", "eructation")      // 568647356
    )

    collisionPairs.foreach { case (w1, w2) =>
      // String.hashCode is documented to use a specific algorithm, but check just in case
      assert(w1.hashCode === w2.hashCode)
    }

    map.insertAll((1 to 100000).iterator.map(_.toString).map(i => (i, i)))
    collisionPairs.foreach { case (w1, w2) =>
      map.insert(w1, w2)
      map.insert(w2, w1)
    }

    // A map of collision pairs in both directions
    val collisionPairsMap = (collisionPairs ++ collisionPairs.map(_.swap)).toMap

    // Avoid map.size or map.iterator.length because this destructively sorts the underlying map
    var count = 0

    val it = map.iterator
    while (it.hasNext) {
      val kv = it.next()
      val expectedValue = ArrayBuffer[String](collisionPairsMap.getOrElse(kv._1, kv._1))
      assert(kv._2.equals(expectedValue))
      count += 1
    }
    assert(count === 100000 + collisionPairs.size * 2)
  }

  test("spilling with many hash collisions") {
    val conf = new SparkConf(true)
    conf.set("spark.shuffle.memoryFraction", "0.0001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    val map = new ExternalAppendOnlyMap[FixedHashObject, Int, Int](_ => 1, _ + _, _ + _)

    // Insert 10 copies each of lots of objects whose hash codes are either 0 or 1. This causes
    // problems if the map fails to group together the objects with the same code (SPARK-2043).
    for (i <- 1 to 10) {
      for (j <- 1 to 10000) {
        map.insert(FixedHashObject(j, j % 2), 1)
      }
    }

    val it = map.iterator
    var count = 0
    while (it.hasNext) {
      val kv = it.next()
      assert(kv._2 === 10)
      count += 1
    }
    assert(count === 10000)
  }

  test("spilling with hash collisions using the Int.MaxValue key") {
    val conf = new SparkConf(true)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    val map = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](
      createCombiner, mergeValue, mergeCombiners)

    (1 to 100000).foreach { i => map.insert(i, i) }
    map.insert(Int.MaxValue, Int.MaxValue)

    val it = map.iterator
    while (it.hasNext) {
      // Should not throw NoSuchElementException
      it.next()
    }
  }

  test("spilling with null keys and values") {
    val conf = new SparkConf(true)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    val map = new ExternalAppendOnlyMap[Int, Int, ArrayBuffer[Int]](
      createCombiner, mergeValue, mergeCombiners)

    map.insertAll((1 to 100000).iterator.map(i => (i, i)))
    map.insert(null.asInstanceOf[Int], 1)
    map.insert(1, null.asInstanceOf[Int])
    map.insert(null.asInstanceOf[Int], null.asInstanceOf[Int])

    val it = map.iterator
    while (it.hasNext) {
      // Should not throw NullPointerException
      it.next()
    }
  }

}
