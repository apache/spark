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
import scala.concurrent.duration._
import scala.ref.WeakReference

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Tests.TEST_MEMORY
import org.apache.spark.io.CompressionCodec
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.util.CompletionIterator

class ExternalAppendOnlyMapSuite extends SparkFunSuite
  with LocalSparkContext
  with Eventually
  with Matchers {
  import TestUtils.{assertNotSpilled, assertSpilled}

  private def createCombiner[T](i: T) = ArrayBuffer[T](i)
  private def mergeValue[T](buffer: ArrayBuffer[T], i: T): ArrayBuffer[T] = buffer += i
  private def mergeCombiners[T](buf1: ArrayBuffer[T], buf2: ArrayBuffer[T]): ArrayBuffer[T] =
    buf1 ++= buf2

  private def createExternalMap[T] = {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    new ExternalAppendOnlyMap[T, T, ArrayBuffer[T]](
      createCombiner[T], mergeValue[T], mergeCombiners[T], context = context)
  }

  private def createSparkConf(loadDefaults: Boolean, codec: Option[String] = None): SparkConf = {
    val conf = new SparkConf(loadDefaults)
    // Make the Java serializer write a reset instruction (TC_RESET) after each object to test
    // for a bug we had with bytes written past the last object in a batch (SPARK-2792)
    conf.set(SERIALIZER_OBJECT_STREAM_RESET, 1)
    conf.set(SERIALIZER, "org.apache.spark.serializer.JavaSerializer")
    conf.set(SHUFFLE_SPILL_COMPRESS, codec.isDefined)
    conf.set(SHUFFLE_COMPRESS, codec.isDefined)
    codec.foreach { c => conf.set(IO_COMPRESSION_CODEC, c) }
    // Ensure that we actually have multiple batches per spill file
    conf.set(SHUFFLE_SPILL_BATCH_SIZE, 10L)
    conf
  }

  test("single insert") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)
    val map = createExternalMap[Int]
    map.insert(1, 10)
    val it = map.iterator
    assert(it.hasNext)
    val kv = it.next()
    assert(kv._1 === 1 && kv._2 === ArrayBuffer[Int](10))
    assert(!it.hasNext)
    sc.stop()
  }

  test("multiple insert") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)
    val map = createExternalMap[Int]
    map.insert(1, 10)
    map.insert(2, 20)
    map.insert(3, 30)
    val it = map.iterator
    assert(it.hasNext)
    assert(it.toSet === Set[(Int, ArrayBuffer[Int])](
      (1, ArrayBuffer[Int](10)),
      (2, ArrayBuffer[Int](20)),
      (3, ArrayBuffer[Int](30))))
    sc.stop()
  }

  test("insert with collision") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)
    val map = createExternalMap[Int]

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
    assert(result === Set[(Int, Set[Int])](
      (1, Set[Int](10, 100, 1000)),
      (2, Set[Int](20, 200)),
      (3, Set[Int](30))))
    sc.stop()
  }

  test("ordering") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)

    val map1 = createExternalMap[Int]
    map1.insert(1, 10)
    map1.insert(2, 20)
    map1.insert(3, 30)

    val map2 = createExternalMap[Int]
    map2.insert(2, 20)
    map2.insert(3, 30)
    map2.insert(1, 10)

    val map3 = createExternalMap[Int]
    map3.insert(3, 30)
    map3.insert(1, 10)
    map3.insert(2, 20)

    val it1 = map1.iterator
    val it2 = map2.iterator
    val it3 = map3.iterator

    var kv1 = it1.next()
    var kv2 = it2.next()
    var kv3 = it3.next()
    assert(kv1._1 === kv2._1 && kv2._1 === kv3._1)
    assert(kv1._2 === kv2._2 && kv2._2 === kv3._2)

    kv1 = it1.next()
    kv2 = it2.next()
    kv3 = it3.next()
    assert(kv1._1 === kv2._1 && kv2._1 === kv3._1)
    assert(kv1._2 === kv2._2 && kv2._2 === kv3._2)

    kv1 = it1.next()
    kv2 = it2.next()
    kv3 = it3.next()
    assert(kv1._1 === kv2._1 && kv2._1 === kv3._1)
    assert(kv1._2 === kv2._2 && kv2._2 === kv3._2)
    sc.stop()
  }

  test("null keys and values") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)

    val map = createExternalMap[Int]
    val nullInt = null.asInstanceOf[Int]
    map.insert(1, 5)
    map.insert(2, 6)
    map.insert(3, 7)
    map.insert(4, nullInt)
    map.insert(nullInt, 8)
    map.insert(nullInt, nullInt)
      val result = map.iterator.toSet[(Int, ArrayBuffer[Int])].map(kv => (kv._1, kv._2.sorted))
    assert(result === Set[(Int, Seq[Int])](
      (1, Seq[Int](5)),
      (2, Seq[Int](6)),
      (3, Seq[Int](7)),
      (4, Seq[Int](nullInt)),
      (nullInt, Seq[Int](nullInt, 8))
    ))

    sc.stop()
  }

  test("simple aggregator") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)

    // reduceByKey
    val rdd = sc.parallelize(1 to 10).map(i => (i%2, 1))
    val result1 = rdd.reduceByKey(_ + _).collect()
    assert(result1.toSet === Set[(Int, Int)]((0, 5), (1, 5)))

    // groupByKey
    val result2 = rdd.groupByKey().collect().map(x => (x._1, x._2.toList)).toSet
    assert(result2.toSet === Set[(Int, Seq[Int])]
      ((0, List[Int](1, 1, 1, 1, 1)), (1, List[Int](1, 1, 1, 1, 1))))
    sc.stop()
  }

  test("simple cogroup") {
    val conf = createSparkConf(loadDefaults = false)
    sc = new SparkContext("local", "test", conf)
    val rdd1 = sc.parallelize(1 to 4).map(i => (i, i))
    val rdd2 = sc.parallelize(1 to 4).map(i => (i%2, i))
    val result = rdd1.cogroup(rdd2).collect()

    result.foreach { case (i, (seq1, seq2)) =>
      i match {
        case 0 => assert(seq1.toSet === Set[Int]() && seq2.toSet === Set[Int](2, 4))
        case 1 => assert(seq1.toSet === Set[Int](1) && seq2.toSet === Set[Int](1, 3))
        case 2 => assert(seq1.toSet === Set[Int](2) && seq2.toSet === Set[Int]())
        case 3 => assert(seq1.toSet === Set[Int](3) && seq2.toSet === Set[Int]())
        case 4 => assert(seq1.toSet === Set[Int](4) && seq2.toSet === Set[Int]())
      }
    }
    sc.stop()
  }

  test("spilling") {
    testSimpleSpilling()
  }

  private def testSimpleSpillingForAllCodecs(encrypt: Boolean): Unit = {
    // Keep track of which compression codec we're using to report in test failure messages
    var lastCompressionCodec: Option[String] = None
    try {
      CompressionCodec.ALL_COMPRESSION_CODECS.foreach { c =>
        lastCompressionCodec = Some(c)
        testSimpleSpilling(Some(c), encrypt)
      }
    } catch {
      // Include compression codec used in test failure message
      // We need to catch Throwable here because assertion failures are not covered by Exceptions
      case t: Throwable =>
        val compressionMessage = lastCompressionCodec
          .map { c => "with compression using codec " + c }
          .getOrElse("without compression")
        val newException = new Exception(s"Test failed $compressionMessage:\n\n${t.getMessage}")
        newException.setStackTrace(t.getStackTrace)
        throw newException
    }
  }

  test("spilling with compression") {
    testSimpleSpillingForAllCodecs(encrypt = false)
  }

  test("spilling with compression and encryption") {
    testSimpleSpillingForAllCodecs(encrypt = true)
  }

  /**
   * Test spilling through simple aggregations and cogroups.
   * If a compression codec is provided, use it. Otherwise, do not compress spills.
   */
  private def testSimpleSpilling(codec: Option[String] = None, encrypt: Boolean = false): Unit = {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true, codec)  // Load defaults for Spark home
    conf.set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, size / 4)
    conf.set(IO_ENCRYPTION_ENABLED, encrypt)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)

    assertSpilled(sc, "reduceByKey") {
      val result = sc.parallelize(0 until size)
        .map { i => (i / 2, i) }.reduceByKey(math.max).collect()
      assert(result.length === size / 2)
      result.foreach { case (k, v) =>
        val expected = k * 2 + 1
        assert(v === expected, s"Value for $k was wrong: expected $expected, got $v")
      }
    }

    assertSpilled(sc, "groupByKey") {
      val result = sc.parallelize(0 until size).map { i => (i / 2, i) }.groupByKey().collect()
      assert(result.length == size / 2)
      result.foreach { case (i, seq) =>
        val actual = seq.toSet
        val expected = Set(i * 2, i * 2 + 1)
        assert(actual === expected, s"Value for $i was wrong: expected $expected, got $actual")
      }
    }

    assertSpilled(sc, "cogroup") {
      val rdd1 = sc.parallelize(0 until size).map { i => (i / 2, i) }
      val rdd2 = sc.parallelize(0 until size).map { i => (i / 2, i) }
      val result = rdd1.cogroup(rdd2).collect()
      assert(result.length === size / 2)
      result.foreach { case (i, (seq1, seq2)) =>
        val actual1 = seq1.toSet
        val actual2 = seq2.toSet
        val expected = Set(i * 2, i * 2 + 1)
        assert(actual1 === expected, s"Value 1 for $i was wrong: expected $expected, got $actual1")
        assert(actual2 === expected, s"Value 2 for $i was wrong: expected $expected, got $actual2")
      }
    }

    sc.stop()
  }

  test("ExternalAppendOnlyMap shouldn't fail when forced to spill before calling its iterator") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true)
    conf.set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, size / 2)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val map = createExternalMap[String]
    val consumer = createExternalMap[String]
    map.insertAll((1 to size).iterator.map(_.toString).map(i => (i, i)))
    assert(map.spill(10000, consumer) == 0L)
  }

  test("spilling with hash collisions") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true)
    conf.set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, size / 2)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val map = createExternalMap[String]

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

    map.insertAll((1 to size).iterator.map(_.toString).map(i => (i, i)))
    collisionPairs.foreach { case (w1, w2) =>
      map.insert(w1, w2)
      map.insert(w2, w1)
    }
    assert(map.numSpills > 0, "map did not spill")

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
    assert(count === size + collisionPairs.size * 2)
    sc.stop()
  }

  test("spilling with many hash collisions") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true)
    conf.set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, size / 2)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val map =
      new ExternalAppendOnlyMap[FixedHashObject, Int, Int](_ => 1, _ + _, _ + _, context = context)

    // Insert 10 copies each of lots of objects whose hash codes are either 0 or 1. This causes
    // problems if the map fails to group together the objects with the same code (SPARK-2043).
    for (i <- 1 to 10) {
      for (j <- 1 to size) {
        map.insert(FixedHashObject(j, j % 2), 1)
      }
    }
    assert(map.numSpills > 0, "map did not spill")

    val it = map.iterator
    var count = 0
    while (it.hasNext) {
      val kv = it.next()
      assert(kv._2 === 10)
      count += 1
    }
    assert(count === size)
    sc.stop()
  }

  test("spilling with hash collisions using the Int.MaxValue key") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true)
    conf.set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, size / 2)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val map = createExternalMap[Int]

    (1 to size).foreach { i => map.insert(i, i) }
    map.insert(Int.MaxValue, Int.MaxValue)
    assert(map.numSpills > 0, "map did not spill")

    val it = map.iterator
    while (it.hasNext) {
      // Should not throw NoSuchElementException
      it.next()
    }
    sc.stop()
  }

  test("spilling with null keys and values") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true)
    conf.set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, size / 2)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val map = createExternalMap[Int]

    map.insertAll((1 to size).iterator.map(i => (i, i)))
    map.insert(null.asInstanceOf[Int], 1)
    map.insert(1, null.asInstanceOf[Int])
    map.insert(null.asInstanceOf[Int], null.asInstanceOf[Int])
    assert(map.numSpills > 0, "map did not spill")

    val it = map.iterator
    while (it.hasNext) {
      // Should not throw NullPointerException
      it.next()
    }
    sc.stop()
  }

  test("SPARK-22713 spill during iteration leaks internal map") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val map = createExternalMap[Int]

    map.insertAll((0 until size).iterator.map(i => (i / 10, i)))
    assert(map.numSpills == 0, "map was not supposed to spill")

    val it = map.iterator
    assert(it.isInstanceOf[CompletionIterator[_, _]])
    // org.apache.spark.util.collection.AppendOnlyMap.destructiveSortedIterator returns
    // an instance of an anonymous Iterator class.

    val underlyingMapRef = WeakReference(map.currentMap)

    {
      // direct asserts introduced some macro generated code that held a reference to the map
      val tmpIsNull = null == underlyingMapRef.get.orNull
      assert(!tmpIsNull)
    }

    val first50Keys = for ( _ <- 0 until 50) yield {
      val (k, vs) = it.next()
      val sortedVs = vs.sorted
      assert(sortedVs == (0 until 10).map(10 * k + _))
      k
    }
    assert(map.numSpills == 0)
    map.spill(Long.MaxValue, null)
    // these asserts try to show that we're no longer holding references to the underlying map.
    // it'd be nice to use something like
    // https://github.com/scala/scala/blob/2.13.x/test/junit/scala/tools/testing/AssertUtil.scala
    // (lines 69-89)
    // assert(map.currentMap == null)
    eventually(timeout(5.seconds), interval(200.milliseconds)) {
      System.gc()
      // direct asserts introduced some macro generated code that held a reference to the map
      val tmpIsNull = null == underlyingMapRef.get.orNull
      assert(tmpIsNull)
    }


    val next50Keys = for ( _ <- 0 until 50) yield {
      val (k, vs) = it.next()
      val sortedVs = vs.sorted
      assert(sortedVs == (0 until 10).map(10 * k + _))
      k
    }
    assert(!it.hasNext)
    val keys = (first50Keys ++ next50Keys).sorted
    assert(keys == (0 until 100))
  }

  test("drop all references to the underlying map once the iterator is exhausted") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val map = createExternalMap[Int]

    map.insertAll((0 until size).iterator.map(i => (i / 10, i)))
    assert(map.numSpills == 0, "map was not supposed to spill")

    val underlyingMapRef = WeakReference(map.currentMap)

    {
      // direct asserts introduced some macro generated code that held a reference to the map
      val tmpIsNull = null == underlyingMapRef.get.orNull
      assert(!tmpIsNull)
    }

    val it = map.iterator
    assert( it.isInstanceOf[CompletionIterator[_, _]])


    val keys = it.map{
      case (k, vs) =>
        val sortedVs = vs.sorted
        assert(sortedVs == (0 until 10).map(10 * k + _))
        k
    }
    .toList
    .sorted

    assert(it.isEmpty)
    assert(keys == (0 until 100).toList)

    assert(map.numSpills == 0)
    // these asserts try to show that we're no longer holding references to the underlying map.
    // it'd be nice to use something like
    // https://github.com/scala/scala/blob/2.13.x/test/junit/scala/tools/testing/AssertUtil.scala
    // (lines 69-89)
    assert(map.currentMap == null)

    eventually {
      Thread.sleep(500)
      System.gc()
      // direct asserts introduced some macro generated code that held a reference to the map
      val tmpIsNull = null == underlyingMapRef.get.orNull
      assert(tmpIsNull)
    }

    assert(it.toList.isEmpty)
  }

  test("SPARK-22713 external aggregation updates peak execution memory") {
    val spillThreshold = 1000
    val conf = createSparkConf(loadDefaults = false)
      .set(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD, spillThreshold)
    sc = new SparkContext("local", "test", conf)
    // No spilling
    AccumulatorSuite.verifyPeakExecutionMemorySet(sc, "external map without spilling") {
      assertNotSpilled(sc, "verify peak memory") {
        sc.parallelize(1 to spillThreshold / 2, 2).map { i => (i, i) }.reduceByKey(_ + _).count()
      }
    }
    // With spilling
    AccumulatorSuite.verifyPeakExecutionMemorySet(sc, "external map with spilling") {
      assertSpilled(sc, "verify peak memory") {
        sc.parallelize(1 to spillThreshold * 3, 2).map { i => (i, i) }.reduceByKey(_ + _).count()
      }
    }
  }

  test("force to spill for external aggregation") {
    val conf = createSparkConf(loadDefaults = false)
      .set(MEMORY_STORAGE_FRACTION, 0.999)
      .set(TEST_MEMORY, 471859200L)
      .set(SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD, 0)
    sc = new SparkContext("local", "test", conf)
    val N = 200000
    sc.parallelize(1 to N, 2)
      .map { i => (i, i) }
      .groupByKey()
      .reduceByKey(_ ++ _)
      .count()
  }

}
