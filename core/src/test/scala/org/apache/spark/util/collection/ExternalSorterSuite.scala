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

import java.util.Comparator

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparators, RecordPointerAndKeyPrefix, UnsafeSortDataFormat}


class ExternalSorterSuite extends SparkFunSuite with LocalSparkContext {
  import TestUtils.{assertNotSpilled, assertSpilled}

  testWithMultipleSer("empty data stream")(emptyDataStream)

  testWithMultipleSer("few elements per partition")(fewElementsPerPartition)

  testWithMultipleSer("empty partitions with spilling")(emptyPartitionsWithSpilling)

  // Load defaults, otherwise SPARK_HOME is not found
  testWithMultipleSer("spilling in local cluster", loadDefaults = true) {
    (conf: SparkConf) => testSpillingInLocalCluster(conf, 2)
  }

  testWithMultipleSer("spilling in local cluster with many reduce tasks", loadDefaults = true) {
    (conf: SparkConf) => testSpillingInLocalCluster(conf, 100)
  }

  test("cleanup of intermediate files in sorter") {
    cleanupIntermediateFilesInSorter(withFailures = false)
  }

  test("cleanup of intermediate files in sorter with failures") {
    cleanupIntermediateFilesInSorter(withFailures = true)
  }

  test("cleanup of intermediate files in shuffle") {
    cleanupIntermediateFilesInShuffle(withFailures = false)
  }

  test("cleanup of intermediate files in shuffle with failures") {
    cleanupIntermediateFilesInShuffle(withFailures = true)
  }

  testWithMultipleSer("no sorting or partial aggregation") { (conf: SparkConf) =>
    basicSorterTest(conf, withPartialAgg = false, withOrdering = false, withSpilling = false)
  }

  testWithMultipleSer("no sorting or partial aggregation with spilling") { (conf: SparkConf) =>
    basicSorterTest(conf, withPartialAgg = false, withOrdering = false, withSpilling = true)
  }

  testWithMultipleSer("sorting, no partial aggregation") { (conf: SparkConf) =>
    basicSorterTest(conf, withPartialAgg = false, withOrdering = true, withSpilling = false)
  }

  testWithMultipleSer("sorting, no partial aggregation with spilling") { (conf: SparkConf) =>
    basicSorterTest(conf, withPartialAgg = false, withOrdering = true, withSpilling = true)
  }

  testWithMultipleSer("partial aggregation, no sorting") { (conf: SparkConf) =>
    basicSorterTest(conf, withPartialAgg = true, withOrdering = false, withSpilling = false)
  }

  testWithMultipleSer("partial aggregation, no sorting with spilling") { (conf: SparkConf) =>
    basicSorterTest(conf, withPartialAgg = true, withOrdering = false, withSpilling = true)
  }

  testWithMultipleSer("partial aggregation and sorting") { (conf: SparkConf) =>
    basicSorterTest(conf, withPartialAgg = true, withOrdering = true, withSpilling = false)
  }

  testWithMultipleSer("partial aggregation and sorting with spilling") { (conf: SparkConf) =>
    basicSorterTest(conf, withPartialAgg = true, withOrdering = true, withSpilling = true)
  }

  testWithMultipleSer("sort without breaking sorting contracts", loadDefaults = true)(
    sortWithoutBreakingSortingContracts)

  // This test is ignored by default as it requires a fairly large heap size (16GB)
  ignore("sort without breaking timsort contracts for large arrays") {
    val size = 300000000
    // To manifest the bug observed in SPARK-8428 and SPARK-13850, we explicitly use an array of
    // the form [150000000, 150000001, 150000002, ...., 300000000, 0, 1, 2, ..., 149999999]
    // that can trigger copyRange() in TimSort.mergeLo() or TimSort.mergeHi()
    val ref = Array.tabulate[Long](size) { i => if (i < size / 2) size / 2 + i else i }
    val buf = new LongArray(MemoryBlock.fromLongArray(ref))

    new Sorter(UnsafeSortDataFormat.INSTANCE).sort(
      buf, 0, size, new Comparator[RecordPointerAndKeyPrefix] {
        override def compare(
            r1: RecordPointerAndKeyPrefix,
            r2: RecordPointerAndKeyPrefix): Int = {
          PrefixComparators.LONG.compare(r1.keyPrefix, r2.keyPrefix)
        }
      })
  }

  test("spilling with hash collisions") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true, kryo = false)
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 2).toString)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)

    def createCombiner(i: String): ArrayBuffer[String] = ArrayBuffer[String](i)
    def mergeValue(buffer: ArrayBuffer[String], i: String): ArrayBuffer[String] = buffer += i
    def mergeCombiners(
        buffer1: ArrayBuffer[String],
        buffer2: ArrayBuffer[String]): ArrayBuffer[String] = buffer1 ++= buffer2

    val agg = new Aggregator[String, String, ArrayBuffer[String]](
      createCombiner _, mergeValue _, mergeCombiners _)

    val sorter = new ExternalSorter[String, String, ArrayBuffer[String]](
      context, Some(agg), None, None, None)

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

    val toInsert = (1 to size).iterator.map(_.toString).map(s => (s, s)) ++
      collisionPairs.iterator ++ collisionPairs.iterator.map(_.swap)

    sorter.insertAll(toInsert)
    assert(sorter.numSpills > 0, "sorter did not spill")

    // A map of collision pairs in both directions
    val collisionPairsMap = (collisionPairs ++ collisionPairs.map(_.swap)).toMap

    // Avoid map.size or map.iterator.length because this destructively sorts the underlying map
    var count = 0

    val it = sorter.iterator
    while (it.hasNext) {
      val kv = it.next()
      val expectedValue = ArrayBuffer[String](collisionPairsMap.getOrElse(kv._1, kv._1))
      assert(kv._2.equals(expectedValue))
      count += 1
    }
    assert(count === size + collisionPairs.size * 2)
  }

  test("spilling with many hash collisions") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true, kryo = false)
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 2).toString)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val agg = new Aggregator[FixedHashObject, Int, Int](_ => 1, _ + _, _ + _)
    val sorter = new ExternalSorter[FixedHashObject, Int, Int](context, Some(agg), None, None, None)
    // Insert 10 copies each of lots of objects whose hash codes are either 0 or 1. This causes
    // problems if the map fails to group together the objects with the same code (SPARK-2043).
    val toInsert = for (i <- 1 to 10; j <- 1 to size) yield (FixedHashObject(j, j % 2), 1)
    sorter.insertAll(toInsert.iterator)
    assert(sorter.numSpills > 0, "sorter did not spill")
    val it = sorter.iterator
    var count = 0
    while (it.hasNext) {
      val kv = it.next()
      assert(kv._2 === 10)
      count += 1
    }
    assert(count === size)
  }

  test("spilling with hash collisions using the Int.MaxValue key") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true, kryo = false)
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 2).toString)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)

    def createCombiner(i: Int): ArrayBuffer[Int] = ArrayBuffer[Int](i)
    def mergeValue(buffer: ArrayBuffer[Int], i: Int): ArrayBuffer[Int] = buffer += i
    def mergeCombiners(buf1: ArrayBuffer[Int], buf2: ArrayBuffer[Int]): ArrayBuffer[Int] = {
      buf1 ++= buf2
    }

    val agg = new Aggregator[Int, Int, ArrayBuffer[Int]](createCombiner, mergeValue, mergeCombiners)
    val sorter =
      new ExternalSorter[Int, Int, ArrayBuffer[Int]](context, Some(agg), None, None, None)
    sorter.insertAll(
      (1 to size).iterator.map(i => (i, i)) ++ Iterator((Int.MaxValue, Int.MaxValue)))
    assert(sorter.numSpills > 0, "sorter did not spill")
    val it = sorter.iterator
    while (it.hasNext) {
      // Should not throw NoSuchElementException
      it.next()
    }
  }

  test("spilling with null keys and values") {
    val size = 1000
    val conf = createSparkConf(loadDefaults = true, kryo = false)
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 2).toString)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)

    def createCombiner(i: String): ArrayBuffer[String] = ArrayBuffer[String](i)
    def mergeValue(buffer: ArrayBuffer[String], i: String): ArrayBuffer[String] = buffer += i
    def mergeCombiners(buf1: ArrayBuffer[String], buf2: ArrayBuffer[String]): ArrayBuffer[String] =
      buf1 ++= buf2

    val agg = new Aggregator[String, String, ArrayBuffer[String]](
      createCombiner, mergeValue, mergeCombiners)

    val sorter = new ExternalSorter[String, String, ArrayBuffer[String]](
      context, Some(agg), None, None, None)

    sorter.insertAll((1 to size).iterator.map(i => (i.toString, i.toString)) ++ Iterator(
      (null.asInstanceOf[String], "1"),
      ("1", null.asInstanceOf[String]),
      (null.asInstanceOf[String], null.asInstanceOf[String])
    ))
    assert(sorter.numSpills > 0, "sorter did not spill")
    val it = sorter.iterator
    while (it.hasNext) {
      // Should not throw NullPointerException
      it.next()
    }
  }

  /* ============================= *
   |  Helper test utility methods  |
   * ============================= */

  private def createSparkConf(loadDefaults: Boolean, kryo: Boolean): SparkConf = {
    val conf = new SparkConf(loadDefaults)
    if (kryo) {
      conf.set("spark.serializer", classOf[KryoSerializer].getName)
    } else {
      // Make the Java serializer write a reset instruction (TC_RESET) after each object to test
      // for a bug we had with bytes written past the last object in a batch (SPARK-2792)
      conf.set("spark.serializer.objectStreamReset", "1")
      conf.set("spark.serializer", classOf[JavaSerializer].getName)
    }
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "0")
    // Ensure that we actually have multiple batches per spill file
    conf.set("spark.shuffle.spill.batchSize", "10")
    conf.set("spark.shuffle.spill.initialMemoryThreshold", "512")
    conf
  }

  /**
   * Run a test multiple times, each time with a different serializer.
   */
  private def testWithMultipleSer(
      name: String,
      loadDefaults: Boolean = false)(body: (SparkConf => Unit)): Unit = {
    test(name + " with kryo ser") {
      body(createSparkConf(loadDefaults, kryo = true))
    }
    test(name + " with java ser") {
      body(createSparkConf(loadDefaults, kryo = false))
    }
  }

  /* =========================================== *
   |  Helper methods that contain the test body  |
   * =========================================== */

  private def emptyDataStream(conf: SparkConf) {
    conf.set("spark.shuffle.manager", "sort")
    sc = new SparkContext("local", "test", conf)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)

    val agg = new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
    val ord = implicitly[Ordering[Int]]

    // Both aggregator and ordering
    val sorter = new ExternalSorter[Int, Int, Int](
      context, Some(agg), Some(new HashPartitioner(3)), Some(ord), None)
    assert(sorter.iterator.toSeq === Seq())
    sorter.stop()

    // Only aggregator
    val sorter2 = new ExternalSorter[Int, Int, Int](
      context, Some(agg), Some(new HashPartitioner(3)), None, None)
    assert(sorter2.iterator.toSeq === Seq())
    sorter2.stop()

    // Only ordering
    val sorter3 = new ExternalSorter[Int, Int, Int](
      context, None, Some(new HashPartitioner(3)), Some(ord), None)
    assert(sorter3.iterator.toSeq === Seq())
    sorter3.stop()

    // Neither aggregator nor ordering
    val sorter4 = new ExternalSorter[Int, Int, Int](
      context, None, Some(new HashPartitioner(3)), None, None)
    assert(sorter4.iterator.toSeq === Seq())
    sorter4.stop()
  }

  private def fewElementsPerPartition(conf: SparkConf) {
    conf.set("spark.shuffle.manager", "sort")
    sc = new SparkContext("local", "test", conf)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)

    val agg = new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
    val ord = implicitly[Ordering[Int]]
    val elements = Set((1, 1), (2, 2), (5, 5))
    val expected = Set(
      (0, Set()), (1, Set((1, 1))), (2, Set((2, 2))), (3, Set()), (4, Set()),
      (5, Set((5, 5))), (6, Set()))

    // Both aggregator and ordering
    val sorter = new ExternalSorter[Int, Int, Int](
      context, Some(agg), Some(new HashPartitioner(7)), Some(ord), None)
    sorter.insertAll(elements.iterator)
    assert(sorter.partitionedIterator.map(p => (p._1, p._2.toSet)).toSet === expected)
    sorter.stop()

    // Only aggregator
    val sorter2 = new ExternalSorter[Int, Int, Int](
      context, Some(agg), Some(new HashPartitioner(7)), None, None)
    sorter2.insertAll(elements.iterator)
    assert(sorter2.partitionedIterator.map(p => (p._1, p._2.toSet)).toSet === expected)
    sorter2.stop()

    // Only ordering
    val sorter3 = new ExternalSorter[Int, Int, Int](
      context, None, Some(new HashPartitioner(7)), Some(ord), None)
    sorter3.insertAll(elements.iterator)
    assert(sorter3.partitionedIterator.map(p => (p._1, p._2.toSet)).toSet === expected)
    sorter3.stop()

    // Neither aggregator nor ordering
    val sorter4 = new ExternalSorter[Int, Int, Int](
      context, None, Some(new HashPartitioner(7)), None, None)
    sorter4.insertAll(elements.iterator)
    assert(sorter4.partitionedIterator.map(p => (p._1, p._2.toSet)).toSet === expected)
    sorter4.stop()
  }

  private def emptyPartitionsWithSpilling(conf: SparkConf) {
    val size = 1000
    conf.set("spark.shuffle.manager", "sort")
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 2).toString)
    sc = new SparkContext("local", "test", conf)
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)

    val ord = implicitly[Ordering[Int]]
    val elements = Iterator((1, 1), (5, 5)) ++ (0 until size).iterator.map(x => (2, 2))

    val sorter = new ExternalSorter[Int, Int, Int](
      context, None, Some(new HashPartitioner(7)), Some(ord), None)
    sorter.insertAll(elements)
    assert(sorter.numSpills > 0, "sorter did not spill")
    val iter = sorter.partitionedIterator.map(p => (p._1, p._2.toList))
    assert(iter.next() === (0, Nil))
    assert(iter.next() === (1, List((1, 1))))
    assert(iter.next() === (2, (0 until 1000).map(x => (2, 2)).toList))
    assert(iter.next() === (3, Nil))
    assert(iter.next() === (4, Nil))
    assert(iter.next() === (5, List((5, 5))))
    assert(iter.next() === (6, Nil))
    sorter.stop()
  }

  private def testSpillingInLocalCluster(conf: SparkConf, numReduceTasks: Int) {
    val size = 5000
    conf.set("spark.shuffle.manager", "sort")
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 4).toString)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)

    assertSpilled(sc, "reduceByKey") {
      val result = sc.parallelize(0 until size)
        .map { i => (i / 2, i) }
        .reduceByKey(math.max _, numReduceTasks)
        .collect()
      assert(result.length === size / 2)
      result.foreach { case (k, v) =>
        val expected = k * 2 + 1
        assert(v === expected, s"Value for $k was wrong: expected $expected, got $v")
      }
    }

    assertSpilled(sc, "groupByKey") {
      val result = sc.parallelize(0 until size)
        .map { i => (i / 2, i) }
        .groupByKey(numReduceTasks)
        .collect()
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
      val result = rdd1.cogroup(rdd2, numReduceTasks).collect()
      assert(result.length === size / 2)
      result.foreach { case (i, (seq1, seq2)) =>
        val actual1 = seq1.toSet
        val actual2 = seq2.toSet
        val expected = Set(i * 2, i * 2 + 1)
        assert(actual1 === expected, s"Value 1 for $i was wrong: expected $expected, got $actual1")
        assert(actual2 === expected, s"Value 2 for $i was wrong: expected $expected, got $actual2")
      }
    }

    assertSpilled(sc, "sortByKey") {
      val result = sc.parallelize(0 until size)
        .map { i => (i / 2, i) }
        .sortByKey(numPartitions = numReduceTasks)
        .collect()
      val expected = (0 until size).map { i => (i / 2, i) }.toArray
      assert(result.length === size)
      result.zipWithIndex.foreach { case ((k, _), i) =>
        val (expectedKey, _) = expected(i)
        assert(k === expectedKey, s"Value for $i was wrong: expected $expectedKey, got $k")
      }
    }
  }

  private def cleanupIntermediateFilesInSorter(withFailures: Boolean): Unit = {
    val size = 1200
    val conf = createSparkConf(loadDefaults = false, kryo = false)
    conf.set("spark.shuffle.manager", "sort")
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 4).toString)
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = sc.env.blockManager.diskBlockManager
    val ord = implicitly[Ordering[Int]]
    val expectedSize = if (withFailures) size - 1 else size
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val sorter = new ExternalSorter[Int, Int, Int](
      context, None, Some(new HashPartitioner(3)), Some(ord), None)
    if (withFailures) {
      intercept[SparkException] {
        sorter.insertAll((0 until size).iterator.map { i =>
          if (i == size - 1) { throw new SparkException("intentional failure") }
          (i, i)
        })
      }
    } else {
      sorter.insertAll((0 until size).iterator.map(i => (i, i)))
    }
    assert(sorter.iterator.toSet === (0 until expectedSize).map(i => (i, i)).toSet)
    assert(sorter.numSpills > 0, "sorter did not spill")
    assert(diskBlockManager.getAllFiles().nonEmpty, "sorter did not spill")
    sorter.stop()
    assert(diskBlockManager.getAllFiles().isEmpty, "spilled files were not cleaned up")
  }

  private def cleanupIntermediateFilesInShuffle(withFailures: Boolean): Unit = {
    val size = 1200
    val conf = createSparkConf(loadDefaults = false, kryo = false)
    conf.set("spark.shuffle.manager", "sort")
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 4).toString)
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = sc.env.blockManager.diskBlockManager
    val data = sc.parallelize(0 until size, 2).map { i =>
      if (withFailures && i == size - 1) {
        throw new SparkException("intentional failure")
      }
      (i, i)
    }

    assertSpilled(sc, "test shuffle cleanup") {
      if (withFailures) {
        intercept[SparkException] {
          data.reduceByKey(_ + _).count()
        }
        // After the shuffle, there should be only 2 files on disk: the output of task 1 and
        // its index. All other files (map 2's output and intermediate merge files) should
        // have been deleted.
        assert(diskBlockManager.getAllFiles().length === 2)
      } else {
        assert(data.reduceByKey(_ + _).count() === size)
        // After the shuffle, there should be only 4 files on disk: the output of both tasks
        // and their indices. All intermediate merge files should have been deleted.
        assert(diskBlockManager.getAllFiles().length === 4)
      }
    }
  }

  private def basicSorterTest(
      conf: SparkConf,
      withPartialAgg: Boolean,
      withOrdering: Boolean,
      withSpilling: Boolean) {
    val size = 1000
    if (withSpilling) {
      conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 2).toString)
    }
    conf.set("spark.shuffle.manager", "sort")
    sc = new SparkContext("local", "test", conf)
    val agg =
      if (withPartialAgg) {
        Some(new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j))
      } else {
        None
      }
    val ord = if (withOrdering) Some(implicitly[Ordering[Int]]) else None
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val sorter =
      new ExternalSorter[Int, Int, Int](context, agg, Some(new HashPartitioner(3)), ord, None)
    sorter.insertAll((0 until size).iterator.map { i => (i / 4, i) })
    if (withSpilling) {
      assert(sorter.numSpills > 0, "sorter did not spill")
    } else {
      assert(sorter.numSpills === 0, "sorter spilled")
    }
    val results = sorter.partitionedIterator.map { case (p, vs) => (p, vs.toSet) }.toSet
    val expected = (0 until 3).map { p =>
      var v = (0 until size).map { i => (i / 4, i) }.filter { case (k, _) => k % 3 == p }.toSet
      if (withPartialAgg) {
        v = v.groupBy(_._1).mapValues { s => s.map(_._2).sum }.toSet
      }
      (p, v.toSet)
    }.toSet
    assert(results === expected)
  }

  private def sortWithoutBreakingSortingContracts(conf: SparkConf) {
    val size = 100000
    val conf = createSparkConf(loadDefaults = true, kryo = false)
    conf.set("spark.shuffle.manager", "sort")
    conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 2).toString)
    sc = new SparkContext("local-cluster[1,1,1024]", "test", conf)

    // Using wrongOrdering to show integer overflow introduced exception.
    val rand = new Random(100L)
    val wrongOrdering = new Ordering[String] {
      override def compare(a: String, b: String): Int = {
        val h1 = if (a == null) 0 else a.hashCode()
        val h2 = if (b == null) 0 else b.hashCode()
        h1 - h2
      }
    }

    val testData = Array.tabulate(size) { _ => rand.nextInt().toString }

    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val sorter1 = new ExternalSorter[String, String, String](
      context, None, None, Some(wrongOrdering), None)
    val thrown = intercept[IllegalArgumentException] {
      sorter1.insertAll(testData.iterator.map(i => (i, i)))
      assert(sorter1.numSpills > 0, "sorter did not spill")
      sorter1.iterator
    }

    assert(thrown.getClass === classOf[IllegalArgumentException])
    assert(thrown.getMessage.contains("Comparison method violates its general contract"))
    sorter1.stop()

    // Using aggregation and external spill to make sure ExternalSorter using
    // partitionKeyComparator.
    def createCombiner(i: String): ArrayBuffer[String] = ArrayBuffer(i)
    def mergeValue(c: ArrayBuffer[String], i: String): ArrayBuffer[String] = c += i
    def mergeCombiners(c1: ArrayBuffer[String], c2: ArrayBuffer[String]): ArrayBuffer[String] =
      c1 ++= c2

    val agg = new Aggregator[String, String, ArrayBuffer[String]](
      createCombiner, mergeValue, mergeCombiners)

    val sorter2 = new ExternalSorter[String, String, ArrayBuffer[String]](
      context, Some(agg), None, None, None)
    sorter2.insertAll(testData.iterator.map(i => (i, i)))
    assert(sorter2.numSpills > 0, "sorter did not spill")

    // To validate the hash ordering of key
    var minKey = Int.MinValue
    sorter2.iterator.foreach { case (k, v) =>
      val h = k.hashCode()
      assert(h >= minKey)
      minKey = h
    }

    sorter2.stop()
  }

  test("sorting updates peak execution memory") {
    val spillThreshold = 1000
    val conf = createSparkConf(loadDefaults = false, kryo = false)
      .set("spark.shuffle.manager", "sort")
      .set("spark.shuffle.spill.numElementsForceSpillThreshold", spillThreshold.toString)
    sc = new SparkContext("local", "test", conf)
    // Avoid aggregating here to make sure we're not also using ExternalAppendOnlyMap
    // No spilling
    AccumulatorSuite.verifyPeakExecutionMemorySet(sc, "external sorter without spilling") {
      assertNotSpilled(sc, "verify peak memory") {
        sc.parallelize(1 to spillThreshold / 2, 2).repartition(100).count()
      }
    }
    // With spilling
    AccumulatorSuite.verifyPeakExecutionMemorySet(sc, "external sorter with spilling") {
      assertSpilled(sc, "verify peak memory") {
        sc.parallelize(1 to spillThreshold * 3, 2).repartition(100).count()
      }
    }
  }
}
