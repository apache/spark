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

import org.scalatest.PrivateMethodTester

import scala.util.Random

import org.apache.spark._
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}

class ExternalSorterSuite extends SparkFunSuite with LocalSparkContext with PrivateMethodTester {
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
    // Ensure that we actually have multiple batches per spill file
    conf.set("spark.shuffle.spill.batchSize", "10")
    conf
  }

  private def assertBypassedMergeSort(sorter: ExternalSorter[_, _, _]): Unit = {
    val bypassMergeSort = PrivateMethod[Boolean]('bypassMergeSort)
    assert(sorter.invokePrivate(bypassMergeSort()), "sorter did not bypass merge-sort")
  }

  private def assertDidNotBypassMergeSort(sorter: ExternalSorter[_, _, _]): Unit = {
    val bypassMergeSort = PrivateMethod[Boolean]('bypassMergeSort)
    assert(!sorter.invokePrivate(bypassMergeSort()), "sorter bypassed merge-sort")
  }

  test("empty data stream with kryo ser") {
    emptyDataStream(createSparkConf(false, true))
  }

  test("empty data stream with java ser") {
    emptyDataStream(createSparkConf(false, false))
  }

  def emptyDataStream(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val agg = new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
    val ord = implicitly[Ordering[Int]]

    // Both aggregator and ordering
    val sorter = new ExternalSorter[Int, Int, Int](
      Some(agg), Some(new HashPartitioner(3)), Some(ord), None)
    assert(sorter.iterator.toSeq === Seq())
    sorter.stop()

    // Only aggregator
    val sorter2 = new ExternalSorter[Int, Int, Int](
      Some(agg), Some(new HashPartitioner(3)), None, None)
    assert(sorter2.iterator.toSeq === Seq())
    sorter2.stop()

    // Only ordering
    val sorter3 = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(3)), Some(ord), None)
    assert(sorter3.iterator.toSeq === Seq())
    sorter3.stop()

    // Neither aggregator nor ordering
    val sorter4 = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(3)), None, None)
    assert(sorter4.iterator.toSeq === Seq())
    sorter4.stop()
  }

  test("few elements per partition with kryo ser") {
    fewElementsPerPartition(createSparkConf(false, true))
  }

  test("few elements per partition with java ser") {
    fewElementsPerPartition(createSparkConf(false, false))
  }

  def fewElementsPerPartition(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val agg = new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
    val ord = implicitly[Ordering[Int]]
    val elements = Set((1, 1), (2, 2), (5, 5))
    val expected = Set(
      (0, Set()), (1, Set((1, 1))), (2, Set((2, 2))), (3, Set()), (4, Set()),
      (5, Set((5, 5))), (6, Set()))

    // Both aggregator and ordering
    val sorter = new ExternalSorter[Int, Int, Int](
      Some(agg), Some(new HashPartitioner(7)), Some(ord), None)
    sorter.insertAll(elements.iterator)
    assert(sorter.partitionedIterator.map(p => (p._1, p._2.toSet)).toSet === expected)
    sorter.stop()

    // Only aggregator
    val sorter2 = new ExternalSorter[Int, Int, Int](
      Some(agg), Some(new HashPartitioner(7)), None, None)
    sorter2.insertAll(elements.iterator)
    assert(sorter2.partitionedIterator.map(p => (p._1, p._2.toSet)).toSet === expected)
    sorter2.stop()

    // Only ordering
    val sorter3 = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(7)), Some(ord), None)
    sorter3.insertAll(elements.iterator)
    assert(sorter3.partitionedIterator.map(p => (p._1, p._2.toSet)).toSet === expected)
    sorter3.stop()

    // Neither aggregator nor ordering
    val sorter4 = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(7)), None, None)
    sorter4.insertAll(elements.iterator)
    assert(sorter4.partitionedIterator.map(p => (p._1, p._2.toSet)).toSet === expected)
    sorter4.stop()
  }

  test("empty partitions with spilling with kryo ser") {
    emptyPartitionsWithSpilling(createSparkConf(false, true))
  }

  test("empty partitions with spilling with java ser") {
    emptyPartitionsWithSpilling(createSparkConf(false, false))
  }

  def emptyPartitionsWithSpilling(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.spill.initialMemoryThreshold", "512")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val ord = implicitly[Ordering[Int]]
    val elements = Iterator((1, 1), (5, 5)) ++ (0 until 100000).iterator.map(x => (2, 2))

    val sorter = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(7)), Some(ord), None)
    assertDidNotBypassMergeSort(sorter)
    sorter.insertAll(elements)
    assert(sc.env.blockManager.diskBlockManager.getAllFiles().length > 0) // Make sure it spilled
    val iter = sorter.partitionedIterator.map(p => (p._1, p._2.toList))
    assert(iter.next() === (0, Nil))
    assert(iter.next() === (1, List((1, 1))))
    assert(iter.next() === (2, (0 until 100000).map(x => (2, 2)).toList))
    assert(iter.next() === (3, Nil))
    assert(iter.next() === (4, Nil))
    assert(iter.next() === (5, List((5, 5))))
    assert(iter.next() === (6, Nil))
    sorter.stop()
  }

  test("empty partitions with spilling, bypass merge-sort with kryo ser") {
    emptyPartitionerWithSpillingBypassMergeSort(createSparkConf(false, true))
  }

  test("empty partitions with spilling, bypass merge-sort with java ser") {
    emptyPartitionerWithSpillingBypassMergeSort(createSparkConf(false, false))
  }

  def emptyPartitionerWithSpillingBypassMergeSort(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.spill.initialMemoryThreshold", "512")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val elements = Iterator((1, 1), (5, 5)) ++ (0 until 100000).iterator.map(x => (2, 2))

    val sorter = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(7)), None, None)
    assertBypassedMergeSort(sorter)
    sorter.insertAll(elements)
    assert(sc.env.blockManager.diskBlockManager.getAllFiles().length > 0) // Make sure it spilled
    val iter = sorter.partitionedIterator.map(p => (p._1, p._2.toList))
    assert(iter.next() === (0, Nil))
    assert(iter.next() === (1, List((1, 1))))
    assert(iter.next() === (2, (0 until 100000).map(x => (2, 2)).toList))
    assert(iter.next() === (3, Nil))
    assert(iter.next() === (4, Nil))
    assert(iter.next() === (5, List((5, 5))))
    assert(iter.next() === (6, Nil))
    sorter.stop()
  }

  test("spilling in local cluster with kryo ser") {
    // Load defaults, otherwise SPARK_HOME is not found
    testSpillingInLocalCluster(createSparkConf(true, true))
  }

  test("spilling in local cluster with java ser") {
    // Load defaults, otherwise SPARK_HOME is not found
    testSpillingInLocalCluster(createSparkConf(true, false))
  }

  def testSpillingInLocalCluster(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
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

    // larger cogroup - should spill ~7 times
    val rddD1 = sc.parallelize(0 until 10000).map(i => (i/2, i))
    val rddD2 = sc.parallelize(0 until 10000).map(i => (i/2, i))
    val resultD = rddD1.cogroup(rddD2).collect()
    assert(resultD.length == 5000)
    resultD.foreach { case(i, (seq1, seq2)) =>
      val expected = Set(i * 2, i * 2 + 1)
      if (seq1.toSet != expected) {
        fail(s"Value 1 for ${i} was wrong: expected ${expected}, got ${seq1.toSet}")
      }
      if (seq2.toSet != expected) {
        fail(s"Value 2 for ${i} was wrong: expected ${expected}, got ${seq2.toSet}")
      }
    }

    // sortByKey - should spill ~17 times
    val rddE = sc.parallelize(0 until 100000).map(i => (i/4, i))
    val resultE = rddE.sortByKey().collect().toSeq
    assert(resultE === (0 until 100000).map(i => (i/4, i)).toSeq)
  }

  test("spilling in local cluster with many reduce tasks with kryo ser") {
    spillingInLocalClusterWithManyReduceTasks(createSparkConf(true, true))
  }

  test("spilling in local cluster with many reduce tasks with java ser") {
    spillingInLocalClusterWithManyReduceTasks(createSparkConf(true, false))
  }

  def spillingInLocalClusterWithManyReduceTasks(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local-cluster[2,1,512]", "test", conf)

    // reduceByKey - should spill ~4 times per executor
    val rddA = sc.parallelize(0 until 100000).map(i => (i/2, i))
    val resultA = rddA.reduceByKey(math.max _, 100).collect()
    assert(resultA.length == 50000)
    resultA.foreach { case(k, v) =>
      if (v != k * 2 + 1) {
        fail(s"Value for ${k} was wrong: expected ${k * 2 + 1}, got ${v}")
      }
    }

    // groupByKey - should spill ~8 times per executor
    val rddB = sc.parallelize(0 until 100000).map(i => (i/4, i))
    val resultB = rddB.groupByKey(100).collect()
    assert(resultB.length == 25000)
    resultB.foreach { case(i, seq) =>
      val expected = Set(i * 4, i * 4 + 1, i * 4 + 2, i * 4 + 3)
      if (seq.toSet != expected) {
        fail(s"Value for ${i} was wrong: expected ${expected}, got ${seq.toSet}")
      }
    }

    // cogroup - should spill ~4 times per executor
    val rddC1 = sc.parallelize(0 until 10000).map(i => (i, i))
    val rddC2 = sc.parallelize(0 until 10000).map(i => (i%1000, i))
    val resultC = rddC1.cogroup(rddC2, 100).collect()
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

    // larger cogroup - should spill ~4 times per executor
    val rddD1 = sc.parallelize(0 until 10000).map(i => (i/2, i))
    val rddD2 = sc.parallelize(0 until 10000).map(i => (i/2, i))
    val resultD = rddD1.cogroup(rddD2).collect()
    assert(resultD.length == 5000)
    resultD.foreach { case(i, (seq1, seq2)) =>
      val expected = Set(i * 2, i * 2 + 1)
      if (seq1.toSet != expected) {
        fail(s"Value 1 for ${i} was wrong: expected ${expected}, got ${seq1.toSet}")
      }
      if (seq2.toSet != expected) {
        fail(s"Value 2 for ${i} was wrong: expected ${expected}, got ${seq2.toSet}")
      }
    }

    // sortByKey - should spill ~8 times per executor
    val rddE = sc.parallelize(0 until 100000).map(i => (i/4, i))
    val resultE = rddE.sortByKey().collect().toSeq
    assert(resultE === (0 until 100000).map(i => (i/4, i)).toSeq)
  }

  test("cleanup of intermediate files in sorter") {
    val conf = createSparkConf(true, false)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val ord = implicitly[Ordering[Int]]

    val sorter = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(3)), Some(ord), None)
    assertDidNotBypassMergeSort(sorter)
    sorter.insertAll((0 until 120000).iterator.map(i => (i, i)))
    assert(diskBlockManager.getAllFiles().length > 0)
    sorter.stop()
    assert(diskBlockManager.getAllBlocks().length === 0)

    val sorter2 = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(3)), Some(ord), None)
    assertDidNotBypassMergeSort(sorter2)
    sorter2.insertAll((0 until 120000).iterator.map(i => (i, i)))
    assert(diskBlockManager.getAllFiles().length > 0)
    assert(sorter2.iterator.toSet === (0 until 120000).map(i => (i, i)).toSet)
    sorter2.stop()
    assert(diskBlockManager.getAllBlocks().length === 0)
  }

  test("cleanup of intermediate files in sorter, bypass merge-sort") {
    val conf = createSparkConf(true, false)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val sorter = new ExternalSorter[Int, Int, Int](None, Some(new HashPartitioner(3)), None, None)
    assertBypassedMergeSort(sorter)
    sorter.insertAll((0 until 100000).iterator.map(i => (i, i)))
    assert(diskBlockManager.getAllFiles().length > 0)
    sorter.stop()
    assert(diskBlockManager.getAllBlocks().length === 0)

    val sorter2 = new ExternalSorter[Int, Int, Int](None, Some(new HashPartitioner(3)), None, None)
    assertBypassedMergeSort(sorter2)
    sorter2.insertAll((0 until 100000).iterator.map(i => (i, i)))
    assert(diskBlockManager.getAllFiles().length > 0)
    assert(sorter2.iterator.toSet === (0 until 100000).map(i => (i, i)).toSet)
    sorter2.stop()
    assert(diskBlockManager.getAllBlocks().length === 0)
  }

  test("cleanup of intermediate files in sorter if there are errors") {
    val conf = createSparkConf(true, false)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val ord = implicitly[Ordering[Int]]

    val sorter = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(3)), Some(ord), None)
    assertDidNotBypassMergeSort(sorter)
    intercept[SparkException] {
      sorter.insertAll((0 until 120000).iterator.map(i => {
        if (i == 119990) {
          throw new SparkException("Intentional failure")
        }
        (i, i)
      }))
    }
    assert(diskBlockManager.getAllFiles().length > 0)
    sorter.stop()
    assert(diskBlockManager.getAllBlocks().length === 0)
  }

  test("cleanup of intermediate files in sorter if there are errors, bypass merge-sort") {
    val conf = createSparkConf(true, false)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val sorter = new ExternalSorter[Int, Int, Int](None, Some(new HashPartitioner(3)), None, None)
    assertBypassedMergeSort(sorter)
    intercept[SparkException] {
      sorter.insertAll((0 until 100000).iterator.map(i => {
        if (i == 99990) {
          throw new SparkException("Intentional failure")
        }
        (i, i)
      }))
    }
    assert(diskBlockManager.getAllFiles().length > 0)
    sorter.stop()
    assert(diskBlockManager.getAllBlocks().length === 0)
  }

  test("cleanup of intermediate files in shuffle") {
    val conf = createSparkConf(false, false)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val data = sc.parallelize(0 until 100000, 2).map(i => (i, i))
    assert(data.reduceByKey(_ + _).count() === 100000)

    // After the shuffle, there should be only 4 files on disk: our two map output files and
    // their index files. All other intermediate files should've been deleted.
    assert(diskBlockManager.getAllFiles().length === 4)
  }

  test("cleanup of intermediate files in shuffle with errors") {
    val conf = createSparkConf(false, false)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val data = sc.parallelize(0 until 100000, 2).map(i => {
      if (i == 99990) {
        throw new Exception("Intentional failure")
      }
      (i, i)
    })
    intercept[SparkException] {
      data.reduceByKey(_ + _).count()
    }

    // After the shuffle, there should be only 2 files on disk: the output of task 1 and its index.
    // All other files (map 2's output and intermediate merge files) should've been deleted.
    assert(diskBlockManager.getAllFiles().length === 2)
  }

  test("no partial aggregation or sorting with kryo ser") {
    noPartialAggregationOrSorting(createSparkConf(false, true))
  }

  test("no partial aggregation or sorting with java ser") {
    noPartialAggregationOrSorting(createSparkConf(false, false))
  }

  def noPartialAggregationOrSorting(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val sorter = new ExternalSorter[Int, Int, Int](None, Some(new HashPartitioner(3)), None, None)
    sorter.insertAll((0 until 100000).iterator.map(i => (i / 4, i)))
    val results = sorter.partitionedIterator.map{case (p, vs) => (p, vs.toSet)}.toSet
    val expected = (0 until 3).map(p => {
      (p, (0 until 100000).map(i => (i / 4, i)).filter(_._1 % 3 == p).toSet)
    }).toSet
    assert(results === expected)
  }

  test("partial aggregation without spill with kryo ser") {
    partialAggregationWithoutSpill(createSparkConf(false, true))
  }

  test("partial aggregation without spill with java ser") {
    partialAggregationWithoutSpill(createSparkConf(false, false))
  }

  def partialAggregationWithoutSpill(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val agg = new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
    val sorter = new ExternalSorter(Some(agg), Some(new HashPartitioner(3)), None, None)
    sorter.insertAll((0 until 100).iterator.map(i => (i / 2, i)))
    val results = sorter.partitionedIterator.map{case (p, vs) => (p, vs.toSet)}.toSet
    val expected = (0 until 3).map(p => {
      (p, (0 until 50).map(i => (i, i * 4 + 1)).filter(_._1 % 3 == p).toSet)
    }).toSet
    assert(results === expected)
  }

  test("partial aggregation with spill, no ordering with kryo ser") {
    partialAggregationWIthSpillNoOrdering(createSparkConf(false, true))
  }

  test("partial aggregation with spill, no ordering with java ser") {
    partialAggregationWIthSpillNoOrdering(createSparkConf(false, false))
  }

  def partialAggregationWIthSpillNoOrdering(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val agg = new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
    val sorter = new ExternalSorter(Some(agg), Some(new HashPartitioner(3)), None, None)
    sorter.insertAll((0 until 100000).iterator.map(i => (i / 2, i)))
    val results = sorter.partitionedIterator.map{case (p, vs) => (p, vs.toSet)}.toSet
    val expected = (0 until 3).map(p => {
      (p, (0 until 50000).map(i => (i, i * 4 + 1)).filter(_._1 % 3 == p).toSet)
    }).toSet
    assert(results === expected)
  }

  test("partial aggregation with spill, with ordering with kryo ser") {
    partialAggregationWithSpillWithOrdering(createSparkConf(false, true))
  }


  test("partial aggregation with spill, with ordering with java ser") {
    partialAggregationWithSpillWithOrdering(createSparkConf(false, false))
  }

  def partialAggregationWithSpillWithOrdering(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val agg = new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
    val ord = implicitly[Ordering[Int]]
    val sorter = new ExternalSorter(Some(agg), Some(new HashPartitioner(3)), Some(ord), None)

    // avoid combine before spill
    sorter.insertAll((0 until 50000).iterator.map(i => (i , 2 * i)))
    sorter.insertAll((0 until 50000).iterator.map(i => (i, 2 * i + 1)))
    val results = sorter.partitionedIterator.map{case (p, vs) => (p, vs.toSet)}.toSet
    val expected = (0 until 3).map(p => {
      (p, (0 until 50000).map(i => (i, i * 4 + 1)).filter(_._1 % 3 == p).toSet)
    }).toSet
    assert(results === expected)
  }

  test("sorting without aggregation, no spill with kryo ser") {
    sortingWithoutAggregationNoSpill(createSparkConf(false, true))
  }

  test("sorting without aggregation, no spill with java ser") {
    sortingWithoutAggregationNoSpill(createSparkConf(false, false))
  }

  def sortingWithoutAggregationNoSpill(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val ord = implicitly[Ordering[Int]]
    val sorter = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(3)), Some(ord), None)
    sorter.insertAll((0 until 100).iterator.map(i => (i, i)))
    val results = sorter.partitionedIterator.map{case (p, vs) => (p, vs.toSeq)}.toSeq
    val expected = (0 until 3).map(p => {
      (p, (0 until 100).map(i => (i, i)).filter(_._1 % 3 == p).toSeq)
    }).toSeq
    assert(results === expected)
  }

  test("sorting without aggregation, with spill with kryo ser") {
    sortingWithoutAggregationWithSpill(createSparkConf(false, true))
  }

  test("sorting without aggregation, with spill with java ser") {
    sortingWithoutAggregationWithSpill(createSparkConf(false, false))
  }

  def sortingWithoutAggregationWithSpill(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val ord = implicitly[Ordering[Int]]
    val sorter = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(3)), Some(ord), None)
    sorter.insertAll((0 until 100000).iterator.map(i => (i, i)))
    val results = sorter.partitionedIterator.map{case (p, vs) => (p, vs.toSeq)}.toSeq
    val expected = (0 until 3).map(p => {
      (p, (0 until 100000).map(i => (i, i)).filter(_._1 % 3 == p).toSeq)
    }).toSeq
    assert(results === expected)
  }

  test("spilling with hash collisions") {
    val conf = createSparkConf(true, false)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    def createCombiner(i: String): ArrayBuffer[String] = ArrayBuffer[String](i)
    def mergeValue(buffer: ArrayBuffer[String], i: String): ArrayBuffer[String] = buffer += i
    def mergeCombiners(buffer1: ArrayBuffer[String], buffer2: ArrayBuffer[String])
      : ArrayBuffer[String] = buffer1 ++= buffer2

    val agg = new Aggregator[String, String, ArrayBuffer[String]](
      createCombiner _, mergeValue _, mergeCombiners _)

    val sorter = new ExternalSorter[String, String, ArrayBuffer[String]](
      Some(agg), None, None, None)

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

    val toInsert = (1 to 100000).iterator.map(_.toString).map(s => (s, s)) ++
      collisionPairs.iterator ++ collisionPairs.iterator.map(_.swap)

    sorter.insertAll(toInsert)

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
    assert(count === 100000 + collisionPairs.size * 2)
  }

  test("spilling with many hash collisions") {
    val conf = createSparkConf(true, false)
    conf.set("spark.shuffle.memoryFraction", "0.0001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    val agg = new Aggregator[FixedHashObject, Int, Int](_ => 1, _ + _, _ + _)
    val sorter = new ExternalSorter[FixedHashObject, Int, Int](Some(agg), None, None, None)

    // Insert 10 copies each of lots of objects whose hash codes are either 0 or 1. This causes
    // problems if the map fails to group together the objects with the same code (SPARK-2043).
    val toInsert = for (i <- 1 to 10; j <- 1 to 10000) yield (FixedHashObject(j, j % 2), 1)
    sorter.insertAll(toInsert.iterator)

    val it = sorter.iterator
    var count = 0
    while (it.hasNext) {
      val kv = it.next()
      assert(kv._2 === 10)
      count += 1
    }
    assert(count === 10000)
  }

  test("spilling with hash collisions using the Int.MaxValue key") {
    val conf = createSparkConf(true, false)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    def createCombiner(i: Int): ArrayBuffer[Int] = ArrayBuffer[Int](i)
    def mergeValue(buffer: ArrayBuffer[Int], i: Int): ArrayBuffer[Int] = buffer += i
    def mergeCombiners(buf1: ArrayBuffer[Int], buf2: ArrayBuffer[Int]): ArrayBuffer[Int] = {
      buf1 ++= buf2
    }

    val agg = new Aggregator[Int, Int, ArrayBuffer[Int]](createCombiner, mergeValue, mergeCombiners)
    val sorter = new ExternalSorter[Int, Int, ArrayBuffer[Int]](Some(agg), None, None, None)

    sorter.insertAll(
      (1 to 100000).iterator.map(i => (i, i)) ++ Iterator((Int.MaxValue, Int.MaxValue)))

    val it = sorter.iterator
    while (it.hasNext) {
      // Should not throw NoSuchElementException
      it.next()
    }
  }

  test("spilling with null keys and values") {
    val conf = createSparkConf(true, false)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    def createCombiner(i: String): ArrayBuffer[String] = ArrayBuffer[String](i)
    def mergeValue(buffer: ArrayBuffer[String], i: String): ArrayBuffer[String] = buffer += i
    def mergeCombiners(buf1: ArrayBuffer[String], buf2: ArrayBuffer[String]): ArrayBuffer[String] =
      buf1 ++= buf2

    val agg = new Aggregator[String, String, ArrayBuffer[String]](
      createCombiner, mergeValue, mergeCombiners)

    val sorter = new ExternalSorter[String, String, ArrayBuffer[String]](
      Some(agg), None, None, None)

    sorter.insertAll((1 to 100000).iterator.map(i => (i.toString, i.toString)) ++ Iterator(
      (null.asInstanceOf[String], "1"),
      ("1", null.asInstanceOf[String]),
      (null.asInstanceOf[String], null.asInstanceOf[String])
    ))

    val it = sorter.iterator
    while (it.hasNext) {
      // Should not throw NullPointerException
      it.next()
    }
  }

  test("conditions for bypassing merge-sort") {
    val conf = createSparkConf(false, false)
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)

    val agg = new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j)
    val ord = implicitly[Ordering[Int]]

    // Numbers of partitions that are above and below the default bypassMergeThreshold
    val FEW_PARTITIONS = 50
    val MANY_PARTITIONS = 10000

    // Sorters with no ordering or aggregator: should bypass unless # of partitions is high

    val sorter1 = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(FEW_PARTITIONS)), None, None)
    assertBypassedMergeSort(sorter1)

    val sorter2 = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(MANY_PARTITIONS)), None, None)
    assertDidNotBypassMergeSort(sorter2)

    // Sorters with an ordering or aggregator: should not bypass even if they have few partitions

    val sorter3 = new ExternalSorter[Int, Int, Int](
      None, Some(new HashPartitioner(FEW_PARTITIONS)), Some(ord), None)
    assertDidNotBypassMergeSort(sorter3)

    val sorter4 = new ExternalSorter[Int, Int, Int](
      Some(agg), Some(new HashPartitioner(FEW_PARTITIONS)), None, None)
    assertDidNotBypassMergeSort(sorter4)
  }

  test("sort without breaking sorting contracts with kryo ser") {
    sortWithoutBreakingSortingContracts(createSparkConf(true, true))
  }

  test("sort without breaking sorting contracts with java ser") {
    sortWithoutBreakingSortingContracts(createSparkConf(true, false))
  }

  def sortWithoutBreakingSortingContracts(conf: SparkConf) {
    conf.set("spark.shuffle.memoryFraction", "0.01")
    conf.set("spark.shuffle.manager", "sort")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    // Using wrongOrdering to show integer overflow introduced exception.
    val rand = new Random(100L)
    val wrongOrdering = new Ordering[String] {
      override def compare(a: String, b: String): Int = {
        val h1 = if (a == null) 0 else a.hashCode()
        val h2 = if (b == null) 0 else b.hashCode()
        h1 - h2
      }
    }

    val testData = Array.tabulate(100000) { _ => rand.nextInt().toString }

    val sorter1 = new ExternalSorter[String, String, String](
      None, None, Some(wrongOrdering), None)
    val thrown = intercept[IllegalArgumentException] {
      sorter1.insertAll(testData.iterator.map(i => (i, i)))
      sorter1.iterator
    }

    assert(thrown.getClass() === classOf[IllegalArgumentException])
    assert(thrown.getMessage().contains("Comparison method violates its general contract"))
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
      Some(agg), None, None, None)
    sorter2.insertAll(testData.iterator.map(i => (i, i)))

    // To validate the hash ordering of key
    var minKey = Int.MinValue
    sorter2.iterator.foreach { case (k, v) =>
      val h = k.hashCode()
      assert(h >= minKey)
      minKey = h
    }

    sorter2.stop()
 }
}
