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

package org.apache.spark

import java.util.{Locale, Properties}
import java.util.concurrent.{Callable, CyclicBarrier, Executors, ExecutorService}

import org.scalatest.Matchers

import org.apache.spark.ShuffleSuite.NonJavaSerializableClass
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.{CoGroupedRDD, OrderedRDDFunctions, RDD, ShuffledRDD, SubtractedRDD}
import org.apache.spark.scheduler.{MapStatus, MyRDD, SparkListener, SparkListenerTaskEnd}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.storage.{ShuffleBlockId, ShuffleDataBlockId}
import org.apache.spark.util.{MutablePair, Utils}

abstract class ShuffleSuite extends SparkFunSuite with Matchers with LocalSparkContext {

  val conf = new SparkConf(loadDefaults = false)

  // Ensure that the DAGScheduler doesn't retry stages whose fetches fail, so that we accurately
  // test that the shuffle works (rather than retrying until all blocks are local to one Executor).
  conf.set("spark.test.noStageRetry", "true")

  test("groupByKey without compression") {
    val myConf = conf.clone().set("spark.shuffle.compress", "false")
    sc = new SparkContext("local", "test", myConf)
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 4)
    val groups = pairs.groupByKey(4).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("shuffle non-zero block size") {
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    val NUM_BLOCKS = 3

    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (x, new NonJavaSerializableClass(x * 2))
    }
    // If the Kryo serializer is not used correctly, the shuffle would fail because the
    // default Java serializer cannot handle the non serializable class.
    val c = new ShuffledRDD[Int,
      NonJavaSerializableClass,
      NonJavaSerializableClass](b, new HashPartitioner(NUM_BLOCKS))
    c.setSerializer(new KryoSerializer(conf))
    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId

    assert(c.count === 10)

    // All blocks must have non-zero size
    (0 until NUM_BLOCKS).foreach { id =>
      val statuses = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId, id)
      assert(statuses.forall(_._2.forall(blockIdSizePair => blockIdSizePair._2 > 0)))
    }
  }

  test("shuffle serializer") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (x, new NonJavaSerializableClass(x * 2))
    }
    // If the Kryo serializer is not used correctly, the shuffle would fail because the
    // default Java serializer cannot handle the non serializable class.
    val c = new ShuffledRDD[Int,
      NonJavaSerializableClass,
      NonJavaSerializableClass](b, new HashPartitioner(3))
    c.setSerializer(new KryoSerializer(conf))
    assert(c.count === 10)
  }

  test("zero sized blocks") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)

    // 201 partitions (greater than "spark.shuffle.sort.bypassMergeThreshold") from 4 keys
    val NUM_BLOCKS = 201
    val a = sc.parallelize(1 to 4, NUM_BLOCKS)
    val b = a.map(x => (x, x*2))

    // NOTE: The default Java serializer doesn't create zero-sized blocks.
    //       So, use Kryo
    val c = new ShuffledRDD[Int, Int, Int](b, new HashPartitioner(NUM_BLOCKS))
      .setSerializer(new KryoSerializer(conf))

    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    assert(c.count === 4)

    val blockSizes = (0 until NUM_BLOCKS).flatMap { id =>
      val statuses = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId, id)
      statuses.flatMap(_._2.map(_._2))
    }
    val nonEmptyBlocks = blockSizes.filter(x => x > 0)

    // We should have at most 4 non-zero sized partitions
    assert(nonEmptyBlocks.size <= 4)
  }

  test("zero sized blocks without kryo") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)

    // 201 partitions (greater than "spark.shuffle.sort.bypassMergeThreshold") from 4 keys
    val NUM_BLOCKS = 201
    val a = sc.parallelize(1 to 4, NUM_BLOCKS)
    val b = a.map(x => (x, x*2))

    // NOTE: The default Java serializer should create zero-sized blocks
    val c = new ShuffledRDD[Int, Int, Int](b, new HashPartitioner(NUM_BLOCKS))

    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    assert(c.count === 4)

    val blockSizes = (0 until NUM_BLOCKS).flatMap { id =>
      val statuses = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(shuffleId, id)
      statuses.flatMap(_._2.map(_._2))
    }
    val nonEmptyBlocks = blockSizes.filter(x => x > 0)

    // We should have at most 4 non-zero sized partitions
    assert(nonEmptyBlocks.size <= 4)
  }

  test("shuffle on mutable pairs") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    def p[T1, T2](_1: T1, _2: T2): MutablePair[T1, T2] = MutablePair(_1, _2)
    val data = Array(p(1, 1), p(1, 2), p(1, 3), p(2, 1))
    val pairs: RDD[MutablePair[Int, Int]] = sc.parallelize(data, 2)
    val results = new ShuffledRDD[Int, Int, Int](pairs,
      new HashPartitioner(2)).collect()

    data.foreach { pair => results should contain ((pair._1, pair._2)) }
  }

  test("sorting on mutable pairs") {
    // This is not in SortingSuite because of the local cluster setup.
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    def p[T1, T2](_1: T1, _2: T2): MutablePair[T1, T2] = MutablePair(_1, _2)
    val data = Array(p(1, 11), p(3, 33), p(100, 100), p(2, 22))
    val pairs: RDD[MutablePair[Int, Int]] = sc.parallelize(data, 2)
    val results = new OrderedRDDFunctions[Int, Int, MutablePair[Int, Int]](pairs)
      .sortByKey().collect()
    results(0) should be ((1, 11))
    results(1) should be ((2, 22))
    results(2) should be ((3, 33))
    results(3) should be ((100, 100))
  }

  test("cogroup using mutable pairs") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    def p[T1, T2](_1: T1, _2: T2): MutablePair[T1, T2] = MutablePair(_1, _2)
    val data1 = Seq(p(1, 1), p(1, 2), p(1, 3), p(2, 1))
    val data2 = Seq(p(1, "11"), p(1, "12"), p(2, "22"), p(3, "3"))
    val pairs1: RDD[MutablePair[Int, Int]] = sc.parallelize(data1, 2)
    val pairs2: RDD[MutablePair[Int, String]] = sc.parallelize(data2, 2)
    val results = new CoGroupedRDD[Int](Seq(pairs1, pairs2), new HashPartitioner(2))
      .map(p => (p._1, p._2.map(_.toArray)))
      .collectAsMap()

    assert(results(1)(0).length === 3)
    assert(results(1)(0).contains(1))
    assert(results(1)(0).contains(2))
    assert(results(1)(0).contains(3))
    assert(results(1)(1).length === 2)
    assert(results(1)(1).contains("11"))
    assert(results(1)(1).contains("12"))
    assert(results(2)(0).length === 1)
    assert(results(2)(0).contains(1))
    assert(results(2)(1).length === 1)
    assert(results(2)(1).contains("22"))
    assert(results(3)(0).length === 0)
    assert(results(3)(1).contains("3"))
  }

  test("subtract mutable pairs") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    def p[T1, T2](_1: T1, _2: T2): MutablePair[T1, T2] = MutablePair(_1, _2)
    val data1 = Seq(p(1, 1), p(1, 2), p(1, 3), p(2, 1), p(3, 33))
    val data2 = Seq(p(1, "11"), p(1, "12"), p(2, "22"))
    val pairs1: RDD[MutablePair[Int, Int]] = sc.parallelize(data1, 2)
    val pairs2: RDD[MutablePair[Int, String]] = sc.parallelize(data2, 2)
    val results = new SubtractedRDD(pairs1, pairs2, new HashPartitioner(2)).collect()
    results should have length (1)
    // substracted rdd return results as Tuple2
    results(0) should be ((3, 33))
  }

  test("sort with Java non serializable class - Kryo") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    val myConf = conf.clone().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = new SparkContext("local-cluster[2,1,1024]", "test", myConf)
    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (new NonJavaSerializableClass(x), x)
    }
    // If the Kryo serializer is not used correctly, the shuffle would fail because the
    // default Java serializer cannot handle the non serializable class.
    val c = b.sortByKey().map(x => x._2)
    assert(c.collect() === Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  }

  test("sort with Java non serializable class - Java") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (new NonJavaSerializableClass(x), x)
    }
    // default Java serializer cannot handle the non serializable class.
    val thrown = intercept[SparkException] {
      b.sortByKey().collect()
    }

    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.toLowerCase(Locale.ROOT).contains("serializable"))
  }

  test("shuffle with different compression settings (SPARK-3426)") {
    for (
      shuffleSpillCompress <- Set(true, false);
      shuffleCompress <- Set(true, false)
    ) {
      val myConf = conf.clone()
        .setAppName("test")
        .setMaster("local")
        .set("spark.shuffle.spill.compress", shuffleSpillCompress.toString)
        .set("spark.shuffle.compress", shuffleCompress.toString)
      resetSparkContext()
      sc = new SparkContext(myConf)
      val diskBlockManager = sc.env.blockManager.diskBlockManager
      try {
        assert(diskBlockManager.getAllFiles().isEmpty)
        sc.parallelize(0 until 10).map(i => (i / 4, i)).groupByKey().collect()
        assert(diskBlockManager.getAllFiles().nonEmpty)
      } catch {
        case e: Exception =>
          val errMsg = s"Failed with spark.shuffle.spill.compress=$shuffleSpillCompress," +
            s" spark.shuffle.compress=$shuffleCompress"
          throw new Exception(errMsg, e)
      }
    }
  }

  test("[SPARK-4085] rerun map stage if reduce stage cannot find its local shuffle file") {
    val myConf = conf.clone().set("spark.test.noStageRetry", "false")
    sc = new SparkContext("local", "test", myConf)
    val rdd = sc.parallelize(1 to 10, 2).map((_, 1)).reduceByKey(_ + _)
    rdd.count()

    // Delete one of the local shuffle blocks.
    val hashFile = sc.env.blockManager.diskBlockManager.getFile(new ShuffleBlockId(0, 0, 0))
    val sortFile = sc.env.blockManager.diskBlockManager.getFile(new ShuffleDataBlockId(0, 0, 0))
    assert(hashFile.exists() || sortFile.exists())

    if (hashFile.exists()) {
      hashFile.delete()
    }
    if (sortFile.exists()) {
      sortFile.delete()
    }

    // This count should retry the execution of the previous stage and rerun shuffle.
    rdd.count()
  }

  test("metrics for shuffle without aggregation") {
    sc = new SparkContext("local", "test", conf.clone())
    val numRecords = 10000

    val metrics = ShuffleSuite.runAndReturnMetrics(sc) {
      sc.parallelize(1 to numRecords, 4)
        .map(key => (key, 1))
        .groupByKey()
        .collect()
    }

    assert(metrics.recordsRead === numRecords)
    assert(metrics.recordsWritten === numRecords)
    assert(metrics.bytesWritten === metrics.byresRead)
    assert(metrics.bytesWritten > 0)
  }

  test("metrics for shuffle with aggregation") {
    sc = new SparkContext("local", "test", conf.clone())
    val numRecords = 10000

    val metrics = ShuffleSuite.runAndReturnMetrics(sc) {
      sc.parallelize(1 to numRecords, 4)
        .flatMap(key => Array.fill(100)((key, 1)))
        .countByKey()
    }

    assert(metrics.recordsRead === numRecords)
    assert(metrics.recordsWritten === numRecords)
    assert(metrics.bytesWritten === metrics.byresRead)
    assert(metrics.bytesWritten > 0)
  }

  test("multiple simultaneous attempts for one task (SPARK-8029)") {
    sc = new SparkContext("local", "test", conf)
    val mapTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val manager = sc.env.shuffleManager

    val taskMemoryManager = new TaskMemoryManager(sc.env.memoryManager, 0L)
    val metricsSystem = sc.env.metricsSystem
    val shuffleMapRdd = new MyRDD(sc, 1, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleHandle = manager.registerShuffle(0, 1, shuffleDep)

    // first attempt -- its successful
    val writer1 = manager.getWriter[Int, Int](shuffleHandle, 0,
      new TaskContextImpl(0, 0, 0L, 0, taskMemoryManager, new Properties, metricsSystem))
    val data1 = (1 to 10).map { x => x -> x}

    // second attempt -- also successful.  We'll write out different data,
    // just to simulate the fact that the records may get written differently
    // depending on what gets spilled, what gets combined, etc.
    val writer2 = manager.getWriter[Int, Int](shuffleHandle, 0,
      new TaskContextImpl(0, 0, 1L, 0, taskMemoryManager, new Properties, metricsSystem))
    val data2 = (11 to 20).map { x => x -> x}

    // interleave writes of both attempts -- we want to test that both attempts can occur
    // simultaneously, and everything is still OK

    def writeAndClose(
      writer: ShuffleWriter[Int, Int])(
      iter: Iterator[(Int, Int)]): Option[MapStatus] = {
      val files = writer.write(iter)
      writer.stop(true)
    }
    val interleaver = new InterleaveIterators(
      data1, writeAndClose(writer1), data2, writeAndClose(writer2))
    val (mapOutput1, mapOutput2) = interleaver.run()

    // check that we can read the map output and it has the right data
    assert(mapOutput1.isDefined)
    assert(mapOutput2.isDefined)
    assert(mapOutput1.get.location === mapOutput2.get.location)
    assert(mapOutput1.get.getSizeForBlock(0) === mapOutput1.get.getSizeForBlock(0))

    // register one of the map outputs -- doesn't matter which one
    mapOutput1.foreach { case mapStatus =>
      mapTrackerMaster.registerMapOutputs(0, Array(mapStatus))
    }

    val reader = manager.getReader[Int, Int](shuffleHandle, 0, 1,
      new TaskContextImpl(1, 0, 2L, 0, taskMemoryManager, new Properties, metricsSystem))
    val readData = reader.read().toIndexedSeq
    assert(readData === data1.toIndexedSeq || readData === data2.toIndexedSeq)

    manager.unregisterShuffle(0)
  }
}

/**
 * Utility to help tests make sure that we can process two different iterators simultaneously
 * in different threads.  This makes sure that in your test, you don't completely process data1 with
 * f1 before processing data2 with f2 (or vice versa).  It adds a barrier so that the functions only
 * process one element, before pausing to wait for the other function to "catch up".
 */
class InterleaveIterators[T, R](
  data1: Seq[T],
  f1: Iterator[T] => R,
  data2: Seq[T],
  f2: Iterator[T] => R) {

  require(data1.size == data2.size)

  val barrier = new CyclicBarrier(2)
  class BarrierIterator[E](id: Int, sub: Iterator[E]) extends Iterator[E] {
    def hasNext: Boolean = sub.hasNext

    def next: E = {
      barrier.await()
      sub.next()
    }
  }

  val c1 = new Callable[R] {
    override def call(): R = f1(new BarrierIterator(1, data1.iterator))
  }
  val c2 = new Callable[R] {
    override def call(): R = f2(new BarrierIterator(2, data2.iterator))
  }

  val e: ExecutorService = Executors.newFixedThreadPool(2)

  def run(): (R, R) = {
    val future1 = e.submit(c1)
    val future2 = e.submit(c2)
    val r1 = future1.get()
    val r2 = future2.get()
    e.shutdown()
    (r1, r2)
  }
}

object ShuffleSuite {

  def mergeCombineException(x: Int, y: Int): Int = {
    throw new SparkException("Exception for map-side combine.")
  }

  class NonJavaSerializableClass(val value: Int) extends Comparable[NonJavaSerializableClass] {
    override def compareTo(o: NonJavaSerializableClass): Int = {
      value - o.value
    }
  }

  case class AggregatedShuffleMetrics(
    recordsWritten: Long,
    recordsRead: Long,
    bytesWritten: Long,
    byresRead: Long)

  def runAndReturnMetrics(sc: SparkContext)(job: => Unit): AggregatedShuffleMetrics = {
    @volatile var recordsWritten: Long = 0
    @volatile var recordsRead: Long = 0
    @volatile var bytesWritten: Long = 0
    @volatile var bytesRead: Long = 0
    val listener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        recordsWritten += taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten
        bytesWritten += taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten
        recordsRead += taskEnd.taskMetrics.shuffleReadMetrics.recordsRead
        bytesRead += taskEnd.taskMetrics.shuffleReadMetrics.totalBytesRead
      }
    }
    sc.addSparkListener(listener)

    job

    sc.listenerBus.waitUntilEmpty(500)
    AggregatedShuffleMetrics(recordsWritten, recordsRead, bytesWritten, bytesRead)
  }
}
