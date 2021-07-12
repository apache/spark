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

package org.apache.spark.shuffle.sort

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Mockito._
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.must.Matchers

import org.apache.spark.{Aggregator, DebugFilesystem, Partitioner, SharedSparkContext, ShuffleDependency, SparkContext, SparkFunSuite}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleChecksumTestHelper}
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

class SortShuffleWriterSuite
  extends SparkFunSuite
    with SharedSparkContext
    with Matchers
    with PrivateMethodTester
    with ShuffleChecksumTestHelper {

  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockManager: BlockManager = _

  private val shuffleId = 0
  private val numMaps = 5
  private var shuffleHandle: BaseShuffleHandle[Int, Int, Int] = _
  private val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)
  private val serializer = new JavaSerializer(conf)
  private var shuffleExecutorComponents: ShuffleExecutorComponents = _

  private val partitioner = new Partitioner() {
    def numPartitions = numMaps
    def getPartition(key: Any) = Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.openMocks(this).close()
    shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.partitioner).thenReturn(partitioner)
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, dependency)
    }
    shuffleExecutorComponents = new LocalDiskShuffleExecutorComponents(
      conf, blockManager, shuffleBlockResolver)
  }

  override def afterAll(): Unit = {
    try {
      shuffleBlockResolver.stop()
    } finally {
      super.afterAll()
    }
  }

  test("write empty iterator") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = new SortShuffleWriter[Int, Int, Int](
      shuffleHandle,
      mapId = 1,
      context,
      shuffleExecutorComponents)
    writer.write(Iterator.empty)
    writer.stop(success = true)
    val dataFile = shuffleBlockResolver.getDataFile(shuffleId, 1)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(!dataFile.exists())
    assert(writeMetrics.bytesWritten === 0)
    assert(writeMetrics.recordsWritten === 0)
  }

  test("write with some records") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val records = List[(Int, Int)]((1, 2), (2, 3), (4, 4), (6, 5))
    val writer = new SortShuffleWriter[Int, Int, Int](
      shuffleHandle,
      mapId = 2,
      context,
      shuffleExecutorComponents)
    writer.write(records.toIterator)
    writer.stop(success = true)
    val dataFile = shuffleBlockResolver.getDataFile(shuffleId, 2)
    val writeMetrics = context.taskMetrics().shuffleWriteMetrics
    assert(dataFile.exists())
    assert(dataFile.length() === writeMetrics.bytesWritten)
    assert(records.size === writeMetrics.recordsWritten)
  }

  Seq((true, false, false),
    (true, true, false),
    (true, false, true),
    (true, true, true),
    (false, false, false),
    (false, true, false),
    (false, false, true),
    (false, true, true)).foreach { case (doSpill, doAgg, doOrder) =>
    test(s"write checksum file (spill=$doSpill, aggregator=$doAgg, order=$doOrder)") {
      val aggregator = if (doAgg) {
        Some(Aggregator[Int, Int, Int](
          v => v,
          (c, v) => c + v,
          (c1, c2) => c1 + c2))
      } else None
      val order = if (doOrder) {
        Some(new Ordering[Int] {
          override def compare(x: Int, y: Int): Int = x - y
        })
      } else None

      val shuffleHandle = {
        val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
        when(dependency.partitioner).thenReturn(partitioner)
        when(dependency.serializer).thenReturn(serializer)
        when(dependency.aggregator).thenReturn(aggregator)
        when(dependency.keyOrdering).thenReturn(order)
        new BaseShuffleHandle[Int, Int, Int](shuffleId, dependency)
      }

      // FIXME: this can affect other tests (if any) after this set of tests
      //  since `sc` is global.
      sc.stop()
      conf.set("spark.shuffle.spill.numElementsForceSpillThreshold",
        if (doSpill) "0" else Int.MaxValue.toString)
      conf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      val localSC = new SparkContext("local[4]", "test", conf)
      val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)
      val context = MemoryTestingUtils.fakeTaskContext(localSC.env)
      val records = List[(Int, Int)](
        (0, 1), (1, 2), (0, 2), (1, 3), (2, 3), (3, 4), (4, 5), (3, 5), (4, 6))
      val numPartition = shuffleHandle.dependency.partitioner.numPartitions
      val writer = new SortShuffleWriter[Int, Int, Int](
        shuffleHandle,
        mapId = 0,
        context,
        new LocalDiskShuffleExecutorComponents(
          conf, shuffleBlockResolver._blockManager, shuffleBlockResolver))
      writer.write(records.toIterator)
      val sorterMethod = PrivateMethod[ExternalSorter[_, _, _]](Symbol("sorter"))
      val sorter = writer.invokePrivate(sorterMethod())
      val expectSpillSize = if (doSpill) records.size else 0
      assert(sorter.numSpills === expectSpillSize)
      writer.stop(success = true)
      val checksumFile = shuffleBlockResolver.getChecksumFile(shuffleId, 0)
      assert(checksumFile.exists())
      assert(checksumFile.length() === 8 * numPartition)
      val dataFile = shuffleBlockResolver.getDataFile(shuffleId, 0)
      val indexFile = shuffleBlockResolver.getIndexFile(shuffleId, 0)
      compareChecksums(numPartition, checksumFile, dataFile, indexFile)
      localSC.stop()
    }
  }
}
