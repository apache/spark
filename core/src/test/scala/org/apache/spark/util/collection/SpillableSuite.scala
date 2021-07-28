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

import java.io.{File, IOException}
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.config
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.serializer.{KryoSerializer, SerializerInstance, SerializerManager}
import org.apache.spark.storage.{BlockId, BlockManager, DiskBlockManager, DiskBlockObjectWriter, TempLocalBlockId, TempShuffleBlockId}
import org.apache.spark.util.{Utils => UUtils}

class SpillableSuite extends SparkFunSuite with BeforeAndAfterEach {

  private val spillFilesCreated = ArrayBuffer.empty[File]
  private val localFilesCreated = ArrayBuffer.empty[File]
  private val metricsCreated = ArrayBuffer.empty[ShuffleWriteMetrics]

  private var tempDir: File = _
  private var conf: SparkConf = _
  private var taskMemoryManager: TaskMemoryManager = _

  private var blockManager: BlockManager = _
  private var diskBlockManager: DiskBlockManager = _
  private var taskContext: TaskContext = _

  override protected def beforeEach(): Unit = {
    tempDir = UUtils.createTempDir(null, "test")
    spillFilesCreated.clear()
    localFilesCreated.clear()
    metricsCreated.clear()

    val env: SparkEnv = mock(classOf[SparkEnv])
    SparkEnv.set(env)

    conf = new SparkConf()
    when(SparkEnv.get.conf).thenReturn(conf)

    val serializer = new KryoSerializer(conf)
    when(SparkEnv.get.serializer).thenReturn(serializer)

    blockManager = mock(classOf[BlockManager])
    when(SparkEnv.get.blockManager).thenReturn(blockManager)

    val manager = new SerializerManager(serializer, conf)
    when(blockManager.serializerManager).thenReturn(manager)

    diskBlockManager = mock(classOf[DiskBlockManager])
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)

    taskContext = mock(classOf[TaskContext])
    val memoryManager = new TestMemoryManager(conf)
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    when(taskContext.taskMemoryManager()).thenReturn(taskMemoryManager)

    when(diskBlockManager.createTempShuffleBlock())
      .thenAnswer((_: InvocationOnMock) => {
        val blockId = TempShuffleBlockId(UUID.randomUUID)
        val file = File.createTempFile("spillFile", ".spill", tempDir)
        spillFilesCreated += file
        (blockId, file)
      })

    when(diskBlockManager.createTempLocalBlock())
      .thenAnswer((_: InvocationOnMock) => {
        val blockId = TempLocalBlockId(UUID.randomUUID)
        val file = File.createTempFile("localFile", ".local", tempDir)
        localFilesCreated += file
        (blockId, file)
      })
  }

  override protected def afterEach(): Unit = {
    UUtils.deleteRecursively(tempDir)
    SparkEnv.set(null)

    val leakedMemory = taskMemoryManager.cleanUpAllAllocatedMemory
    if (leakedMemory != 0) {
      fail("Test leaked " + leakedMemory + " bytes of managed memory")
    }
  }

  test("SPARK-36242 Spill File should not exists if writer close fails") {
    // Prepare the data and ensure that the amount of data let the `spill()` method
    // to enter the `objectsWritten > 0` branch
    val writeSize = conf.get(config.SHUFFLE_SPILL_BATCH_SIZE) + 1
    val dataBuffer = new PartitionedPairBuffer[Int, Int]
    (0 until writeSize.toInt).foreach(i => dataBuffer.insert(0, 0, i))

    val externalSorter = new TestExternalSorter[Int, Int, Int](taskContext)

    // Mock the answer of `blockManager.getDiskWriter` and let the `close()` method of
    // `DiskBlockObjectWriter` throw IOException.
    val errorMessage = "Spill file close failed"
    when(blockManager.getDiskWriter(
      any(classOf[BlockId]),
      any(classOf[File]),
      any(classOf[SerializerInstance]),
      anyInt(),
      any(classOf[ShuffleWriteMetrics])
    )).thenAnswer((invocation: InvocationOnMock) => {
      val args = invocation.getArguments
      new DiskBlockObjectWriter(
        args(1).asInstanceOf[File],
        blockManager.serializerManager,
        args(2).asInstanceOf[SerializerInstance],
        args(3).asInstanceOf[Int],
        false,
        args(4).asInstanceOf[ShuffleWriteMetrics],
        args(0).asInstanceOf[BlockId]
      ) {
        override def close(): Unit = throw new IOException(errorMessage)
      }
    })

    val ioe = intercept[IOException] {
      externalSorter.spill(dataBuffer)
    }

    ioe.getMessage.equals(errorMessage)
    // The `TempShuffleBlock` create by diskBlockManager
    // will remain before SPARK-36242
    assert(!spillFilesCreated(0).exists())
  }

  test("spill method in ExternalSorter") {

    mockDiskBlockObjectWriter()

    // Test data size corresponds to three different scenarios:
    // 1. spillBatchSize -> `objectsWritten == 0`
    // 2. spillBatchSize + 1 -> `objectsWritten > 0`
    // 3. 0 -> Not enter `inMemoryIterator.hasNext` loop and `objectsWritten == 0`
    val dataSizes = {
      val spillBatchSize = conf.get(config.SHUFFLE_SPILL_BATCH_SIZE)
      Seq(spillBatchSize, spillBatchSize + 1, 0)
    }

    dataSizes.foreach { dataSize =>
      val dataBuffer = new PartitionedPairBuffer[Int, Int]
      (0 until dataSize.toInt).foreach(i => dataBuffer.insert(0, 0, i))
      val externalSorter = new TestExternalSorter[Int, Int, Int](taskContext)
      externalSorter.spill(dataBuffer)
    }

    // Verify recordsWritten same as data size
    assert(metricsCreated.length == dataSizes.length)
    metricsCreated.zip(dataSizes).foreach {
      case (metrics, dataSize) => assert(metrics.recordsWritten == dataSize)
    }

    // Verify bytesWritten same as file size
    assert(metricsCreated.length == spillFilesCreated.length)
    spillFilesCreated.foreach(file => assert(file.exists()))
    metricsCreated.zip(spillFilesCreated).foreach {
      case (metrics, file) => assert(metrics.bytesWritten == file.length())
    }
  }

  test("spill method in ExternalAppendOnlyMap") {

    mockDiskBlockObjectWriter()

    // Test data size corresponds to three different scenarios:
    // 1. spillBatchSize -> `objectsWritten == 0`
    // 2. spillBatchSize + 1 -> `objectsWritten > 0`
    // 3. 0 -> Not enter `inMemoryIterator.hasNext` loop and `objectsWritten == 0`
    val dataSizes = {
      val spillBatchSize = conf.get(config.SHUFFLE_SPILL_BATCH_SIZE)
      Seq(spillBatchSize, spillBatchSize + 1, 0)
    }

    dataSizes.foreach { dataSize =>
      val externalAppendOnlyMap =
        new TestExternalAppendOnlyMap[Int, Int, Int](_ => 1, _ + _, _ + _, context = taskContext)
      (0 until dataSize.toInt).foreach(i => externalAppendOnlyMap.currentMap.update(i, i))
      externalAppendOnlyMap.spill(new SizeTrackingAppendOnlyMap)
    }

    // Verify recordsWritten same as data size
    assert(metricsCreated.length == dataSizes.length)
    metricsCreated.zip(dataSizes).foreach {
      case (metrics, dataSize) => assert(metrics.recordsWritten == dataSize)
    }

    // Verify bytesWritten same as file size
    assert(metricsCreated.length == localFilesCreated.length)
    localFilesCreated.foreach(file => assert(file.exists()))
    metricsCreated.zip(localFilesCreated).foreach {
      case (metrics, file) => assert(metrics.bytesWritten == file.length())
    }
  }

  private def mockDiskBlockObjectWriter(): Unit = {
    when(blockManager.getDiskWriter(
      any(classOf[BlockId]),
      any(classOf[File]),
      any(classOf[SerializerInstance]),
      anyInt(),
      any(classOf[ShuffleWriteMetrics])
    )).thenAnswer((invocation: InvocationOnMock) => {
      val args = invocation.getArguments
      val shuffleWriteMetrics = args(4).asInstanceOf[ShuffleWriteMetrics]
      metricsCreated += shuffleWriteMetrics
      new DiskBlockObjectWriter(
        args(1).asInstanceOf[File],
        blockManager.serializerManager,
        args(2).asInstanceOf[SerializerInstance],
        args(3).asInstanceOf[Int],
        false,
        shuffleWriteMetrics,
        args(0).asInstanceOf[BlockId]
      )
    })
  }
}

/**
 * `TestExternalSorter` used to expand the access scope of the spill method
 * in `ExternalSorter`.
 */
private[this] class TestExternalSorter[K, V, C](context: TaskContext)
  extends ExternalSorter[K, V, C](context) {
  override def spill(collection: WritablePartitionedPairCollection[K, C]): Unit =
    super.spill(collection)
}

/**
 * `TestExternalAppendOnlyMap` used to expand the access scope of the spill method
 * in `ExternalAppendOnlyMap`.
 */
private[this] class TestExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    context: TaskContext)
  extends ExternalAppendOnlyMap[K, V, C](
    createCombiner, mergeValue, mergeCombiners, context = context) {
  override def spill(collection: SizeTracker): Unit = super.spill(collection)
}
