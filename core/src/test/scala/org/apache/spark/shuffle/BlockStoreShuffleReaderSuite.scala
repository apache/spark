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

package org.apache.spark.shuffle

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer

import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.{mock, when}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Wrapper for a managed buffer that keeps track of how many times retain and release are called.
 *
 * We need to define this class ourselves instead of using a spy because the NioManagedBuffer class
 * is final (final classes cannot be spied on).
 */
class RecordingManagedBuffer(underlyingBuffer: NioManagedBuffer) extends ManagedBuffer {
  var callsToRetain = 0
  var callsToRelease = 0

  override def size(): Long = underlyingBuffer.size()
  override def nioByteBuffer(): ByteBuffer = underlyingBuffer.nioByteBuffer()
  override def createInputStream(): InputStream = underlyingBuffer.createInputStream()
  override def convertToNetty(): AnyRef = underlyingBuffer.convertToNetty()
  override def convertToNettyForSsl(): AnyRef = underlyingBuffer.convertToNettyForSsl()

  override def retain(): ManagedBuffer = {
    callsToRetain += 1
    underlyingBuffer.retain()
  }
  override def release(): ManagedBuffer = {
    callsToRelease += 1
    underlyingBuffer.release()
  }
}

class BlockStoreShuffleReaderSuite extends SparkFunSuite with LocalSparkContext {

  /**
   * This test makes sure that, when data is read from a HashShuffleReader, the underlying
   * ManagedBuffers that contain the data are eventually released.
   */
  test("read() releases resources on completion") {
    val testConf = new SparkConf(false)
    // Create a SparkContext as a convenient way of setting SparkEnv (needed because some of the
    // shuffle code calls SparkEnv.get()).
    sc = new SparkContext("local", "test", testConf)

    val reduceId = 15
    val shuffleId = 22
    val numMaps = 6
    val keyValuePairsPerMap = 10
    val serializer = new JavaSerializer(testConf)

    // Make a mock BlockManager that will return RecordingManagedByteBuffers of data, so that we
    // can ensure retain() and release() are properly called.
    val blockManager = mock(classOf[BlockManager])

    // Create a buffer with some randomly generated key-value pairs to use as the shuffle data
    // from each mappers (all mappers return the same shuffle data).
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    (0 until keyValuePairsPerMap).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(2*i)
    }

    // Setup the mocked BlockManager to return RecordingManagedBuffers.
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)
    val buffers = (0 until numMaps).map { mapId =>
      // Create a ManagedBuffer with the shuffle data.
      val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))
      val managedBuffer = new RecordingManagedBuffer(nioBuffer)

      // Setup the blockManager mock so the buffer gets returned when the shuffle code tries to
      // fetch shuffle data.
      val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
      when(blockManager.getLocalBlockData(meq(shuffleBlockId))).thenReturn(managedBuffer)
      managedBuffer
    }

    // Make a mocked MapOutputTracker for the shuffle reader to use to determine what
    // shuffle data to read.
    val mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, reduceId, reduceId + 1)).thenReturn {
      // Test a scenario where all data is local, to avoid creating a bunch of additional mocks
      // for the code to read data over the network.
      val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
        val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
        (shuffleBlockId, byteOutputStream.size().toLong, mapId)
      }
      Seq((localBlockManagerId, shuffleBlockIdsAndSizes)).iterator
    }

    // Create a mocked shuffle handle to pass into HashShuffleReader.
    val shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, dependency)
    }

    val serializerManager = new SerializerManager(
      serializer,
      new SparkConf()
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))

    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, reduceId, reduceId + 1)
    val shuffleReader = new BlockStoreShuffleReader(
      shuffleHandle,
      blocksByAddress,
      taskContext,
      metrics,
      serializerManager,
      blockManager)

    assert(shuffleReader.read().length === keyValuePairsPerMap * numMaps)

    // Calling .length above will have exhausted the iterator; make sure that exhausting the
    // iterator caused retain and release to be called on each buffer.
    buffers.foreach { buffer =>
      assert(buffer.callsToRetain === 1)
      assert(buffer.callsToRelease === 1)
    }
  }

  /**
   * Creates a reader of local map outputs, listed in reverse. Map output `m` holds the records
   * `(m, value)` for each value in `mapValues(m)`, in one block per reduce partition.
   */
  private def mapOutputReader(
      mapValues: Seq[Seq[Int]],
      numReducePartitions: Int = 1): BlockStoreShuffleReader[Int, Int] = {
    val testConf = new SparkConf(false)
    sc = new SparkContext("local", "test", testConf)

    val shuffleId = 3
    val serializer = new JavaSerializer(testConf)
    // As in the test above, the BlockManager is mocked so that each map output is served as a
    // local block from an in-memory buffer, without running a shuffle map stage.
    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    val blockInfos = mapValues.zipWithIndex.flatMap { case (values, mapIndex) =>
      val byteOutputStream = new ByteArrayOutputStream()
      val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
      values.foreach { value =>
        serializationStream.writeKey(mapIndex)
        serializationStream.writeValue(value)
      }
      serializationStream.close()
      val bytes = byteOutputStream.toByteArray
      (0 until numReducePartitions).map { reduceId =>
        val shuffleBlockId = ShuffleBlockId(shuffleId, 100 + mapIndex, reduceId)
        val managedBuffer = new NioManagedBuffer(ByteBuffer.wrap(bytes))
        when(blockManager.getLocalBlockData(meq(shuffleBlockId))).thenReturn(managedBuffer)
        (shuffleBlockId, bytes.length.toLong, mapIndex)
      }
    }

    // The reader only uses the dependency's serializer, aggregator and key ordering, so the
    // dependency is mocked rather than built from a real RDD and partitioner.
    val shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, dependency)
    }
    val serializerManager = new SerializerManager(
      serializer,
      new SparkConf()
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))
    val taskContext = TaskContext.empty()
    new BlockStoreShuffleReader(
      shuffleHandle,
      Seq((localBlockManagerId, blockInfos.reverse)).iterator,
      taskContext,
      taskContext.taskMetrics.createTempShuffleReadMetrics(),
      serializerManager,
      blockManager)
  }

  test("readMapOutputs() labels each map output with its map index") {
    val numMaps = 4
    val (mapIndexes, mapOutputIterator) =
      mapOutputReader((0 until numMaps).map(mapIndex => 0 to mapIndex)).readMapOutputs()
    val mapOutputs = mapOutputIterator.map { case (mapIndex, records) =>
      mapIndex -> records.map(record => (record._1, record._2)).toList
    }.toList
    assert(mapIndexes.toSeq === (0 until numMaps))
    // Local blocks are read in the order they are listed, here in reverse.
    assert(mapOutputs.map(_._1) === (0 until numMaps).reverse)
    mapOutputs.foreach { case (mapIndex, records) =>
      assert(records === (0 to mapIndex).map(value => (mapIndex, value)))
    }
  }

  test("readMapOutputs() requires a single block per map output") {
    // With two reduce partitions, each map output has two blocks and would be returned twice.
    val reader = mapOutputReader(Seq(Seq(1), Seq(2)), numReducePartitions = 2)
    val e = intercept[IllegalArgumentException](reader.readMapOutputs())
    assert(e.getMessage.contains("with a single block per map output"))
  }
}
