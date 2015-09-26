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

import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.JavaSerializer
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

    // Create a return function to use for the mocked wrapForCompression method that just returns
    // the original input stream.
    val dummyCompressionFunction = new Answer[InputStream] {
      override def answer(invocation: InvocationOnMock): InputStream =
        invocation.getArguments()(1).asInstanceOf[InputStream]
    }

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
      when(blockManager.getBlockData(shuffleBlockId)).thenReturn(managedBuffer)
      when(blockManager.wrapForCompression(meq(shuffleBlockId), isA(classOf[InputStream])))
        .thenAnswer(dummyCompressionFunction)

      managedBuffer
    }

    // Make a mocked MapOutputTracker for the shuffle reader to use to determine what
    // shuffle data to read.
    val mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesByExecutorId(shuffleId, reduceId, reduceId + 1)).thenReturn {
      // Test a scenario where all data is local, to avoid creating a bunch of additional mocks
      // for the code to read data over the network.
      val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
        val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
        (shuffleBlockId, byteOutputStream.size().toLong)
      }
      Seq((localBlockManagerId, shuffleBlockIdsAndSizes))
    }

    // Create a mocked shuffle handle to pass into HashShuffleReader.
    val shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.serializer).thenReturn(Some(serializer))
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }

    val shuffleReader = new BlockStoreShuffleReader(
      shuffleHandle,
      reduceId,
      reduceId + 1,
      TaskContext.empty(),
      blockManager,
      mapOutputTracker)

    assert(shuffleReader.read().length === keyValuePairsPerMap * numMaps)

    // Calling .length above will have exhausted the iterator; make sure that exhausting the
    // iterator caused retain and release to be called on each buffer.
    buffers.foreach { buffer =>
      assert(buffer.callsToRetain === 1)
      assert(buffer.callsToRelease === 1)
    }
  }
}
