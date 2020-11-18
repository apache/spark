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

import java.io.File
import java.net.ConnectException
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfterEach

import org.apache.spark._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient}
import org.apache.spark.network.shuffle.ErrorHandler.BlockPushErrorHandler
import org.apache.spark.network.util.TransportConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage._

class PushShuffleComponentSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency: ShuffleDependency[Int, Int, Int] = _
  @Mock(answer = RETURNS_SMART_NULLS) private var shuffleClient: BlockStoreClient = _

  private var conf: SparkConf = _
  private var pushedBlocks = new ArrayBuffer[String]

  override def beforeEach(): Unit = {
    super.beforeEach()
    conf = new SparkConf(loadDefaults = false)
    MockitoAnnotations.initMocks(this)
    when(dependency.partitioner).thenReturn(new HashPartitioner(8))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    when(dependency.getMergerLocs).thenReturn(Seq(BlockManagerId("test-client", "test-client", 1)))
    conf.set("spark.shuffle.push.based.enabled", "true")
    conf.set("spark.shuffle.service.enabled", "true")
    // Set the env because the shuffler writer gets the shuffle client instance from the env.
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)
    when(blockManager.blockStoreClient).thenReturn(shuffleClient)
  }

  override def afterEach(): Unit = {
    pushedBlocks.clear()
    super.afterEach()
  }

  private def interceptPushedBlocksForSuccess(): Unit = {
    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        pushedBlocks ++= blocks
        val managedBuffers = invocation.getArguments()(3).asInstanceOf[Array[ManagedBuffer]]
        val blockFetchListener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        (blocks, managedBuffers).zipped.foreach((blockId, buffer) => {
          blockFetchListener.onBlockFetchSuccess(blockId, buffer)
        })
      })
  }

  test("Basic block push") {
    interceptPushedBlocksForSuccess()
    new TestPushShuffleComponent(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0, conf)
        .initiateBlockPush()
    verify(shuffleClient, times(1))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == dependency.partitioner.numPartitions)
    PushShuffleComponent.stop()
  }

  test("Large blocks are skipped for push") {
    conf.set("spark.shuffle.push.maxBlockSizeToPush", "1k")
    interceptPushedBlocksForSuccess()
    new TestPushShuffleComponent(mock(classOf[File]), Array(2, 2, 2, 2, 2, 2, 2, 1100),
      dependency, 0, conf).initiateBlockPush()
    verify(shuffleClient, times(1))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == dependency.partitioner.numPartitions - 1)
    PushShuffleComponent.stop()
  }

  test("Number of blocks in flight per address are limited by maxBlocksInFlightPerAddress") {
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "1")
    interceptPushedBlocksForSuccess()
    new TestPushShuffleComponent(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0, conf)
        .initiateBlockPush()
    verify(shuffleClient, times(8))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == dependency.partitioner.numPartitions)
    PushShuffleComponent.stop()
  }

  test("Hit maxBlocksInFlightPerAddress limit so that the blocks are deferred") {
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "2")
    var blockPendingResponse : String = null
    var listener : BlockFetchingListener = null
    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        pushedBlocks ++= blocks
        val managedBuffers = invocation.getArguments()(3).asInstanceOf[Array[ManagedBuffer]]
        val blockFetchListener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        // Expecting 2 blocks
        assert(blocks.length == 2)
        if (blockPendingResponse == null) {
          blockPendingResponse = blocks(1)
          listener = blockFetchListener
          // Respond with success only for the first block which will cause all the rest of the
          // blocks to be deferred
          blockFetchListener.onBlockFetchSuccess(blocks(0), managedBuffers(0))
        } else {
          (blocks, managedBuffers).zipped.foreach((blockId, buffer) => {
            blockFetchListener.onBlockFetchSuccess(blockId, buffer)
          })
        }
      })
    new TestPushShuffleComponent(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0, conf)
        .initiateBlockPush()
    verify(shuffleClient, times(1))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == 2)
    // this will trigger push of deferred blocks
    listener.onBlockFetchSuccess(blockPendingResponse, mock(classOf[ManagedBuffer]))
    verify(shuffleClient, times(4))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == 8)
    PushShuffleComponent.stop()
  }

  test("Number of shuffle blocks grouped in a single push request is limited by " +
      "maxBlockBatchSize") {
    conf.set("spark.shuffle.push.maxBlockBatchSize", "1m")
    interceptPushedBlocksForSuccess()
    new TestPushShuffleComponent(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 512 * 1024 }, dependency, 0, conf)
        .initiateBlockPush()
    verify(shuffleClient, times(4))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == dependency.partitioner.numPartitions)
    PushShuffleComponent.stop()
  }

  test("Error retries") {
    val pushShuffleSupport = new PushShuffleComponent(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0, conf)
    val errorHandler = pushShuffleSupport.createErrorHandler()
    assert(
      !errorHandler.shouldRetryError(new RuntimeException(
        new IllegalArgumentException(BlockPushErrorHandler.TOO_LATE_MESSAGE_SUFFIX))))
    assert(errorHandler.shouldRetryError(new RuntimeException(new ConnectException())))
    assert(
      errorHandler.shouldRetryError(new RuntimeException(new IllegalArgumentException(
        BlockPushErrorHandler.BLOCK_APPEND_COLLISION_DETECTED_MSG_PREFIX))))
    assert (errorHandler.shouldRetryError(new Throwable()))
  }

  test("Error logging") {
    val pushShuffleSupport = new PushShuffleComponent(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0, conf)
    val errorHandler = pushShuffleSupport.createErrorHandler()
    assert(
      !errorHandler.shouldLogError(new RuntimeException(
        new IllegalArgumentException(BlockPushErrorHandler.TOO_LATE_MESSAGE_SUFFIX))))
    assert(!errorHandler.shouldLogError(new RuntimeException(
      new IllegalArgumentException(
        BlockPushErrorHandler.BLOCK_APPEND_COLLISION_DETECTED_MSG_PREFIX))))
    assert(errorHandler.shouldLogError(new Throwable()))
  }

  test("Connect exceptions removes all the push requests for that host") {
    when(dependency.getMergerLocs).thenReturn(
      Seq(BlockManagerId("client1", "client1", 1), BlockManagerId("client2", "client2", 2)))
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "2")
    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        pushedBlocks ++= blocks
        val blockFetchListener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        blocks.foreach(blockId => {
          blockFetchListener.onBlockFetchFailure(
            blockId, new RuntimeException(new ConnectException()))
        })
      })
    new TestPushShuffleComponent(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0, conf)
        .initiateBlockPush()
    verify(shuffleClient, times(2))
      .pushBlocks(any(), any(), any(), any(), any())
    // 2 blocks for each merger locations
    assert(pushedBlocks.length == 4)
  }

  private class TestPushShuffleComponent(
      dataFile: File,
      partitionLengths: Array[Long],
      dep: ShuffleDependency[_, _, _],
      partitionId: Int,
      conf: SparkConf)
    extends PushShuffleComponent(dataFile, partitionLengths, dep, partitionId, conf) {

    override protected def submitTask(task: Runnable): Unit = {
     // Making this synchronous for testing
      task.run()
    }

    def getPartitionLengths(): Array[Long] = {
      partitionLengths
    }

    override protected def createRequestBuffer(
        conf: TransportConf,
        dataFile: File,
        offset: Long,
        length: Long): ManagedBuffer = {
      val managedBuffer = mock(classOf[ManagedBuffer])
      val byteBuffer = new Array[Byte](length.toInt)
      when(managedBuffer.nioByteBuffer()).thenReturn(ByteBuffer.wrap(byteBuffer))
      managedBuffer
    }
  }
}
