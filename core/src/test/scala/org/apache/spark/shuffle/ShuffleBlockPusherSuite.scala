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

import java.io.{File, FileNotFoundException, IOException}
import java.net.ConnectException
import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, Semaphore}

import scala.collection.mutable.ArrayBuffer

import org.mockito.{ArgumentMatchers, Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

import org.apache.spark._
import org.apache.spark.executor.CoarseGrainedExecutorBackend
import org.apache.spark.internal.config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.server.BlockPushNonFatalFailure
import org.apache.spark.network.server.BlockPushNonFatalFailure.ReturnCode
import org.apache.spark.network.shuffle.{BlockPushingListener, BlockStoreClient}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.ShuffleBlockPusher.PushRequest
import org.apache.spark.storage._
import org.apache.spark.util.ThreadUtils

class ShuffleBlockPusherSuite extends SparkFunSuite {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency: ShuffleDependency[Int, Int, Int] = _
  @Mock(answer = RETURNS_SMART_NULLS) private var shuffleClient: BlockStoreClient = _
  @Mock(answer = RETURNS_SMART_NULLS) private var executorBackend: CoarseGrainedExecutorBackend = _

  private var conf: SparkConf = _
  private var pushedBlocks = new ArrayBuffer[String]

  override def beforeEach(): Unit = {
    super.beforeEach()
    conf = new SparkConf(loadDefaults = false)
    MockitoAnnotations.openMocks(this).close()
    when(dependency.shuffleId).thenReturn(0)
    when(dependency.partitioner).thenReturn(new HashPartitioner(8))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    when(dependency.getMergerLocs).thenReturn(Seq(BlockManagerId("test-client", "test-client", 1)))
    // Set the env because the shuffler writer gets the shuffle client instance from the env.
    val mockEnv = mock(classOf[SparkEnv])
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.blockManager).thenReturn(blockManager)
    SparkEnv.set(mockEnv)
    when(SparkEnv.get.executorBackend).thenReturn(Some(executorBackend))
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
        val blockPushListener = invocation.getArguments()(4).asInstanceOf[BlockPushingListener]
        (blocks, managedBuffers).zipped.foreach((blockId, buffer) => {
          blockPushListener.onBlockPushSuccess(blockId, buffer)
        })
      })
  }

  private def verifyPushRequests(
      pushRequests: Seq[PushRequest],
      expectedSizes: Seq[Int]): Unit = {
    (pushRequests, expectedSizes).zipped.foreach((req, size) => {
      assert(req.size == size)
    })
  }

  private def verifyBlockPushCompleted(
      blockPusher: ShuffleBlockPusher): Unit = {
    verify(executorBackend, times(1))
      .notifyDriverAboutPushCompletion(dependency.shuffleId, 0, 0)
    assert(blockPusher.isPushCompletionNotified)
  }

  test("A batch of blocks is limited by maxBlocksBatchSize") {
    interceptPushedBlocksForSuccess()
    conf.set("spark.shuffle.push.maxBlockBatchSize", "1m")
    conf.set("spark.shuffle.push.maxBlockSizeToPush", "2048k")
    val blockPusher = new TestShuffleBlockPusher(conf)
    val mergerLocs = dependency.getMergerLocs.map(loc => BlockManagerId("", loc.host, loc.port))
    val largeBlockSize = 2 * 1024 * 1024
    blockPusher.initiateBlockPush(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 5 }, dependency, 0)
    val pushRequests = blockPusher.prepareBlockPushRequests(5, 0, 0, 0,
      mock(classOf[File]), Array(2, 2, 2, largeBlockSize, largeBlockSize), mergerLocs,
      mock(classOf[TransportConf]))
    blockPusher.runPendingTasks()
    assert(pushRequests.length == 3)
    verifyBlockPushCompleted(blockPusher)
    verifyPushRequests(pushRequests, Seq(6, largeBlockSize, largeBlockSize))
  }

  test("Large blocks are excluded in the preparation") {
    interceptPushedBlocksForSuccess()
    conf.set("spark.shuffle.push.maxBlockSizeToPush", "1k")
    val blockPusher = new TestShuffleBlockPusher(conf)
    val mergerLocs = dependency.getMergerLocs.map(loc => BlockManagerId("", loc.host, loc.port))
    blockPusher.initiateBlockPush(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 5 }, dependency, 0)
    val pushRequests = blockPusher.prepareBlockPushRequests(5, 0, 0, 0,
      mock(classOf[File]), Array(2, 2, 2, 1028, 1024), mergerLocs, mock(classOf[TransportConf]))
    blockPusher.runPendingTasks()
    assert(pushRequests.length == 2)
    verifyPushRequests(pushRequests, Seq(6, 1024))
    verifyBlockPushCompleted(blockPusher)
  }

  test("Number of blocks in a push request are limited by maxBlocksInFlightPerAddress ") {
    interceptPushedBlocksForSuccess()
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "1")
    val blockPusher = new TestShuffleBlockPusher(conf)
    val mergerLocs = dependency.getMergerLocs.map(loc => BlockManagerId("", loc.host, loc.port))
    blockPusher.initiateBlockPush(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 5 }, dependency, 0)
    val pushRequests = blockPusher.prepareBlockPushRequests(5, 0, 0, 0,
      mock(classOf[File]), Array(2, 2, 2, 2, 2), mergerLocs, mock(classOf[TransportConf]))
    blockPusher.runPendingTasks()
    assert(pushRequests.length == 5)
    verifyPushRequests(pushRequests, Seq(2, 2, 2, 2, 2))
    verifyBlockPushCompleted(blockPusher)
  }

  test("SPARK-33701: Ensure all the blocks are pushed before notifying driver" +
    " about push completion") {
    conf.set(REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS, 12)
    conf.set("spark.shuffle.push.maxBlockBatchSize", "20b")
    val latch = new CountDownLatch(1)
    // Different remote servers to send 2 different requests to ensure that all the blocks
    // are pushed before notifying driver about push completion
    when(dependency.getMergerLocs).thenReturn(Seq(BlockManagerId("test-client", "test-client", 1),
      BlockManagerId("slow-client", "slow-client", 1)))
    when(shuffleClient.pushBlocks(ArgumentMatchers.eq("slow-client"), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        val blockPushListener = invocation.getArguments()(4).asInstanceOf[BlockPushingListener]
        latch.await()
        // Add a small wait here to delay the "onBlockPushSuccess" to mimic the real world
        Thread.sleep(500)
        blocks.foreach { blockId =>
          blockPushListener.onBlockPushSuccess(blockId, mock(classOf[ManagedBuffer]))
        }
      })
    when(shuffleClient.pushBlocks(ArgumentMatchers.eq("test-client"), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        val blockPushListener = invocation.getArguments()(4).asInstanceOf[BlockPushingListener]
        latch.await()
        blocks.foreach { blockId =>
          blockPushListener.onBlockPushSuccess(blockId, mock(classOf[ManagedBuffer]))
        }
      })
    val semaphore = new Semaphore(0)
    val blockPusher = new ConcurrentTestBlockPusher(conf, semaphore)
    val mergerLocs = dependency.getMergerLocs.map(loc => BlockManagerId("", loc.host, loc.port))
    blockPusher.initiateBlockPush(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 5 }, dependency, 0)
    val pushRequests = blockPusher.prepareBlockPushRequests(5, 0, 0, 0,
      mock(classOf[File]), Array(2, 2, 2, 2, 2), mergerLocs, mock(classOf[TransportConf]))
    latch.countDown()
    latch.countDown()
    semaphore.acquire()
    assert(blockPusher.bytesInFlight <= 0)
    assert(pushRequests.length == 2)
    verifyPushRequests(pushRequests, Seq(6, 4))
    verifyBlockPushCompleted(blockPusher)
  }

  test("Basic block push") {
    interceptPushedBlocksForSuccess()
    val blockPusher = new TestShuffleBlockPusher(conf)
    blockPusher.initiateBlockPush(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0)
    blockPusher.runPendingTasks()
    verify(shuffleClient, times(1))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == dependency.partitioner.numPartitions)
    verifyBlockPushCompleted(blockPusher)
    ShuffleBlockPusher.stop()
  }

  test("Large blocks are skipped for push") {
    conf.set("spark.shuffle.push.maxBlockSizeToPush", "1k")
    interceptPushedBlocksForSuccess()
    val pusher = new TestShuffleBlockPusher(conf)
    pusher.initiateBlockPush(
      mock(classOf[File]), Array(2, 2, 2, 2, 2, 2, 2, 1100), dependency, 0)
    pusher.runPendingTasks()
    verify(shuffleClient, times(1))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == dependency.partitioner.numPartitions - 1)
    verifyBlockPushCompleted(pusher)
    ShuffleBlockPusher.stop()
  }

  test("Number of blocks in flight per address are limited by maxBlocksInFlightPerAddress") {
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "1")
    interceptPushedBlocksForSuccess()
    val pusher = new TestShuffleBlockPusher(conf)
    pusher.initiateBlockPush(
      mock(classOf[File]), Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0)
    pusher.runPendingTasks()
    verify(shuffleClient, times(8))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == dependency.partitioner.numPartitions)
    verifyBlockPushCompleted(pusher)
    ShuffleBlockPusher.stop()
  }

  test("Hit maxBlocksInFlightPerAddress limit so that the blocks are deferred") {
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "2")
    var blockPendingResponse : String = null
    var listener : BlockPushingListener = null
    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        pushedBlocks ++= blocks
        val managedBuffers = invocation.getArguments()(3).asInstanceOf[Array[ManagedBuffer]]
        val blockPushListener = invocation.getArguments()(4).asInstanceOf[BlockPushingListener]
        // Expecting 2 blocks
        assert(blocks.length == 2)
        if (blockPendingResponse == null) {
          blockPendingResponse = blocks(1)
          listener = blockPushListener
          // Respond with success only for the first block which will cause all the rest of the
          // blocks to be deferred
          blockPushListener.onBlockPushSuccess(blocks(0), managedBuffers(0))
        } else {
          (blocks, managedBuffers).zipped.foreach((blockId, buffer) => {
            blockPushListener.onBlockPushSuccess(blockId, buffer)
          })
        }
      })
    val pusher = new TestShuffleBlockPusher(conf)
    pusher.initiateBlockPush(
      mock(classOf[File]), Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0)
    pusher.runPendingTasks()
    verify(shuffleClient, times(1))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == 2)
    // this will trigger push of deferred blocks
    listener.onBlockPushSuccess(blockPendingResponse, mock(classOf[ManagedBuffer]))
    pusher.runPendingTasks()
    verify(shuffleClient, times(4))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == 8)
    verifyBlockPushCompleted(pusher)
    ShuffleBlockPusher.stop()
  }

  test("Number of shuffle blocks grouped in a single push request is limited by " +
      "maxBlockBatchSize") {
    conf.set("spark.shuffle.push.maxBlockBatchSize", "1m")
    interceptPushedBlocksForSuccess()
    val pusher = new TestShuffleBlockPusher(conf)
    pusher.initiateBlockPush(mock(classOf[File]),
      Array.fill(dependency.partitioner.numPartitions) { 512 * 1024 }, dependency, 0)
    pusher.runPendingTasks()
    verify(shuffleClient, times(4))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == dependency.partitioner.numPartitions)
    verifyBlockPushCompleted(pusher)
    ShuffleBlockPusher.stop()
  }

  test("Error retries") {
    val pusher = new ShuffleBlockPusher(conf)
    val errorHandler = pusher.createErrorHandler()
    assert(
      !errorHandler.shouldRetryError(new BlockPushNonFatalFailure(
        ReturnCode.TOO_LATE_BLOCK_PUSH, "")))
    assert(
      !errorHandler.shouldRetryError(new BlockPushNonFatalFailure(
        ReturnCode.TOO_OLD_ATTEMPT_PUSH, "")))
    assert(
      !errorHandler.shouldRetryError(new BlockPushNonFatalFailure(
        ReturnCode.STALE_BLOCK_PUSH, "")))
    assert(errorHandler.shouldRetryError(new RuntimeException(new ConnectException())))
    assert(
      errorHandler.shouldRetryError(new BlockPushNonFatalFailure(
        ReturnCode.BLOCK_APPEND_COLLISION_DETECTED, "")))
    assert (errorHandler.shouldRetryError(new Throwable()))
  }

  test("Error logging") {
    val pusher = new ShuffleBlockPusher(conf)
    val errorHandler = pusher.createErrorHandler()
    assert(
      !errorHandler.shouldLogError(new BlockPushNonFatalFailure(
        ReturnCode.TOO_LATE_BLOCK_PUSH, "")))
    assert(
      !errorHandler.shouldLogError(new BlockPushNonFatalFailure(
        ReturnCode.TOO_OLD_ATTEMPT_PUSH, "")))
    assert(
      !errorHandler.shouldLogError(new BlockPushNonFatalFailure(
        ReturnCode.STALE_BLOCK_PUSH, "")))
    assert(!errorHandler.shouldLogError(new BlockPushNonFatalFailure(
      ReturnCode.BLOCK_APPEND_COLLISION_DETECTED, "")))
    assert(errorHandler.shouldLogError(new Throwable()))
  }

  test("Blocks are continued to push even when a block push fails with collision " +
      "exception") {
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "1")
    val pusher = new TestShuffleBlockPusher(conf)
    var failBlock: Boolean = true
    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        val blockPushListener = invocation.getArguments()(4).asInstanceOf[BlockPushingListener]
        blocks.foreach(blockId => {
          if (failBlock) {
            failBlock = false
            // Fail the first block with the collision exception.
            blockPushListener.onBlockPushFailure(blockId, new BlockPushNonFatalFailure(
              ReturnCode.BLOCK_APPEND_COLLISION_DETECTED, ""))
          } else {
            pushedBlocks += blockId
            blockPushListener.onBlockPushSuccess(blockId, mock(classOf[ManagedBuffer]))
          }
        })
      })
    pusher.initiateBlockPush(
      mock(classOf[File]), Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0)
    pusher.runPendingTasks()
    verify(shuffleClient, times(8))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.length == 7)
    verifyBlockPushCompleted(pusher)
  }

  test("More blocks are not pushed when a block push fails with too late " +
      "exception") {
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "1")
    val pusher = new TestShuffleBlockPusher(conf)
    var failBlock: Boolean = true
    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        val blockPushListener = invocation.getArguments()(4).asInstanceOf[BlockPushingListener]
        blocks.foreach(blockId => {
          if (failBlock) {
            failBlock = false
            // Fail the first block with the too late exception.
            blockPushListener.onBlockPushFailure(blockId, new BlockPushNonFatalFailure(
              ReturnCode.TOO_LATE_BLOCK_PUSH, ""))
          } else {
            pushedBlocks += blockId
            blockPushListener.onBlockPushSuccess(blockId, mock(classOf[ManagedBuffer]))
          }
        })
      })
    pusher.initiateBlockPush(
      mock(classOf[File]), Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0)
    pusher.runPendingTasks()
    verify(shuffleClient, times(1))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pushedBlocks.isEmpty)
  }

  test("Connect exceptions remove all the push requests for that host") {
    when(dependency.getMergerLocs).thenReturn(
      Seq(BlockManagerId("client1", "client1", 1), BlockManagerId("client2", "client2", 2)))
    conf.set("spark.reducer.maxBlocksInFlightPerAddress", "2")
    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        pushedBlocks ++= blocks
        val blockPushListener = invocation.getArguments()(4).asInstanceOf[BlockPushingListener]
        blocks.foreach(blockId => {
          blockPushListener.onBlockPushFailure(
            blockId, new RuntimeException(new ConnectException()))
        })
      })
    val pusher = new TestShuffleBlockPusher(conf)
    pusher.initiateBlockPush(
      mock(classOf[File]), Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0)
    pusher.runPendingTasks()
    verify(shuffleClient, times(2))
      .pushBlocks(any(), any(), any(), any(), any())
    // 2 blocks for each merger locations
    assert(pushedBlocks.length == 4)
    assert(pusher.unreachableBlockMgrs.size == 2)
    verifyBlockPushCompleted(pusher)
  }

  test("SPARK-36255: FileNotFoundException stops the push") {
    when(dependency.getMergerLocs).thenReturn(
      Seq(BlockManagerId("client1", "client1", 1), BlockManagerId("client2", "client2", 2)))
    conf.set("spark.reducer.maxReqsInFlight", "1")
    val pusher = new TestShuffleBlockPusher(conf)
    when(shuffleClient.pushBlocks(any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val pushedBlocks = invocation.getArguments()(2).asInstanceOf[Array[String]]
        val blockPushListener = invocation.getArguments()(4).asInstanceOf[BlockPushingListener]
        pushedBlocks.foreach(blockId => {
          blockPushListener.onBlockPushFailure(
            blockId, new IOException("Failed to send RPC",
              new FileNotFoundException("file not found")))
        })
      })
    pusher.initiateBlockPush(
      mock(classOf[File]), Array.fill(dependency.partitioner.numPartitions) { 2 }, dependency, 0)
    pusher.runPendingTasks()
    verify(shuffleClient, times(1))
      .pushBlocks(any(), any(), any(), any(), any())
    assert(pusher.tasks.isEmpty)
    ShuffleBlockPusher.stop()
  }

  private class TestShuffleBlockPusher(
      conf: SparkConf) extends ShuffleBlockPusher(conf) {
    val tasks = new LinkedBlockingQueue[Runnable]

    override protected def submitTask(task: Runnable): Unit = {
      tasks.add(task)
    }

    def runPendingTasks(): Unit = {
      // This ensures that all the submitted tasks - updateStateAndCheckIfPushMore and pushUpToMax
      // are run synchronously.
      while (!tasks.isEmpty) {
        tasks.take().run()
      }
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

  private class ConcurrentTestBlockPusher(conf: SparkConf, semaphore: Semaphore)
      extends TestShuffleBlockPusher(conf) {
    val blockPusher = ThreadUtils.newDaemonFixedThreadPool(1, "test-block-pusher")

    override protected def submitTask(task: Runnable): Unit = {
      blockPusher.execute(task)
    }

    override def notifyDriverAboutPushCompletion(): Unit = {
      super.notifyDriverAboutPushCompletion()
      semaphore.release()
    }
  }
}
