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

package org.apache.spark.storage

import java.io._
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{CompletableFuture, Semaphore}
import java.util.zip.CheckedInputStream

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import com.google.common.io.ByteStreams
import io.netty.util.internal.OutOfDirectMemoryError
import org.apache.log4j.Level
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{doThrow, mock, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.roaringbitmap.RoaringBitmap
import org.scalatest.PrivateMethodTester

import org.apache.spark.{MapOutputTracker, SparkFunSuite, TaskContext}
import org.apache.spark.MapOutputTracker.SHUFFLE_PUSH_MAP_ID
import org.apache.spark.network._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, ExternalBlockStoreClient, MergedBlockMeta, MergedBlocksMetaListener}
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.apache.spark.storage.BlockManagerId.SHUFFLE_MERGER_IDENTIFIER
import org.apache.spark.storage.ShuffleBlockFetcherIterator._
import org.apache.spark.util.Utils


class ShuffleBlockFetcherIteratorSuite extends SparkFunSuite with PrivateMethodTester {

  private var transfer: BlockTransferService = _
  private var mapOutputTracker: MapOutputTracker = _

  override def beforeEach(): Unit = {
    transfer = mock(classOf[BlockTransferService])
    mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesForMergeResult(any(), any(), any()))
      .thenReturn(Seq.empty.iterator)
  }

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  private def answerFetchBlocks(answer: Answer[Unit]): Unit =
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any())).thenAnswer(answer)

  private def verifyFetchBlocksInvocationCount(expectedCount: Int): Unit =
    verify(transfer, times(expectedCount)).fetchBlocks(any(), any(), any(), any(), any(), any())

  // Some of the tests are quite tricky because we are testing the cleanup behavior
  // in the presence of faults.

  /** Configures `transfer` (mock [[BlockTransferService]]) to return data from the given map. */
  private def configureMockTransfer(data: Map[BlockId, ManagedBuffer]): Unit = {
    answerFetchBlocks { invocation =>
      val blocks = invocation.getArgument[Array[String]](3)
      val listener = invocation.getArgument[BlockFetchingListener](4)

      for (blockId <- blocks) {
        if (data.contains(BlockId(blockId))) {
          listener.onBlockFetchSuccess(blockId, data(BlockId(blockId)))
        } else {
          listener.onBlockFetchFailure(blockId, new BlockNotFoundException(blockId))
        }
      }
    }
  }

  /** Configures `transfer` (mock [[BlockTransferService]]) which mimics the Netty OOM issue. */
  private def configureNettyOOMMockTransfer(
      data: Map[BlockId, ManagedBuffer],
      oomBlockIndex: Int,
      throwOnce: Boolean): Unit = {
    var hasThrowOOM = false
    answerFetchBlocks { invocation =>
      val blocks = invocation.getArguments()(3).asInstanceOf[Array[String]]
      val listener = invocation.getArgument[BlockFetchingListener](4)
      for ((blockId, i) <- blocks.zipWithIndex) {
        if (!hasThrowOOM && i == oomBlockIndex) {
          hasThrowOOM = throwOnce
          val ctor = classOf[OutOfDirectMemoryError]
            .getDeclaredConstructor(classOf[java.lang.String])
          ctor.setAccessible(true)
          listener.onBlockFetchFailure(blockId, ctor.newInstance("failed to allocate memory"))
        } else if (data.contains(BlockId(blockId))) {
          listener.onBlockFetchSuccess(blockId, data(BlockId(blockId)))
        } else {
          listener.onBlockFetchFailure(blockId, new BlockNotFoundException(blockId))
        }
      }
    }
  }

  private def createMockBlockManager(): BlockManager = {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-local-host", 1)
    doReturn(localBmId).when(blockManager).blockManagerId
    // By default, the mock BlockManager returns None for hostLocalDirManager. One could
    // still use initHostLocalDirManager() to specify a custom hostLocalDirManager.
    doReturn(None).when(blockManager).hostLocalDirManager
    blockManager
  }

  private def initHostLocalDirManager(
      blockManager: BlockManager,
      hostLocalDirs: Map[String, Array[String]]): Unit = {
    val mockExternalBlockStoreClient = mock(classOf[ExternalBlockStoreClient])
    val hostLocalDirManager = new HostLocalDirManager(
      futureExecutionContext = global,
      cacheSize = 1,
      blockStoreClient = mockExternalBlockStoreClient)

    when(blockManager.hostLocalDirManager).thenReturn(Some(hostLocalDirManager))
    when(mockExternalBlockStoreClient.getHostLocalDirs(any(), any(), any(), any()))
      .thenAnswer { invocation =>
        import scala.collection.JavaConverters._
        invocation.getArgument[CompletableFuture[java.util.Map[String, Array[String]]]](3)
          .complete(hostLocalDirs.asJava)
      }

    blockManager.hostLocalDirManager = Some(hostLocalDirManager)
  }

  // Create a mock managed buffer for testing
  def createMockManagedBuffer(size: Int = 1): ManagedBuffer = {
    val mockManagedBuffer = mock(classOf[ManagedBuffer])
    val in = mock(classOf[InputStream])
    when(in.read(any())).thenReturn(1)
    when(in.read(any(), any(), any())).thenReturn(1)
    when(mockManagedBuffer.createInputStream()).thenReturn(in)
    when(mockManagedBuffer.size()).thenReturn(size)
    mockManagedBuffer
  }

  def verifyBufferRelease(buffer: ManagedBuffer, inputStream: InputStream): Unit = {
    // Note: ShuffleBlockFetcherIterator wraps input streams in a BufferReleasingInputStream
    val wrappedInputStream = inputStream.asInstanceOf[BufferReleasingInputStream]
    verify(buffer, times(0)).release()
    val delegateAccess = PrivateMethod[InputStream](Symbol("delegate"))
    var in = wrappedInputStream.invokePrivate(delegateAccess())
    if (in.isInstanceOf[CheckedInputStream]) {
      val underlyingInputFiled = classOf[CheckedInputStream].getSuperclass.getDeclaredField("in")
      underlyingInputFiled.setAccessible(true)
      in = underlyingInputFiled.get(in.asInstanceOf[CheckedInputStream]).asInstanceOf[InputStream]
    }
    verify(in, times(0)).close()
    wrappedInputStream.close()
    verify(buffer, times(1)).release()
    verify(in, times(1)).close()
    wrappedInputStream.close() // close should be idempotent
    verify(buffer, times(1)).release()
    verify(in, times(1)).close()
  }

  // scalastyle:off argcount
  private def createShuffleBlockIteratorWithDefaults(
      blocksByAddress: Map[BlockManagerId, Seq[(BlockId, Long, Int)]],
      taskContext: Option[TaskContext] = None,
      streamWrapperLimitSize: Option[Long] = None,
      blockManager: Option[BlockManager] = None,
      maxBytesInFlight: Long = Long.MaxValue,
      maxReqsInFlight: Int = Int.MaxValue,
      maxBlocksInFlightPerAddress: Int = Int.MaxValue,
      maxReqSizeShuffleToMem: Int = Int.MaxValue,
      maxAttemptsOnNettyOOM: Int = 10,
      detectCorrupt: Boolean = true,
      detectCorruptUseExtraMemory: Boolean = true,
      checksumEnabled: Boolean = true,
      checksumAlgorithm: String = "ADLER32",
      shuffleMetrics: Option[ShuffleReadMetricsReporter] = None,
      doBatchFetch: Boolean = false): ShuffleBlockFetcherIterator = {
    val tContext = taskContext.getOrElse(TaskContext.empty())
    new ShuffleBlockFetcherIterator(
      tContext,
      transfer,
      blockManager.getOrElse(createMockBlockManager()),
      mapOutputTracker,
      blocksByAddress.toIterator,
      (_, in) => streamWrapperLimitSize.map(new LimitedInputStream(in, _)).getOrElse(in),
      maxBytesInFlight,
      maxReqsInFlight,
      maxBlocksInFlightPerAddress,
      maxReqSizeShuffleToMem,
      maxAttemptsOnNettyOOM,
      detectCorrupt,
      detectCorruptUseExtraMemory,
      checksumEnabled,
      checksumAlgorithm,
      shuffleMetrics.getOrElse(tContext.taskMetrics().createTempShuffleReadMetrics()),
      doBatchFetch)
  }
  // scalastyle:on argcount

  /**
   * Convert a list of block IDs into a list of blocks with metadata, assuming all blocks have the
   * same size and index.
   */
  private def toBlockList(
      blockIds: Traversable[BlockId],
      blockSize: Long,
      blockMapIndex: Int): Seq[(BlockId, Long, Int)] = {
    blockIds.map(blockId => (blockId, blockSize, blockMapIndex)).toSeq
  }

  test("SPARK-36206: diagnose the block when it's corrupted twice") {
    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer()
    )
    answerFetchBlocks { invocation =>
      val listener = invocation.getArgument[BlockFetchingListener](4)
      listener.onBlockFetchSuccess(ShuffleBlockId(0, 0, 0).toString, mockCorruptBuffer())
    }

    val logAppender = new LogAppender("diagnose corruption")
    withLogAppender(logAppender) {
      val iterator = createShuffleBlockIteratorWithDefaults(
        Map(remoteBmId -> toBlockList(blocks.keys, 1L, 0)),
        streamWrapperLimitSize = Some(100)
      )
      intercept[FetchFailedException](iterator.next())
      // The block will be fetched twice due to retry
      verify(transfer, times(2))
        .fetchBlocks(any(), any(), any(), any(), any(), any())
      // only diagnose once
      assert(logAppender.loggingEvents.count(
        _.getRenderedMessage.contains("Start corruption diagnosis")) === 1)
    }
  }

  test("SPARK-36206: diagnose the block when it's corrupted " +
    "inside BufferReleasingInputStream") {
    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer()
    )
    answerFetchBlocks { invocation =>
      val listener = invocation.getArgument[BlockFetchingListener](4)
      listener.onBlockFetchSuccess(
        ShuffleBlockId(0, 0, 0).toString,
        mockCorruptBuffer(100, 50))
    }

    val logAppender = new LogAppender("diagnose corruption")
    withLogAppender(logAppender) {
      val iterator = createShuffleBlockIteratorWithDefaults(
        Map(remoteBmId -> toBlockList(blocks.keys, 1L, 0)),
        streamWrapperLimitSize = Some(100),
        maxBytesInFlight = 100
      )
      intercept[FetchFailedException] {
        val inputStream = iterator.next()._2
        // Consume the data to trigger the corruption
        ByteStreams.readFully(inputStream, new Array[Byte](100))
      }
      // The block will be fetched only once because corruption can't be detected in
      // maxBytesInFlight/3 of the data size
      verify(transfer, times(1))
        .fetchBlocks(any(), any(), any(), any(), any(), any())
      // only diagnose once
      assert(logAppender.loggingEvents.exists(
        _.getRenderedMessage.contains("Start corruption diagnosis")))
    }
  }

  test("successful 3 local + 4 host local + 2 remote reads") {
    val blockManager = createMockBlockManager()
    val localBmId = blockManager.blockManagerId

    // Make sure blockManager.getBlockData would return the blocks
    val localBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer())
    localBlocks.foreach { case (blockId, buf) =>
      doReturn(buf).when(blockManager).getLocalBlockData(meq(blockId))
    }

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-remote-client-1", "test-remote-host", 2)
    val remoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 3, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 0) -> createMockManagedBuffer())

    configureMockTransfer(remoteBlocks)

    // Create a block manager running on the same host (host-local)
    val hostLocalBmId = BlockManagerId("test-host-local-client-1", "test-local-host", 3)
    val hostLocalBlocks = 5.to(8).map(ShuffleBlockId(0, _, 0) -> createMockManagedBuffer()).toMap

    hostLocalBlocks.foreach { case (blockId, buf) =>
      doReturn(buf)
        .when(blockManager)
        .getHostLocalShuffleData(meq(blockId), any())
    }
    val hostLocalDirs = Map("test-host-local-client-1" -> Array("local-dir"))
    // returning local dir for hostLocalBmId
    initHostLocalDirManager(blockManager, hostLocalDirs)

    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(
        localBmId -> toBlockList(localBlocks.keys, 1L, 0),
        remoteBmId -> toBlockList(remoteBlocks.keys, 1L, 1),
        hostLocalBmId -> toBlockList(hostLocalBlocks.keys, 1L, 1)
      ),
      blockManager = Some(blockManager)
    )

    // 3 local blocks fetched in initialization
    verify(blockManager, times(3)).getLocalBlockData(any())

    val allBlocks = localBlocks ++ remoteBlocks ++ hostLocalBlocks
    for (i <- 0 until allBlocks.size) {
      assert(iterator.hasNext,
        s"iterator should have ${allBlocks.size} elements but actually has $i elements")
      val (blockId, inputStream) = iterator.next()

      // Make sure we release buffers when a wrapped input stream is closed.
      val mockBuf = allBlocks(blockId)
      verifyBufferRelease(mockBuf, inputStream)
    }

    // 4 host-local locks fetched
    verify(blockManager, times(4))
      .getHostLocalShuffleData(any(), meq(Array("local-dir")))

    // 2 remote blocks are read from the same block manager
    verifyFetchBlocksInvocationCount(1)
    assert(blockManager.hostLocalDirManager.get.getCachedHostLocalDirs.size === 1)
  }

  test("error during accessing host local dirs for executors") {
    val blockManager = createMockBlockManager()
    val hostLocalBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer())

    hostLocalBlocks.foreach { case (blockId, buf) =>
      doReturn(buf)
        .when(blockManager)
        .getHostLocalShuffleData(meq(blockId.asInstanceOf[ShuffleBlockId]), any())
    }
    val hostLocalBmId = BlockManagerId("test-host-local-client-1", "test-local-host", 3)

    val mockExternalBlockStoreClient = mock(classOf[ExternalBlockStoreClient])
    val hostLocalDirManager = new HostLocalDirManager(
      futureExecutionContext = global,
      cacheSize = 1,
      blockStoreClient = mockExternalBlockStoreClient)

    when(blockManager.hostLocalDirManager).thenReturn(Some(hostLocalDirManager))
    when(mockExternalBlockStoreClient.getHostLocalDirs(any(), any(), any(), any()))
      .thenAnswer { invocation =>
        invocation.getArgument[CompletableFuture[java.util.Map[String, Array[String]]]](3)
          .completeExceptionally(new Throwable("failed fetch"))
      }

    blockManager.hostLocalDirManager = Some(hostLocalDirManager)

    configureMockTransfer(Map())
    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(hostLocalBmId -> toBlockList(hostLocalBlocks.keys, 1L, 1))
    )
    intercept[FetchFailedException] { iterator.next() }
  }

  test("Hit maxBytesInFlight limitation before maxBlocksInFlightPerAddress") {
    val remoteBmId1 = BlockManagerId("test-remote-client-1", "test-remote-host1", 1)
    val remoteBmId2 = BlockManagerId("test-remote-client-2", "test-remote-host2", 2)
    val blockId1 = ShuffleBlockId(0, 1, 0)
    val blockId2 = ShuffleBlockId(1, 1, 0)
    configureMockTransfer(Map(
      blockId1 -> createMockManagedBuffer(1000),
      blockId2 -> createMockManagedBuffer(1000)))
    val iterator = createShuffleBlockIteratorWithDefaults(Map(
      remoteBmId1 -> toBlockList(Seq(blockId1), 1000L, 0),
      remoteBmId2 -> toBlockList(Seq(blockId2), 1000L, 0)
    ), maxBytesInFlight = 1000L)
    // After initialize() we'll have 2 FetchRequests and each is 1000 bytes. So only the
    // first FetchRequests can be sent, and the second one will hit maxBytesInFlight so
    // it won't be sent.
    verifyFetchBlocksInvocationCount(1)
    assert(iterator.hasNext)
    // next() will trigger off sending deferred request
    iterator.next()
    // the second FetchRequest should be sent at this time
    verifyFetchBlocksInvocationCount(2)
    assert(iterator.hasNext)
    iterator.next()
    assert(!iterator.hasNext)
  }

  test("Hit maxBlocksInFlightPerAddress limitation before maxBytesInFlight") {
    val remoteBmId = BlockManagerId("test-remote-client-1", "test-remote-host", 2)
    val blocks = 1.to(3).map(ShuffleBlockId(0, _, 0))
    configureMockTransfer(blocks.map(_ -> createMockManagedBuffer()).toMap)
    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(remoteBmId -> toBlockList(blocks, 1000L, 0)),
      maxBlocksInFlightPerAddress = 2
    )
    // After initialize(), we'll have 2 FetchRequests that one has 2 blocks inside and another one
    // has only one block. So only the first FetchRequest can be sent. The second FetchRequest will
    // hit maxBlocksInFlightPerAddress so it won't be sent.
    verifyFetchBlocksInvocationCount(1)
    // the first request packaged 2 blocks, so we also need to
    // call next() for 2 times to exhaust the iterator.
    assert(iterator.hasNext)
    iterator.next()
    assert(iterator.hasNext)
    iterator.next()
    verifyFetchBlocksInvocationCount(2)
    assert(iterator.hasNext)
    iterator.next()
    assert(!iterator.hasNext)
  }

  test("fetch continuous blocks in batch successful 3 local + 4 host local + 2 remote reads") {
    val blockManager = createMockBlockManager()
    val localBmId = blockManager.blockManagerId
    // Make sure blockManager.getBlockData would return the merged block
    val localBlocks = Seq[BlockId](
      ShuffleBlockId(0, 0, 0),
      ShuffleBlockId(0, 0, 1),
      ShuffleBlockId(0, 0, 2))
    val mergedLocalBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockBatchId(0, 0, 0, 3) -> createMockManagedBuffer())
    mergedLocalBlocks.foreach { case (blockId, buf) =>
      doReturn(buf).when(blockManager).getLocalBlockData(meq(blockId))
    }

    // Make sure remote blocks would return the merged block
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val remoteBlocks = Seq[BlockId](
      ShuffleBlockId(0, 3, 0),
      ShuffleBlockId(0, 3, 1))
    val mergedRemoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockBatchId(0, 3, 0, 2) -> createMockManagedBuffer())
    configureMockTransfer(mergedRemoteBlocks)

     // Create a block manager running on the same host (host-local)
    val hostLocalBmId = BlockManagerId("test-host-local-client-1", "test-local-host", 3)
    val hostLocalBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 4, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 1) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 3) -> createMockManagedBuffer())
    val mergedHostLocalBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockBatchId(0, 4, 0, 4) -> createMockManagedBuffer())

    mergedHostLocalBlocks.foreach { case (blockId, buf) =>
      doReturn(buf)
        .when(blockManager)
        .getHostLocalShuffleData(meq(blockId.asInstanceOf[ShuffleBlockBatchId]), any())
    }
    val hostLocalDirs = Map("test-host-local-client-1" -> Array("local-dir"))
    // returning local dir for hostLocalBmId
    initHostLocalDirManager(blockManager, hostLocalDirs)

    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(
        localBmId -> toBlockList(localBlocks, 1L, 0),
        remoteBmId -> toBlockList(remoteBlocks, 1L, 1),
        hostLocalBmId -> toBlockList(hostLocalBlocks.keys, 1L, 1)
      ),
      blockManager = Some(blockManager),
      doBatchFetch = true
    )

    // 3 local blocks batch fetched in initialization
    verify(blockManager, times(1)).getLocalBlockData(any())

    val allBlocks = mergedLocalBlocks ++ mergedRemoteBlocks ++ mergedHostLocalBlocks
    for (i <- 0 until 3) {
      assert(iterator.hasNext, s"iterator should have 3 elements but actually has $i elements")
      val (blockId, inputStream) = iterator.next()
      verifyFetchBlocksInvocationCount(1)
      // Make sure we release buffers when a wrapped input stream is closed.
      val mockBuf = allBlocks(blockId)
      verifyBufferRelease(mockBuf, inputStream)
    }

    // 4 host-local locks fetched
    verify(blockManager, times(1))
      .getHostLocalShuffleData(any(), meq(Array("local-dir")))

    assert(blockManager.hostLocalDirManager.get.getCachedHostLocalDirs.size === 1)
  }

  test("fetch continuous blocks in batch should respect maxBytesInFlight") {
    // Make sure remote blocks would return the merged block
    val remoteBmId1 = BlockManagerId("test-client-1", "test-client-1", 1)
    val remoteBmId2 = BlockManagerId("test-client-2", "test-client-2", 2)
    val remoteBlocks1 = (0 until 15).map(ShuffleBlockId(0, 3, _))
    val remoteBlocks2 = Seq[BlockId](ShuffleBlockId(0, 4, 0), ShuffleBlockId(0, 4, 1))
    val mergedRemoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockBatchId(0, 3, 0, 3) -> createMockManagedBuffer(),
      ShuffleBlockBatchId(0, 3, 3, 6) -> createMockManagedBuffer(),
      ShuffleBlockBatchId(0, 3, 6, 9) -> createMockManagedBuffer(),
      ShuffleBlockBatchId(0, 3, 9, 12) -> createMockManagedBuffer(),
      ShuffleBlockBatchId(0, 3, 12, 15) -> createMockManagedBuffer(),
      ShuffleBlockBatchId(0, 4, 0, 2) -> createMockManagedBuffer())
    configureMockTransfer(mergedRemoteBlocks)

    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(
        remoteBmId1 -> toBlockList(remoteBlocks1, 100L, 1),
        remoteBmId2 -> toBlockList(remoteBlocks2, 100L, 1)
      ),
      maxBytesInFlight = 1500,
      doBatchFetch = true
    )

    var numResults = 0
    // After initialize(), there will be 6 FetchRequests. And each of the first 5 requests
    // includes 1 merged block which is merged from 3 shuffle blocks. The last request has 1 merged
    // block which merged from 2 shuffle blocks. So, only the first 5 requests(5 * 3 * 100 >= 1500)
    // can be sent. The 6th FetchRequest will hit maxBlocksInFlightPerAddress so it won't
    // be sent.
    verifyFetchBlocksInvocationCount(5)
    while (iterator.hasNext) {
      val (blockId, inputStream) = iterator.next()
      // Make sure we release buffers when a wrapped input stream is closed.
      val mockBuf = mergedRemoteBlocks(blockId)
      verifyBufferRelease(mockBuf, inputStream)
      numResults += 1
    }
    // The 6th request will be sent after next() is called.
    verifyFetchBlocksInvocationCount(6)
    assert(numResults == 6)
  }

  test("SPARK-35910: Update remoteBlockBytes based on merged fetch request") {
    val blockManager = createMockBlockManager()
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val remoteBlocks = Seq[ShuffleBlockId](
      ShuffleBlockId(0, 3, 0),
      ShuffleBlockId(0, 3, 1),
      ShuffleBlockId(0, 3, 2),
      ShuffleBlockId(0, 4, 1))
    val remoteBlockList = remoteBlocks.map(id => (id, id.mapId * 31L + id.reduceId, 1)).toSeq
    val expectedSizeInBytes = Utils.bytesToString(remoteBlockList.map(_._2).sum)
    val appender = new LogAppender(expectedSizeInBytes)
    withLogAppender(appender, level = Some(Level.INFO)) {
      createShuffleBlockIteratorWithDefaults(
        Map(remoteBmId -> remoteBlockList),
        blockManager = Some(blockManager),
        doBatchFetch = true
      )
    }
    assert(appender.loggingEvents.exists(
      _.getRenderedMessage.contains(s"2 ($expectedSizeInBytes) remote blocks")),
      "remote blocks should be merged to 2 blocks and kept the actual size")
  }

  test("fetch continuous blocks in batch should respect maxBlocksInFlightPerAddress") {
    // Make sure remote blocks would return the merged block
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 1)
    val remoteBlocks = Seq(
      ShuffleBlockId(0, 3, 0),
      ShuffleBlockId(0, 3, 1),
      ShuffleBlockId(0, 4, 0),
      ShuffleBlockId(0, 4, 1),
      ShuffleBlockId(0, 5, 0))
    val mergedRemoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockBatchId(0, 3, 0, 2) -> createMockManagedBuffer(),
      ShuffleBlockBatchId(0, 4, 0, 2) -> createMockManagedBuffer(),
      ShuffleBlockBatchId(0, 5, 0, 1) -> createMockManagedBuffer())

    configureMockTransfer(mergedRemoteBlocks)
    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(remoteBmId -> toBlockList(remoteBlocks, 100L, 1)),
      maxBlocksInFlightPerAddress = 2,
      doBatchFetch = true
    )
    var numResults = 0
    // After initialize(), there will be 2 FetchRequests. First one has 2 merged blocks and each
    // of them is merged from 2 shuffle blocks, second one has 1 merged block which is merged from
    // 1 shuffle block. So only the first FetchRequest can be sent. The second FetchRequest will
    // hit maxBlocksInFlightPerAddress so it won't be sent.
    verifyFetchBlocksInvocationCount(1)
    while (iterator.hasNext) {
      val (blockId, inputStream) = iterator.next()
      // Make sure we release buffers when a wrapped input stream is closed.
      val mockBuf = mergedRemoteBlocks(blockId)
      verifyBufferRelease(mockBuf, inputStream)
      numResults += 1
    }
    // The second request will be sent after next() is called.
    verifyFetchBlocksInvocationCount(2)
    assert(numResults == 3)
  }

  test("release current unexhausted buffer in case the task completes early") {
    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer())

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    answerFetchBlocks { invocation =>
      val listener = invocation.getArgument[BlockFetchingListener](4)
      Future {
        // Return the first two blocks, and wait till task completion before returning the 3rd one
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 1, 0).toString, blocks(ShuffleBlockId(0, 1, 0)))
        sem.acquire()
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 2, 0).toString, blocks(ShuffleBlockId(0, 2, 0)))
      }
    }

    val taskContext = TaskContext.empty()
    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(remoteBmId -> toBlockList(blocks.keys, 1L, 0)),
      taskContext = Some(taskContext)
    )

    verify(blocks(ShuffleBlockId(0, 0, 0)), times(0)).release()
    iterator.next()._2.close() // close() first block's input stream
    verify(blocks(ShuffleBlockId(0, 0, 0)), times(1)).release()

    // Get the 2nd block but do not exhaust the iterator
    val subIter = iterator.next()._2

    // Complete the task; then the 2nd block buffer should be exhausted
    verify(blocks(ShuffleBlockId(0, 1, 0)), times(0)).release()
    taskContext.markTaskCompleted(None)
    verify(blocks(ShuffleBlockId(0, 1, 0)), times(1)).release()

    // The 3rd block should not be retained because the iterator is already in zombie state
    sem.release()
    verify(blocks(ShuffleBlockId(0, 2, 0)), times(0)).retain()
    verify(blocks(ShuffleBlockId(0, 2, 0)), times(0)).release()
  }

  test("fail all blocks if any of the remote request fails") {
    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    answerFetchBlocks { invocation =>
      val listener = invocation.getArgument[BlockFetchingListener](4)
      Future {
        // Return the first block, and then fail.
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
        listener.onBlockFetchFailure(
          ShuffleBlockId(0, 1, 0).toString, new BlockNotFoundException("blah"))
          listener.onBlockFetchFailure(
            ShuffleBlockId(0, 2, 0).toString, new BlockNotFoundException("blah"))
        sem.release()
      }
    }

    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(remoteBmId -> toBlockList(blocks.keys, 1L, 0))
    )

    // Continue only after the mock calls onBlockFetchFailure
    sem.acquire()

    // The first block should be returned without an exception, and the last two should throw
    // FetchFailedExceptions (due to failure)
    iterator.next()
    intercept[FetchFailedException] { iterator.next() }
    intercept[FetchFailedException] { iterator.next() }
  }

  private def mockCorruptBuffer(size: Long = 1L, corruptAt: Int = 0): ManagedBuffer = {
    val corruptStream = new CorruptStream(corruptAt)
    val corruptBuffer = mock(classOf[ManagedBuffer])
    when(corruptBuffer.size()).thenReturn(size)
    when(corruptBuffer.createInputStream()).thenReturn(corruptStream)
    corruptBuffer
  }

  private class CorruptStream(corruptAt: Long = 0L) extends InputStream {
    var pos = 0
    var closed = false

    override def read(): Int = {
      if (pos >= corruptAt) {
        throw new IOException("corrupt")
      } else {
        pos += 1
        pos
      }
    }

    override def read(dest: Array[Byte], off: Int, len: Int): Int = {
      super.read(dest, off, len)
    }

    override def close(): Unit = { closed = true }
  }

  test("retry corrupt blocks") {
    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)
    val corruptLocalBuffer = new FileSegmentManagedBuffer(null, new File("a"), 0, 100)

    answerFetchBlocks { invocation =>
      val listener = invocation.getArgument[BlockFetchingListener](4)
      Future {
        // Return the first block, and then fail.
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 1, 0).toString, mockCorruptBuffer())
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 2, 0).toString, corruptLocalBuffer)
        sem.release()
      }
    }

    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(remoteBmId -> toBlockList(blocks.keys, 1L, 0)),
      streamWrapperLimitSize = Some(100)
    )

    // Continue only after the mock calls onBlockFetchFailure
    sem.acquire()

    // The first block should be returned without an exception
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 0))

    answerFetchBlocks { invocation =>
      val listener = invocation.getArgument[BlockFetchingListener](4)
      Future {
        // Return the first block, and then fail.
        listener.onBlockFetchSuccess(ShuffleBlockId(0, 1, 0).toString, mockCorruptBuffer())
        sem.release()
      }
    }

    // The next block is corrupt local block (the second one is corrupt and retried)
    intercept[FetchFailedException] { iterator.next() }

    sem.acquire()
    intercept[FetchFailedException] { iterator.next() }
  }

  test("big blocks are also checked for corruption") {
    val streamLength = 10000L

    // This stream will throw IOException when the first byte is read
    val corruptBuffer1 = mockCorruptBuffer(streamLength, 0)
    val blockManagerId1 = BlockManagerId("remote-client-1", "remote-client-1", 1)
    val shuffleBlockId1 = ShuffleBlockId(0, 1, 0)

    val streamNotCorruptTill = 8 * 1024
    // This stream will throw exception after streamNotCorruptTill bytes are read
    val corruptBuffer2 = mockCorruptBuffer(streamLength, streamNotCorruptTill)
    val blockManagerId2 = BlockManagerId("remote-client-2", "remote-client-2", 2)
    val shuffleBlockId2 = ShuffleBlockId(0, 2, 0)

    configureMockTransfer(
      Map(shuffleBlockId1 -> corruptBuffer1, shuffleBlockId2 -> corruptBuffer2))
    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(
        blockManagerId1 -> toBlockList(Seq(shuffleBlockId1), corruptBuffer1.size(), 1),
        blockManagerId2 -> toBlockList(Seq(shuffleBlockId2), corruptBuffer2.size(), 2)
      ),
      streamWrapperLimitSize = Some(streamLength),
      maxBytesInFlight = 3 * 1024
    )

    // We'll get back the block which has corruption after maxBytesInFlight/3 because the other
    // block will detect corruption on first fetch, and then get added to the queue again for
    // a retry
    val (id, st) = iterator.next()
    assert(id === shuffleBlockId2)

    // The other block will throw a FetchFailedException
    intercept[FetchFailedException] {
      iterator.next()
    }

    // Following will succeed as it reads part of the stream which is not corrupt. This will read
    // maxBytesInFlight/3 bytes from the portion copied into memory, and remaining from the
    // underlying stream
    new DataInputStream(st).readFully(
      new Array[Byte](streamNotCorruptTill), 0, streamNotCorruptTill)

    // Following will fail as it reads the remaining part of the stream which is corrupt
    intercept[FetchFailedException] { st.read() }

    // Buffers are mocked and they return the original input corrupt streams
    assert(corruptBuffer1.createInputStream().asInstanceOf[CorruptStream].closed)
    assert(corruptBuffer2.createInputStream().asInstanceOf[CorruptStream].closed)
  }

  test("ensure big blocks available as a concatenated stream can be read") {
    val tmpDir = Utils.createTempDir()
    val tmpFile = new File(tmpDir, "someFile.txt")
    val os = new FileOutputStream(tmpFile)
    val buf = ByteBuffer.allocate(10000)
    for (i <- 1 to 2500) {
      buf.putInt(i)
    }
    os.write(buf.array())
    os.close()
    val managedBuffer = new FileSegmentManagedBuffer(null, tmpFile, 0, 10000)

    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId
    doReturn(managedBuffer).when(blockManager).getLocalBlockData(meq(ShuffleBlockId(0, 0, 0)))
    configureMockTransfer(Map(ShuffleBlockId(0, 0, 0) -> managedBuffer))

    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(localBmId -> toBlockList(Seq(ShuffleBlockId(0, 0, 0)), 10000L, 0)),
      blockManager = Some(blockManager),
      streamWrapperLimitSize = Some(10000),
      maxBytesInFlight = 2048 // force concatenation of stream by limiting bytes in flight
    )
    val (_, st) = iterator.next()
    // Check that the test setup is correct -- make sure we have a concatenated stream.
    assert (st.asInstanceOf[BufferReleasingInputStream].delegate.isInstanceOf[SequenceInputStream])

    val dst = new DataInputStream(st)
    for (i <- 1 to 2500) {
      assert(i === dst.readInt())
    }
    assert(dst.read() === -1)
    dst.close()
  }

  test("retry corrupt blocks (disabled)") {
    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    answerFetchBlocks { invocation =>
      val listener = invocation.getArgument[BlockFetchingListener](4)
      Future {
        // Return the first block, and then fail.
        listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 1, 0).toString, mockCorruptBuffer())
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 2, 0).toString, mockCorruptBuffer())
        sem.release()
      }
    }

    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(remoteBmId -> toBlockList(blocks.keys, 1L, 0)),
      streamWrapperLimitSize = Some(100),
      detectCorruptUseExtraMemory = false
    )

    // Continue only after the mock calls onBlockFetchFailure
    sem.acquire()

    // The first block should be returned without an exception
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 0))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockId(0, 1, 0))
    val (id3, _) = iterator.next()
    assert(id3 === ShuffleBlockId(0, 2, 0))
  }

  test("Blocks should be shuffled to disk when size of the request is above the" +
    " threshold(maxReqSizeShuffleToMem).") {
    val blockManager = createMockBlockManager()
    val diskBlockManager = mock(classOf[DiskBlockManager])
    val tmpDir = Utils.createTempDir()
    doReturn{
      val blockId = TempLocalBlockId(UUID.randomUUID())
      (blockId, new File(tmpDir, blockId.name))
    }.when(diskBlockManager).createTempLocalBlock()
    doReturn(diskBlockManager).when(blockManager).diskBlockManager

    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val remoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer())
    var tempFileManager: DownloadFileManager = null
    answerFetchBlocks { invocation =>
      val listener = invocation.getArgument[BlockFetchingListener](4)
      tempFileManager = invocation.getArgument[DownloadFileManager](5)
      Future {
        listener.onBlockFetchSuccess(
          ShuffleBlockId(0, 0, 0).toString, remoteBlocks(ShuffleBlockId(0, 0, 0)))
      }
    }

    def fetchShuffleBlock(blockSize: Long): Unit = {
      // Use default `maxBytesInFlight` and `maxReqsInFlight` (`Int.MaxValue`) so that during the
      // construction of `ShuffleBlockFetcherIterator`, all requests to fetch remote shuffle blocks
      // are issued. The `maxReqSizeShuffleToMem` is hard-coded as 200 here.
      createShuffleBlockIteratorWithDefaults(
        Map(remoteBmId -> toBlockList(remoteBlocks.keys, blockSize, 0)),
        blockManager = Some(blockManager),
        maxReqSizeShuffleToMem = 200)
    }

    fetchShuffleBlock(100L)
    // `maxReqSizeShuffleToMem` is 200, which is greater than the block size 100, so don't fetch
    // shuffle block to disk.
    assert(tempFileManager == null)

    fetchShuffleBlock(300L)
    // `maxReqSizeShuffleToMem` is 200, which is smaller than the block size 300, so fetch
    // shuffle block to disk.
    assert(tempFileManager != null)
  }

  test("fail zero-size blocks") {
    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer()
    )

    configureMockTransfer(blocks.mapValues(_ => createMockManagedBuffer(0)).toMap)

    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(remoteBmId -> toBlockList(blocks.keys, 1L, 0))
    )

    // All blocks fetched return zero length and should trigger a receive-side error:
    val e = intercept[FetchFailedException] { iterator.next() }
    assert(e.getMessage.contains("Received a zero-size buffer"))
  }

  test("SPARK-31521: correct the fetch size when merging blocks into a merged block") {
    val bId1 = ShuffleBlockBatchId(0, 0, 0, 5)
    val bId2 = ShuffleBlockId(0, 0, 6)
    val bId3 = ShuffleBlockId(0, 0, 7)
    val block1 = FetchBlockInfo(bId1, 40, 0)
    val block2 = FetchBlockInfo(bId2, 50, 0)
    val block3 = FetchBlockInfo(bId3, 60, 0)
    val inputBlocks = Seq(block1, block2, block3)

    val mergedBlocks = ShuffleBlockFetcherIterator.
      mergeContinuousShuffleBlockIdsIfNeeded(inputBlocks, true)
    assert(mergedBlocks.size === 1)
    val mergedBlock = mergedBlocks.head
    val mergedBlockId = mergedBlock.blockId.asInstanceOf[ShuffleBlockBatchId]
    assert(mergedBlockId.startReduceId === bId1.startReduceId)
    assert(mergedBlockId.endReduceId === bId3.reduceId + 1)
    assert(mergedBlock.size === inputBlocks.map(_.size).sum)
  }

  test("SPARK-27991: defer shuffle fetch request (one block) on Netty OOM") {
    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-remote-client-1", "test-remote-host", 2)
    val remoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer())

    configureNettyOOMMockTransfer(remoteBlocks, oomBlockIndex = 0, throwOnce = true)

    val blocksByAddress = Map[BlockManagerId, Seq[(BlockId, Long, Int)]](
      (remoteBmId, toBlockList(remoteBlocks.keys, 1, 1))
    )

    val iterator = createShuffleBlockIteratorWithDefaults(
      blocksByAddress = blocksByAddress,
      // set maxBlocksInFlightPerAddress=1 so these 2 blocks
      // would be grouped into 2 separate requests
      maxBlocksInFlightPerAddress = 1)

    for (i <- 0 until remoteBlocks.size) {
      assert(iterator.hasNext,
        s"iterator should have ${remoteBlocks.size} elements but actually has $i elements")
      val (blockId, inputStream) = iterator.next()

      // Make sure we release buffers when a wrapped input stream is closed.
      val mockBuf = remoteBlocks(blockId)
      verifyBufferRelease(mockBuf, inputStream)
    }

    // 1st fetch request (contains 1 block) would fail due to Netty OOM
    // 2nd fetch request retry the block of the 1st fetch request
    // 3rd fetch request is a normal fetch
    verifyFetchBlocksInvocationCount(3)
    assert(!ShuffleBlockFetcherIterator.isNettyOOMOnShuffle.get())
  }

  Seq(0, 1, 2).foreach { oomBlockIndex =>
    test(s"SPARK-27991: defer shuffle fetch request (multiple blocks) on Netty OOM, " +
      s"oomBlockIndex=$oomBlockIndex") {
      val blockManager = createMockBlockManager()

      // Make sure remote blocks would return
      val remoteBmId = BlockManagerId("test-remote-client-1", "test-remote-host", 2)
      val remoteBlocks = Map[BlockId, ManagedBuffer](
        ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
        ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
        ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer())

      configureNettyOOMMockTransfer(remoteBlocks, oomBlockIndex = oomBlockIndex, throwOnce = true)

      val blocksByAddress = Map[BlockManagerId, Seq[(BlockId, Long, Int)]](
        (remoteBmId, toBlockList(remoteBlocks.keys, 1L, 1))
      )

      val iterator = createShuffleBlockIteratorWithDefaults(
        blocksByAddress = blocksByAddress,
        // set maxBlocksInFlightPerAddress=3, so these 3 blocks would be grouped into 1 request
        maxBlocksInFlightPerAddress = 3)

      for (i <- 0 until remoteBlocks.size) {
        assert(iterator.hasNext,
          s"iterator should have ${remoteBlocks.size} elements but actually has $i elements")
        val (blockId, inputStream) = iterator.next()

        // Make sure we release buffers when a wrapped input stream is closed.
        val mockBuf = remoteBlocks(blockId)
        verifyBufferRelease(mockBuf, inputStream)
      }

      // 1st fetch request (contains 3 blocks) would fail on the someone block due to Netty OOM
      // but succeed for the remaining blocks
      // 2nd fetch request retry the failed block of the 1st fetch
      verifyFetchBlocksInvocationCount(2)
      assert(!ShuffleBlockFetcherIterator.isNettyOOMOnShuffle.get())
    }
  }

  test("SPARK-27991: block shouldn't retry endlessly on Netty OOM") {
    val blockManager = createMockBlockManager()

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-remote-client-1", "test-remote-host", 2)
    val remoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer())

    configureNettyOOMMockTransfer(remoteBlocks, oomBlockIndex = 0, throwOnce = false)

    val blocksByAddress = Map[BlockManagerId, Seq[(BlockId, Long, Int)]](
      (remoteBmId, toBlockList(remoteBlocks.keys, 1L, 1))
    )

    val iterator = createShuffleBlockIteratorWithDefaults(
      blocksByAddress = blocksByAddress,
      // set maxBlocksInFlightPerAddress=1 so these 2 blocks
      // would be grouped into 2 separate requests
      maxBlocksInFlightPerAddress = 1)

    val e = intercept[FetchFailedException] {
      iterator.next()
    }
    assert(e.getMessage.contains("fetch failed after 10 retries due to Netty OOM"))
  }

  /**
   * Prepares the transfer to trigger success for all the blocks present in blockChunks. It will
   * trigger failure of block which is not part of blockChunks.
   */
  private def configureMockTransferForPushShuffle(
     blocksSem: Semaphore,
     blockChunks: Map[BlockId, ManagedBuffer]): Unit = {
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val regularBlocks = invocation.getArguments()(3).asInstanceOf[Array[String]]
        val blockFetchListener =
          invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        Future {
          regularBlocks.foreach(blockId => {
            val shuffleBlock = BlockId(blockId)
            if (!blockChunks.contains(shuffleBlock)) {
              // force failure
              blockFetchListener.onBlockFetchFailure(
                blockId, new RuntimeException("failed to fetch"))
            } else {
              blockFetchListener.onBlockFetchSuccess(blockId, blockChunks(shuffleBlock))
            }
            blocksSem.release()
          })
        }
      })
  }

  test("SPARK-32922: fetch remote push-merged block meta") {
    val blocksByAddress = Map[BlockManagerId, Seq[(BlockId, Long, Int)]](
      (BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, "push-merged-host", 1),
        toBlockList(Seq(ShuffleMergedBlockId(0, 0, 2)), 2L,
          SHUFFLE_PUSH_MAP_ID)),
      (BlockManagerId("remote-client-1", "remote-host-1", 1),
        toBlockList(Seq(ShuffleBlockId(0, 0, 2), ShuffleBlockId(0, 3, 2)), 1L, 1))
    )
    val blockChunks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 3, 2) -> createMockManagedBuffer(),
      ShuffleBlockChunkId(0, 0, 2, 0) -> createMockManagedBuffer(),
      ShuffleBlockChunkId(0, 0, 2, 1) -> createMockManagedBuffer()
    )
    val blocksSem = new Semaphore(0)
    configureMockTransferForPushShuffle(blocksSem, blockChunks)

    val metaSem = new Semaphore(0)
    val pushMergedBlockMeta = mock(classOf[MergedBlockMeta])
    when(pushMergedBlockMeta.getNumChunks).thenReturn(2)
    when(pushMergedBlockMeta.getChunksBitmapBuffer).thenReturn(mock(classOf[ManagedBuffer]))
    val roaringBitmaps = Array(new RoaringBitmap, new RoaringBitmap)
    when(pushMergedBlockMeta.readChunkBitmaps()).thenReturn(roaringBitmaps)
    when(transfer.getMergedBlockMeta(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val metaListener = invocation.getArguments()(5).asInstanceOf[MergedBlocksMetaListener]
        Future {
          val shuffleId = invocation.getArguments()(2).asInstanceOf[Int]
          val shuffleMergeId = invocation.getArguments()(3).asInstanceOf[Int]
          val reduceId = invocation.getArguments()(4).asInstanceOf[Int]
          logInfo(s"acquiring semaphore for host = ${invocation.getArguments()(0)}, " +
            s"port = ${invocation.getArguments()(1)}, " +
            s"shuffleId = $shuffleId, shuffleMergeId = $shuffleMergeId, reduceId = $reduceId")
          metaSem.acquire()
          metaListener.onSuccess(shuffleId, shuffleMergeId, reduceId, pushMergedBlockMeta)
        }
      })
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress)
    blocksSem.acquire(2)
    // The first block should be returned without an exception
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 2))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockId(0, 3, 2))
    metaSem.release()
    val (id3, _) = iterator.next()
    blocksSem.acquire()
    assert(id3 === ShuffleBlockChunkId(0, 0, 2, 0))
    val (id4, _) = iterator.next()
    blocksSem.acquire()
    assert(id4 === ShuffleBlockChunkId(0, 0, 2, 1))
    assert(!iterator.hasNext)
  }

  test("SPARK-32922: failed to fetch remote push-merged block meta so fallback to " +
    "original blocks.") {
    val remoteBmId = BlockManagerId("remote-client", "remote-host-1", 1)
    val blocksByAddress = Map[BlockManagerId, Seq[(BlockId, Long, Int)]](
      (BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, "push-merged-host", 1),
        toBlockList(Seq(ShuffleMergedBlockId(0, 0, 2)), 2L,
          SHUFFLE_PUSH_MAP_ID)),
      (remoteBmId, toBlockList(Seq(ShuffleBlockId(0, 0, 2), ShuffleBlockId(0, 3, 2)), 1L, 1)))

    val blockChunks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 3, 2) -> createMockManagedBuffer()
    )
    when(mapOutputTracker.getMapSizesForMergeResult(0, 2)).thenReturn(
      Seq((remoteBmId, toBlockList(
        Seq(ShuffleBlockId(0, 1, 2), ShuffleBlockId(0, 2, 2)), 1L, 1))).iterator)
    val blocksSem = new Semaphore(0)
    configureMockTransferForPushShuffle(blocksSem, blockChunks)
    when(transfer.getMergedBlockMeta(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val metaListener = invocation.getArguments()(5).asInstanceOf[MergedBlocksMetaListener]
        val shuffleId = invocation.getArguments()(2).asInstanceOf[Int]
        val shuffleMergeId = invocation.getArguments()(3).asInstanceOf[Int]
        val reduceId = invocation.getArguments()(4).asInstanceOf[Int]
        Future {
          metaListener.onFailure(shuffleId, shuffleMergeId, reduceId,
            new RuntimeException("forced error"))
        }
      })
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress)
    blocksSem.acquire(2)
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 2))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockId(0, 3, 2))
    val (id3, _) = iterator.next()
    blocksSem.acquire(2)
    assert(id3 === ShuffleBlockId(0, 1, 2))
    val (id4, _) = iterator.next()
    assert(id4 === ShuffleBlockId(0, 2, 2))
    assert(!iterator.hasNext)
  }

  test("SPARK-32922: iterator has just 1 push-merged block and fails to fetch the meta") {
    val remoteBmId = BlockManagerId("remote-client", "remote-host-1", 1)
    val blocksByAddress = Map[BlockManagerId, Seq[(BlockId, Long, Int)]](
      (BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, "push-merged-host", 1),
        toBlockList(Seq(ShuffleMergedBlockId(0, 0, 2)), 2L,
          SHUFFLE_PUSH_MAP_ID)))

    val blockChunks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 2) -> createMockManagedBuffer()
    )
    when(mapOutputTracker.getMapSizesForMergeResult(0, 2)).thenReturn(
      Seq((remoteBmId, toBlockList(
        Seq(ShuffleBlockId(0, 0, 2), ShuffleBlockId(0, 1, 2)), 1L, 1))).iterator)
    val blocksSem = new Semaphore(0)
    configureMockTransferForPushShuffle(blocksSem, blockChunks)
    when(transfer.getMergedBlockMeta(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val metaListener = invocation.getArguments()(5).asInstanceOf[MergedBlocksMetaListener]
        val shuffleId = invocation.getArguments()(2).asInstanceOf[Int]
        val shuffleMergeId = invocation.getArguments()(3).asInstanceOf[Int]
        val reduceId = invocation.getArguments()(4).asInstanceOf[Int]
        Future {
          metaListener.onFailure(shuffleId, shuffleMergeId, reduceId,
            new RuntimeException("forced error"))
        }
      })
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress)
    val (id1, _) = iterator.next()
    blocksSem.acquire(2)
    assert(id1 === ShuffleBlockId(0, 0, 2))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockId(0, 1, 2))
    assert(!iterator.hasNext)
  }

  private def createMockPushMergedBlockMeta(
      numChunks: Int,
      bitmaps: Array[RoaringBitmap]): MergedBlockMeta = {
    val pushMergedBlockMeta = mock(classOf[MergedBlockMeta])
    when(pushMergedBlockMeta.getNumChunks).thenReturn(numChunks)
    if (bitmaps == null) {
      when(pushMergedBlockMeta.readChunkBitmaps()).thenThrow(new IOException("forced error"))
    } else {
      when(pushMergedBlockMeta.readChunkBitmaps()).thenReturn(bitmaps)
    }
    doReturn(createMockManagedBuffer()).when(pushMergedBlockMeta).getChunksBitmapBuffer
    pushMergedBlockMeta
  }

  private def prepareForFallbackToLocalBlocks(
      blockManager: BlockManager,
      localDirsMap : Map[String, Array[String]],
      failReadingLocalChunksMeta: Boolean = false):
    Map[BlockManagerId, Seq[(BlockId, Long, Int)]] = {
    val localHost = "test-local-host"
    val localBmId = BlockManagerId("test-client", localHost, 1)
    doReturn(localBmId).when(blockManager).blockManagerId
    initHostLocalDirManager(blockManager, localDirsMap)

    val blockBuffers = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 3, 2) -> createMockManagedBuffer()
    )

    doReturn(blockBuffers(ShuffleBlockId(0, 0, 2))).when(blockManager)
      .getLocalBlockData(ShuffleBlockId(0, 0, 2))
    doReturn(blockBuffers(ShuffleBlockId(0, 1, 2))).when(blockManager)
      .getLocalBlockData(ShuffleBlockId(0, 1, 2))
    doReturn(blockBuffers(ShuffleBlockId(0, 2, 2))).when(blockManager)
      .getLocalBlockData(ShuffleBlockId(0, 2, 2))
    doReturn(blockBuffers(ShuffleBlockId(0, 3, 2))).when(blockManager)
      .getLocalBlockData(ShuffleBlockId(0, 3, 2))

    val dirsForMergedData = localDirsMap(SHUFFLE_MERGER_IDENTIFIER)
    doReturn(Seq(createMockManagedBuffer(2))).when(blockManager)
      .getLocalMergedBlockData(ShuffleMergedBlockId(0, 0, 2), dirsForMergedData)

    // Get a valid chunk meta for this test
    val bitmaps = Array(new RoaringBitmap)
    bitmaps(0).add(1) // chunk 0 has mapId 1
    bitmaps(0).add(2) // chunk 0 has mapId 2
    val pushMergedBlockMeta: MergedBlockMeta = if (failReadingLocalChunksMeta) {
      createMockPushMergedBlockMeta(bitmaps.length, null)
    } else {
      createMockPushMergedBlockMeta(bitmaps.length, bitmaps)
    }
    when(blockManager.getLocalMergedBlockMeta(ShuffleMergedBlockId(0, 0, 2),
      dirsForMergedData)).thenReturn(pushMergedBlockMeta)
    when(mapOutputTracker.getMapSizesForMergeResult(0, 2)).thenReturn(
      Seq((localBmId,
        toBlockList(Seq(ShuffleBlockId(0, 1, 2), ShuffleBlockId(0, 2, 2)), 1L, 1))).iterator)
    when(mapOutputTracker.getMapSizesForMergeResult(0, 2, bitmaps(0)))
      .thenReturn(Seq((localBmId,
        toBlockList(Seq(ShuffleBlockId(0, 1, 2), ShuffleBlockId(0, 2, 2)), 1L, 1))).iterator)
    val pushMergedBmId = BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, localHost, 1)
    Map[BlockManagerId, Seq[(BlockId, Long, Int)]](
      (localBmId, toBlockList(Seq(ShuffleBlockId(0, 0, 2), ShuffleBlockId(0, 3, 2)), 1L, 1)),
      (pushMergedBmId, toBlockList(
        Seq(ShuffleMergedBlockId(0, 0, 2)), 2L, SHUFFLE_PUSH_MAP_ID)))
  }

  private def verifyLocalBlocksFromFallback(iterator: ShuffleBlockFetcherIterator): Unit = {
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 2))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockId(0, 3, 2))
    val (id3, _) = iterator.next()
    assert(id3 === ShuffleBlockId(0, 1, 2))
    val (id4, _) = iterator.next()
    assert(id4 === ShuffleBlockId(0, 2, 2))
    assert(!iterator.hasNext)
  }

  test("SPARK-32922: failure to fetch push-merged-local meta should fallback to fetch " +
    "original shuffle blocks") {
    val blockManager = mock(classOf[BlockManager])
    val localDirs = Array("testPath1", "testPath2")
    val blocksByAddress = prepareForFallbackToLocalBlocks(
      blockManager, Map(SHUFFLE_MERGER_IDENTIFIER -> localDirs))
    doThrow(new RuntimeException("Forced error")).when(blockManager)
      .getLocalMergedBlockMeta(ShuffleMergedBlockId(0, 0, 2), localDirs)
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress,
      blockManager = Some(blockManager))
    verifyLocalBlocksFromFallback(iterator)
  }

  test("SPARK-32922: failure to reading chunkBitmaps of push-merged-local meta should " +
    "fallback to original shuffle blocks") {
    val blockManager = mock(classOf[BlockManager])
    val localDirs = Array("local-dir")
    val blocksByAddress = prepareForFallbackToLocalBlocks(
      blockManager, Map(SHUFFLE_MERGER_IDENTIFIER -> localDirs),
      failReadingLocalChunksMeta = true)
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress,
      blockManager = Some(blockManager), streamWrapperLimitSize = Some(100))
    verifyLocalBlocksFromFallback(iterator)
  }

  test("SPARK-32922: failure to fetch push-merged-local data should fallback to fetch " +
    "original shuffle blocks") {
    val blockManager = mock(classOf[BlockManager])
    val localDirs = Array("testPath1", "testPath2")
    val blocksByAddress = prepareForFallbackToLocalBlocks(
      blockManager, Map(SHUFFLE_MERGER_IDENTIFIER -> localDirs))
    doThrow(new RuntimeException("Forced error")).when(blockManager)
      .getLocalMergedBlockData(ShuffleMergedBlockId(0, 0, 2), localDirs)
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress,
      blockManager = Some(blockManager))
    verifyLocalBlocksFromFallback(iterator)
  }

  test("SPARK-32922: failure to fetch push-merged-local meta of a single merged block " +
    "should not drop the fetch of other push-merged-local blocks") {
    val blockManager = mock(classOf[BlockManager])
    val localDirs = Array("testPath1", "testPath2")
    prepareForFallbackToLocalBlocks(
      blockManager, Map(SHUFFLE_MERGER_IDENTIFIER -> localDirs))
    val localHost = "test-local-host"
    val localBmId = BlockManagerId("test-client", localHost, 1)
    val pushMergedBmId = BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, localHost, 1)
    val blocksByAddress = Map[BlockManagerId, Seq[(BlockId, Long, Int)]](
      (localBmId, toBlockList(Seq(ShuffleBlockId(0, 0, 2), ShuffleBlockId(0, 3, 2)), 1L, 1)),
      (pushMergedBmId, toBlockList(Seq(ShuffleMergedBlockId(0, 0, 2),
        ShuffleMergedBlockId(0, 0, 3)), 2L, SHUFFLE_PUSH_MAP_ID)))
    doThrow(new RuntimeException("Forced error")).when(blockManager)
      .getLocalMergedBlockMeta(ShuffleMergedBlockId(0, 0, 2), localDirs)
    // Create a valid chunk meta for partition 3
    val bitmaps = Array(new RoaringBitmap)
    bitmaps(0).add(1) // chunk 0 has mapId 1
    doReturn(createMockPushMergedBlockMeta(bitmaps.length, bitmaps)).when(blockManager)
      .getLocalMergedBlockMeta(ShuffleMergedBlockId(0, 0, 3), localDirs)
    // Return valid buffer for chunk in partition 3
    doReturn(Seq(createMockManagedBuffer(2))).when(blockManager)
      .getLocalMergedBlockData(ShuffleMergedBlockId(0, 0, 3), localDirs)
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress,
      blockManager = Some(blockManager))
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 2))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockId(0, 3, 2))
    val (id3, _) = iterator.next()
    assert(id3 === ShuffleBlockId(0, 1, 2))
    val (id4, _) = iterator.next()
    assert(id4 === ShuffleBlockId(0, 2, 2))
    val (id5, _) = iterator.next()
    assert(id5 === ShuffleBlockChunkId(0, 0, 3, 0))
    assert(!iterator.hasNext)
  }

  test("SPARK-32922: failure to fetch push-merged block as well as fallback block should throw " +
    "a FetchFailedException") {
    val blockManager = mock(classOf[BlockManager])
    val localDirs = Array("testPath1", "testPath2")
    val localBmId = BlockManagerId("test-client", "test-local-host", 1)
    doReturn(localBmId).when(blockManager).blockManagerId
    val localDirsMap = Map(SHUFFLE_MERGER_IDENTIFIER -> localDirs)
    initHostLocalDirManager(blockManager, localDirsMap)

    doReturn(createMockManagedBuffer()).when(blockManager)
      .getLocalBlockData(ShuffleBlockId(0, 0, 2))
    // Force to fail reading of original block (0, 1, 2) that will throw a FetchFailed exception.
    doThrow(new RuntimeException("Forced error")).when(blockManager)
      .getLocalBlockData(ShuffleBlockId(0, 1, 2))

    val dirsForMergedData = localDirsMap(SHUFFLE_MERGER_IDENTIFIER)
    // Since bitmaps are null, this will fail reading the push-merged block meta causing fallback to
    // initiate.
    val pushMergedBlockMeta: MergedBlockMeta = createMockPushMergedBlockMeta(2, null)
    when(blockManager.getLocalMergedBlockMeta(ShuffleMergedBlockId(0, 0, 2),
      dirsForMergedData)).thenReturn(pushMergedBlockMeta)
    when(mapOutputTracker.getMapSizesForMergeResult(0, 2)).thenReturn(
      Seq((localBmId,
        toBlockList(Seq(ShuffleBlockId(0, 0, 2), ShuffleBlockId(0, 1, 2)), 1L, 1))).iterator)

    val blocksByAddress = Map[BlockManagerId, Seq[(BlockId, Long, Int)]](
      (BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, "test-local-host", 1), toBlockList(
        Seq(ShuffleMergedBlockId(0, 0, 2)), 2L, SHUFFLE_PUSH_MAP_ID)))
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress,
      blockManager = Some(blockManager))
    // 1st instance of iterator.next() returns the original shuffle block (0, 0, 2)
    assert(iterator.next()._1 === ShuffleBlockId(0, 0, 2))
    // 2nd instance of iterator.next() throws FetchFailedException
    intercept[FetchFailedException] { iterator.next() }
  }

  test("SPARK-32922: failure to fetch push-merged-local block should fallback to fetch " +
    "original shuffle blocks which contain host-local blocks") {
    val blockManager = mock(classOf[BlockManager])
    // BlockManagerId from another executor on the same host
    val hostLocalBmId = BlockManagerId("test-client-1", "test-local-host", 1)
    val hostLocalDirs = Map("test-client-1" -> Array("local-dir"),
      SHUFFLE_MERGER_IDENTIFIER -> Array("local-dir"))
    val blocksByAddress = prepareForFallbackToLocalBlocks(blockManager, hostLocalDirs)

    doThrow(new RuntimeException("Forced error")).when(blockManager)
      .getLocalMergedBlockData(ShuffleMergedBlockId(0, 0, 2), Array("local-dir"))
    // host local read for a shuffle block
    doReturn(createMockManagedBuffer()).when(blockManager)
      .getHostLocalShuffleData(ShuffleBlockId(0, 2, 2), Array("local-dir"))
    when(mapOutputTracker.getMapSizesForMergeResult(0, 2)).thenAnswer(
      (_: InvocationOnMock) => {
        Seq((blockManager.blockManagerId, toBlockList(Seq(ShuffleBlockId(0, 1, 2)), 1L, 1)),
          (hostLocalBmId, toBlockList(Seq(ShuffleBlockId(0, 2, 2)), 1L, 1))).iterator
      })
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress,
      blockManager = Some(blockManager))
    verifyLocalBlocksFromFallback(iterator)
  }

  test("SPARK-32922: fetch host local blocks with push-merged block during initialization " +
    "and fallback to host locals blocks") {
    val blockManager = mock(classOf[BlockManager])
    // BlockManagerId of another executor on the same host
    val hostLocalBmId = BlockManagerId("test-client-1", "test-local-host", 1)
    val originalHostLocalBmId = BlockManagerId("test-client-2", "test-local-host", 1)
    val hostLocalDirs = Map(hostLocalBmId.executorId -> Array("local-dir"),
      SHUFFLE_MERGER_IDENTIFIER -> Array("local-dir"),
      originalHostLocalBmId.executorId -> Array("local-dir"))

    val hostLocalBlocks = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (hostLocalBmId, Seq((ShuffleBlockId(0, 5, 2), 1L, 1))))

    val blocksByAddress = prepareForFallbackToLocalBlocks(
      blockManager, hostLocalDirs) ++ hostLocalBlocks

    doThrow(new RuntimeException("Forced error")).when(blockManager)
      .getLocalMergedBlockData(ShuffleMergedBlockId(0, 0, 2), Array("local-dir"))
    // host Local read for this original shuffle block
    doReturn(createMockManagedBuffer()).when(blockManager)
      .getHostLocalShuffleData(ShuffleBlockId(0, 1, 2), Array("local-dir"))
    doReturn(createMockManagedBuffer()).when(blockManager)
      .getHostLocalShuffleData(ShuffleBlockId(0, 5, 2), Array("local-dir"))
    when(mapOutputTracker.getMapSizesForMergeResult(0, 2)).thenAnswer(
      (_: InvocationOnMock) => {
        Seq((blockManager.blockManagerId, toBlockList(Seq(ShuffleBlockId(0, 2, 2)), 1L, 1)),
          (originalHostLocalBmId, toBlockList(Seq(ShuffleBlockId(0, 1, 2)), 1L, 1))).iterator
      })
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress,
      blockManager = Some(blockManager))
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 2))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockId(0, 3, 2))
    val (id3, _) = iterator.next()
    assert(id3 === ShuffleBlockId(0, 5, 2))
    val (id4, _) = iterator.next()
    assert(id4 === ShuffleBlockId(0, 2, 2))
    val (id5, _) = iterator.next()
    assert(id5 === ShuffleBlockId(0, 1, 2))
    assert(!iterator.hasNext)
  }

  test("SPARK-32922: failure while reading local shuffle chunks should fallback to original " +
    "shuffle blocks") {
    val blockManager = mock(classOf[BlockManager])
    val localDirs = Array("local-dir")
    val blocksByAddress = prepareForFallbackToLocalBlocks(
      blockManager, Map(SHUFFLE_MERGER_IDENTIFIER -> localDirs))
    // This will throw an IOException when input stream is created from the ManagedBuffer
    doReturn(Seq({
      new FileSegmentManagedBuffer(null, new File("non-existent"), 0, 100)
      })).when(blockManager).getLocalMergedBlockData(
      ShuffleMergedBlockId(0, 0, 2), localDirs)
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress,
      blockManager = Some(blockManager))
    verifyLocalBlocksFromFallback(iterator)
  }

  test("SPARK-32922: fallback to original shuffle block when a push-merged shuffle chunk " +
    "is corrupt") {
    val blockManager = mock(classOf[BlockManager])
    val localDirs = Array("local-dir")
    val blocksByAddress = prepareForFallbackToLocalBlocks(
      blockManager, Map(SHUFFLE_MERGER_IDENTIFIER -> localDirs))
    val corruptBuffer = createMockManagedBuffer(2)
    doReturn(Seq({corruptBuffer})).when(blockManager)
      .getLocalMergedBlockData(ShuffleMergedBlockId(0, 0, 2), localDirs)
    val corruptStream = mock(classOf[InputStream])
    when(corruptStream.read(any(), any(), any())).thenThrow(new IOException("corrupt"))
    doReturn(corruptStream).when(corruptBuffer).createInputStream()
    val iterator = createShuffleBlockIteratorWithDefaults(blocksByAddress,
      blockManager = Some(blockManager), streamWrapperLimitSize = Some(100))
    verifyLocalBlocksFromFallback(iterator)
  }

  test("SPARK-32922: fallback to original blocks when failed to fetch remote shuffle chunk") {
    val blockChunks = Map[BlockId, ManagedBuffer](
      ShuffleBlockChunkId(0, 0, 2, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 3, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 5, 2) -> createMockManagedBuffer()
    )
    val blocksSem = new Semaphore(0)
    configureMockTransferForPushShuffle(blocksSem, blockChunks)
    val bitmaps = Array(new RoaringBitmap, new RoaringBitmap)
    bitmaps(1).add(3)
    bitmaps(1).add(4)
    bitmaps(1).add(5)
    val pushMergedBlockMeta = createMockPushMergedBlockMeta(2, bitmaps)
    when(transfer.getMergedBlockMeta(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val metaListener = invocation.getArguments()(5).asInstanceOf[MergedBlocksMetaListener]
        val shuffleId = invocation.getArguments()(2).asInstanceOf[Int]
        val shuffleMergeId = invocation.getArguments()(3).asInstanceOf[Int]
        val reduceId = invocation.getArguments()(4).asInstanceOf[Int]
        Future {
          metaListener.onSuccess(shuffleId, shuffleMergeId, reduceId, pushMergedBlockMeta)
        }
      })
    val fallbackBlocksByAddr = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (BlockManagerId("remote-client", "remote-host-2", 1),
        toBlockList(Seq(ShuffleBlockId(0, 3, 2), ShuffleBlockId(0, 4, 2),
          ShuffleBlockId(0, 5, 2)), 4L, 1)))
    when(mapOutputTracker.getMapSizesForMergeResult(any(), any(), any()))
      .thenReturn(fallbackBlocksByAddr.iterator)
    val iterator = createShuffleBlockIteratorWithDefaults(Map(
      BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, "remote-client-1", 1) ->
        toBlockList(Seq(ShuffleMergedBlockId(0, 0, 2)),
          12L, SHUFFLE_PUSH_MAP_ID)))
    val (id1, _) = iterator.next()
    blocksSem.acquire(1)
    assert(id1 === ShuffleBlockChunkId(0, 0, 2, 0))
    val (id3, _) = iterator.next()
    blocksSem.acquire(3)
    assert(id3 === ShuffleBlockId(0, 3, 2))
    val (id4, _) = iterator.next()
    assert(id4 === ShuffleBlockId(0, 4, 2))
    val (id5, _) = iterator.next()
    assert(id5 === ShuffleBlockId(0, 5, 2))
    assert(!iterator.hasNext)
  }

  test("SPARK-32922: fallback to original blocks when failed to parse remote merged block meta") {
    val blockChunks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 2) -> createMockManagedBuffer()
    )
    when(mapOutputTracker.getMapSizesForMergeResult(0, 2)).thenReturn(
      Seq((BlockManagerId("remote-client-1", "remote-host-1", 1),
        toBlockList(Seq(ShuffleBlockId(0, 0, 2), ShuffleBlockId(0, 1, 2)), 1L, 1))).iterator)
    val blocksSem = new Semaphore(0)
    configureMockTransferForPushShuffle(blocksSem, blockChunks)
    val pushMergedBlockMeta = createMockPushMergedBlockMeta(2, null)
    when(transfer.getMergedBlockMeta(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val metaListener = invocation.getArguments()(5).asInstanceOf[MergedBlocksMetaListener]
        val shuffleId = invocation.getArguments()(2).asInstanceOf[Int]
        val shuffleMergeId = invocation.getArguments()(3).asInstanceOf[Int]
        val reduceId = invocation.getArguments()(4).asInstanceOf[Int]
        Future {
          metaListener.onSuccess(shuffleId, shuffleMergeId, reduceId, pushMergedBlockMeta)
        }
      })
    val remoteMergedBlockMgrId = BlockManagerId(
      SHUFFLE_MERGER_IDENTIFIER, "remote-host-2", 1)
    val iterator = createShuffleBlockIteratorWithDefaults(
      Map(remoteMergedBlockMgrId -> toBlockList(
        Seq(ShuffleMergedBlockId(0, 0, 2)), 2L, SHUFFLE_PUSH_MAP_ID)))
    val (id1, _) = iterator.next()
    blocksSem.acquire(2)
    assert(id1 === ShuffleBlockId(0, 0, 2))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockId(0, 1, 2))
    assert(!iterator.hasNext)
  }

  test("SPARK-32922: failure to fetch a remote shuffle chunk initiates the fallback of " +
    "pending shuffle chunks immediately") {
    val blockChunks = Map[BlockId, ManagedBuffer](
      ShuffleBlockChunkId(0, 0, 2, 0) -> createMockManagedBuffer(),
      // ShuffleBlockChunk(0, 2, 1) will cause a failure as it is not in block-chunks.
      ShuffleBlockChunkId(0, 0, 2, 2) -> createMockManagedBuffer(),
      ShuffleBlockChunkId(0, 0, 2, 3) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 3, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 5, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 6, 2) -> createMockManagedBuffer()
    )
    val blocksSem = new Semaphore(0)
    configureMockTransferForPushShuffle(blocksSem, blockChunks)

    val metaSem = new Semaphore(0)
    val pushMergedBlockMeta = mock(classOf[MergedBlockMeta])
    when(pushMergedBlockMeta.getNumChunks).thenReturn(4)
    when(pushMergedBlockMeta.getChunksBitmapBuffer).thenReturn(mock(classOf[ManagedBuffer]))
    val roaringBitmaps = Array.fill[RoaringBitmap](4)(new RoaringBitmap)
    when(pushMergedBlockMeta.readChunkBitmaps()).thenReturn(roaringBitmaps)
    when(transfer.getMergedBlockMeta(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val metaListener = invocation.getArguments()(5).asInstanceOf[MergedBlocksMetaListener]
        val shuffleId = invocation.getArguments()(2).asInstanceOf[Int]
        val shuffleMergeId = invocation.getArguments()(3).asInstanceOf[Int]
        val reduceId = invocation.getArguments()(4).asInstanceOf[Int]
        Future {
          logInfo(s"acquiring semaphore for host = ${invocation.getArguments()(0)}, " +
            s"port = ${invocation.getArguments()(1)}, " +
            s"shuffleId = $shuffleId, shuffleMergeId = $shuffleMergeId, reduceId = $reduceId")
          metaSem.release()
          metaListener.onSuccess(shuffleId, shuffleMergeId, reduceId, pushMergedBlockMeta)
        }
      })
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val fallbackBlocksByAddr = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (remoteBmId, toBlockList(Seq(ShuffleBlockId(0, 3, 2), ShuffleBlockId(0, 4, 2),
        ShuffleBlockId(0, 5, 2), ShuffleBlockId(0, 6, 2)), 1L, 1)))
    when(mapOutputTracker.getMapSizesForMergeResult(any(), any(), any()))
      .thenReturn(fallbackBlocksByAddr.iterator)

    val iterator = createShuffleBlockIteratorWithDefaults(Map(
      BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, "test-client-1", 2) ->
        toBlockList(Seq(ShuffleMergedBlockId(0, 0, 2)),
          16L, SHUFFLE_PUSH_MAP_ID)),
      maxBytesInFlight = 4)
    metaSem.acquire(1)
    val (id1, _) = iterator.next()
    blocksSem.acquire(1)
    assert(id1 === ShuffleBlockChunkId(0, 0, 2, 0))
    val regularBlocks = new mutable.HashSet[BlockId]()
    val (id2, _) = iterator.next()
    blocksSem.acquire(1)
    regularBlocks.add(id2)
    val (id3, _) = iterator.next()
    blocksSem.acquire(1)
    regularBlocks.add(id3)
    val (id4, _) = iterator.next()
    blocksSem.acquire(1)
    regularBlocks.add(id4)
    val (id5, _) = iterator.next()
    blocksSem.acquire(1)
    regularBlocks.add(id5)
    assert(!iterator.hasNext)
    assert(regularBlocks === Set(ShuffleBlockId(0, 3, 2), ShuffleBlockId(0, 4, 2),
      ShuffleBlockId(0, 5, 2), ShuffleBlockId(0, 6, 2)))
  }

  test("SPARK-32922: failure to fetch a remote shuffle chunk initiates the fallback of " +
    "pending shuffle chunks immediately which got deferred") {
    val blockChunks = Map[BlockId, ManagedBuffer](
      ShuffleBlockChunkId(0, 0, 2, 0) -> createMockManagedBuffer(),
      ShuffleBlockChunkId(0, 0, 2, 1) -> createMockManagedBuffer(),
      ShuffleBlockChunkId(0, 0, 2, 2) -> createMockManagedBuffer(),
      // ShuffleBlockChunkId(0, 2, 3) will cause failure as it is not in bock chunks
      ShuffleBlockChunkId(0, 0, 2, 4) -> createMockManagedBuffer(),
      ShuffleBlockChunkId(0, 0, 2, 5) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 3, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 5, 2) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 6, 2) -> createMockManagedBuffer()
    )
    val blocksSem = new Semaphore(0)
    configureMockTransferForPushShuffle(blocksSem, blockChunks)
    val metaSem = new Semaphore(0)
    val pushMergedBlockMeta = mock(classOf[MergedBlockMeta])
    when(pushMergedBlockMeta.getNumChunks).thenReturn(6)
    when(pushMergedBlockMeta.getChunksBitmapBuffer).thenReturn(mock(classOf[ManagedBuffer]))
    val roaringBitmaps = Array.fill[RoaringBitmap](6)(new RoaringBitmap)
    when(pushMergedBlockMeta.readChunkBitmaps()).thenReturn(roaringBitmaps)
    when(transfer.getMergedBlockMeta(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val metaListener = invocation.getArguments()(5).asInstanceOf[MergedBlocksMetaListener]
        val shuffleId = invocation.getArguments()(2).asInstanceOf[Int]
        val shuffleMergeId = invocation.getArguments()(3).asInstanceOf[Int]
        val reduceId = invocation.getArguments()(4).asInstanceOf[Int]
        Future {
          logInfo(s"acquiring semaphore for host = ${invocation.getArguments()(0)}, " +
            s"port = ${invocation.getArguments()(1)}, " +
            s"shuffleId = $shuffleId, shuffleMergeId = $shuffleMergeId, reduceId = $reduceId")
          metaSem.release()
          metaListener.onSuccess(shuffleId, shuffleMergeId, reduceId, pushMergedBlockMeta)
        }
      })
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val fallbackBlocksByAddr = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (remoteBmId, toBlockList(Seq(ShuffleBlockId(0, 3, 2), ShuffleBlockId(0, 4, 2),
      ShuffleBlockId(0, 5, 2), ShuffleBlockId(0, 6, 2)), 1L, 1)))
    when(mapOutputTracker.getMapSizesForMergeResult(any(), any(), any()))
      .thenReturn(fallbackBlocksByAddr.iterator)

    val iterator = createShuffleBlockIteratorWithDefaults(Map(
      BlockManagerId(SHUFFLE_MERGER_IDENTIFIER, "test-client-1", 2) ->
        toBlockList(Seq(ShuffleMergedBlockId(0, 0, 2)), 24L,
          SHUFFLE_PUSH_MAP_ID)),
      maxBytesInFlight = 8, maxBlocksInFlightPerAddress = 1)
    metaSem.acquire(1)
    val (id1, _) = iterator.next()
    blocksSem.acquire(2)
    assert(id1 === ShuffleBlockChunkId(0, 0, 2, 0))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockChunkId(0, 0, 2, 1))
    val (id3, _) = iterator.next()
    blocksSem.acquire(1)
    assert(id3 === ShuffleBlockChunkId(0, 0, 2, 2))
    val regularBlocks = new mutable.HashSet[BlockId]()
    val (id4, _) = iterator.next()
    blocksSem.acquire(1)
    regularBlocks.add(id4)
    val (id5, _) = iterator.next()
    blocksSem.acquire(1)
    regularBlocks.add(id5)
    val (id6, _) = iterator.next()
    blocksSem.acquire(1)
    regularBlocks.add(id6)
    val (id7, _) = iterator.next()
    blocksSem.acquire(1)
    regularBlocks.add(id7)
    assert(!iterator.hasNext)
    assert(regularBlocks === Set[ShuffleBlockId](ShuffleBlockId(0, 3, 2), ShuffleBlockId(0, 4, 2),
      ShuffleBlockId(0, 5, 2), ShuffleBlockId(0, 6, 2)))
  }

}
