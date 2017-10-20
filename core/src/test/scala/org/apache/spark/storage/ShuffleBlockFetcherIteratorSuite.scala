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

import java.io.{File, InputStream, IOException}
import java.util.UUID
import java.util.concurrent.Semaphore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkFunSuite, TaskContext}
import org.apache.spark.network._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, TempShuffleFileManager}
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils


class ShuffleBlockFetcherIteratorSuite extends SparkFunSuite with PrivateMethodTester {
  // Some of the tests are quite tricky because we are testing the cleanup behavior
  // in the presence of faults.

  /** Creates a mock [[BlockTransferService]] that returns data from the given map. */
  private def createMockTransfer(data: Map[BlockId, ManagedBuffer]): BlockTransferService = {
    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val blocks = invocation.getArguments()(3).asInstanceOf[Array[String]]
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]

        for (blockId <- blocks) {
          if (data.contains(BlockId(blockId))) {
            listener.onBlockFetchSuccess(blockId, data(BlockId(blockId)))
          } else {
            listener.onBlockFetchFailure(blockId, new BlockNotFoundException(blockId))
          }
        }
      }
    })
    transfer
  }

  // Create a mock managed buffer for testing
  def createMockManagedBuffer(): ManagedBuffer = {
    val mockManagedBuffer = mock(classOf[ManagedBuffer])
    val in = mock(classOf[InputStream])
    when(in.read(any())).thenReturn(1)
    when(in.read(any(), any(), any())).thenReturn(1)
    when(mockManagedBuffer.createInputStream()).thenReturn(in)
    mockManagedBuffer
  }

  test("successful 3 local reads + 2 remote reads") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure blockManager.getBlockData would return the blocks
    val localBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer())
    localBlocks.foreach { case (blockId, buf) =>
      doReturn(buf).when(blockManager).getBlockData(meq(blockId))
    }

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val remoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 3, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 0) -> createMockManagedBuffer())

    val transfer = createMockTransfer(remoteBlocks)

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (localBmId, localBlocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq),
      (remoteBmId, remoteBlocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new ShuffleBlockFetcherIterator(
      TaskContext.empty(),
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => in,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true)

    // 3 local blocks fetched in initialization
    verify(blockManager, times(3)).getBlockData(any())

    for (i <- 0 until 5) {
      assert(iterator.hasNext, s"iterator should have 5 elements but actually has $i elements")
      val (blockId, inputStream) = iterator.next()

      // Make sure we release buffers when a wrapped input stream is closed.
      val mockBuf = localBlocks.getOrElse(blockId, remoteBlocks(blockId))
      // Note: ShuffleBlockFetcherIterator wraps input streams in a BufferReleasingInputStream
      val wrappedInputStream = inputStream.asInstanceOf[BufferReleasingInputStream]
      verify(mockBuf, times(0)).release()
      val delegateAccess = PrivateMethod[InputStream]('delegate)

      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(0)).close()
      wrappedInputStream.close()
      verify(mockBuf, times(1)).release()
      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(1)).close()
      wrappedInputStream.close() // close should be idempotent
      verify(mockBuf, times(1)).release()
      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(1)).close()
    }

    // 3 local blocks, and 2 remote blocks
    // (but from the same block manager so one call to fetchBlocks)
    verify(blockManager, times(3)).getBlockData(any())
    verify(transfer, times(1)).fetchBlocks(any(), any(), any(), any(), any(), any())
  }

  test("release current unexhausted buffer in case the task completes early") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer())

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
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
    })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq))

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => in,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true)

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
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
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
    })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq))

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => in,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true)

    // Continue only after the mock calls onBlockFetchFailure
    sem.acquire()

    // The first block should be returned without an exception, and the last two should throw
    // FetchFailedExceptions (due to failure)
    iterator.next()
    intercept[FetchFailedException] { iterator.next() }
    intercept[FetchFailedException] { iterator.next() }
  }

  test("retry corrupt blocks") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    val corruptStream = mock(classOf[InputStream])
    when(corruptStream.read(any(), any(), any())).thenThrow(new IOException("corrupt"))
    val corruptBuffer = mock(classOf[ManagedBuffer])
    when(corruptBuffer.createInputStream()).thenReturn(corruptStream)
    val corruptLocalBuffer = new FileSegmentManagedBuffer(null, new File("a"), 0, 100)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        Future {
          // Return the first block, and then fail.
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 1, 0).toString, corruptBuffer)
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 2, 0).toString, corruptLocalBuffer)
          sem.release()
        }
      }
    })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq))

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => new LimitedInputStream(in, 100),
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true)

    // Continue only after the mock calls onBlockFetchFailure
    sem.acquire()

    // The first block should be returned without an exception
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 0))

    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        Future {
          // Return the first block, and then fail.
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 1, 0).toString, corruptBuffer)
          sem.release()
        }
      }
    })

    // The next block is corrupt local block (the second one is corrupt and retried)
    intercept[FetchFailedException] { iterator.next() }

    sem.acquire()
    intercept[FetchFailedException] { iterator.next() }
  }

  test("retry corrupt blocks (disabled)") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    val corruptStream = mock(classOf[InputStream])
    when(corruptStream.read(any(), any(), any())).thenThrow(new IOException("corrupt"))
    val corruptBuffer = mock(classOf[ManagedBuffer])
    when(corruptBuffer.createInputStream()).thenReturn(corruptStream)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        Future {
          // Return the first block, and then fail.
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 1, 0).toString, corruptBuffer)
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 2, 0).toString, corruptBuffer)
          sem.release()
        }
      }
    })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq))

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => new LimitedInputStream(in, 100),
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      false)

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
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

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
    val transfer = mock(classOf[BlockTransferService])
    var tempShuffleFileManager: TempShuffleFileManager = null
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
          tempShuffleFileManager = invocation.getArguments()(5).asInstanceOf[TempShuffleFileManager]
          Future {
            listener.onBlockFetchSuccess(
              ShuffleBlockId(0, 0, 0).toString, remoteBlocks(ShuffleBlockId(0, 0, 0)))
          }
        }
      })

    def fetchShuffleBlock(blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])]): Unit = {
      // Set `maxBytesInFlight` and `maxReqsInFlight` to `Int.MaxValue`, so that during the
      // construction of `ShuffleBlockFetcherIterator`, all requests to fetch remote shuffle blocks
      // are issued. The `maxReqSizeShuffleToMem` is hard-coded as 200 here.
      new ShuffleBlockFetcherIterator(
        TaskContext.empty(),
        transfer,
        blockManager,
        blocksByAddress,
        (_, in) => in,
        maxBytesInFlight = Int.MaxValue,
        maxReqsInFlight = Int.MaxValue,
        maxBlocksInFlightPerAddress = Int.MaxValue,
        maxReqSizeShuffleToMem = 200,
        detectCorrupt = true)
    }

    val blocksByAddress1 = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (remoteBmId, remoteBlocks.keys.map(blockId => (blockId, 100L)).toSeq))
    fetchShuffleBlock(blocksByAddress1)
    // `maxReqSizeShuffleToMem` is 200, which is greater than the block size 100, so don't fetch
    // shuffle block to disk.
    assert(tempShuffleFileManager == null)

    val blocksByAddress2 = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (remoteBmId, remoteBlocks.keys.map(blockId => (blockId, 300L)).toSeq))
    fetchShuffleBlock(blocksByAddress2)
    // `maxReqSizeShuffleToMem` is 200, which is smaller than the block size 300, so fetch
    // shuffle block to disk.
    assert(tempShuffleFileManager != null)
  }
}
