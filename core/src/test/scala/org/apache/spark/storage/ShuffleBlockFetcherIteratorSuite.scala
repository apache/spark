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

import java.util.concurrent.Semaphore

import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.{TaskContextImpl, TaskContext}
import org.apache.spark.network.{BlockFetchingListener, BlockTransferService}

import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.scalatest.FunSuite

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.TestSerializer


class ShuffleBlockFetcherIteratorSuite extends FunSuite {
  // Some of the tests are quite tricky because we are testing the cleanup behavior
  // in the presence of faults.

  /** Creates a mock [[BlockTransferService]] that returns data from the given map. */
  private def createMockTransfer(data: Map[BlockId, ManagedBuffer]): BlockTransferService = {
    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val blocks = invocation.getArguments()(2).asInstanceOf[Seq[String]]
        val listener = invocation.getArguments()(3).asInstanceOf[BlockFetchingListener]

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

  private val conf = new SparkConf

  test("successful 3 local reads + 2 remote reads") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure blockManager.getBlockData would return the blocks
    val localBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> mock(classOf[ManagedBuffer]),
      ShuffleBlockId(0, 1, 0) -> mock(classOf[ManagedBuffer]),
      ShuffleBlockId(0, 2, 0) -> mock(classOf[ManagedBuffer]))
    localBlocks.foreach { case (blockId, buf) =>
      doReturn(buf).when(blockManager).getBlockData(meq(blockId))
    }

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val remoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 3, 0) -> mock(classOf[ManagedBuffer]),
      ShuffleBlockId(0, 4, 0) -> mock(classOf[ManagedBuffer])
    )

    val transfer = createMockTransfer(remoteBlocks)

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (localBmId, localBlocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq),
      (remoteBmId, remoteBlocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new ShuffleBlockFetcherIterator(
      new TaskContextImpl(0, 0, 0),
      transfer,
      blockManager,
      blocksByAddress,
      new TestSerializer,
      48 * 1024 * 1024)

    // 3 local blocks fetched in initialization
    verify(blockManager, times(3)).getBlockData(any())

    for (i <- 0 until 5) {
      assert(iterator.hasNext, s"iterator should have 5 elements but actually has $i elements")
      val (blockId, subIterator) = iterator.next()
      assert(subIterator.isDefined,
        s"iterator should have 5 elements defined but actually has $i elements")

      // Make sure we release the buffer once the iterator is exhausted.
      val mockBuf = localBlocks.getOrElse(blockId, remoteBlocks(blockId))
      verify(mockBuf, times(0)).release()
      subIterator.get.foreach(_ => Unit)  // exhaust the iterator
      verify(mockBuf, times(1)).release()
    }

    // 3 local blocks, and 2 remote blocks
    // (but from the same block manager so one call to fetchBlocks)
    verify(blockManager, times(3)).getBlockData(any())
    verify(transfer, times(1)).fetchBlocks(any(), any(), any(), any())
  }

  test("release current unexhausted buffer in case the task completes early") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> mock(classOf[ManagedBuffer]),
      ShuffleBlockId(0, 1, 0) -> mock(classOf[ManagedBuffer]),
      ShuffleBlockId(0, 2, 0) -> mock(classOf[ManagedBuffer])
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(3).asInstanceOf[BlockFetchingListener]
        future {
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

    val taskContext = new TaskContextImpl(0, 0, 0)
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      new TestSerializer,
      48 * 1024 * 1024)

    // Exhaust the first block, and then it should be released.
    iterator.next()._2.get.foreach(_ => Unit)
    verify(blocks(ShuffleBlockId(0, 0, 0)), times(1)).release()

    // Get the 2nd block but do not exhaust the iterator
    val subIter = iterator.next()._2.get

    // Complete the task; then the 2nd block buffer should be exhausted
    verify(blocks(ShuffleBlockId(0, 1, 0)), times(0)).release()
    taskContext.markTaskCompleted()
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
      ShuffleBlockId(0, 0, 0) -> mock(classOf[ManagedBuffer]),
      ShuffleBlockId(0, 1, 0) -> mock(classOf[ManagedBuffer]),
      ShuffleBlockId(0, 2, 0) -> mock(classOf[ManagedBuffer])
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(3).asInstanceOf[BlockFetchingListener]
        future {
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

    val taskContext = new TaskContextImpl(0, 0, 0)
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      new TestSerializer,
      48 * 1024 * 1024)

    // Continue only after the mock calls onBlockFetchFailure
    sem.acquire()

    // The first block should be defined, and the last two are not defined (due to failure)
    assert(iterator.next()._2.isDefined === true)
    assert(iterator.next()._2.isDefined === false)
    assert(iterator.next()._2.isDefined === false)
  }
}
