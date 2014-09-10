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

import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.scalatest.FunSuite

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.network._
import org.apache.spark.serializer.TestSerializer


class ShuffleBlockFetcherIteratorSuite extends FunSuite {

  val conf = new SparkConf

  test("handle successful local reads") {
    val buf = mock(classOf[ManagedBuffer])
    val blockManager = mock(classOf[BlockManager])
    doReturn(BlockManagerId("test-client", "test-client", 1)).when(blockManager).blockManagerId

    val blockIds = Array[BlockId](
      ShuffleBlockId(0, 0, 0),
      ShuffleBlockId(0, 1, 0),
      ShuffleBlockId(0, 2, 0),
      ShuffleBlockId(0, 3, 0),
      ShuffleBlockId(0, 4, 0))

    // All blocks should be fetched successfully
    blockIds.foreach { blockId =>
      doReturn(buf).when(blockManager).getBlockData(meq(blockId.toString))
    }

    val bmId = BlockManagerId("test-client", "test-client", 1)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, blockIds.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new ShuffleBlockFetcherIterator(
      new TaskContext(0, 0, 0),
      mock(classOf[BlockTransferService]),
      blockManager,
      blocksByAddress,
      new TestSerializer,
      48 * 1024 * 1024)

    // Local blocks are fetched immediately.
    verify(blockManager, times(5)).getBlockData(any())

    for (i <- 0 until 5) {
      assert(iterator.hasNext, s"iterator should have 5 elements but actually has $i elements")
      assert(iterator.next()._2.isDefined,
        s"iterator should have 5 elements defined but actually has $i elements")
    }
    // No more fetching of local blocks.
    verify(blockManager, times(5)).getBlockData(any())
  }

  test("handle remote fetch failures in BlockTransferService") {
    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(3).asInstanceOf[BlockFetchingListener]
        listener.onBlockFetchFailure(new Exception("blah"))
      }
    })

    val blockManager = mock(classOf[BlockManager])

    when(blockManager.blockManagerId).thenReturn(BlockManagerId("test-client", "test-client", 1))

    val blId1 = ShuffleBlockId(0, 0, 0)
    val blId2 = ShuffleBlockId(0, 1, 0)
    val bmId = BlockManagerId("test-server", "test-server", 1)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, Seq((blId1, 1L), (blId2, 1L))))

    val iterator = new ShuffleBlockFetcherIterator(
      new TaskContext(0, 0, 0),
      transfer,
      blockManager,
      blocksByAddress,
      new TestSerializer,
      48 * 1024 * 1024)

    iterator.foreach { case (_, iterOption) =>
      assert(!iterOption.isDefined)
    }
  }
}
