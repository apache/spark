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

import org.apache.spark.{TaskContextImpl, TaskContext}
import org.apache.spark.network.{BlockFetchingListener, BlockTransferService}

import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.scalatest.FunSuite


class ShuffleBlockFetcherIteratorSuite extends FunSuite {

  test("handle local read failures in BlockManager") {
    val transfer = mock(classOf[BlockTransferService])
    val blockManager = mock(classOf[BlockManager])
    doReturn(BlockManagerId("test-client", "test-client", 1)).when(blockManager).blockManagerId

    val blIds = Array[BlockId](
      ShuffleBlockId(0,0,0),
      ShuffleBlockId(0,1,0),
      ShuffleBlockId(0,2,0),
      ShuffleBlockId(0,3,0),
      ShuffleBlockId(0,4,0))

    val optItr = mock(classOf[Option[Iterator[Any]]])
    val answer = new Answer[Option[Iterator[Any]]] {
      override def answer(invocation: InvocationOnMock) = Option[Iterator[Any]] {
        throw new Exception
      }
    }

    // 3rd block is going to fail
    doReturn(optItr).when(blockManager).getLocalShuffleFromDisk(meq(blIds(0)), any())
    doReturn(optItr).when(blockManager).getLocalShuffleFromDisk(meq(blIds(1)), any())
    doAnswer(answer).when(blockManager).getLocalShuffleFromDisk(meq(blIds(2)), any())
    doReturn(optItr).when(blockManager).getLocalShuffleFromDisk(meq(blIds(3)), any())
    doReturn(optItr).when(blockManager).getLocalShuffleFromDisk(meq(blIds(4)), any())

    val bmId = BlockManagerId("test-client", "test-client", 1)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, blIds.map(blId => (blId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new ShuffleBlockFetcherIterator(
      new TaskContextImpl(0, 0, 0),
      transfer,
      blockManager,
      blocksByAddress,
      null,
      48 * 1024 * 1024)

    // Without exhausting the iterator, the iterator should be lazy and not call
    // getLocalShuffleFromDisk.
    verify(blockManager, times(0)).getLocalShuffleFromDisk(any(), any())

    assert(iterator.hasNext, "iterator should have 5 elements but actually has no elements")
    // the 2nd element of the tuple returned by iterator.next should be defined when
    // fetching successfully
    assert(iterator.next()._2.isDefined,
      "1st element should be defined but is not actually defined")
    verify(blockManager, times(1)).getLocalShuffleFromDisk(any(), any())

    assert(iterator.hasNext, "iterator should have 5 elements but actually has 1 element")
    assert(iterator.next()._2.isDefined,
      "2nd element should be defined but is not actually defined")
    verify(blockManager, times(2)).getLocalShuffleFromDisk(any(), any())

    assert(iterator.hasNext, "iterator should have 5 elements but actually has 2 elements")
    // 3rd fetch should be failed
    intercept[Exception] {
      iterator.next()
    }
    verify(blockManager, times(3)).getLocalShuffleFromDisk(any(), any())
  }

  test("handle local read successes") {
    val transfer = mock(classOf[BlockTransferService])
    val blockManager = mock(classOf[BlockManager])
    doReturn(BlockManagerId("test-client", "test-client", 1)).when(blockManager).blockManagerId

    val blIds = Array[BlockId](
      ShuffleBlockId(0,0,0),
      ShuffleBlockId(0,1,0),
      ShuffleBlockId(0,2,0),
      ShuffleBlockId(0,3,0),
      ShuffleBlockId(0,4,0))

    val optItr = mock(classOf[Option[Iterator[Any]]])

    // All blocks should be fetched successfully
    doReturn(optItr).when(blockManager).getLocalShuffleFromDisk(meq(blIds(0)), any())
    doReturn(optItr).when(blockManager).getLocalShuffleFromDisk(meq(blIds(1)), any())
    doReturn(optItr).when(blockManager).getLocalShuffleFromDisk(meq(blIds(2)), any())
    doReturn(optItr).when(blockManager).getLocalShuffleFromDisk(meq(blIds(3)), any())
    doReturn(optItr).when(blockManager).getLocalShuffleFromDisk(meq(blIds(4)), any())

    val bmId = BlockManagerId("test-client", "test-client", 1)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, blIds.map(blId => (blId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new ShuffleBlockFetcherIterator(
      new TaskContextImpl(0, 0, 0),
      transfer,
      blockManager,
      blocksByAddress,
      null,
      48 * 1024 * 1024)

    // Without exhausting the iterator, the iterator should be lazy and not call getLocalShuffleFromDisk.
    verify(blockManager, times(0)).getLocalShuffleFromDisk(any(), any())

    assert(iterator.hasNext, "iterator should have 5 elements but actually has no elements")
    assert(iterator.next()._2.isDefined,
      "All elements should be defined but 1st element is not actually defined")
    assert(iterator.hasNext, "iterator should have 5 elements but actually has 1 element")
    assert(iterator.next()._2.isDefined,
      "All elements should be defined but 2nd element is not actually defined")
    assert(iterator.hasNext, "iterator should have 5 elements but actually has 2 elements")
    assert(iterator.next()._2.isDefined,
      "All elements should be defined but 3rd element is not actually defined")
    assert(iterator.hasNext, "iterator should have 5 elements but actually has 3 elements")
    assert(iterator.next()._2.isDefined,
      "All elements should be defined but 4th element is not actually defined")
    assert(iterator.hasNext, "iterator should have 5 elements but actually has 4 elements")
    assert(iterator.next()._2.isDefined,
      "All elements should be defined but 5th element is not actually defined")

    verify(blockManager, times(5)).getLocalShuffleFromDisk(any(), any())
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
      new TaskContextImpl(0, 0, 0),
      transfer,
      blockManager,
      blocksByAddress,
      null,
      48 * 1024 * 1024)

    iterator.foreach { case (_, iterOption) =>
      assert(!iterOption.isDefined)
    }
  }
}
