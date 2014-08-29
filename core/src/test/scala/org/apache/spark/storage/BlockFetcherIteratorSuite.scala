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

import java.io.IOException
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest.{FunSuite, Matchers}

import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.storage.BlockFetcherIterator._
import org.apache.spark.network.{ConnectionManager, Message}
import org.apache.spark.executor.ShuffleReadMetrics

class BlockFetcherIteratorSuite extends FunSuite with Matchers {

  test("block fetch from local fails using BasicBlockFetcherIterator") {
    val blockManager = mock(classOf[BlockManager])
    val connManager = mock(classOf[ConnectionManager])
    doReturn(connManager).when(blockManager).connectionManager
    doReturn(BlockManagerId("test-client", "test-client", 1)).when(blockManager).blockManagerId

    doReturn((48 * 1024 * 1024).asInstanceOf[Long]).when(blockManager).maxBytesInFlight

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
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(0)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(1)), any())
    doAnswer(answer).when(blockManager).getLocalFromDisk(meq(blIds(2)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(3)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(4)), any())

    val bmId = BlockManagerId("test-client", "test-client", 1)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, blIds.map(blId => (blId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new BasicBlockFetcherIterator(blockManager, blocksByAddress, null,
      new ShuffleReadMetrics())

    iterator.initialize()

    // Without exhausting the iterator, the iterator should be lazy and not call getLocalFromDisk.
    verify(blockManager, times(0)).getLocalFromDisk(any(), any())

    assert(iterator.hasNext, "iterator should have 5 elements but actually has no elements")
    // the 2nd element of the tuple returned by iterator.next should be defined when fetching successfully
    assert(iterator.next()._2.isDefined, "1st element should be defined but is not actually defined")
    verify(blockManager, times(1)).getLocalFromDisk(any(), any())

    assert(iterator.hasNext, "iterator should have 5 elements but actually has 1 element")
    assert(iterator.next()._2.isDefined, "2nd element should be defined but is not actually defined")
    verify(blockManager, times(2)).getLocalFromDisk(any(), any())

    assert(iterator.hasNext, "iterator should have 5 elements but actually has 2 elements")
    // 3rd fetch should be failed
    intercept[Exception] {
      iterator.next()
    }
    verify(blockManager, times(3)).getLocalFromDisk(any(), any())
  }


  test("block fetch from local succeed using BasicBlockFetcherIterator") {
    val blockManager = mock(classOf[BlockManager])
    val connManager = mock(classOf[ConnectionManager])
    doReturn(connManager).when(blockManager).connectionManager
    doReturn(BlockManagerId("test-client", "test-client", 1)).when(blockManager).blockManagerId

    doReturn((48 * 1024 * 1024).asInstanceOf[Long]).when(blockManager).maxBytesInFlight

    val blIds = Array[BlockId](
      ShuffleBlockId(0,0,0),
      ShuffleBlockId(0,1,0),
      ShuffleBlockId(0,2,0),
      ShuffleBlockId(0,3,0),
      ShuffleBlockId(0,4,0))

    val optItr = mock(classOf[Option[Iterator[Any]]])
 
   // All blocks should be fetched successfully
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(0)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(1)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(2)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(3)), any())
    doReturn(optItr).when(blockManager).getLocalFromDisk(meq(blIds(4)), any())

    val bmId = BlockManagerId("test-client", "test-client", 1)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, blIds.map(blId => (blId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new BasicBlockFetcherIterator(blockManager, blocksByAddress, null,
      new ShuffleReadMetrics())

    iterator.initialize()

    // Without exhausting the iterator, the iterator should be lazy and not call getLocalFromDisk.
    verify(blockManager, times(0)).getLocalFromDisk(any(), any())

    assert(iterator.hasNext, "iterator should have 5 elements but actually has no elements")
    assert(iterator.next._2.isDefined, "All elements should be defined but 1st element is not actually defined") 
    assert(iterator.hasNext, "iterator should have 5 elements but actually has 1 element")
    assert(iterator.next._2.isDefined, "All elements should be defined but 2nd element is not actually defined") 
    assert(iterator.hasNext, "iterator should have 5 elements but actually has 2 elements")
    assert(iterator.next._2.isDefined, "All elements should be defined but 3rd element is not actually defined") 
    assert(iterator.hasNext, "iterator should have 5 elements but actually has 3 elements")
    assert(iterator.next._2.isDefined, "All elements should be defined but 4th element is not actually defined") 
    assert(iterator.hasNext, "iterator should have 5 elements but actually has 4 elements")
    assert(iterator.next._2.isDefined, "All elements should be defined but 5th element is not actually defined")

    verify(blockManager, times(5)).getLocalFromDisk(any(), any())
  }

  test("block fetch from remote fails using BasicBlockFetcherIterator") {
    val blockManager = mock(classOf[BlockManager])
    val connManager = mock(classOf[ConnectionManager])
    when(blockManager.connectionManager).thenReturn(connManager)

    val f = future {
      throw new IOException("Send failed or we received an error ACK")
    }
    when(connManager.sendMessageReliably(any(),
      any())).thenReturn(f)
    when(blockManager.futureExecContext).thenReturn(global)

    when(blockManager.blockManagerId).thenReturn(
      BlockManagerId("test-client", "test-client", 1))
    when(blockManager.maxBytesInFlight).thenReturn(48 * 1024 * 1024)

    val blId1 = ShuffleBlockId(0,0,0)
    val blId2 = ShuffleBlockId(0,1,0)
    val bmId = BlockManagerId("test-server", "test-server", 1)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, Seq((blId1, 1L), (blId2, 1L)))
    )

    val iterator = new BasicBlockFetcherIterator(blockManager,
      blocksByAddress, null, new ShuffleReadMetrics())

    iterator.initialize()
    iterator.foreach{
      case (_, r) => {
        (!r.isDefined) should be(true)
      }
    }
  }

  test("block fetch from remote succeed using BasicBlockFetcherIterator") {
    val blockManager = mock(classOf[BlockManager])
    val connManager = mock(classOf[ConnectionManager])
    when(blockManager.connectionManager).thenReturn(connManager)

    val blId1 = ShuffleBlockId(0,0,0)
    val blId2 = ShuffleBlockId(0,1,0)
    val buf1 = ByteBuffer.allocate(4)
    val buf2 = ByteBuffer.allocate(4)
    buf1.putInt(1)
    buf1.flip()
    buf2.putInt(1)
    buf2.flip()
    val blockMessage1 = BlockMessage.fromGotBlock(GotBlock(blId1, buf1))
    val blockMessage2 = BlockMessage.fromGotBlock(GotBlock(blId2, buf2))
    val blockMessageArray = new BlockMessageArray(
      Seq(blockMessage1, blockMessage2))

    val bufferMessage = blockMessageArray.toBufferMessage
    val buffer = ByteBuffer.allocate(bufferMessage.size)
    val arrayBuffer = new ArrayBuffer[ByteBuffer]
    bufferMessage.buffers.foreach{ b =>
      buffer.put(b)
    }
    buffer.flip()
    arrayBuffer += buffer

    val f = future {
      Message.createBufferMessage(arrayBuffer)
    }
    when(connManager.sendMessageReliably(any(),
      any())).thenReturn(f)
    when(blockManager.futureExecContext).thenReturn(global)

    when(blockManager.blockManagerId).thenReturn(
      BlockManagerId("test-client", "test-client", 1))
    when(blockManager.maxBytesInFlight).thenReturn(48 * 1024 * 1024)

    val bmId = BlockManagerId("test-server", "test-server", 1)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, Seq((blId1, 1L), (blId2, 1L)))
    )

    val iterator = new BasicBlockFetcherIterator(blockManager,
      blocksByAddress, null, new ShuffleReadMetrics())
    iterator.initialize()
    iterator.foreach{
      case (_, r) => {
        (r.isDefined) should be(true)
      }
    }
  }
}
