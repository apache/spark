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

import org.scalatest.{FunSuite, Matchers}

import org.mockito.Mockito.{mock, when}
import org.mockito.Matchers.any

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark._
import org.apache.spark.storage.BlockFetcherIterator._
import org.apache.spark.network.{ConnectionManager, ConnectionManagerId,
                                 Message}

class BlockFetcherIteratorSuite extends FunSuite with Matchers {

  test("block fetch from remote fails using BasicBlockFetcherIterator") {
    val blockManager = mock(classOf[BlockManager])
    val connManager = mock(classOf[ConnectionManager])
    when(blockManager.connectionManager).thenReturn(connManager)

    val f = future {
      val message = Message.createBufferMessage(0)
      message.hasError = true
      val someMessage = Some(message)
      someMessage
    }
    when(connManager.sendMessageReliably(any(),
      any())).thenReturn(f)
    when(blockManager.futureExecContext).thenReturn(global)

    when(blockManager.blockManagerId).thenReturn(
      BlockManagerId("test-client", "test-client", 1, 0))
    when(blockManager.maxBytesInFlight).thenReturn(48 * 1024 * 1024)

    val blId1 = ShuffleBlockId(0,0,0)
    val blId2 = ShuffleBlockId(0,1,0)
    val bmId = BlockManagerId("test-server", "test-server",1 , 0)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, Seq((blId1, 1), (blId2, 1)))
    )

    val iterator = new BasicBlockFetcherIterator(blockManager,
      blocksByAddress, null)

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

    val someMessage = Some(Message.createBufferMessage(arrayBuffer))

    val f = future {
      someMessage
    }
    when(connManager.sendMessageReliably(any(),
      any())).thenReturn(f)
    when(blockManager.futureExecContext).thenReturn(global)
  
    when(blockManager.blockManagerId).thenReturn(
      BlockManagerId("test-client", "test-client", 1, 0))
    when(blockManager.maxBytesInFlight).thenReturn(48 * 1024 * 1024)

    val bmId = BlockManagerId("test-server", "test-server",1 , 0)
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (bmId, Seq((blId1, 1), (blId2, 1)))
    )

    val iterator = new BasicBlockFetcherIterator(blockManager,
      blocksByAddress, null)
    iterator.initialize()
    iterator.foreach{
      case (_, r) => {
        (r.isDefined) should be(true)
      }
    }
  }
}
