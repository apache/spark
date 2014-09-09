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

package org.apache.spark.network.netty

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel

import org.scalatest.{FunSuite, PrivateMethodTester}

import org.apache.spark.network._


class BlockClientHandlerSuite extends FunSuite with PrivateMethodTester {

  private def sizeOfOutstandingRequests(handler: BlockClientHandler): Int = {
    val outstandingRequests = PrivateMethod[java.util.Map[_, _]]('outstandingRequests)
    handler.invokePrivate(outstandingRequests()).size
  }

  test("handling block data (successful fetch)") {
    val blockId = "test_block"
    val blockData = "blahblahblahblahblah"

    var parsedBlockId: String = ""
    var parsedBlockData: String = ""
    val handler = new BlockClientHandler
    handler.addRequest(blockId,
      new BlockFetchingListener {
        override def onBlockFetchFailure(exception: Throwable): Unit = {
          throw new UnsupportedOperationException
        }
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          parsedBlockId = blockId
          val bytes = new Array[Byte](data.size.toInt)
          data.nioByteBuffer().get(bytes)
          parsedBlockData = new String(bytes)
        }
      }
    )

    val outstandingRequests = PrivateMethod[java.util.Map[_, _]]('outstandingRequests)
    assert(handler.invokePrivate(outstandingRequests()).size === 1)

    val channel = new EmbeddedChannel(handler)
    val buf = ByteBuffer.allocate(blockData.size)  // 4 bytes for the length field itself
    buf.put(blockData.getBytes)
    buf.flip()

    channel.writeInbound(BlockFetchSuccess(blockId, new NioByteBufferManagedBuffer(buf)))

    assert(parsedBlockId === blockId)
    assert(parsedBlockData === blockData)
    assert(handler.invokePrivate(outstandingRequests()).size === 0)
    assert(channel.finish() === false)
  }

  test("handling error message (failed fetch)") {
    val blockId = "test_block"
    val errorMsg = "error erro5r error err4or error3 error6 error erro1r"

    var parsedErrorMsg: String = ""
    val handler = new BlockClientHandler
    handler.addRequest(blockId,
      new BlockFetchingListener {
        override def onBlockFetchFailure(exception: Throwable): Unit = {
          parsedErrorMsg = exception.getMessage
        }

        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          throw new UnsupportedOperationException
        }
      }
    )

    assert(sizeOfOutstandingRequests(handler) === 1)

    val channel = new EmbeddedChannel(handler)
    channel.writeInbound(BlockFetchFailure(blockId, errorMsg))
    assert(parsedErrorMsg === errorMsg)
    assert(sizeOfOutstandingRequests(handler) === 0)
    assert(channel.finish() === false)
  }

  ignore("clear all outstanding request upon connection close") {
    val errorCount = new AtomicInteger(0)
    val successCount = new AtomicInteger(0)
    val handler = new BlockClientHandler

    val listener = new BlockFetchingListener {
      override def onBlockFetchFailure(exception: Throwable): Unit = {
        errorCount.incrementAndGet()
      }
      override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
        successCount.incrementAndGet()
      }
    }

    handler.addRequest("b1", listener)
    handler.addRequest("b2", listener)
    handler.addRequest("b3", listener)
    assert(sizeOfOutstandingRequests(handler) === 3)

    val channel = new EmbeddedChannel(handler)
    channel.writeInbound(BlockFetchSuccess("b1", new NettyByteBufManagedBuffer(Unpooled.buffer())))
    // Need to figure out a way to generate an exception
    assert(successCount.get() === 1)
    assert(errorCount.get() === 2)
    assert(sizeOfOutstandingRequests(handler) === 0)
    assert(channel.finish() === false)
  }
}
