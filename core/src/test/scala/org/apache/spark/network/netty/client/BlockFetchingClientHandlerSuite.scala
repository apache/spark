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

package org.apache.spark.network.netty.client

import java.nio.ByteBuffer

import com.google.common.base.Charsets.UTF_8
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel

import org.scalatest.{PrivateMethodTester, FunSuite}


class BlockFetchingClientHandlerSuite extends FunSuite with PrivateMethodTester {

  test("handling block data (successful fetch)") {
    val blockId = "test_block"
    val blockData = "blahblahblahblahblah"
    val totalLength = 4 + blockId.length + blockData.length

    var parsedBlockId: String = ""
    var parsedBlockData: String = ""
    val handler = new BlockFetchingClientHandler
    handler.addRequest(blockId,
      new BlockClientListener {
        override def onFetchFailure(blockId: String, errorMsg: String): Unit = ???
        override def onFetchSuccess(bid: String, refCntBuf: ReferenceCountedBuffer): Unit = {
          parsedBlockId = bid
          val bytes = new Array[Byte](refCntBuf.byteBuffer().remaining)
          refCntBuf.byteBuffer().get(bytes)
          parsedBlockData = new String(bytes, UTF_8)
        }
      }
    )

    val outstandingRequests = PrivateMethod[java.util.Map[_, _]]('outstandingRequests)
    assert(handler.invokePrivate(outstandingRequests()).size === 1)

    val channel = new EmbeddedChannel(handler)
    val buf = ByteBuffer.allocate(totalLength + 4)  // 4 bytes for the length field itself
    buf.putInt(totalLength)
    buf.putInt(blockId.length)
    buf.put(blockId.getBytes)
    buf.put(blockData.getBytes)
    buf.flip()

    channel.writeInbound(Unpooled.wrappedBuffer(buf))
    assert(parsedBlockId === blockId)
    assert(parsedBlockData === blockData)

    assert(handler.invokePrivate(outstandingRequests()).size === 0)

    channel.close()
  }

  test("handling error message (failed fetch)") {
    val blockId = "test_block"
    val errorMsg = "error erro5r error err4or error3 error6 error erro1r"
    val totalLength = 4 + blockId.length + errorMsg.length

    var parsedBlockId: String = ""
    var parsedErrorMsg: String = ""
    val handler = new BlockFetchingClientHandler
    handler.addRequest(blockId, new BlockClientListener {
      override def onFetchFailure(bid: String, msg: String) ={
        parsedBlockId = bid
        parsedErrorMsg = msg
      }
      override def onFetchSuccess(bid: String, refCntBuf: ReferenceCountedBuffer) = ???
    })

    val outstandingRequests = PrivateMethod[java.util.Map[_, _]]('outstandingRequests)
    assert(handler.invokePrivate(outstandingRequests()).size === 1)

    val channel = new EmbeddedChannel(handler)
    val buf = ByteBuffer.allocate(totalLength + 4)  // 4 bytes for the length field itself
    buf.putInt(totalLength)
    buf.putInt(-blockId.length)
    buf.put(blockId.getBytes)
    buf.put(errorMsg.getBytes)
    buf.flip()

    channel.writeInbound(Unpooled.wrappedBuffer(buf))
    assert(parsedBlockId === blockId)
    assert(parsedErrorMsg === errorMsg)

    assert(handler.invokePrivate(outstandingRequests()).size === 0)

    channel.close()
  }
}
