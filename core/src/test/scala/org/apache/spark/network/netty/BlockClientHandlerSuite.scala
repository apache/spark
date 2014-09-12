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

import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel

import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => meq}

import org.scalatest.{FunSuite, PrivateMethodTester}

import org.apache.spark.network._


class BlockClientHandlerSuite extends FunSuite with PrivateMethodTester {

  /** Helper method to get num. outstanding requests from a private field using reflection. */
  private def sizeOfOutstandingRequests(handler: BlockClientHandler): Int = {
    val f = handler.getClass.getDeclaredField(
      "org$apache$spark$network$netty$BlockClientHandler$$outstandingRequests")
    f.setAccessible(true)
    f.get(handler).asInstanceOf[java.util.Map[_, _]].size
  }

  test("handling block data (successful fetch)") {
    val blockId = "test_block"
    val blockData = "blahblahblahblahblah"
    val handler = new BlockClientHandler
    val listener = mock(classOf[BlockFetchingListener])
    handler.addRequest(blockId, listener)
    assert(sizeOfOutstandingRequests(handler) === 1)

    val channel = new EmbeddedChannel(handler)
    val buf = ByteBuffer.allocate(blockData.size)  // 4 bytes for the length field itself
    buf.put(blockData.getBytes)
    buf.flip()

    channel.writeInbound(BlockFetchSuccess(blockId, new NioManagedBuffer(buf)))
    verify(listener, times(1)).onBlockFetchSuccess(meq(blockId), any())
    assert(sizeOfOutstandingRequests(handler) === 0)
    assert(channel.finish() === false)
  }

  test("handling error message (failed fetch)") {
    val blockId = "test_block"
    val handler = new BlockClientHandler
    val listener = mock(classOf[BlockFetchingListener])
    handler.addRequest(blockId, listener)
    assert(sizeOfOutstandingRequests(handler) === 1)

    val channel = new EmbeddedChannel(handler)
    channel.writeInbound(BlockFetchFailure(blockId, "some error msg"))
    verify(listener, times(0)).onBlockFetchSuccess(any(), any())
    verify(listener, times(1)).onBlockFetchFailure(meq(blockId), any())
    assert(sizeOfOutstandingRequests(handler) === 0)
    assert(channel.finish() === false)
  }

  test("clear all outstanding request upon uncaught exception") {
    val handler = new BlockClientHandler
    val listener = mock(classOf[BlockFetchingListener])
    handler.addRequest("b1", listener)
    handler.addRequest("b2", listener)
    handler.addRequest("b3", listener)
    assert(sizeOfOutstandingRequests(handler) === 3)

    val channel = new EmbeddedChannel(handler)
    channel.writeInbound(BlockFetchSuccess("b1", new NettyManagedBuffer(Unpooled.buffer())))
    channel.pipeline().fireExceptionCaught(new Exception("duh duh duh"))

    // should fail both b2 and b3
    verify(listener, times(1)).onBlockFetchSuccess(any(), any())
    verify(listener, times(2)).onBlockFetchFailure(any(), any())
    assert(sizeOfOutstandingRequests(handler) === 0)
    assert(channel.finish() === false)
  }

  test("clear all outstanding request upon connection close") {
    val handler = new BlockClientHandler
    val listener = mock(classOf[BlockFetchingListener])
    handler.addRequest("c1", listener)
    handler.addRequest("c2", listener)
    handler.addRequest("c3", listener)
    assert(sizeOfOutstandingRequests(handler) === 3)

    val channel = new EmbeddedChannel(handler)
    channel.writeInbound(BlockFetchSuccess("c1", new NettyManagedBuffer(Unpooled.buffer())))
    channel.finish()

    // should fail both b2 and b3
    verify(listener, times(1)).onBlockFetchSuccess(any(), any())
    verify(listener, times(2)).onBlockFetchFailure(any(), any())
    assert(sizeOfOutstandingRequests(handler) === 0)
    assert(channel.finish() === false)
  }
}
