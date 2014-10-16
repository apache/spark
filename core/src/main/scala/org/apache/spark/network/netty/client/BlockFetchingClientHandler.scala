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

import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import org.apache.spark.Logging


/**
 * Handler that processes server responses. It uses the protocol documented in
 * [[org.apache.spark.network.netty.server.BlockServer]].
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
private[client]
class BlockFetchingClientHandler extends SimpleChannelInboundHandler[ByteBuf] with Logging {

  /** Tracks the list of outstanding requests and their listeners on success/failure. */
  private val outstandingRequests = java.util.Collections.synchronizedMap {
    new java.util.HashMap[String, BlockClientListener]
  }

  def addRequest(blockId: String, listener: BlockClientListener): Unit = {
    outstandingRequests.put(blockId, listener)
  }

  def removeRequest(blockId: String): Unit = {
    outstandingRequests.remove(blockId)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    val errorMsg = s"Exception in connection from ${ctx.channel.remoteAddress}: ${cause.getMessage}"
    logError(errorMsg, cause)

    // Fire the failure callback for all outstanding blocks
    outstandingRequests.synchronized {
      val iter = outstandingRequests.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        entry.getValue.onFetchFailure(entry.getKey, errorMsg)
      }
      outstandingRequests.clear()
    }

    ctx.close()
  }

  override def channelRead0(ctx: ChannelHandlerContext, in: ByteBuf) {
    val totalLen = in.readInt()
    val blockIdLen = in.readInt()
    val blockIdBytes = new Array[Byte](math.abs(blockIdLen))
    in.readBytes(blockIdBytes)
    val blockId = new String(blockIdBytes)
    val blockSize = totalLen - math.abs(blockIdLen) - 4

    def server = ctx.channel.remoteAddress.toString

    // blockIdLen is negative when it is an error message.
    if (blockIdLen < 0) {
      val errorMessageBytes = new Array[Byte](blockSize)
      in.readBytes(errorMessageBytes)
      val errorMsg = new String(errorMessageBytes)
      logTrace(s"Received block $blockId ($blockSize B) with error $errorMsg from $server")

      val listener = outstandingRequests.get(blockId)
      if (listener == null) {
        // Ignore callback
        logWarning(s"Got a response for block $blockId but it is not in our outstanding requests")
      } else {
        outstandingRequests.remove(blockId)
        listener.onFetchFailure(blockId, errorMsg)
      }
    } else {
      logTrace(s"Received block $blockId ($blockSize B) from $server")

      val listener = outstandingRequests.get(blockId)
      if (listener == null) {
        // Ignore callback
        logWarning(s"Got a response for block $blockId but it is not in our outstanding requests")
      } else {
        outstandingRequests.remove(blockId)
        listener.onFetchSuccess(blockId, new ReferenceCountedBuffer(in))
      }
    }
  }
}
