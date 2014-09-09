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

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import org.apache.spark.Logging
import org.apache.spark.network.BlockFetchingListener


/**
 * Handler that processes server responses, in response to requests issued from [[BlockClient]].
 * It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
private[netty]
class BlockClientHandler extends SimpleChannelInboundHandler[ServerResponse] with Logging {

  /** Tracks the list of outstanding requests and their listeners on success/failure. */
  private[this] val outstandingRequests = java.util.Collections.synchronizedMap {
    new java.util.HashMap[String, BlockFetchingListener]
  }

  def addRequest(blockId: String, listener: BlockFetchingListener): Unit = {
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
        entry.getValue.onBlockFetchFailure(cause)
      }
      outstandingRequests.clear()
    }

    ctx.close()
  }

  override def channelRead0(ctx: ChannelHandlerContext, response: ServerResponse) {
    val server = ctx.channel.remoteAddress.toString
    response match {
      case BlockFetchSuccess(blockId, buf) =>
        val listener = outstandingRequests.get(blockId)
        if (listener == null) {
          logWarning(s"Got a response for block $blockId from $server but it is not outstanding")
        } else {
          outstandingRequests.remove(blockId)
          listener.onBlockFetchSuccess(blockId, buf)
        }
      case BlockFetchFailure(blockId, errorMsg) =>
        val listener = outstandingRequests.get(blockId)
        if (listener == null) {
          logWarning(
            s"Got a response for block $blockId from $server ($errorMsg) but it is not outstanding")
        } else {
          outstandingRequests.remove(blockId)
          listener.onBlockFetchFailure(new RuntimeException(errorMsg))
        }
    }
  }
}
