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

import io.netty.channel._

import org.apache.spark.Logging
import org.apache.spark.network.{ManagedBuffer, BlockDataManager}


/**
 * A handler that processes requests from clients and writes block data back.
 *
 * The messages should have been processed by the pipeline setup by BlockServerChannelInitializer.
 */
private[netty] class BlockServerHandler(dataProvider: BlockDataManager)
  extends SimpleChannelInboundHandler[ClientRequest] with Logging {

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logError(s"Exception in connection from ${ctx.channel.remoteAddress}", cause)
    ctx.close()
  }

  override def channelRead0(ctx: ChannelHandlerContext, request: ClientRequest): Unit = {
    request match {
      case BlockFetchRequest(blockIds) =>
        blockIds.foreach(processBlockRequest(ctx, _))
      case BlockUploadRequest(blockId, data) =>
        // TODO(rxin): handle upload.
    }
  }  // end of channelRead0

  private def processBlockRequest(ctx: ChannelHandlerContext, blockId: String): Unit = {
    // A helper function to send error message back to the client.
    def client = ctx.channel.remoteAddress.toString

    def respondWithError(error: String): Unit = {
      ctx.writeAndFlush(new BlockFetchFailure(blockId, error)).addListener(
        new ChannelFutureListener {
          override def operationComplete(future: ChannelFuture) {
            if (!future.isSuccess) {
              // TODO: Maybe log the success case as well.
              logError(s"Error sending error back to $client", future.cause)
              ctx.close()
            }
          }
        }
      )
    }

    logTrace(s"Received request from $client to fetch block $blockId")

    // First make sure we can find the block. If not, send error back to the user.
    var buf: ManagedBuffer = null
    try {
      buf = dataProvider.getBlockData(blockId)
    } catch {
      case e: Exception =>
        logError(s"Error opening block $blockId for request from $client", e)
        respondWithError(e.getMessage)
        return
    }

    ctx.writeAndFlush(new BlockFetchSuccess(blockId, buf)).addListener(
      new ChannelFutureListener {
        override def operationComplete(future: ChannelFuture): Unit = {
          if (future.isSuccess) {
            logTrace(s"Sent block $blockId (${buf.size} B) back to $client")
          } else {
            logError(
              s"Error sending block $blockId to $client; closing connection", future.cause)
            ctx.close()
          }
        }
      }
    )
  }  // end of processBlockRequest
}
