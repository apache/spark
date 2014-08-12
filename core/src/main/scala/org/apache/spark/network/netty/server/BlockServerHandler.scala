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

package org.apache.spark.network.netty.server

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.nio.channels.FileChannel

import io.netty.channel._

import org.apache.spark.Logging
import org.apache.spark.network.netty.PathResolver
import org.apache.spark.storage.BlockId


/**
 * A handler that processes requests from clients and writes block data back.
 *
 * The messages should have been processed by a LineBasedFrameDecoder and a StringDecoder first
 * so channelRead0 is called once per line (i.e. per block id).
 */
private[server]
class BlockServerHandler(p: PathResolver)
  extends SimpleChannelInboundHandler[String] with Logging {

  override def channelRead0(ctx: ChannelHandlerContext, blockId: String): Unit = {
    // client in the form of hostname:port
    val client = {
      val remoteAddr = ctx.channel.remoteAddress.asInstanceOf[InetSocketAddress]
      remoteAddr.getHostName + ":" + remoteAddr.getPort
    }

    // A helper function to send error message back to the client.
    def respondWithError(error: String): Unit = {
      ctx.writeAndFlush(new BlockHeader(-1, blockId, Some(error))).addListener(
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

    var fileChannel: FileChannel = null
    var offset: Long = 0
    var blockSize: Long = 0

    // First make sure we can find the block. If not, send error back to the user.
    try {
      val segment = p.getBlockLocation(BlockId(blockId))
      fileChannel = new FileInputStream(segment.file).getChannel
      offset = segment.offset
      blockSize = segment.length
    } catch {
      case e: Exception =>
        logError(s"Error opening block $blockId for request from $client", e)
        blockSize = -1
        respondWithError(e.getMessage)
    }

    // Send error message back if the block is too large. Even though we are capable of sending
    // large (2G+) blocks, the receiving end cannot handle it so let's fail fast.
    // Once we fixed the receiving end to be able to process large blocks, this should be removed.
    // Also make sure we update BlockHeaderEncoder to support length > 2G.
    if (blockSize > Int.MaxValue) {
      respondWithError(s"Block $blockId size ($blockSize) greater than 2G")
    }

    // Found the block. Send it back.
    if (fileChannel != null && blockSize >= 0) {
      val listener = new ChannelFutureListener {
        override def operationComplete(future: ChannelFuture) {
          if (future.isSuccess) {
            logTrace(s"Sent block $blockId ($blockSize B) back to $client")
          } else {
            logError(s"Error sending block $blockId to $client; closing connection", future.cause)
            ctx.close()
          }
        }
      }
      val region = new DefaultFileRegion(fileChannel, offset, blockSize)
      ctx.writeAndFlush(new BlockHeader(blockSize.toInt, blockId)).addListener(listener)
      ctx.writeAndFlush(region).addListener(listener)
    }
  }  // end of channelRead0
}
