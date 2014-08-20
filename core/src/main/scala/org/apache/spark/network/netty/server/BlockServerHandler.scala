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
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import io.netty.buffer.Unpooled
import io.netty.channel._

import org.apache.spark.Logging
import org.apache.spark.storage.{FileSegment, BlockDataProvider}


/**
 * A handler that processes requests from clients and writes block data back.
 *
 * The messages should have been processed by a LineBasedFrameDecoder and a StringDecoder first
 * so channelRead0 is called once per line (i.e. per block id).
 */
private[server]
class BlockServerHandler(dataProvider: BlockDataProvider)
  extends SimpleChannelInboundHandler[String] with Logging {

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logError(s"Exception in connection from ${ctx.channel.remoteAddress}", cause)
    ctx.close()
  }

  override def channelRead0(ctx: ChannelHandlerContext, blockId: String): Unit = {
    def client = ctx.channel.remoteAddress.toString

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

    def writeFileSegment(segment: FileSegment): Unit = {
      // Send error message back if the block is too large. Even though we are capable of sending
      // large (2G+) blocks, the receiving end cannot handle it so let's fail fast.
      // Once we fixed the receiving end to be able to process large blocks, this should be removed.
      // Also make sure we update BlockHeaderEncoder to support length > 2G.

      // See [[BlockHeaderEncoder]] for the way length is encoded.
      if (segment.length + blockId.length + 4 > Int.MaxValue) {
        respondWithError(s"Block $blockId size ($segment.length) greater than 2G")
        return
      }

      var fileChannel: FileChannel = null
      try {
        fileChannel = new FileInputStream(segment.file).getChannel
      } catch {
        case e: Exception =>
          logError(
            s"Error opening channel for $blockId in ${segment.file} for request from $client", e)
          respondWithError(e.getMessage)
      }

      // Found the block. Send it back.
      if (fileChannel != null) {
        // Write the header and block data. In the case of failures, the listener on the block data
        // write should close the connection.
        ctx.write(new BlockHeader(segment.length.toInt, blockId))

        val region = new DefaultFileRegion(fileChannel, segment.offset, segment.length)
        ctx.writeAndFlush(region).addListener(new ChannelFutureListener {
          override def operationComplete(future: ChannelFuture) {
            if (future.isSuccess) {
              logTrace(s"Sent block $blockId (${segment.length} B) back to $client")
            } else {
              logError(s"Error sending block $blockId to $client; closing connection", future.cause)
              ctx.close()
            }
          }
        })
      }
    }

    def writeByteBuffer(buf: ByteBuffer): Unit = {
      ctx.write(new BlockHeader(buf.remaining, blockId))
      ctx.writeAndFlush(Unpooled.wrappedBuffer(buf)).addListener(new ChannelFutureListener {
        override def operationComplete(future: ChannelFuture) {
          if (future.isSuccess) {
            logTrace(s"Sent block $blockId (${buf.remaining} B) back to $client")
          } else {
            logError(s"Error sending block $blockId to $client; closing connection", future.cause)
            ctx.close()
          }
        }
      })
    }

    logTrace(s"Received request from $client to fetch block $blockId")

    var blockData: Either[FileSegment, ByteBuffer] = null

    // First make sure we can find the block. If not, send error back to the user.
    try {
      blockData = dataProvider.getBlockData(blockId)
    } catch {
      case e: Exception =>
        logError(s"Error opening block $blockId for request from $client", e)
        respondWithError(e.getMessage)
        return
    }

    blockData match {
      case Left(segment) => writeFileSegment(segment)
      case Right(buf) => writeByteBuffer(buf)
    }

  }  // end of channelRead0
}
