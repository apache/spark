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
 */
private[client]
class BlockFetchingClientHandler extends SimpleChannelInboundHandler[ByteBuf] with Logging {

  var blockFetchSuccessCallback: (String, ReferenceCountedBuffer) => Unit = _
  var blockFetchFailureCallback: (String, String) => Unit = _

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logError(s"Exception in connection from ${ctx.channel.remoteAddress}", cause)
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
      blockFetchFailureCallback(blockId, errorMsg)
    } else {
      logTrace(s"Received block $blockId ($blockSize B) from $server")
      blockFetchSuccessCallback(blockId, new ReferenceCountedBuffer(in))
    }
  }
}
