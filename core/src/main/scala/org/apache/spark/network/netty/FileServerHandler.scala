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

import java.io.FileInputStream

import io.netty.channel.{DefaultFileRegion, ChannelHandlerContext, SimpleChannelInboundHandler}

import org.apache.spark.Logging
import org.apache.spark.storage.{BlockId, FileSegment}


class FileServerHandler(pResolver: PathResolver)
  extends SimpleChannelInboundHandler[String] with Logging {

  override def channelRead0(ctx: ChannelHandlerContext, blockIdString: String): Unit = {
    val blockId: BlockId = BlockId(blockIdString)
    val fileSegment: FileSegment = pResolver.getBlockLocation(blockId)
    if (fileSegment == null) {
      return
    }
    val file = fileSegment.file
    if (file.exists) {
      if (!file.isFile) {
        ctx.write(new FileHeader(0, blockId).buffer)
        ctx.flush()
        return
      }
      val length: Long = fileSegment.length
      if (length > Integer.MAX_VALUE || length <= 0) {
        ctx.write(new FileHeader(0, blockId).buffer)
        ctx.flush()
        return
      }
      ctx.write(new FileHeader(length.toInt, blockId).buffer)
      try {
        val channel = new FileInputStream(file).getChannel
        ctx.write(new DefaultFileRegion(channel, fileSegment.offset, fileSegment.length))
      } catch {
        case e: Exception =>
          logError("Exception: ", e)
      }
    } else {
      ctx.write(new FileHeader(0, blockId).buffer)
    }
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logError("Exception: ", cause)
    ctx.close()
  }
}
