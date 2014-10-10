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

import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import org.apache.spark.storage.BlockId


abstract class FileClientHandler extends SimpleChannelInboundHandler[ByteBuf] {

  private var currentHeader: FileHeader = null

  @volatile
  private var handlerCalled: Boolean = false

  def isComplete: Boolean = handlerCalled

  def handle(ctx: ChannelHandlerContext, in: ByteBuf, header: FileHeader)

  def handleError(blockId: BlockId)

  override def channelRead0(ctx: ChannelHandlerContext, in: ByteBuf) {
    if (currentHeader == null && in.readableBytes >= FileHeader.HEADER_SIZE) {
      currentHeader = FileHeader.create(in.readBytes(FileHeader.HEADER_SIZE))
    }
    if (in.readableBytes >= currentHeader.fileLen) {
      handle(ctx, in, currentHeader)
      handlerCalled = true
      currentHeader = null
      ctx.close()
    }
  }
}
