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

package org.apache.spark.network.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundByteHandlerAdapter;

import org.apache.spark.storage.BlockId;

abstract class FileClientHandler extends ChannelInboundByteHandlerAdapter {

  private FileHeader currentHeader = null;

  private volatile boolean handlerCalled = false;

  public boolean isComplete() {
    return handlerCalled;
  }

  public abstract void handle(ChannelHandlerContext ctx, ByteBuf in, FileHeader header);
  public abstract void handleError(BlockId blockId);

  @Override
  public ByteBuf newInboundBuffer(ChannelHandlerContext ctx) {
    // Use direct buffer if possible.
    return ctx.alloc().ioBuffer();
  }

  @Override
  public void inboundBufferUpdated(ChannelHandlerContext ctx, ByteBuf in) {
    // get header
    if (currentHeader == null && in.readableBytes() >= FileHeader.HEADER_SIZE()) {
      currentHeader = FileHeader.create(in.readBytes(FileHeader.HEADER_SIZE()));
    }
    // get file
    if(in.readableBytes() >= currentHeader.fileLen()) {
      handle(ctx, in, currentHeader);
      handlerCalled = true;
      currentHeader = null;
      ctx.close();
    }
  }

}

