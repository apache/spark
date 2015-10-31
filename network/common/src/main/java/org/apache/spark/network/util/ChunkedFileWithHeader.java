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
package org.apache.spark.network.util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedFile;
import io.netty.buffer.Unpooled;
import io.netty.handler.stream.ChunkedInput;

import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 *
 */
public class ChunkedFileWithHeader implements ChunkedInput<ByteBuf> {

  private final ByteBuf header;
  private final ChunkedFile chunkedFile;

  /**
   *
   * @param header
   * @param body
   * @throws IOException
   */
  public ChunkedFileWithHeader(ByteBuf header, Object body) {
    Preconditions.checkArgument(
      (body instanceof ChunkedFile), "Body must be an instance of ChunkedFile");
    this.chunkedFile = (ChunkedFile)body;
    this.header = header;
  }

  /**
   *
   * @param ctx
   * @return
   * @throws Exception
   */
  @Override
  public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
    long offset = chunkedFile.currentOffset();
    if (offset >= chunkedFile.endOffset()) {
      return null;
    }

    int length = header.readableBytes();
    if (length > 0) {
      ByteBuf buf = Unpooled.wrappedBuffer(header, chunkedFile.readChunk(ctx));
      header.skipBytes(length);
      return buf;
    } else {
      return chunkedFile.readChunk(ctx);
    }
  }

  @Override
  public boolean isEndOfInput() throws Exception {
    return chunkedFile.isEndOfInput();
  }

  @Override
  public void close() throws Exception {
    chunkedFile.close();
    if (header.refCnt() > 0) {
      header.release();
    }
  }
}
