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

import java.io.File;
import java.io.FileInputStream;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.DefaultFileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.FileSegment;

class FileServerHandler extends SimpleChannelInboundHandler<String> {

  private static final Logger LOG = LoggerFactory.getLogger(FileServerHandler.class.getName());

  private final PathResolver pResolver;

  FileServerHandler(PathResolver pResolver){
    this.pResolver = pResolver;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, String blockIdString) {
    BlockId blockId = BlockId.apply(blockIdString);
    FileSegment fileSegment = pResolver.getBlockLocation(blockId);
    // if getBlockLocation returns null, close the channel
    if (fileSegment == null) {
      //ctx.close();
      return;
    }
    File file = fileSegment.file();
    if (file.exists()) {
      if (!file.isFile()) {
        ctx.write(new FileHeader(0, blockId).buffer());
        ctx.flush();
        return;
      }
      long length = fileSegment.length();
      if (length > Integer.MAX_VALUE || length <= 0) {
        ctx.write(new FileHeader(0, blockId).buffer());
        ctx.flush();
        return;
      }
      int len = (int) length;
      ctx.write((new FileHeader(len, blockId)).buffer());
      try {
        ctx.write(new DefaultFileRegion(new FileInputStream(file)
          .getChannel(), fileSegment.offset(), fileSegment.length()));
      } catch (Exception e) {
          LOG.error("Exception: ", e);
      }
    } else {
      ctx.write(new FileHeader(0, blockId).buffer());
    }
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception: ", cause);
    ctx.close();
  }
}
