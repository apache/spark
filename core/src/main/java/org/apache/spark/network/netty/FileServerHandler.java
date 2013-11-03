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
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.DefaultFileRegion;

import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.FileSegment;

class FileServerHandler extends ChannelInboundMessageHandlerAdapter<String> {

  PathResolver pResolver;

  public FileServerHandler(PathResolver pResolver){
    this.pResolver = pResolver;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, String blockIdString) {
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
      int len = new Long(length).intValue();
      ctx.write((new FileHeader(len, blockId)).buffer());
      try {
        ctx.sendFile(new DefaultFileRegion(new FileInputStream(file)
          .getChannel(), fileSegment.offset(), fileSegment.length()));
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      ctx.write(new FileHeader(0, blockId).buffer());
    }
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
}
