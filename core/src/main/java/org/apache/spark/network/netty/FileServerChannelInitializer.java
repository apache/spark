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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;


class FileServerChannelInitializer extends ChannelInitializer<SocketChannel> {

  PathResolver pResolver;

  public FileServerChannelInitializer(PathResolver pResolver) {
    this.pResolver = pResolver;
  }

  @Override
  public void initChannel(SocketChannel channel) {
    channel.pipeline()
      .addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()))
      .addLast("strDecoder", new StringDecoder())
      .addLast("handler", new FileServerHandler(pResolver));
  }
}
