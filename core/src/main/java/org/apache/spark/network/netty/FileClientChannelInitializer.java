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

import io.netty.buffer.BufType;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.string.StringEncoder;


class FileClientChannelInitializer extends ChannelInitializer<SocketChannel> {

  private FileClientHandler fhandler;

  public FileClientChannelInitializer(FileClientHandler handler) {
    fhandler = handler;
  }

  @Override
  public void initChannel(SocketChannel channel) {
    // file no more than 2G
    channel.pipeline()
      .addLast("encoder", new StringEncoder(BufType.BYTE))
      .addLast("handler", fhandler);
  }
}
