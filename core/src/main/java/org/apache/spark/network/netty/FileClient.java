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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.oio.OioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileClient {

  private Logger LOG = LoggerFactory.getLogger(this.getClass().getName());
  private FileClientHandler handler = null;
  private Channel channel = null;
  private Bootstrap bootstrap = null;
  private int connectTimeout = 60*1000; // 1 min

  public FileClient(FileClientHandler handler, int connectTimeout) {
    this.handler = handler;
    this.connectTimeout = connectTimeout;
  }

  public void init() {
    bootstrap = new Bootstrap();
    bootstrap.group(new OioEventLoopGroup())
      .channel(OioSocketChannel.class)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
      .handler(new FileClientChannelInitializer(handler));
  }

  public void connect(String host, int port) {
    try {
      // Start the connection attempt.
      channel = bootstrap.connect(host, port).sync().channel();
      // ChannelFuture cf = channel.closeFuture();
      //cf.addListener(new ChannelCloseListener(this));
    } catch (InterruptedException e) {
      close();
    }
  }

  public void waitForClose() {
    try {
      channel.closeFuture().sync();
    } catch (InterruptedException e) {
      LOG.warn("FileClient interrupted", e);
    }
  }

  public void sendRequest(String file) {
    //assert(file == null);
    //assert(channel == null);
    channel.write(file + "\r\n");
  }

  public void close() {
    if(channel != null) {
      channel.close();
      channel = null;
    }
    if ( bootstrap!=null) {
      bootstrap.shutdown();
      bootstrap = null;
    }
  }
}
