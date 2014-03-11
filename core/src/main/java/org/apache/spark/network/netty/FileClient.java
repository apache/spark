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

import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.oio.OioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileClient {

  private static final Logger LOG = LoggerFactory.getLogger(FileClient.class.getName());

  private final FileClientHandler handler;
  private Channel channel = null;
  private Bootstrap bootstrap = null;
  private EventLoopGroup group = null;
  private final int connectTimeout;
  private final int sendTimeout = 60; // 1 min

  FileClient(FileClientHandler handler, int connectTimeout) {
    this.handler = handler;
    this.connectTimeout = connectTimeout;
  }

  public void init() {
    group = new OioEventLoopGroup();
    bootstrap = new Bootstrap();
    bootstrap.group(group)
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
      LOG.warn("FileClient interrupted while trying to connect", e);
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
      try {
          // Should be able to send the message to network link channel.
          boolean bSent = channel.writeAndFlush(file + "\r\n").await(sendTimeout, TimeUnit.SECONDS);
          if (!bSent) {
              throw new RuntimeException("Failed to send");
          }
      } catch (InterruptedException e) {
          LOG.error("Error", e);
      }
  }

  public void close() {
    if (group != null) {
      group.shutdownGracefully();
      group = null;
      bootstrap = null;
    }
  }
}
