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

import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.oio.OioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Server that accept the path of a file an echo back its content.
 */
class FileServer {

  private Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

  private ServerBootstrap bootstrap = null;
  private ChannelFuture channelFuture = null;
  private int port = 0;
  private Thread blockingThread = null;

  public FileServer(PathResolver pResolver, int port) {
    InetSocketAddress addr = new InetSocketAddress(port);

    // Configure the server.
    bootstrap = new ServerBootstrap();
    bootstrap.group(new OioEventLoopGroup(), new OioEventLoopGroup())
        .channel(OioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, 100)
        .option(ChannelOption.SO_RCVBUF, 1500)
        .childHandler(new FileServerChannelInitializer(pResolver));
    // Start the server.
    channelFuture = bootstrap.bind(addr);
    try {
      // Get the address we bound to.
      InetSocketAddress boundAddress =
        ((InetSocketAddress) channelFuture.sync().channel().localAddress());
      this.port = boundAddress.getPort();
    } catch (InterruptedException ie) {
      this.port = 0;
    }
  }

  /**
   * Start the file server asynchronously in a new thread.
   */
  public void start() {
    blockingThread = new Thread() {
      public void run() {
        try {
          channelFuture.channel().closeFuture().sync();
          LOG.info("FileServer exiting");
        } catch (InterruptedException e) {
          LOG.error("File server start got interrupted", e);
        }
        // NOTE: bootstrap is shutdown in stop()
      }
    };
    blockingThread.setDaemon(true);
    blockingThread.start();
  }

  public int getPort() {
    return port;
  }

  public void stop() {
    // Close the bound channel.
    if (channelFuture != null) {
      channelFuture.channel().close();
      channelFuture = null;
    }
    // Shutdown bootstrap.
    if (bootstrap != null) {
      bootstrap.shutdown();
      bootstrap = null;
    }
    // TODO: Shutdown all accepted channels as well ?
  }
}
