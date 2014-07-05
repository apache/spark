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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server that accept the path of a file an echo back its content.
 */
class FileServer {

  private static final Logger LOG = LoggerFactory.getLogger(FileServer.class.getName());

  private EventLoopGroup bossGroup = null;
  private EventLoopGroup workerGroup = null;
  private ChannelFuture channelFuture = null;
  private int port = 0;

  FileServer(PathResolver pResolver, int port, SparkConf conf) {
    InetSocketAddress addr = new InetSocketAddress(port);

    // Configure the server.
    bossGroup = new OioEventLoopGroup();
    workerGroup = new OioEventLoopGroup();

    ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.group(bossGroup, workerGroup)
        .channel(OioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, conf.getInt("spark.shuffle.fileserver.backlog", 100))
        .option(ChannelOption.SO_RCVBUF, conf.getInt("spark.shuffle.fileserver.receivebuf", 1536))
        .childHandler(new FileServerChannelInitializer(pResolver))
        .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, conf.getInt("spark.shuffle.fileserver.watermark.high", 1024) * 1024)
        .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, conf.getInt("spark.shuffle.fileserver.watermark.low", 256) * 1024)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
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
    Thread blockingThread = new Thread() {
      @Override
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
      channelFuture.channel().close().awaitUninterruptibly();
      channelFuture = null;
    }

    // Shutdown event groups
    if (bossGroup != null) {
       bossGroup.shutdownGracefully();
       bossGroup = null;
    }

    if (workerGroup != null) {
       workerGroup.shutdownGracefully();
       workerGroup = null;
    }
    // TODO: Shutdown all accepted channels as well ?
  }
}
