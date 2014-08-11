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

package org.apache.spark.network.netty

import java.net.InetSocketAddress

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelFuture, ChannelOption, EventLoopGroup}
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.oio.OioServerSocketChannel

import org.apache.spark.Logging

/**
 * Server that accept the path of a file an echo back its content.
 */
class FileServer(pResolver: PathResolver, private var port: Int) extends Logging {

  private val addr: InetSocketAddress = new InetSocketAddress(port)
  private var bossGroup: EventLoopGroup = new OioEventLoopGroup
  private var workerGroup: EventLoopGroup = new OioEventLoopGroup

  private var channelFuture: ChannelFuture = {
    val bootstrap = new ServerBootstrap
    bootstrap.group(bossGroup, workerGroup)
      .channel(classOf[OioServerSocketChannel])
      .option(ChannelOption.SO_BACKLOG, java.lang.Integer.valueOf(100))
      .option(ChannelOption.SO_RCVBUF, java.lang.Integer.valueOf(1500))
      .childHandler(new FileServerChannelInitializer(pResolver))
    bootstrap.bind(addr)
  }

  try {
    val boundAddress = channelFuture.sync.channel.localAddress.asInstanceOf[InetSocketAddress]
    port = boundAddress.getPort
  } catch {
    case ie: InterruptedException =>
      port = 0
  }

  /** Start the file server asynchronously in a new thread. */
  def start(): Unit = {
    val blockingThread: Thread = new Thread {
      override def run(): Unit = {
        try {
          channelFuture.channel.closeFuture.sync
          logInfo("FileServer exiting")
        } catch {
          case e: InterruptedException =>
            logError("File server start got interrupted", e)
        }
        // NOTE: bootstrap is shutdown in stop()
      }
    }
    blockingThread.setDaemon(true)
    blockingThread.start()
  }

  def getPort: Int = port

  def stop(): Unit = {
    if (channelFuture != null) {
      channelFuture.channel().close().awaitUninterruptibly()
      channelFuture = null
    }
    if (bossGroup != null) {
      bossGroup.shutdownGracefully()
      bossGroup = null
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully()
      workerGroup = null
    }
  }
}

