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
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.oio.OioServerSocketChannel
import io.netty.channel.{ChannelInitializer, ChannelFuture, ChannelOption}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.network.BlockDataManager
import org.apache.spark.util.Utils


/**
 * Server for the [[NettyBlockTransferService]].
 */
private[netty]
class BlockServer(conf: NettyConfig, dataProvider: BlockDataManager) extends Logging {

  def port: Int = _port

  def hostName: String = _hostName

  private var _port: Int = conf.serverPort
  private var _hostName: String = ""
  private var bootstrap: ServerBootstrap = _
  private var channelFuture: ChannelFuture = _

  init()

  /** Initialize the server. */
  private def init(): Unit = {
    bootstrap = new ServerBootstrap
    val threadFactory = Utils.namedThreadFactory("spark-netty-server")

    // Use only one thread to accept connections, and 2 * num_cores for worker.
    def initNio(): Unit = {
      val bossGroup = new NioEventLoopGroup(0, threadFactory)
      val workerGroup = bossGroup
      bootstrap.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
    }
    def initOio(): Unit = {
      val bossGroup = new OioEventLoopGroup(0, threadFactory)
      val workerGroup = bossGroup
      bootstrap.group(bossGroup, workerGroup).channel(classOf[OioServerSocketChannel])
    }
    def initEpoll(): Unit = {
      val bossGroup = new EpollEventLoopGroup(0, threadFactory)
      val workerGroup = bossGroup
      bootstrap.group(bossGroup, workerGroup).channel(classOf[EpollServerSocketChannel])
    }

    conf.ioMode match {
      case "nio" => initNio()
      case "oio" => initOio()
      case "epoll" => initEpoll()
      case "auto" => if (Epoll.isAvailable) initEpoll() else initNio()
    }

    // Use pooled buffers to reduce temporary buffer allocation
    bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
    bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

    // Various (advanced) user-configured settings.
    conf.backLog.foreach { backLog =>
      bootstrap.option[java.lang.Integer](ChannelOption.SO_BACKLOG, backLog)
    }
    conf.receiveBuf.foreach { receiveBuf =>
      bootstrap.childOption[java.lang.Integer](ChannelOption.SO_RCVBUF, receiveBuf)
    }
    conf.sendBuf.foreach { sendBuf =>
      bootstrap.childOption[java.lang.Integer](ChannelOption.SO_SNDBUF, sendBuf)
    }

    bootstrap.childHandler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline
          .addLast("frameDecoder", ProtocolUtils.createFrameDecoder())
          .addLast("clientRequestDecoder", new ClientRequestDecoder)
          .addLast("serverResponseEncoder", new ServerResponseEncoder)
          .addLast("handler", new BlockServerHandler(dataProvider))
      }
    })

    channelFuture = bootstrap.bind(new InetSocketAddress(_port))
    channelFuture.sync()

    val addr = channelFuture.channel.localAddress.asInstanceOf[InetSocketAddress]
    _port = addr.getPort
    // _hostName = addr.getHostName
    _hostName = Utils.localHostName()
  }

  /** Shutdown the server. */
  def stop(): Unit = {
    if (channelFuture != null) {
      channelFuture.channel().close().awaitUninterruptibly()
      channelFuture = null
    }
    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully()
    }
    if (bootstrap != null && bootstrap.childGroup() != null) {
      bootstrap.childGroup().shutdownGracefully()
    }
    bootstrap = null
  }
}
