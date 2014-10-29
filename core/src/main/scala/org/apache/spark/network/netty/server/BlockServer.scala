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

package org.apache.spark.network.netty.server

import java.net.InetSocketAddress

import com.google.common.base.Charsets.UTF_8
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.{ChannelFuture, ChannelInitializer, ChannelOption}
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.oio.OioServerSocketChannel
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.StringDecoder

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.network.netty.NettyConfig
import org.apache.spark.storage.BlockDataProvider
import org.apache.spark.util.Utils


/**
 * Server for serving Spark data blocks.
 * This should be used together with [[org.apache.spark.network.netty.client.BlockFetchingClient]].
 *
 * Protocol for requesting blocks (client to server):
 *   One block id per line, e.g. to request 3 blocks: "block1\nblock2\nblock3\n"
 *
 * Protocol for sending blocks (server to client):
 *   frame-length (4 bytes), block-id-length (4 bytes), block-id, block-data.
 *
 *   frame-length should not include the length of itself.
 *   If block-id-length is negative, then this is an error message rather than block-data. The real
 *   length is the absolute value of the frame-length.
 *
 */
private[spark]
class BlockServer(conf: NettyConfig, dataProvider: BlockDataProvider) extends Logging {

  def this(sparkConf: SparkConf, dataProvider: BlockDataProvider) = {
    this(new NettyConfig(sparkConf), dataProvider)
  }

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
    val bossThreadFactory = Utils.namedThreadFactory("spark-shuffle-server-boss")
    val workerThreadFactory = Utils.namedThreadFactory("spark-shuffle-server-worker")

    // Use only one thread to accept connections, and 2 * num_cores for worker.
    def initNio(): Unit = {
      val bossGroup = new NioEventLoopGroup(1, bossThreadFactory)
      val workerGroup = new NioEventLoopGroup(0, workerThreadFactory)
      workerGroup.setIoRatio(conf.ioRatio)
      bootstrap.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
    }
    def initOio(): Unit = {
      val bossGroup = new OioEventLoopGroup(1, bossThreadFactory)
      val workerGroup = new OioEventLoopGroup(0, workerThreadFactory)
      bootstrap.group(bossGroup, workerGroup).channel(classOf[OioServerSocketChannel])
    }
    def initEpoll(): Unit = {
      val bossGroup = new EpollEventLoopGroup(1, bossThreadFactory)
      val workerGroup = new EpollEventLoopGroup(0, workerThreadFactory)
      workerGroup.setIoRatio(conf.ioRatio)
      bootstrap.group(bossGroup, workerGroup).channel(classOf[EpollServerSocketChannel])
    }

    conf.ioMode match {
      case "nio" => initNio()
      case "oio" => initOio()
      case "epoll" => initEpoll()
      case "auto" =>
        // For auto mode, first try epoll (only available on Linux), then nio.
        try {
          initEpoll()
        } catch {
          // TODO: Should we log the throwable? But that always happen on non-Linux systems.
          // Perhaps the right thing to do is to check whether the system is Linux, and then only
          // call initEpoll on Linux.
          case e: Throwable => initNio()
        }
    }

    // Use pooled buffers to reduce temporary buffer allocation
    bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
    bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

    // Various (advanced) user-configured settings.
    conf.backLog.foreach { backLog =>
      bootstrap.option[java.lang.Integer](ChannelOption.SO_BACKLOG, backLog)
    }
    conf.receiveBuf.foreach { receiveBuf =>
      bootstrap.option[java.lang.Integer](ChannelOption.SO_RCVBUF, receiveBuf)
    }
    conf.sendBuf.foreach { sendBuf =>
      bootstrap.option[java.lang.Integer](ChannelOption.SO_SNDBUF, sendBuf)
    }

    bootstrap.childHandler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline
          .addLast("frameDecoder", new LineBasedFrameDecoder(1024))  // max block id length 1024
          .addLast("stringDecoder", new StringDecoder(UTF_8))
          .addLast("blockHeaderEncoder", new BlockHeaderEncoder)
          .addLast("handler", new BlockServerHandler(dataProvider))
      }
    })

    channelFuture = bootstrap.bind(new InetSocketAddress(_port))
    channelFuture.sync()

    val addr = channelFuture.channel.localAddress.asInstanceOf[InetSocketAddress]
    _port = addr.getPort
    _hostName = addr.getHostName
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
