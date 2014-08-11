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

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.{ChannelOption, Channel, ChannelInitializer, EventLoopGroup}
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.socket.oio.OioSocketChannel
import io.netty.handler.codec.string.StringEncoder
import io.netty.util.CharsetUtil

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

/**
 * Factory for creating [[BlockFetchingClient]] by using createClient.
 *
 * This factory reuses the worker thread pool for Netty.
 */
class BlockFetchingClientFactory(conf: SparkConf) {

  /** IO mode: nio, oio, epoll, or auto (try epoll first and then nio). */
  private val ioMode = conf.get("spark.shuffle.io.mode", "auto").toLowerCase
  /** Connection timeout in secs. Default 60 secs. */
  private val connectionTimeout = conf.getInt("spark.shuffle.io.connectionTimeout", 60) * 1000
  /** Timeout in secs for sending data. */
  private val ioTimeout = connectionTimeout

  /** A thread factory so the threads are named (for debugging). */
  private val threadFactory = Utils.namedThreadFactory("spark-shuffle-client")

  /** The following two are instantiated by the [[init]] method, depending the [[ioMode]]. */
  private var socketChannelClass: Class[_ <: Channel] = _
  private var workerGroup: EventLoopGroup = _

  init()

  /** Initialize [[socketChannelClass]] and [[workerGroup]] based on the value of [[ioMode]]. */
  private def init(): Unit = {
    def initOio(): Unit = {
      socketChannelClass = classOf[OioSocketChannel]
      workerGroup = new OioEventLoopGroup(0, threadFactory)
    }
    def initNio(): Unit = {
      socketChannelClass = classOf[NioSocketChannel]
      workerGroup = new NioEventLoopGroup(0, threadFactory)
    }
    def initEpoll(): Unit = {
      socketChannelClass = classOf[EpollSocketChannel]
      workerGroup = new EpollEventLoopGroup(0, threadFactory)
    }

    ioMode match {
      case "nio" => initNio()
      case "oio" => initOio()
      case "epoll" => initEpoll()
      case "auto" =>
        // For auto mode, first try epoll (only available on Linux), then nio.
        try {
          initEpoll()
        } catch {
          case e: IllegalStateException => initNio()
        }
    }
  }

  /** Create a new BlockFetchingClient connecting to the given remote host / port. */
  def createClient(remoteHost: String, remotePort: Int): BlockFetchingClient = {
    val bootstrap = new Bootstrap

    bootstrap.group(workerGroup)
      // Use pooled buffers to reduce temporary buffer allocation
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
      .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout)

    bootstrap.handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline
          .addLast("encoder", new StringEncoder(CharsetUtil.UTF_8))
          //.addLast("handler", handler)
      }
    })

    val cf = bootstrap.connect(remoteHost, remotePort).sync()
    new BlockFetchingClient(cf, ioTimeout)
  }

}
