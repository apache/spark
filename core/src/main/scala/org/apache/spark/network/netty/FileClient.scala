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

import java.util.concurrent.TimeUnit

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.{Channel, ChannelOption}
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.socket.oio.OioSocketChannel

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.Utils


class FileClient(handler: FileClientHandler, conf: SparkConf) extends Logging {

  private var channel: Channel = _
  private var bootstrap: Bootstrap = _
  private val sendTimeout = 60

  /** Initialize FileClient. */
  def init(): Unit = {
    val threadFactory = Utils.namedThreadFactory("spark-shuffle-client")
    bootstrap = new Bootstrap
    def initOio(): Unit = {
      bootstrap.group(new OioEventLoopGroup(0, threadFactory))
        .channel(classOf[OioSocketChannel])
    }
    def initNio(): Unit = {
      bootstrap.group(new NioEventLoopGroup(0, threadFactory))
        .channel(classOf[NioSocketChannel])
    }
    def initEpoll(): Unit = {
      bootstrap.group(new EpollEventLoopGroup(0, threadFactory))
        .channel(classOf[EpollSocketChannel])
    }
    conf.get("spark.shuffle.io.mode", "auto").toLowerCase match {
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

    // Default time out 60 secs
    val connectionTimeout = conf.getInt("spark.shuffle.io.connectionTimeout", 60) * 1000

    bootstrap.handler(new FileClientChannelInitializer(handler))
      // Use pooled byte buf allocator to reduce temporary buffer allocation
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
      .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout)
  }

  def connect(host: String, port: Int) {
    try {
      channel = bootstrap.connect(host, port).sync().channel()
    } catch {
      case e: InterruptedException =>
        logWarning("FileClient interrupted while trying to connect", e)
        close()
    }
  }

  def waitForClose(): Unit = {
    try {
      channel.closeFuture().sync()
    } catch {
      case e: InterruptedException =>
        logWarning("FileClient interrupted", e)
    }
  }

  def sendRequest(file: String): Unit = {
    try {
      val bSent = channel.writeAndFlush(file + "\r\n").await(sendTimeout, TimeUnit.SECONDS)
      if (!bSent) {
        throw new RuntimeException("Failed to send")
      }
    } catch {
      case e: InterruptedException =>
        logError("Error", e)
    }
  }

  /** Shutdown the client. */
  def close(): Unit = {
    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully()
    }
    bootstrap = null
  }
}
