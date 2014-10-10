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
import io.netty.channel.{Channel, ChannelOption, EventLoopGroup}
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.oio.OioSocketChannel

import org.apache.spark.Logging

class FileClient(handler: FileClientHandler, connectTimeout: Int) extends Logging {

  private var channel: Channel = _
  private var bootstrap: Bootstrap = _
  private var group: EventLoopGroup = _
  private val sendTimeout = 60

  def init(): Unit = {
    group = new OioEventLoopGroup
    bootstrap = new Bootstrap
    bootstrap.group(group)
      .channel(classOf[OioSocketChannel])
      .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Integer.valueOf(connectTimeout))
      .handler(new FileClientChannelInitializer(handler))
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
      channel.closeFuture.sync()
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

  def close(): Unit = {
    if (group != null) {
      group.shutdownGracefully()
      group = null
      bootstrap = null
    }
  }
}
