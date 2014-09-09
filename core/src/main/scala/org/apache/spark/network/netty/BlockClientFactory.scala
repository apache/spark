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

import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.socket.oio.OioSocketChannel
import io.netty.channel.{Channel, EventLoopGroup}

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils


/**
 * Factory for creating [[BlockClient]] by using createClient. This factory reuses
 * the worker thread pool for Netty.
 */
private[netty]
class BlockClientFactory(val conf: NettyConfig) {

  def this(sparkConf: SparkConf) = this(new NettyConfig(sparkConf))

  /** A thread factory so the threads are named (for debugging). */
  private[netty] val threadFactory = Utils.namedThreadFactory("spark-shuffle-client")

  /** The following two are instantiated by the [[init]] method, depending ioMode. */
  private[netty] var socketChannelClass: Class[_ <: Channel] = _
  private[netty] var workerGroup: EventLoopGroup = _

  init()

  /** Initialize [[socketChannelClass]] and [[workerGroup]] based on ioMode. */
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

    // For auto mode, first try epoll (only available on Linux), then nio.
    conf.ioMode match {
      case "nio" => initNio()
      case "oio" => initOio()
      case "epoll" => initEpoll()
      case "auto" => if (Epoll.isAvailable) initEpoll() else initNio()
    }
  }

  /**
   * Create a new BlockFetchingClient connecting to the given remote host / port.
   *
   * This blocks until a connection is successfully established.
   *
   * Concurrency: This method is safe to call from multiple threads.
   */
  def createClient(remoteHost: String, remotePort: Int): BlockClient = {
    new BlockClient(this, remoteHost, remotePort)
  }

  def stop(): Unit = {
    if (workerGroup != null) {
      workerGroup.shutdownGracefully()
    }
  }
}
