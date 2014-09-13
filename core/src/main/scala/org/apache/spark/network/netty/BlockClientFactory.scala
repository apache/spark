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

import java.io.Closeable
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel._
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.oio.OioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.socket.oio.OioSocketChannel
import io.netty.util.internal.PlatformDependent

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils


/**
 * Factory for creating [[BlockClient]] by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same [[BlockClient]]
 * for the same remote host. It also shares a single worker thread pool for all [[BlockClient]]s.
 */
private[netty]
class BlockClientFactory(val conf: NettyConfig) extends Closeable {

  def this(sparkConf: SparkConf) = this(new NettyConfig(sparkConf))

  /** A thread factory so the threads are named (for debugging). */
  private[this] val threadFactory = Utils.namedThreadFactory("spark-netty-client")

  /** Socket channel type, initialized by [[init]] depending ioMode. */
  private[this] var socketChannelClass: Class[_ <: Channel] = _

  /** Thread pool shared by all clients. */
  private[this] var workerGroup: EventLoopGroup = _

  private[this] val connectionPool = new ConcurrentHashMap[(String, Int), BlockClient]

  // The encoders are stateless and can be shared among multiple clients.
  private[this] val encoder = new ClientRequestEncoder
  private[this] val decoder = new ServerResponseDecoder

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
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    val cachedClient = connectionPool.get((remoteHost, remotePort))
    if (cachedClient != null && cachedClient.isActive) {
      return cachedClient
    }

    // There is a chance two threads are creating two different clients connecting to the same host.
    // But that's probably ok ...

    val handler = new BlockClientHandler

    val bootstrap = new Bootstrap
    bootstrap.group(workerGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
      .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      .option[Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectTimeoutMs)

    // Use pooled buffers to reduce temporary buffer allocation
    bootstrap.option(ChannelOption.ALLOCATOR, createPooledByteBufAllocator())

    bootstrap.handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline
          .addLast("clientRequestEncoder", encoder)
          .addLast("frameDecoder", ProtocolUtils.createFrameDecoder())
          .addLast("serverResponseDecoder", decoder)
          .addLast("handler", handler)
      }
    })

    // Connect to the remote server
    val cf: ChannelFuture = bootstrap.connect(remoteHost, remotePort)
    if (!cf.awaitUninterruptibly(conf.connectTimeoutMs)) {
      throw new TimeoutException(
        s"Connecting to $remoteHost:$remotePort timed out (${conf.connectTimeoutMs} ms)")
    }

    val client = new BlockClient(cf, handler)
    connectionPool.put((remoteHost, remotePort), client)
    client
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  override def close(): Unit = {
    val iter = connectionPool.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      entry.getValue.close()
      connectionPool.remove(entry.getKey)
    }

    if (workerGroup != null) {
      workerGroup.shutdownGracefully()
    }
  }

  /**
   * Create a pooled ByteBuf allocator but disables the thread-local cache. Thread-local caches
   * are disabled because the ByteBufs are allocated by the event loop thread, but released by the
   * executor thread rather than the event loop thread. Those thread-local caches actually delay
   * the recycling of buffers, leading to larger memory usage.
   */
  private def createPooledByteBufAllocator(): PooledByteBufAllocator = {
    def getPrivateStaticField(name: String): Int = {
      val f = PooledByteBufAllocator.DEFAULT.getClass.getDeclaredField(name)
      f.setAccessible(true)
      f.getInt(null)
    }
    new PooledByteBufAllocator(
      PlatformDependent.directBufferPreferred(),
      getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"),
      getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"),
      getPrivateStaticField("DEFAULT_PAGE_SIZE"),
      getPrivateStaticField("DEFAULT_MAX_ORDER"),
      0,  // tinyCacheSize
      0,  // smallCacheSize
      0   // normalCacheSize
    )
  }
}
