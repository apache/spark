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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * {@link TransportClient} for the same remote host. It also shares a single worker thread pool for
 * all {@link TransportClient}s.
 */
public class TransportClientFactory implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;
  private final ConcurrentHashMap<SocketAddress, TransportClient> connectionPool;

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;

  public TransportClientFactory(TransportContext context) {
    this.context = context;
    this.conf = context.getConf();
    this.connectionPool = new ConcurrentHashMap<SocketAddress, TransportClient>();

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    // TODO: Make thread pool name configurable.
    this.workerGroup = NettyUtils.createEventLoop(ioMode, conf.clientThreads(), "shuffle-client");
  }

  /**
   * Create a new BlockFetchingClient connecting to the given remote host / port.
   *
   * This blocks until a connection is successfully established.
   *
   * Concurrency: This method is safe to call from multiple threads.
   */
  public TransportClient createClient(String remoteHost, int remotePort) {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    TransportClient cachedClient = connectionPool.get(address);
    if (cachedClient != null) {
      if (cachedClient.isActive()) {
        return cachedClient;
      } else {
        connectionPool.remove(address, cachedClient); // Remove inactive clients.
      }
    }

    logger.debug("Creating new connection to " + address);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
      .channel(socketChannelClass)
       // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs());

    // Use pooled buffers to reduce temporary buffer allocation
    bootstrap.option(ChannelOption.ALLOCATOR, createPooledByteBufAllocator());

    final AtomicReference<TransportClient> client = new AtomicReference<TransportClient>();

    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
        client.set(clientHandler.getClient());
      }
    });

    // Connect to the remote server
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.awaitUninterruptibly(conf.connectionTimeoutMs())) {
      throw new RuntimeException(
        String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
    } else if (cf.cause() != null) {
      throw new RuntimeException(String.format("Failed to connect to %s", address), cf.cause());
    }

    // Successful connection -- in the event that two threads raced to create a client, we will
    // use the first one that was put into the connectionPool and close the one we made here.
    assert client.get() != null : "Channel future completed successfully with null client";
    TransportClient oldClient = connectionPool.putIfAbsent(address, client.get());
    if (oldClient == null) {
      return client.get();
    } else {
      logger.debug("Two clients were created concurrently, second one will be disposed.");
      client.get().close();
      return oldClient;
    }
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    for (TransportClient client : connectionPool.values()) {
      try {
        client.close();
      } catch (RuntimeException e) {
        logger.warn("Ignoring exception during close", e);
      }
    }
    connectionPool.clear();

    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
      workerGroup = null;
    }
  }

  /**
   * Create a pooled ByteBuf allocator but disables the thread-local cache. Thread-local caches
   * are disabled because the ByteBufs are allocated by the event loop thread, but released by the
   * executor thread rather than the event loop thread. Those thread-local caches actually delay
   * the recycling of buffers, leading to larger memory usage.
   */
  private PooledByteBufAllocator createPooledByteBufAllocator() {
    return new PooledByteBufAllocator(
        PlatformDependent.directBufferPreferred(),
        getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"),
        getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"),
        getPrivateStaticField("DEFAULT_PAGE_SIZE"),
        getPrivateStaticField("DEFAULT_MAX_ORDER"),
        0,  // tinyCacheSize
        0,  // smallCacheSize
        0   // normalCacheSize
    );
  }

  /** Used to get defaults from Netty's private static fields. */
  private int getPrivateStaticField(String name) {
    try {
      Field f = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.getInt(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
