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

package org.apache.spark.network.util;

import java.util.concurrent.ThreadFactory;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.PlatformDependent;

/**
 * Utilities for creating various Netty constructs based on whether we're using EPOLL or NIO.
 */
public class NettyUtils {

  /**
   * Specifies an upper bound on the number of Netty threads that Spark requires by default.
   * In practice, only 2-4 cores should be required to transfer roughly 10 Gb/s, and each core
   * that we use will have an initial overhead of roughly 32 MB of off-heap memory, which comes
   * at a premium.
   *
   * Thus, this value should still retain maximum throughput and reduce wasted off-heap memory
   * allocation. It can be overridden by setting the number of serverThreads and clientThreads
   * manually in Spark's configuration.
   */
  private static int MAX_DEFAULT_NETTY_THREADS = 8;

  private static final PooledByteBufAllocator[] _sharedPooledByteBufAllocator =
      new PooledByteBufAllocator[2];

  public static long freeDirectMemory() {
    return PlatformDependent.maxDirectMemory() - PlatformDependent.usedDirectMemory();
  }

  /** Creates a new ThreadFactory which prefixes each thread with the given name. */
  public static ThreadFactory createThreadFactory(String threadPoolPrefix) {
    return new DefaultThreadFactory(threadPoolPrefix, true);
  }

  /** Creates a Netty EventLoopGroup based on the IOMode. */
  public static EventLoopGroup createEventLoop(IOMode mode, int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = createThreadFactory(threadPrefix);

    switch (mode) {
      case NIO:
        return new NioEventLoopGroup(numThreads, threadFactory);
      case EPOLL:
        return new EpollEventLoopGroup(numThreads, threadFactory);
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /** Returns the correct (client) SocketChannel class based on IOMode. */
  public static Class<? extends Channel> getClientChannelClass(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioSocketChannel.class;
      case EPOLL:
        return EpollSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /** Returns the correct ServerSocketChannel class based on IOMode. */
  public static Class<? extends ServerChannel> getServerChannelClass(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioServerSocketChannel.class;
      case EPOLL:
        return EpollServerSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /**
   * Creates a LengthFieldBasedFrameDecoder where the first 8 bytes are the length of the frame.
   * This is used before all decoders.
   */
  public static TransportFrameDecoder createFrameDecoder() {
    return new TransportFrameDecoder();
  }

  /** Returns the remote address on the channel or "&lt;unknown remote&gt;" if none exists. */
  public static String getRemoteAddress(Channel channel) {
    if (channel != null && channel.remoteAddress() != null) {
      return channel.remoteAddress().toString();
    }
    return "<unknown remote>";
  }

  /**
   * Returns the default number of threads for both the Netty client and server thread pools.
   * If numUsableCores is 0, we will use Runtime get an approximate number of available cores.
   */
  public static int defaultNumThreads(int numUsableCores) {
    final int availableCores;
    if (numUsableCores > 0) {
      availableCores = numUsableCores;
    } else {
      availableCores = Runtime.getRuntime().availableProcessors();
    }
    return Math.min(availableCores, MAX_DEFAULT_NETTY_THREADS);
  }

  /**
   * Returns the lazily created shared pooled ByteBuf allocator for the specified allowCache
   * parameter value.
   */
  public static synchronized PooledByteBufAllocator getSharedPooledByteBufAllocator(
      boolean allowDirectBufs,
      boolean allowCache) {
    final int index = allowCache ? 0 : 1;
    if (_sharedPooledByteBufAllocator[index] == null) {
      _sharedPooledByteBufAllocator[index] =
        createPooledByteBufAllocator(
          allowDirectBufs,
          allowCache,
          defaultNumThreads(0));
    }
    return _sharedPooledByteBufAllocator[index];
  }

  /**
   * Create a pooled ByteBuf allocator but disables the thread-local cache. Thread-local caches
   * are disabled for TransportClients because the ByteBufs are allocated by the event loop thread,
   * but released by the executor thread rather than the event loop thread. Those thread-local
   * caches actually delay the recycling of buffers, leading to larger memory usage.
   */
  public static PooledByteBufAllocator createPooledByteBufAllocator(
      boolean allowDirectBufs,
      boolean allowCache,
      int numCores) {
    if (numCores == 0) {
      numCores = Runtime.getRuntime().availableProcessors();
    }
    // SPARK-38541: After upgrade to Netty 4.1.75, there are 2 behavior changes of this method:
    // 1. `PooledByteBufAllocator.defaultMaxOrder()` change from 11 to 9, this means the default
    //    `PooledByteBufAllocator` chunk size reduce from 16 MiB to 4 MiB, we need use
    //    `-Dio.netty.allocator.maxOrder=11` to keep the chunk size of PooledByteBufAllocator
    //    to 16m.
    // 2. `PooledByteBufAllocator.defaultUseCacheForAllThreads()` change from true to false, we need
    //    to use `-Dio.netty.allocator.useCacheForAllThreads=true` to
    //    enable `useCacheForAllThreads`.
    return new PooledByteBufAllocator(
      allowDirectBufs && PlatformDependent.directBufferPreferred(),
      Math.min(PooledByteBufAllocator.defaultNumHeapArena(), numCores),
      Math.min(PooledByteBufAllocator.defaultNumDirectArena(), allowDirectBufs ? numCores : 0),
      PooledByteBufAllocator.defaultPageSize(),
      PooledByteBufAllocator.defaultMaxOrder(),
      allowCache ? PooledByteBufAllocator.defaultSmallCacheSize() : 0,
      allowCache ? PooledByteBufAllocator.defaultNormalCacheSize() : 0,
      allowCache ? PooledByteBufAllocator.defaultUseCacheForAllThreads() : false
    );
  }
}
