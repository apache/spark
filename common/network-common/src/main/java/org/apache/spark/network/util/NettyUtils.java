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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadFactory;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueIoHandler;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.PlatformDependent;

/**
 * Utilities for creating various Netty constructs based on whether we're using NIO, EPOLL,
 * , KQUEUE, or AUTO.
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

  /** Message for the unreachable AUTO arms below; resolveMode never returns AUTO. */
  private static final String UNRESOLVED_AUTO_MODE = "AUTO should be resolved by resolveMode";

  /**
   * Resolves {@link IOMode#AUTO} to a concrete transport for the current platform: EPOLL on
   * Linux, KQUEUE on macOS, and NIO otherwise (including when the native transport is not
   * available). Any other mode is returned unchanged. Keeping this in one place stops the
   * event-loop and channel factories below from drifting apart.
   */
  private static volatile boolean epollNativePrepared;

  private static IOMode resolveMode(IOMode mode) {
    if (mode != IOMode.AUTO) {
      return mode;
    }
    if (JavaUtils.isLinux) {
      prepareEpollNativeLibrary();
    }
    if (JavaUtils.isLinux && Epoll.isAvailable()) {
      return IOMode.EPOLL;
    } else if (JavaUtils.isMac && KQueue.isAvailable()) {
      return IOMode.KQUEUE;
    } else {
      return IOMode.NIO;
    }
  }

  /** Creates a Netty EventLoopGroup based on the IOMode. */
  public static EventLoopGroup createEventLoop(IOMode mode, int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = createThreadFactory(threadPrefix);

    IoHandlerFactory handlerFactory = switch (resolveMode(mode)) {
      case NIO -> NioIoHandler.newFactory();
      case EPOLL -> EpollIoHandler.newFactory();
      case KQUEUE -> KQueueIoHandler.newFactory();
      case AUTO -> throw new IllegalStateException(UNRESOLVED_AUTO_MODE);
    };
    return new MultiThreadIoEventLoopGroup(numThreads, threadFactory, handlerFactory);
  }

  /** Returns the correct (client) SocketChannel class based on IOMode. */
  public static Class<? extends Channel> getClientChannelClass(IOMode mode) {
    return switch (resolveMode(mode)) {
      case NIO -> NioSocketChannel.class;
      case EPOLL -> EpollSocketChannel.class;
      case KQUEUE -> KQueueSocketChannel.class;
      case AUTO -> throw new IllegalStateException(UNRESOLVED_AUTO_MODE);
    };
  }

  /** Returns the correct ServerSocketChannel class based on IOMode. */
  public static Class<? extends ServerChannel> getServerChannelClass(IOMode mode) {
    return switch (resolveMode(mode)) {
      case NIO -> NioServerSocketChannel.class;
      case EPOLL -> EpollServerSocketChannel.class;
      case KQUEUE -> KQueueServerSocketChannel.class;
      case AUTO -> throw new IllegalStateException(UNRESOLVED_AUTO_MODE);
    };
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

  /**
   * ByteBuf allocator prefers to allocate direct ByteBuf if both Spark allows to create direct
   * ByteBuf and Netty enables directBufferPreferred.
   */
  public static boolean preferDirectBufs(TransportConf conf) {
    boolean allowDirectBufs;
    if (conf.sharedByteBufAllocators()) {
      allowDirectBufs = conf.preferDirectBufsForSharedByteBufAllocators();
    } else {
      allowDirectBufs = conf.preferDirectBufs();
    }
    return allowDirectBufs && PlatformDependent.directBufferPreferred();
  }

  /**
   * Load Spark's epoll JNI library when another jar embeds a different copy.
   *
   * <p>Netty aborts if more than one
   * {@code META-INF/native/libnetty_transport_native_epoll_<arch>.so} is visible
   * and the bytes differ, then caches that failure for the JVM.
   * {@code ExceptionInInitializerError.getMessage()} is null, so callers only see
   * "epoll: null". sql tests hit this because snowflake-jdbc embeds a Netty 4.1
   * copy beside Spark's 4.2 classifier jar. Loading Spark's copy first registers
   * the JNI symbols, and {@link Epoll}'s initializer then skips Netty's duplicate
   * check. A single copy is left for Netty to load itself.
   */
  private static void prepareEpollNativeLibrary() {
    if (epollNativePrepared) {
      return;
    }
    synchronized (NettyUtils.class) {
      if (epollNativePrepared) {
        return;
      }
      epollNativePrepared = true;
      try {
        String arch = epollNativeArch(System.getProperty("os.arch", ""));
        String resource = "META-INF/native/libnetty_transport_native_epoll_" + arch + ".so";
        ClassLoader loader = NettyUtils.class.getClassLoader();
        Enumeration<URL> found = loader == null
            ? ClassLoader.getSystemResources(resource)
            : loader.getResources(resource);
        List<URL> urls = new ArrayList<>();
        while (found.hasMoreElements()) {
          urls.add(found.nextElement());
        }
        URL sparkCopy = sparkEpollNativeUrl(urls);
        if (sparkCopy == null) {
          return;
        }
        File tmp = File.createTempFile("libnetty_transport_native_epoll_", ".so");
        tmp.deleteOnExit();
        try (InputStream in = sparkCopy.openStream();
             FileOutputStream out = new FileOutputStream(tmp)) {
          in.transferTo(out);
        }
        tmp.setReadable(true, true);
        tmp.setExecutable(true, true);
        System.load(tmp.getAbsolutePath());
      } catch (Exception | LinkageError expected) {
        // Epoll.isAvailable reports the failure if the library did not load.
      }
    }
  }

  /** Netty's normalized arch suffix for the epoll classifier resource name. */
  static String epollNativeArch(String osArch) {
    String arch = osArch.toLowerCase(Locale.ROOT);
    if (arch.equals("amd64") || arch.equals("x86_64")) {
      return "x86_64";
    }
    if (arch.equals("aarch64") || arch.equals("arm64")) {
      return "aarch_64";
    }
    return arch;
  }

  /**
   * The classifier-jar URL to preload, or null when Netty should load on its own.
   * Null covers a single resource and a classpath with no Spark classifier jar.
   */
  static URL sparkEpollNativeUrl(List<URL> urls) {
    if (urls.size() < 2) {
      return null;
    }
    for (URL url : urls) {
      if (url.toString().contains("netty-transport-native-epoll")) {
        return url;
      }
    }
    return null;
  }
}
