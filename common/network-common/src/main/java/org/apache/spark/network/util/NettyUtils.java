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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerSocketChannel;
import io.netty.channel.uring.IoUringSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.PlatformDependent;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

/**
 * Utilities for creating various Netty constructs based on whether we're using NIO, EPOLL,
 * KQUEUE, IO_URING, or AUTO.
 */
public class NettyUtils {

  private static final SparkLogger logger = SparkLoggerFactory.getLogger(NettyUtils.class);

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

  /**
   * Cached result of probing whether io_uring can actually allocate the rings Spark needs on
   * this JVM. {@code null} means not yet probed; non-null is the probed value.
   *
   * <p>{@link IoUring#isAvailable()} only checks that the JNI library loaded and basic syscalls
   * work; it does not detect environments where the kernel allows io_uring but
   * {@code RLIMIT_MEMLOCK} is too low for the submission/completion queue rings (common in
   * containers, GitHub Actions runners, and other restricted environments). The probe creates
   * a {@link MultiThreadIoEventLoopGroup} sized to {@link #MAX_DEFAULT_NETTY_THREADS} -- which
   * is the worst case Spark allocates by default for a single event loop group -- and shuts it
   * down to verify ring allocation actually succeeds. Probing with one thread is insufficient
   * because some restricted environments allow a single ring to fit in memlock but not the eight
   * rings Spark needs in practice.
   */
  private static volatile Boolean ioUringUsable = null;

  /**
   * Guards a one-shot info log line describing the io_uring features the running kernel exposes.
   * Emitted the first time {@link #createEventLoop} is asked for {@link IOMode#IO_URING}, so it
   * appears once per process when -- and only when -- io_uring is actually selected. Useful for
   * triaging shuffle/RPC performance reports: it answers questions like "is {@code SPLICE}
   * supported on this kernel?" and "what is the kernel's max pipe buffer size?" without needing
   * to attach a debugger.
   */
  private static final AtomicBoolean ioUringCapabilitiesLogged = new AtomicBoolean(false);

  public static long freeDirectMemory() {
    return PlatformDependent.maxDirectMemory() - PlatformDependent.usedDirectMemory();
  }

  /**
   * Returns true if io_uring can actually be used on the running JVM. Probes once (with the
   * result cached) by attempting a real ring allocation, which catches environments where
   * {@link IoUring#isAvailable()} returns true but {@code RLIMIT_MEMLOCK} is too low to allocate
   * the submission/completion queues (common in containers, GitHub Actions runners, and other
   * restricted environments).
   *
   * <p>Used by tests that gate execution on io_uring being usable. AUTO mode does not consult
   * this; io_uring is opt-in only via {@link IOMode#IO_URING}, which surfaces the underlying
   * error when ring allocation fails.
   */
  public static boolean isIoUringUsable() {
    Boolean cached = ioUringUsable;
    if (cached != null) {
      return cached;
    }
    synchronized (NettyUtils.class) {
      if (ioUringUsable != null) {
        return ioUringUsable;
      }
      if (!JavaUtils.isLinux || !IoUring.isAvailable()) {
        ioUringUsable = false;
        return false;
      }
      MultiThreadIoEventLoopGroup probe = null;
      try {
        probe = new MultiThreadIoEventLoopGroup(
            MAX_DEFAULT_NETTY_THREADS, IoUringIoHandler.newFactory());
        ioUringUsable = true;
      } catch (Throwable t) {
        logger.warn("io_uring is reported as available but allocation of " +
            MAX_DEFAULT_NETTY_THREADS + " rings failed. " +
            "Common cause: RLIMIT_MEMLOCK too low (containers, restricted environments).", t);
        ioUringUsable = false;
      } finally {
        if (probe != null) {
          probe.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
        }
      }
      return ioUringUsable;
    }
  }

  /**
   * Emits a single info log line summarizing the io_uring features the running kernel exposes,
   * the kernel version, and the system-wide max pipe buffer size. Called from the IO_URING branch
   * of {@link #createEventLoop} so it logs at most once per process and only when io_uring is
   * actually used.
   *
   * <p>The pipe-max-size matters because Netty's {@code IoUringFileRegion} routes FileRegion
   * sends through a {@code splice(2)} pipe (file -&gt; pipe -&gt; socket), bounded by the pipe
   * buffer; a small pipe forces more SQE/CQE round-trips per shuffle chunk.
   */
  private static void logIoUringCapabilitiesOnce() {
    if (!ioUringCapabilitiesLogged.compareAndSet(false, true)) {
      return;
    }
    String pipeMaxSize = "unknown";
    try {
      pipeMaxSize = Files.readString(Path.of("/proc/sys/fs/pipe-max-size")).trim();
    } catch (IOException | RuntimeException ignored) {
      // Not Linux, or /proc not mounted; leave as "unknown".
    }
    logger.info("Netty io_uring transport selected: features=[" + IoUring.featureString() +
        "], kernel=" + System.getProperty("os.version") +
        ", pipe-max-size=" + pipeMaxSize + " bytes");
  }

  /** Creates a new ThreadFactory which prefixes each thread with the given name. */
  public static ThreadFactory createThreadFactory(String threadPoolPrefix) {
    return new DefaultThreadFactory(threadPoolPrefix, true);
  }

  /** Creates a Netty EventLoopGroup based on the IOMode. */
  public static EventLoopGroup createEventLoop(IOMode mode, int numThreads, String threadPrefix) {
    ThreadFactory threadFactory = createThreadFactory(threadPrefix);

    IoHandlerFactory handlerFactory = switch (mode) {
      case NIO -> NioIoHandler.newFactory();
      case EPOLL -> EpollIoHandler.newFactory();
      case KQUEUE -> KQueueIoHandler.newFactory();
      case IO_URING -> {
        logIoUringCapabilitiesOnce();
        yield IoUringIoHandler.newFactory();
      }
      case AUTO -> {
        if (JavaUtils.isLinux && Epoll.isAvailable()) {
          yield EpollIoHandler.newFactory();
        } else if (JavaUtils.isMac && KQueue.isAvailable()) {
          yield KQueueIoHandler.newFactory();
        } else {
          yield NioIoHandler.newFactory();
        }
      }
    };
    return new MultiThreadIoEventLoopGroup(numThreads, threadFactory, handlerFactory);
  }

  /** Returns the correct (client) SocketChannel class based on IOMode. */
  public static Class<? extends Channel> getClientChannelClass(IOMode mode) {
    return switch (mode) {
      case NIO -> NioSocketChannel.class;
      case EPOLL -> EpollSocketChannel.class;
      case KQUEUE -> KQueueSocketChannel.class;
      case IO_URING -> IoUringSocketChannel.class;
      case AUTO -> {
        if (JavaUtils.isLinux && Epoll.isAvailable()) {
          yield EpollSocketChannel.class;
        } else if (JavaUtils.isMac && KQueue.isAvailable()) {
          yield KQueueSocketChannel.class;
        } else {
          yield NioSocketChannel.class;
        }
      }
    };
  }

  /** Returns the correct ServerSocketChannel class based on IOMode. */
  public static Class<? extends ServerChannel> getServerChannelClass(IOMode mode) {
    return switch (mode) {
      case NIO -> NioServerSocketChannel.class;
      case EPOLL -> EpollServerSocketChannel.class;
      case KQUEUE -> KQueueServerSocketChannel.class;
      case IO_URING -> IoUringServerSocketChannel.class;
      case AUTO -> {
        if (JavaUtils.isLinux && Epoll.isAvailable()) {
          yield EpollServerSocketChannel.class;
        } else if (JavaUtils.isMac && KQueue.isAvailable()) {
          yield KQueueServerSocketChannel.class;
        } else {
          yield NioServerSocketChannel.class;
        }
      }
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
}
