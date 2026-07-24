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
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.MetricSet;
import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.*;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 */
public class TransportClientFactory implements Closeable {

  /** A simple data structure to track the pool of clients between two peer nodes. */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;
    volatile long lastConnectionFailed;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
      lastConnectionFailed = 0;
    }
  }

  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;
  private final List<TransportClientBootstrap> clientBootstraps;
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /** Random number generator for picking connections between peers. */
  private final Random rand;
  private final int numConnectionsPerPeer;

  private final Class<? extends Channel> socketChannelClass;
  private final IOMode ioMode;
  // The client worker EventLoopGroup. A netty event-loop thread that dies (e.g. an uncaught error)
  // is never replaced within a fixed-size group, permanently poisoning any channel pinned to it
  // (SPARK-58292). When createClient sees a connection fail because the loop is dead, it replaces
  // this group with a fresh one so subsequent connections bind to live threads; volatile so the
  // swap is visible to concurrent createClient callers.
  private volatile EventLoopGroup workerGroup;
  // Superseded worker groups are not shut down eagerly: their still-live threads may be serving
  // channels that are already open. We keep weak references and shut them down best-effort at
  // close(); their threads are daemon, so a not-yet-collected group cannot block JVM shutdown.
  private final List<WeakReference<EventLoopGroup>> deprecatedWorkerGroups =
    new CopyOnWriteArrayList<>();
  // Whether to recreate the worker group on a dead event loop (SPARK-58292); on by default.
  private final boolean recreateWorkerGroupOnDeadEventLoop;
  // How many times the worker group has been recreated after a dead event loop. Used only to make
  // each recreated group's thread names distinct; mutated under the recreateWorkerGroup lock.
  private int workerGroupRecreationCount;
  private final PooledByteBufAllocator pooledAllocator;
  private final NettyMemoryMetrics metrics;
  private final int fastFailTimeWindow;

  public TransportClientFactory(
      TransportContext context,
      List<TransportClientBootstrap> clientBootstraps) {
    this.context = Objects.requireNonNull(context);
    this.conf = context.getConf();
    this.clientBootstraps = new ArrayList<>(Objects.requireNonNull(clientBootstraps));
    this.connectionPool = new ConcurrentHashMap<>();
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.rand = new Random();

    this.ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    this.workerGroup = NettyUtils.createEventLoop(
        ioMode,
        conf.clientThreads(),
        conf.getModuleName() + "-client");
    if (conf.sharedByteBufAllocators()) {
      this.pooledAllocator = NettyUtils.getSharedPooledByteBufAllocator(
          conf.preferDirectBufsForSharedByteBufAllocators(), false /* allowCache */);
    } else {
      this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
          conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
    }
    this.metrics = new NettyMemoryMetrics(
      this.pooledAllocator, conf.getModuleName() + "-client", conf);
    this.recreateWorkerGroupOnDeadEventLoop = conf.recreateWorkerGroupOnDeadEventLoop();
    fastFailTimeWindow = (int)(conf.ioRetryWaitTimeMs() * 0.95);
  }

  public MetricSet getAllMetrics() {
    return metrics;
  }

  @VisibleForTesting
  EventLoopGroup getWorkerGroup() {
    return workerGroup;
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * We maintain an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
   * and randomly picks one to use. If no client was previously created in the randomly selected
   * spot, this function creates a new client and places it there.
   *
   * If the fastFail parameter is true, fail immediately when the last attempt to the same address
   * failed within the fast fail time window (95 percent of the io wait retry timeout). The
   * assumption is the caller will handle retrying.
   *
   * Prior to the creation of a new TransportClient, we will execute all
   * {@link TransportClientBootstrap}s that are registered with this factory.
   *
   * This blocks until a connection is successfully established and fully bootstrapped.
   *
   * Concurrency: This method is safe to call from multiple threads.
   *
   * @param remoteHost remote address host
   * @param remotePort remote address port
   * @param fastFail whether this call should fail immediately when the last attempt to the same
   *                 address failed with in the last fast fail time window.
   */
  public TransportClient createClient(String remoteHost, int remotePort, boolean fastFail)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    final InetSocketAddress unresolvedAddress =
      InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool = connectionPool.computeIfAbsent(unresolvedAddress,
        key -> new ClientPool(numConnectionsPerPeer));
    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      TransportChannelHandler handler = cachedClient.getChannel().pipeline()
        .get(TransportChannelHandler.class);
      if (handler != null) {
        synchronized (handler) {
          handler.getResponseHandler().updateTimeOfLastRequest();
        }
      }

      if (cachedClient.isActive()) {
        logger.trace("Returning cached connection to {}: {}",
          cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    final String resolvMsg = resolvedAddress.isUnresolved() ? "failed" : "succeed";
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution {} for {} took {} ms",
        MDC.of(LogKeys.STATUS, resolvMsg),
        MDC.of(LogKeys.HOST_PORT, resolvedAddress),
        MDC.of(LogKeys.TIME, hostResolveTimeMs));
    } else {
      logger.trace("DNS resolution {} for {} took {} ms",
          resolvMsg, resolvedAddress, hostResolveTimeMs);
    }

    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.",
            MDC.of(LogKeys.HOST_PORT, resolvedAddress));
        }
      }
      // If this connection should fast fail when last connection failed in last fast fail time
      // window and it did, fail this connection directly.
      if (fastFail && System.currentTimeMillis() - clientPool.lastConnectionFailed <
        fastFailTimeWindow) {
        throw new IOException(
          String.format("Connecting to %s failed in the last %s ms, fail this connection directly",
            resolvedAddress, fastFailTimeWindow));
      }
      try {
        clientPool.clients[clientIndex] = createClient(resolvedAddress);
        clientPool.lastConnectionFailed = 0;
      } catch (IOException e) {
        clientPool.lastConnectionFailed = System.currentTimeMillis();
        throw e;
      }
      return clientPool.clients[clientIndex];
    }
  }

  public TransportClient createClient(String remoteHost, int remotePort)
    throws IOException, InterruptedException {
    return createClient(remoteHost, remotePort, false);
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port.
   * This connection is not pooled.
   *
   * As with {@link #createClient(String, int)}, this method is blocking.
   */
  public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    return createClient(address);
  }

  /** Create a completely new {@link TransportClient} to the remote address. */
  @VisibleForTesting
  TransportClient createClient(InetSocketAddress address)
      throws IOException, InterruptedException {
    logger.debug("Creating new connection to {}", address);

    Bootstrap bootstrap = new Bootstrap();
    int connCreateTimeout = conf.connectionCreationTimeoutMs();
    // Capture the group this connection uses, so that on a dead-event-loop failure we replace
    // exactly this group (and not one a concurrent caller already swapped in).
    final EventLoopGroup connectGroup = workerGroup;
    bootstrap.group(connectGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connCreateTimeout)
      .option(ChannelOption.ALLOCATOR, pooledAllocator);

    if (conf.receiveBuf() > 0) {
      bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }

    if (conf.sendBuf() > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        TransportChannelHandler clientHandler = context.initializePipeline(ch, true);
        clientRef.set(clientHandler.getClient());
        channelRef.set(ch);
      }
    });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);

    try {
      if (connCreateTimeout <= 0) {
        cf.await();
        assert cf.isDone();
        if (cf.isCancelled()) {
          throw new IOException(String.format("Connecting to %s cancelled", address));
        } else if (!cf.isSuccess()) {
          throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
        }
      } else if (!cf.await(connCreateTimeout)) {
        throw new IOException(
          String.format("Connecting to %s timed out (%s ms)",
            address, connCreateTimeout));
      } else if (cf.cause() != null) {
        throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
      }
    } catch (IOException e) {
      // If the connection failed because the channel could not be registered on its netty event
      // loop (the loop's thread has died and netty rejects new tasks with "event executor
      // terminated"), the worker group is permanently degraded: that dead loop is never replaced
      // and keeps being handed out by the round-robin chooser. Replace the group so retries bind
      // to fresh live threads, then rethrow so the caller (e.g. RetryingBlockTransferor) retries.
      recreateWorkerGroupIfEventLoopDead(connectGroup, cf.cause());
      throw e;
    }
    if (context.sslEncryptionEnabled()) {
      final SslHandler sslHandler = cf.channel().pipeline().get(SslHandler.class);
      Future<Channel> future = sslHandler.handshakeFuture().addListener(
        new GenericFutureListener<Future<Channel>>() {
          @Override
          public void operationComplete(final Future<Channel> handshakeFuture) {
            if (handshakeFuture.isSuccess()) {
              logger.debug("{} successfully completed TLS handshake to ", address);
            } else {
              logger.info("failed to complete TLS handshake to {}", handshakeFuture.cause(),
                MDC.of(LogKeys.HOST_PORT, address));
              cf.channel().close();
            }
          }
      });
      if (!future.await(conf.connectionTimeoutMs())) {
        cf.channel().close();
        throw new IOException(
          String.format("Failed to connect to %s within connection timeout", address));
      }
    }

    TransportClient client = clientRef.get();
    Channel channel = channelRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);
    try {
      for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
        clientBootstrap.doBootstrap(client, channel);
      }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
      long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
      logger.error("Exception while bootstrapping client after {} ms", e,
        MDC.of(LogKeys.BOOTSTRAP_TIME, bootstrapTimeMs));
      client.close();
      if (e instanceof RuntimeException re) throw re;
      throw new RuntimeException(e);
    }
    long postBootstrap = System.nanoTime();

    logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
      MDC.of(LogKeys.HOST_PORT, address),
      MDC.of(LogKeys.ELAPSED_TIME, (postBootstrap - preConnect) / 1000000),
      MDC.of(LogKeys.BOOTSTRAP_TIME, (postBootstrap - preBootstrap) / 1000000));

    return client;
  }

  /**
   * If the given connection-failure cause was a rejection by a dead netty event loop (its worker
   * thread terminated and netty rejects new registrations with a
   * {@link RejectedExecutionException}), replace the worker group so subsequent connections bind to
   * fresh, live threads. A dead loop is never replaced within a fixed-size group and keeps being
   * selected by the round-robin chooser, so without this the degradation is permanent. See
   * SPARK-58292.
   */
  private void recreateWorkerGroupIfEventLoopDead(EventLoopGroup connectGroup, Throwable cause) {
    if (!recreateWorkerGroupOnDeadEventLoop) {
      return;
    }
    boolean eventLoopDead = false;
    for (Throwable t = cause; t != null; t = t.getCause()) {
      // Match ONLY the terminated-loop rejection, not a transient task-queue-full rejection.
      // netty's SingleThreadEventExecutor.reject() throws exactly this message when isShutdown();
      // the queue-full handler path throws a RejectedExecutionException with no message.
      if (t instanceof RejectedExecutionException
          && "event executor terminated".equals(t.getMessage())) {
        eventLoopDead = true;
        break;
      }
    }
    if (eventLoopDead) {
      recreateWorkerGroup(connectGroup);
    }
  }

  /**
   * Replace the worker group with a fresh one, if it is still the group the failed connection used
   * ({@code connectGroup}). The superseded group is not shut down here: its still-live threads may
   * be serving channels that are already open. We keep a weak reference and shut it down
   * best-effort at {@link #close()}; its threads are daemon, so a not-yet-collected group cannot
   * block JVM shutdown. Synchronized and identity-guarded so concurrent callers that all hit the
   * same dead group replace it exactly once rather than spawning many groups.
   */
  private synchronized void recreateWorkerGroup(EventLoopGroup connectGroup) {
    // A concurrent caller that hit the same dead group already swapped it out; nothing to do.
    if (workerGroup != connectGroup) {
      return;
    }
    // Use a distinct thread-name prefix so the recreated group is self-identifying in thread
    // dumps and logs. Netty's DefaultThreadFactory also appends an incrementing pool id, so the
    // names would not collide even with the same prefix, but tagging it "-recreated-<n>" makes the
    // dead-event-loop recovery obvious to anyone inspecting the process, and the <n> distinguishes
    // successive recreations if a loop dies more than once.
    workerGroupRecreationCount++;
    workerGroup = NettyUtils.createEventLoop(ioMode, conf.clientThreads(),
        conf.getModuleName() + "-client-recreated-" + workerGroupRecreationCount);
    deprecatedWorkerGroups.add(new WeakReference<>(connectGroup));
    logger.warn("Detected a dead netty client event loop; replaced the worker group. The " +
      "previous group is retained until its open channels drain (SPARK-58292).");
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    if (workerGroup != null && !workerGroup.isShuttingDown()) {
      workerGroup.shutdownGracefully();
    }

    // Shut down any worker groups superseded after a dead-event-loop recreation, if not yet GC'd.
    for (WeakReference<EventLoopGroup> ref : deprecatedWorkerGroups) {
      EventLoopGroup group = ref.get();
      if (group != null && !group.isShuttingDown()) {
        group.shutdownGracefully();
      }
    }
    deprecatedWorkerGroups.clear();
  }
}
