/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle;

import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.common.ServerDetailCollection;
import org.apache.spark.remoteshuffle.decoders.StreamServerVersionDecoder;
import org.apache.spark.remoteshuffle.exceptions.RssAggregateException;
import org.apache.spark.remoteshuffle.execution.ShuffleExecutor;
import org.apache.spark.remoteshuffle.handlers.HttpChannelInboundHandler;
import org.apache.spark.remoteshuffle.handlers.UploadChannelManager;
import org.apache.spark.remoteshuffle.metadata.InMemoryServiceRegistry;
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry;
import org.apache.spark.remoteshuffle.metadata.StandaloneServiceRegistryClient;
import org.apache.spark.remoteshuffle.metrics.M3Stats;
import org.apache.spark.remoteshuffle.metrics.ScheduledMetricCollector;
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage;
import org.apache.spark.remoteshuffle.util.FileUtils;
import org.apache.spark.remoteshuffle.util.NetworkUtils;
import org.apache.spark.remoteshuffle.util.ServerHostAndPort;
import org.apache.spark.remoteshuffle.util.SystemUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.Future;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class StreamServer {
  private static final Logger logger = LoggerFactory.getLogger(StreamServer.class);

  private StreamServerConfig serverConfig;

  private final String hostName;
  private int shufflePort;
  private int httpPort;
  private ShuffleExecutor shuffleExecutor;
  private ServiceRegistry serviceRegistry;

  private EventLoopGroup shuffleBossGroup;
  private EventLoopGroup shuffleWorkerGroup;

  private EventLoopGroup healthCheckEventLoopGroup;

  private UploadChannelManager channelManager;
  private List<Channel> channels = new ArrayList<>(2);

  // this is used when the shuffle server could serve as a registry server
  private final ServerDetailCollection serverDetailCollection = new ServerDetailCollection();

  public StreamServer(StreamServerConfig serverConfig) {
    this(serverConfig, null);
  }

  public StreamServer(StreamServerConfig serverConfig, ServiceRegistry serviceRegistry) {
    this.hostName = NetworkUtils.getLocalFQDN();
    construct(serverConfig, serviceRegistry);
  }

  public String getRootDir() {
    return serverConfig.getRootDirectory();
  }

  private void construct(StreamServerConfig serverConfig, ServiceRegistry serviceRegistry) {
    this.serverConfig = serverConfig;

    // Health check is very light load, thus use very few threads
    final int healthCheckThreads = 2;

    if (serverConfig.isUseEpoll()) {
      shuffleBossGroup = new EpollEventLoopGroup(serverConfig.getNettyAcceptThreads());
      shuffleWorkerGroup = new EpollEventLoopGroup(serverConfig.getNettyWorkerThreads());
      healthCheckEventLoopGroup = new EpollEventLoopGroup(healthCheckThreads);
    } else {
      shuffleBossGroup = new NioEventLoopGroup(serverConfig.getNettyAcceptThreads());
      shuffleWorkerGroup = new NioEventLoopGroup(serverConfig.getNettyWorkerThreads());
      healthCheckEventLoopGroup = new NioEventLoopGroup(healthCheckThreads);
    }

    if (serviceRegistry == null) {
      createServiceRegistry(serverConfig);
    } else {
      this.serviceRegistry = serviceRegistry;
    }

    this.shuffleExecutor = new ShuffleExecutor(serverConfig.getRootDirectory(),
        serverConfig.getStorage(),
        serverConfig.getAppMemoryRetentionMillis(),
        serverConfig.getAppMaxWriteBytes());

    channelManager = new UploadChannelManager();
    channelManager.setMaxConnections(serverConfig.getMaxConnections());
  }

  private void createServiceRegistry(StreamServerConfig serverConfig) {
    switch (serverConfig.getServiceRegistryType()) {
      case ServiceRegistry.TYPE_INMEMORY:
        this.serviceRegistry = new InMemoryServiceRegistry();
        break;
      case ServiceRegistry.TYPE_STANDALONE:
        String registryServer = serverConfig.getRegistryServer();
        if (registryServer != null && !registryServer.isEmpty()) {
          ServerHostAndPort hostAndPort = ServerHostAndPort.fromString(registryServer);
          logger
              .info(String.format("Creating registry client connecting to registry server: %s:%s",
                  hostAndPort.getHost(), hostAndPort.getPort()));
          this.serviceRegistry = new StandaloneServiceRegistryClient(hostAndPort.getHost(),
              hostAndPort.getPort(),
              serverConfig.getNetworkTimeout(),
              "streamServer");
        } else {
          logger.info("Registry server is not specified, will use localhost as registry server" +
              " and create registry client when local stream server is " +
              "started (need to get port at that time)");
        }
        break;
      default:
        throw new RuntimeException(
            "Unknown service registry type: " + serverConfig.getServiceRegistryType());
    }
  }

  private Pair<Channel, Integer> bindPort(ServerBootstrap bootstrap, int port)
      throws InterruptedException, BindException {
    logger.info(String.format("Binding to specified port: %s", port));
    Channel channel = bootstrap.bind(port).sync().channel();
    InetSocketAddress localAddress = (InetSocketAddress) channel.localAddress();
    logger.info(String.format("Bound to local address: %s", localAddress));
    return Pair.of(channel, localAddress.getPort());
  }

  private ServerBootstrap bootstrapChannel(EventLoopGroup bossGroup, EventLoopGroup workerGroup,
                                           int backlogSize,
                                           int timeoutMillis,
                                           Supplier<ChannelHandler[]> handlerSupplier) {
    ServerBootstrap serverBootstrap = bossGroup instanceof EpollEventLoopGroup ?
        new ServerBootstrap().group(bossGroup, workerGroup).channel(EpollServerSocketChannel.class)
        :
        new ServerBootstrap().group(bossGroup, workerGroup).channel(NioServerSocketChannel.class);

    return serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(final SocketChannel ch) {
        ch.pipeline().addLast(handlerSupplier.get());
      }
    })
        .option(ChannelOption.SO_BACKLOG, backlogSize)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMillis)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
  }

  public void run() throws InterruptedException, BindException {
    logger.info(String.format("Number of opened files: %s", SystemUtils.getFileDescriptorCount()));

    String serverId = getServerId();

    Supplier<ChannelHandler[]> streamHandlers = () -> new ChannelHandler[]{
        new StreamServerVersionDecoder(serverId, serverConfig.getIdleTimeoutMillis(),
            shuffleExecutor, channelManager, serverDetailCollection)
    };
    ServerBootstrap streamServerBootstrap = bootstrapChannel(shuffleBossGroup, shuffleWorkerGroup,
        serverConfig.getNetworkBacklog(), serverConfig.getNetworkTimeout(), streamHandlers);

    final int httpBacklog = 32; // HTTP is only used for health check, thus good with small
    // value and not configurable
    Supplier<ChannelHandler[]> httpHandlers = () -> new ChannelHandler[]{
        new HttpServerCodec(),
        new HttpObjectAggregator(512 * 1024),
        new HttpChannelInboundHandler()
    };
    // increase http connection timeout for health check endpoint
    ServerBootstrap httpBootstrap =
        bootstrapChannel(healthCheckEventLoopGroup, healthCheckEventLoopGroup,
            httpBacklog, serverConfig.getNetworkTimeout() * 3, httpHandlers);

    // Bind the ports and save the results so that the channels can be closed later. If the second bind fails,
    // the first one gets cleaned up in the shutdown.
    Pair<Channel, Integer> channelAndPort =
        bindPort(streamServerBootstrap, serverConfig.getShufflePort());
    channels.add(channelAndPort.getKey());
    shufflePort = channelAndPort.getValue();
    logger.info(String.format("ShuffleServer: %s:%s", hostName, shufflePort));

    if (this.serviceRegistry == null && serverConfig.getServiceRegistryType().
        equalsIgnoreCase(ServiceRegistry.TYPE_STANDALONE)) {
      logger
          .info(String.format("Creating registry client connecting to local stream server: %s:%s",
              hostName, shufflePort));
      this.serviceRegistry = new StandaloneServiceRegistryClient(this.hostName, shufflePort,
          serverConfig.getNetworkTimeout(), "streamServer");
    }
    String dataCenter = serverConfig.getDataCenterOrDefault();
    String cluster = serverConfig.getClusterOrDefault();
    String hostAndPort = String.format("%s:%s", hostName, shufflePort);
    logger.info(
        String.format("Registering shuffle server, data center: %s, cluster: %s, server id: %s, " +
            "host and port: %s", dataCenter, cluster, serverId, hostAndPort));
    this.serviceRegistry.registerServer(dataCenter, cluster, serverId, hostAndPort);

    if (serverConfig.getHttpPort() != -1) {
      channelAndPort = bindPort(httpBootstrap, serverConfig.getHttpPort());
      channels.add(channelAndPort.getKey());
      httpPort = channelAndPort.getValue();
      logger.info(String.format("HttpServer: %s:%s", hostName, httpPort));
    } else {
      httpPort = serverConfig.getHttpPort();
    }

    if (serverConfig.getStorage() instanceof ShuffleFileStorage) {
      CompletableFuture.runAsync(() -> {
        FileUtils.cleanupOldFiles(serverConfig.getRootDirectory(), System.currentTimeMillis()
            - serverConfig.getAppFileRetentionMillis());
      });
    }

    M3Stats.getDefaultScope().counter("serverStart").inc(1);
  }

  public int getShufflePort() {
    return shufflePort;
  }

  public String getShuffleConnectionString() {
    return String.format("%s:%s", hostName, shufflePort);
  }

  public int getHttpPort() {
    return httpPort;
  }

  public String getServerId() {
    String envValue = System.getenv("RSS_SERVER_ID");
    if (envValue == null || envValue.isEmpty()) {
      // use host name and root directory to uniquely identify a server
      return String.format("%s:%s", hostName, serverConfig.getRootDirectory());
    } else {
      return envValue;
    }
  }

  public ServerDetail getServerDetail() {
    return new ServerDetail(getServerId(), getShuffleConnectionString());
  }

  public ServiceRegistry getServiceRegistry() {
    return serviceRegistry;
  }

  public ShuffleExecutor getShuffleExecutor() {
    return shuffleExecutor;
  }

  public void shutdown() {
    shutdown(false);
  }

  public void shutdown(boolean wait) {
    List<Throwable> exceptions = new ArrayList<>();

    try {
      serviceRegistry.close();
    } catch (Throwable e) {
      logger.warn("Unable to shutdown metadata store:", e);
      exceptions.add(e);
    }

    for (Channel c : channels) {
      try {
        c.close();
      } catch (Throwable e) {
        logger.warn(String.format("Unable to shutdown channel %s:", c), e);
        exceptions.add(e);
      }
    }

    Future<?> healthFuture = healthCheckEventLoopGroup.shutdownGracefully();
    Future<?> bossFuture = shuffleBossGroup.shutdownGracefully();
    Future<?> workerFuture = shuffleWorkerGroup.shutdownGracefully();

    try {
      healthFuture.get();
    } catch (Throwable ex) {
      logger.warn("Hit exception when shutting down health check event loop group", ex);
      exceptions.add(ex);
    }

    try {
      bossFuture.get();
    } catch (Throwable ex) {
      logger.warn("Hit exception when shutting down shuffle boss event loop group", ex);
      exceptions.add(ex);
    }

    try {
      workerFuture.get();
    } catch (Throwable ex) {
      logger.warn("Hit exception when shutting down shuffle worker event loop group", ex);
      exceptions.add(ex);
    }

    try {
      shuffleExecutor.stop(wait);
    } catch (Throwable e) {
      logger.warn("Unable to shutdown writer executor:", e);
      exceptions.add(e);
    }

    logger.info(String.format("Number of opened files: %s", SystemUtils.getFileDescriptorCount()));

    if (!exceptions.isEmpty()) {
      throw new RssAggregateException(exceptions);
    }
  }

  @Override
  public String toString() {
    return "StreamServer{" +
        "serverId='" + getServerId() + '\'' +
        ", hostName='" + hostName + '\'' +
        ", shufflePort=" + shufflePort +
        '}';
  }

  private static Thread addShutdownHook(StreamServer server) {
    Thread shutdownHook = new Thread(() -> {
      logger.info("Started shutting down server in shutdown hook");
      server.shutdown();
      logger.info("Finished shutting down server in shutdown hook");
    });
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    return shutdownHook;
  }

  public static void main(String[] args) throws Exception {
    StreamServerConfig serverConfig = StreamServerConfig.buildFromArgs(args);
    logger.info(String.format("Starting server (version: %s, revision: %s) with config: %s",
        RssBuildInfo.Version, RssBuildInfo.Revision, serverConfig));
    StreamServer server = new StreamServer(serverConfig);
    server.run();
    addShutdownHook(server);

    ScheduledMetricCollector scheduledMetricCollector =
        new ScheduledMetricCollector(server.serviceRegistry);
    scheduledMetricCollector
        .scheduleCollectingMetrics(server.shuffleExecutor.getLowPriorityExecutorService(),
            serverConfig.getDataCenterOrDefault(),
            serverConfig.getClusterOrDefault());
  }
}
