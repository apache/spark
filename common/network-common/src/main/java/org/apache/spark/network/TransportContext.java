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

package org.apache.spark.network;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import com.codahale.metrics.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.apache.spark.internal.Logger;
import org.apache.spark.internal.LoggerFactory;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.SslMessageEncoder;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.ChunkFetchRequestHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.ssl.SSLFactory;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.NettyLogger;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a
 * {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 */
public class TransportContext implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  private static final NettyLogger nettyLogger = new NettyLogger();
  private final TransportConf conf;
  private final RpcHandler rpcHandler;
  private final boolean closeIdleConnections;
  // Non-null if SSL is enabled, null otherwise.
  @Nullable private final SSLFactory sslFactory;
  // Number of registered connections to the shuffle service
  private Counter registeredConnections = new Counter();

  /**
   * Force to create MessageEncoder and MessageDecoder so that we can make sure they will be created
   * before switching the current context class loader to ExecutorClassLoader.
   *
   * Netty's MessageToMessageEncoder uses Javassist to generate a matcher class and the
   * implementation calls "Class.forName" to check if this calls is already generated. If the
   * following two objects are created in "ExecutorClassLoader.findClass", it will cause
   * "ClassCircularityError". This is because loading this Netty generated class will call
   * "ExecutorClassLoader.findClass" to search this class, and "ExecutorClassLoader" will try to use
   * RPC to load it and cause to load the non-exist matcher class again. JVM will report
   * `ClassCircularityError` to prevent such infinite recursion. (See SPARK-17714)
   */
  private static final MessageToMessageEncoder<Message> ENCODER = MessageEncoder.INSTANCE;
  private static final MessageToMessageEncoder<Message> SSL_ENCODER = SslMessageEncoder.INSTANCE;
  private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

  // Separate thread pool for handling ChunkFetchRequest. This helps to enable throttling
  // max number of TransportServer worker threads that are blocked on writing response
  // of ChunkFetchRequest message back to the client via the underlying channel.
  private final EventLoopGroup chunkFetchWorkers;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false, false);
  }

  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    this(conf, rpcHandler, closeIdleConnections, false);
  }

  /**
   * Enables TransportContext initialization for underlying client and server.
   *
   * @param conf TransportConf
   * @param rpcHandler RpcHandler responsible for handling requests and responses.
   * @param closeIdleConnections Close idle connections if it is set to true.
   * @param isClientOnly This config indicates the TransportContext is only used by a client.
   *                     This config is more important when external shuffle is enabled.
   *                     It stops creating extra event loop and subsequent thread pool
   *                     for shuffle clients to handle chunked fetch requests.
   */
  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections,
      boolean isClientOnly) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.closeIdleConnections = closeIdleConnections;
    this.sslFactory = createSslFactory();

    if (conf.getModuleName() != null &&
        conf.getModuleName().equalsIgnoreCase("shuffle") &&
        !isClientOnly && conf.separateChunkFetchRequest()) {
      chunkFetchWorkers = NettyUtils.createEventLoop(
          IOMode.valueOf(conf.ioMode()),
          conf.chunkFetchHandlerThreads(),
          "shuffle-chunk-fetch-handler");
    } else {
      chunkFetchWorkers = null;
    }
  }

  /**
   * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
   * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
   * to create a Client.
   */
  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(new ArrayList<>());
  }

  /** Create a server which will attempt to bind to a specific port. */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
  }

  /** Create a server which will attempt to bind to a specific host and port. */
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
  }

  /** Creates a new server, binding to any available ephemeral port. */
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  public TransportServer createServer() {
    return createServer(0, new ArrayList<>());
  }

  public TransportChannelHandler initializePipeline(SocketChannel channel, boolean isClient) {
    return initializePipeline(channel, rpcHandler, isClient);
  }

  public boolean sslEncryptionEnabled() {
    return this.sslFactory != null;
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler,
      boolean isClient) {
    try {
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      ChannelPipeline pipeline = channel.pipeline();
      if (nettyLogger.getLoggingHandler() != null) {
        pipeline.addLast("loggingHandler", nettyLogger.getLoggingHandler());
      }

      if (sslEncryptionEnabled()) {
        SslHandler sslHandler;
        try {
          sslHandler = new SslHandler(sslFactory.createSSLEngine(isClient, channel.alloc()));
        } catch (Exception e) {
          throw new IllegalStateException("Error creating Netty SslHandler", e);
        }
        pipeline.addFirst("NettySslEncryptionHandler", sslHandler);
        // Cannot use zero-copy with HTTPS, so we add in our ChunkedWriteHandler just before the
        // MessageEncoder
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
      }

      pipeline
        .addLast("encoder", sslEncryptionEnabled()? SSL_ENCODER : ENCODER)
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
        .addLast("decoder", getDecoder())
        .addLast("idleStateHandler",
          new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      // Use a separate EventLoopGroup to handle ChunkFetchRequest messages for shuffle rpcs.
      if (chunkFetchWorkers != null) {
        ChunkFetchRequestHandler chunkFetchHandler = new ChunkFetchRequestHandler(
          channelHandler.getClient(), rpcHandler.getStreamManager(),
          conf.maxChunksBeingTransferred(), true /* syncModeEnabled */);
        pipeline.addLast(chunkFetchWorkers, "chunkFetchHandler", chunkFetchHandler);
      }
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  protected MessageToMessageDecoder<ByteBuf> getDecoder() {
    return DECODER;
  }

  private SSLFactory createSslFactory() {
    if (conf.sslRpcEnabled()) {
      if (conf.sslRpcEnabledAndKeysAreValid()) {
        return new SSLFactory.Builder()
          .openSslEnabled(conf.sslRpcOpenSslEnabled())
          .requestedProtocol(conf.sslRpcProtocol())
          .requestedCiphers(conf.sslRpcRequestedCiphers())
          .keyStore(conf.sslRpcKeyStore(), conf.sslRpcKeyStorePassword())
          .privateKey(conf.sslRpcPrivateKey())
          .privateKeyPassword(conf.sslRpcPrivateKeyPassword())
          .keyPassword(conf.sslRpcKeyPassword())
          .certChain(conf.sslRpcCertChain())
          .trustStore(
            conf.sslRpcTrustStore(),
            conf.sslRpcTrustStorePassword(),
            conf.sslRpcTrustStoreReloadingEnabled(),
            conf.sslRpctrustStoreReloadIntervalMs())
          .build();
      } else {
        logger.error("RPC SSL encryption enabled but keys not found!" +
          "Please ensure the configured keys are present.");
        throw new IllegalArgumentException("RPC SSL encryption enabled but keys not found!");
      }
    } else {
      return null;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    boolean separateChunkFetchRequest = conf.separateChunkFetchRequest();
    ChunkFetchRequestHandler chunkFetchRequestHandler = null;
    if (!separateChunkFetchRequest) {
      chunkFetchRequestHandler = new ChunkFetchRequestHandler(
        client, rpcHandler.getStreamManager(),
        conf.maxChunksBeingTransferred(), false /* syncModeEnabled */);
    }
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler, conf.maxChunksBeingTransferred(), chunkFetchRequestHandler);
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), separateChunkFetchRequest, closeIdleConnections, this);
  }

  public TransportConf getConf() { return conf; }

  public Counter getRegisteredConnections() {
    return registeredConnections;
  }

  @Override
  public void close() {
    if (chunkFetchWorkers != null) {
      chunkFetchWorkers.shutdownGracefully();
    }
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }
}
