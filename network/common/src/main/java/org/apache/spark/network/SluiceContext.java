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

import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.SluiceClient;
import org.apache.spark.network.client.SluiceClientFactory;
import org.apache.spark.network.client.SluiceResponseHandler;
import org.apache.spark.network.protocol.response.MessageDecoder;
import org.apache.spark.network.protocol.response.MessageEncoder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.SluiceChannelHandler;
import org.apache.spark.network.server.SluiceRequestHandler;
import org.apache.spark.network.server.SluiceServer;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.SluiceConfig;

/**
 * Contains the context to create a {@link SluiceServer}, {@link SluiceClientFactory}, and to setup
 * Netty Channel pipelines with a {@link SluiceChannelHandler}.
 *
 * The SluiceServer and SluiceClientFactory both create a SluiceChannelHandler for each channel.
 * As each SluiceChannelHandler contains a SluiceClient, this enables server processes to send
 * messages back to the client on an existing channel.
 */
public class SluiceContext {
  private final Logger logger = LoggerFactory.getLogger(SluiceContext.class);

  private final SluiceConfig conf;
  private final StreamManager streamManager;
  private final RpcHandler rpcHandler;

  private final MessageEncoder encoder;
  private final MessageDecoder decoder;

  public SluiceContext(SluiceConfig conf, StreamManager streamManager, RpcHandler rpcHandler) {
    this.conf = conf;
    this.streamManager = streamManager;
    this.rpcHandler = rpcHandler;
    this.encoder = new MessageEncoder();
    this.decoder = new MessageDecoder();
  }

  public SluiceClientFactory createClientFactory() {
    return new SluiceClientFactory(this);
  }

  public SluiceServer createServer() {
    return new SluiceServer(this);
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link SluiceChannelHandler} to handle request or response messages.
   *
   * @return Returns the created SluiceChannelHandler, which includes a SluiceClient that can be
   * used to communicate on this channel. The SluiceClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same SluiceClient object.
   */
  public SluiceChannelHandler initializePipeline(SocketChannel channel) {
    try {
      SluiceChannelHandler channelHandler = createChannelHandler(channel);
      channel.pipeline()
        .addLast("encoder", encoder)
        .addLast("frameDecoder", NettyUtils.createFrameDecoder())
        .addLast("decoder", decoder)
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private SluiceChannelHandler createChannelHandler(Channel channel) {
    SluiceResponseHandler responseHandler = new SluiceResponseHandler(channel);
    SluiceClient client = new SluiceClient(channel, responseHandler);
    SluiceRequestHandler requestHandler = new SluiceRequestHandler(channel, client, streamManager,
      rpcHandler);
    return new SluiceChannelHandler(client, responseHandler, requestHandler);
  }

  public SluiceConfig getConf() { return conf; }
}
