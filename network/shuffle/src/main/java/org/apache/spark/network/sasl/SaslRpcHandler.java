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

package org.apache.spark.network.sasl;

import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;

/**
 * RPC Handler which performs SASL authentication before delegating to a child RPC handler.
 * The delegate will only receive messages if the given connection has been successfully
 * authenticated. A connection may be authenticated at most once.
 *
 * Note that the authentication process consists of multiple challenge-response pairs, each of
 * which are individual RPCs.
 */
public class SaslRpcHandler extends RpcHandler {
  private final Logger logger = LoggerFactory.getLogger(SaslRpcHandler.class);

  /** RpcHandler we will delegate to for authenticated connections. */
  private final RpcHandler delegate;

  /** Class which provides secret keys which are shared by server and client on a per-app basis. */
  private final SecretKeyHolder secretKeyHolder;

  /** Maps each channel to its SASL authentication state. */
  private final ConcurrentMap<TransportClient, SparkSaslServer> channelAuthenticationMap;

  public SaslRpcHandler(RpcHandler delegate, SecretKeyHolder secretKeyHolder) {
    this.delegate = delegate;
    this.secretKeyHolder = secretKeyHolder;
    this.channelAuthenticationMap = Maps.newConcurrentMap();
  }

  @Override
  public void receive(TransportClient client, byte[] message, RpcResponseCallback callback) {
    SparkSaslServer saslServer = channelAuthenticationMap.get(client);
    if (saslServer != null && saslServer.isComplete()) {
      // Authentication complete, delegate to base handler.
      delegate.receive(client, message, callback);
      return;
    }

    SaslMessage saslMessage = SaslMessage.decode(Unpooled.wrappedBuffer(message));

    if (saslServer == null) {
      // First message in the handshake, setup the necessary state.
      saslServer = new SparkSaslServer(saslMessage.appId, secretKeyHolder);
      channelAuthenticationMap.put(client, saslServer);
    }

    byte[] response = saslServer.response(saslMessage.payload);
    if (saslServer.isComplete()) {
      logger.debug("SASL authentication successful for channel {}", client);
    }
    callback.onSuccess(response);
  }

  @Override
  public StreamManager getStreamManager() {
    return delegate.getStreamManager();
  }

  @Override
  public void connectionTerminated(TransportClient client) {
    SparkSaslServer saslServer = channelAuthenticationMap.remove(client);
    if (saslServer != null) {
      saslServer.dispose();
    }
  }
}
