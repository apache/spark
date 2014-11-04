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
 */
public class SaslRpcHandler implements RpcHandler {
  private final Logger logger = LoggerFactory.getLogger(SaslRpcHandler.class);

  private final RpcHandler delegate;
  private final SecretKeyHolder secretKeyHolder;
  private final ConcurrentMap<String, SparkSaslServer> channelAuthenticationMap;

  public SaslRpcHandler(RpcHandler delegate, SecretKeyHolder secretKeyHolder) {
    this.delegate = delegate;
    this.secretKeyHolder = secretKeyHolder;
    this.channelAuthenticationMap = Maps.newConcurrentMap();
  }

  @Override
  public void receive(TransportClient client, byte[] message, RpcResponseCallback callback) {
    String channelKey = client.getChannelKey();

    SparkSaslServer saslServer = channelAuthenticationMap.get(channelKey);
    if (saslServer != null && saslServer.isComplete()) {
      // Authentication complete, delegate to base handler.
      delegate.receive(client, message, callback);
      return;
    }

    SaslMessage saslMessage = SaslMessage.decode(Unpooled.wrappedBuffer(message));

    if (saslServer == null) {
      saslServer = new SparkSaslServer(saslMessage.appId, secretKeyHolder);
      channelAuthenticationMap.put(channelKey, saslServer);
    }

    byte[] response = saslServer.response(saslMessage.payload);
    if (saslServer.isComplete()) {
      logger.debug("SASL authentication successful for channel {}", channelKey);
    }
    callback.onSuccess(response);
  }

  @Override
  public StreamManager getStreamManager() {
    return delegate.getStreamManager();
  }
}

/**
 * Encodes a Sasl-related message which is attempting to authenticate using some credentials tagged
 * with the given id. This 'id' allows a single SaslRpcHandler to multiplex different applications
 * who may be using different sets of credentials.
 */
class SaslMessage implements Encodable {

  private static final byte TAG_BYTE = (byte) 0xEA;

  public final String appId;
  public final byte[] payload;

  public SaslMessage(String appId, byte[] payload) {
    this.appId = appId;
    this.payload = payload;
  }

  @Override
  public int encodedLength() {
    return 1 + 4 + appId.getBytes(Charsets.UTF_8).length + 4 + payload.length;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeByte(TAG_BYTE);
    byte[] idBytes = appId.getBytes(Charsets.UTF_8);
    buf.writeInt(idBytes.length);
    buf.writeBytes(appId.getBytes(Charsets.UTF_8));
    buf.writeInt(payload.length);
    buf.writeBytes(payload);
  }

  public static SaslMessage decode(ByteBuf buf) {
    if (buf.readByte() != TAG_BYTE) {
      throw new IllegalStateException("Expected SaslMessage, received something else");
    }

    int idLength = buf.readInt();
    byte[] idBytes = new byte[idLength];
    buf.readBytes(idBytes);

    int payloadLength = buf.readInt();
    byte[] payload = new byte[payloadLength];
    buf.readBytes(payload);

    return new SaslMessage(new String(idBytes, Charsets.UTF_8), payload);
  }
}
