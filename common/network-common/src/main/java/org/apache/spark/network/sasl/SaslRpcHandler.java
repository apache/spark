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

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.security.sasl.Sasl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.AbstractAuthRpcHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * RPC Handler which performs SASL authentication before delegating to a child RPC handler.
 * The delegate will only receive messages if the given connection has been successfully
 * authenticated. A connection may be authenticated at most once.
 *
 * Note that the authentication process consists of multiple challenge-response pairs, each of
 * which are individual RPCs.
 */
public class SaslRpcHandler extends AbstractAuthRpcHandler {
  private static final SparkLogger logger = SparkLoggerFactory.getLogger(SaslRpcHandler.class);

  /** Transport configuration. */
  private final TransportConf conf;

  /** The client channel. */
  private final Channel channel;

  /** Class which provides secret keys which are shared by server and client on a per-app basis. */
  private final SecretKeyHolder secretKeyHolder;

  private SparkSaslServer saslServer;

  public SaslRpcHandler(
      TransportConf conf,
      Channel channel,
      RpcHandler delegate,
      SecretKeyHolder secretKeyHolder) {
    super(delegate);
    this.conf = conf;
    this.channel = channel;
    this.secretKeyHolder = secretKeyHolder;
    this.saslServer = null;
  }

  @Override
  public boolean doAuthChallenge(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback) {
    if (saslServer == null || !saslServer.isComplete()) {
      ByteBuf nettyBuf = Unpooled.wrappedBuffer(message);
      SaslMessage saslMessage;
      try {
        saslMessage = SaslMessage.decode(nettyBuf);
      } finally {
        nettyBuf.release();
      }

      if (saslServer == null) {
        // First message in the handshake, setup the necessary state.
        client.setClientId(saslMessage.appId);
        saslServer = new SparkSaslServer(saslMessage.appId, secretKeyHolder,
          conf.saslServerAlwaysEncrypt());
      }

      byte[] response;
      try {
        response = saslServer.response(JavaUtils.bufferToArray(
          saslMessage.body().nioByteBuffer()));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      callback.onSuccess(ByteBuffer.wrap(response));
    }

    // Setup encryption after the SASL response is sent, otherwise the client can't parse the
    // response. It's ok to change the channel pipeline here since we are processing an incoming
    // message, so the pipeline is busy and no new incoming messages will be fed to it before this
    // method returns. This assumes that the code ensures, through other means, that no outbound
    // messages are being written to the channel while negotiation is still going on.
    if (saslServer.isComplete()) {
      if (!SparkSaslServer.QOP_AUTH_CONF.equals(saslServer.getNegotiatedProperty(Sasl.QOP))) {
        logger.debug("SASL authentication successful for channel {}", client);
        complete(true);
        return true;
      }

      logger.debug("Enabling encryption for channel {}", client);
      SaslEncryption.addToChannel(channel, saslServer, conf.maxSaslEncryptedBlockSize());
      complete(false);
      return true;
    }
    return false;
  }

  @Override
  public void channelInactive(TransportClient client) {
    try {
      super.channelInactive(client);
    } finally {
      if (saslServer != null) {
        saslServer.dispose();
      }
    }
  }

  private void complete(boolean dispose) {
    if (dispose) {
      try {
        saslServer.dispose();
      } catch (RuntimeException e) {
        logger.error("Error while disposing SASL server", e);
      }
    }

    saslServer = null;
  }

}
