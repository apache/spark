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

package org.apache.spark.network.crypto;

import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.sasl.SaslRpcHandler;
import org.apache.spark.network.server.AbstractAuthRpcHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.util.TransportConf;

/**
 * RPC Handler which performs authentication using Spark's auth protocol before delegating to a
 * child RPC handler. If the configuration allows, this handler will delegate messages to a SASL
 * RPC handler for further authentication, to support for clients that do not support Spark's
 * protocol.
 *
 * The delegate will only receive messages if the given connection has been successfully
 * authenticated. A connection may be authenticated at most once.
 */
class AuthRpcHandler extends AbstractAuthRpcHandler {
  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(AuthRpcHandler.class);

  /** Transport configuration. */
  private final TransportConf conf;

  /** The client channel. */
  private final Channel channel;

  /** Class which provides secret keys which are shared by server and client on a per-app basis. */
  private final SecretKeyHolder secretKeyHolder;

  /** RPC handler for auth handshake when falling back to SASL auth. */
  @VisibleForTesting
  SaslRpcHandler saslHandler;

  AuthRpcHandler(
      TransportConf conf,
      Channel channel,
      RpcHandler delegate,
      SecretKeyHolder secretKeyHolder) {
    super(delegate);
    this.conf = conf;
    this.channel = channel;
    this.secretKeyHolder = secretKeyHolder;
  }

  @Override
  protected boolean doAuthChallenge(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback) {
    if (saslHandler != null) {
      return saslHandler.doAuthChallenge(client, message, callback);
    }

    int position = message.position();
    int limit = message.limit();

    AuthMessage challenge;
    try {
      challenge = AuthMessage.decodeMessage(message);
      LOG.debug("Received new auth challenge for client {}.", channel.remoteAddress());
    } catch (RuntimeException e) {
      if (conf.saslFallback()) {
        LOG.warn("Failed to parse new auth challenge, reverting to SASL for client {}.",
          MDC.of(LogKeys.HOST_PORT$.MODULE$, channel.remoteAddress()));
        saslHandler = new SaslRpcHandler(conf, channel, null, secretKeyHolder);
        message.position(position);
        message.limit(limit);
        return saslHandler.doAuthChallenge(client, message, callback);
      } else {
        LOG.debug("Unexpected challenge message from client {}, closing channel.",
          channel.remoteAddress());
        callback.onFailure(new IllegalArgumentException("Unknown challenge message."));
        channel.close();
      }
      return false;
    }

    // Here we have the client challenge, so perform the new auth protocol and set up the channel.
    AuthEngine engine = null;
    try {
      String secret = secretKeyHolder.getSecretKey(challenge.appId());
      Preconditions.checkState(secret != null,
        "Trying to authenticate non-registered app %s.", challenge.appId());
      LOG.debug("Authenticating challenge for app {}.", challenge.appId());
      engine = new AuthEngine(challenge.appId(), secret, conf);
      AuthMessage response = engine.response(challenge);
      ByteBuf responseData = Unpooled.buffer(response.encodedLength());
      response.encode(responseData);
      callback.onSuccess(responseData.nioBuffer());
      engine.sessionCipher().addToChannel(channel);
      client.setClientId(challenge.appId());
    } catch (Exception e) {
      // This is a fatal error: authentication has failed. Close the channel explicitly.
      LOG.debug("Authentication failed for client {}, closing channel.", channel.remoteAddress());
      callback.onFailure(new IllegalArgumentException("Authentication failed."));
      channel.close();
      return false;
    } finally {
      if (engine != null) {
        try {
          engine.close();
        } catch (Exception e) {
          Throwables.throwIfUnchecked(e);
          throw new RuntimeException(e);
        }
      }
    }

    LOG.debug("Authorization successful for client {}.", channel.remoteAddress());
    return true;
  }

}
