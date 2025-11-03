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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeoutException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.sasl.SaslClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.util.TransportConf;

/**
 * Bootstraps a {@link TransportClient} by performing authentication using Spark's auth protocol.
 *
 * This bootstrap falls back to using the SASL bootstrap if the server throws an error during
 * authentication, and the configuration allows it. This is used for backwards compatibility
 * with external shuffle services that do not support the new protocol.
 *
 * It also automatically falls back to SASL if the new encryption backend is disabled, so that
 * callers only need to install this bootstrap when authentication is enabled.
 */
public class AuthClientBootstrap implements TransportClientBootstrap {

  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(AuthClientBootstrap.class);

  private final TransportConf conf;
  private final String appId;
  private final SecretKeyHolder secretKeyHolder;

  public AuthClientBootstrap(
      TransportConf conf,
      String appId,
      SecretKeyHolder secretKeyHolder) {
    this.conf = conf;
    // TODO: right now this behaves like the SASL backend, because when executors start up
    // they don't necessarily know the app ID. So they send a hardcoded "user" that is defined
    // in the SecurityManager, which will also always return the same secret (regardless of the
    // user name). All that's needed here is for this "user" to match on both sides, since that's
    // required by the protocol. At some point, though, it would be better for the actual app ID
    // to be provided here.
    this.appId = appId;
    this.secretKeyHolder = secretKeyHolder;
  }

  @Override
  public void doBootstrap(TransportClient client, Channel channel) {
    if (!conf.encryptionEnabled()) {
      LOG.debug("AES encryption disabled, using old auth protocol.");
      doSaslAuth(client, channel);
      return;
    }

    try {
      doSparkAuth(client, channel);
      client.setClientId(appId);
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      // There isn't a good exception that can be caught here to know whether it's really
      // OK to switch back to SASL (because the server doesn't speak the new protocol). So
      // try it anyway, unless it's a timeout, which is locally fatal. In the worst case
      // things will fail again.
      if (!conf.saslFallback() || e.getCause() instanceof TimeoutException) {
        throw e;
      }

      if (LOG.isDebugEnabled()) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        LOG.debug("New auth protocol failed, trying SASL.", cause);
      } else {
        LOG.info("New auth protocol failed, trying SASL.");
      }
      doSaslAuth(client, channel);
    }
  }

  private void doSparkAuth(TransportClient client, Channel channel)
    throws GeneralSecurityException, IOException {

    String secretKey = secretKeyHolder.getSecretKey(appId);
    try (AuthEngine engine = new AuthEngine(appId, secretKey, conf)) {
      AuthMessage challenge = engine.challenge();
      ByteBuf challengeData = Unpooled.buffer(challenge.encodedLength());
      challenge.encode(challengeData);

      ByteBuffer responseData =
          client.sendRpcSync(challengeData.nioBuffer(), conf.authRTTimeoutMs());
      AuthMessage response = AuthMessage.decodeMessage(responseData);

      engine.deriveSessionCipher(challenge, response);
      engine.sessionCipher().addToChannel(channel);
    }
  }

  private void doSaslAuth(TransportClient client, Channel channel) {
    SaslClientBootstrap sasl = new SaslClientBootstrap(conf, appId, secretKeyHolder);
    sasl.doBootstrap(client, channel);
  }

}
