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
import java.util.Map;
import java.util.Properties;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.sasl.aes.AesCipherOption;
import org.apache.spark.network.sasl.aes.AesCipher;
import org.apache.spark.network.util.TransportConf;

import static org.apache.spark.network.sasl.SparkSaslServer.*;

/**
 * A SASL Client for Spark which simply keeps track of the state of a single SASL session, from the
 * initial state to the "authenticated" state. This client initializes the protocol via a
 * firstToken, which is then followed by a set of challenges and responses.
 */
public class SparkSaslClient implements SaslEncryptionBackend {
  private static final Logger logger = LoggerFactory.getLogger(SparkSaslClient.class);

  private final String secretKeyId;
  private final SecretKeyHolder secretKeyHolder;
  private final String expectedQop;
  private SaslClient saslClient;

  public SparkSaslClient(String secretKeyId, SecretKeyHolder secretKeyHolder, boolean encrypt) {
    this.secretKeyId = secretKeyId;
    this.secretKeyHolder = secretKeyHolder;
    this.expectedQop = encrypt ? QOP_AUTH_CONF : QOP_AUTH;

    Map<String, String> saslProps = ImmutableMap.<String, String>builder()
      .put(Sasl.QOP, expectedQop)
      .build();
    try {
      this.saslClient = Sasl.createSaslClient(new String[] { DIGEST }, null, null, DEFAULT_REALM,
        saslProps, new ClientCallbackHandler());
    } catch (SaslException e) {
      throw Throwables.propagate(e);
    }
  }

  /** Used to initiate SASL handshake with server. */
  public synchronized byte[] firstToken() {
    if (saslClient != null && saslClient.hasInitialResponse()) {
      try {
        return saslClient.evaluateChallenge(new byte[0]);
      } catch (SaslException e) {
        throw Throwables.propagate(e);
      }
    } else {
      return new byte[0];
    }
  }

  /** Determine whether the authentication exchange has completed. */
  public synchronized boolean isComplete() {
    return saslClient != null && saslClient.isComplete();
  }

  /** Returns the value of a negotiated property. */
  public Object getNegotiatedProperty(String name) {
    return saslClient.getNegotiatedProperty(name);
  }

  /**
   * Respond to server's SASL token.
   * @param token contains server's SASL token
   * @return client's response SASL token
   */
  public synchronized byte[] response(byte[] token) {
    try {
      return saslClient != null ? saslClient.evaluateChallenge(token) : new byte[0];
    } catch (SaslException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the
   * SaslClient might be using.
   */
  @Override
  public synchronized void dispose() {
    if (saslClient != null) {
      try {
        saslClient.dispose();
      } catch (SaslException e) {
        // ignore
      } finally {
        saslClient = null;
      }
    }
  }

  /**
   * Negotiate extra encryption options for SASL
   * @param client is transport client used to connect to peer.
   * @param conf contain client transport configuration.
   * @throws IOException
   * @return The object represent the result of negotiate.
   */
  public Object negotiate(TransportClient client, TransportConf conf) throws IOException {
    // Create option for negotiation
    AesCipherOption cipherOption = new AesCipherOption();
    ByteBuf buf = Unpooled.buffer(cipherOption.encodedLength());
    cipherOption.encode(buf);

    // Send option to server and decode received negotiated option
    ByteBuffer response = client.sendRpcSync(buf.nioBuffer(), conf.saslRTTimeoutMs());
    cipherOption = AesCipherOption.decode(Unpooled.wrappedBuffer(response));

    // Decrypt key from option. Server's outKey is client's inKey, and vice versa.
    byte[] outKey = unwrap(cipherOption.inKey, 0, cipherOption.inKey.length);
    byte[] inKey = unwrap(cipherOption.outKey, 0, cipherOption.outKey.length);

    // Enable AES on SaslClient
    Properties properties = new Properties();

    AesCipher cipher = new AesCipher(properties, inKey, outKey,
      cipherOption.outIv, cipherOption.inIv);

    logger.debug("AES enabled for SASL client encryption.");

    return cipher;
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler
   * that works with share secrets.
   */
  private class ClientCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {

      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          logger.trace("SASL client callback: setting username");
          NameCallback nc = (NameCallback) callback;
          nc.setName(encodeIdentifier(secretKeyHolder.getSaslUser(secretKeyId)));
        } else if (callback instanceof PasswordCallback) {
          logger.trace("SASL client callback: setting password");
          PasswordCallback pc = (PasswordCallback) callback;
          pc.setPassword(encodePassword(secretKeyHolder.getSecretKey(secretKeyId)));
        } else if (callback instanceof RealmCallback) {
          logger.trace("SASL client callback: setting realm");
          RealmCallback rc = (RealmCallback) callback;
          rc.setText(rc.getDefaultText());
        } else if (callback instanceof RealmChoiceCallback) {
          // ignore (?)
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
    }
  }

  @Override
  public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
    return saslClient.wrap(data, offset, len);
  }

  @Override
  public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
    return saslClient.unwrap(data, offset, len);
  }

}
