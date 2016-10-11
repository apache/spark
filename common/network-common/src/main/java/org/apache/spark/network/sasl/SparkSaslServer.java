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
import java.nio.charset.StandardCharsets;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.sasl.aes.AesCipherOption;
import org.apache.spark.network.sasl.aes.AesCipher;
import org.apache.spark.network.util.TransportConf;

/**
 * A SASL Server for Spark which simply keeps track of the state of a single SASL session, from the
 * initial state to the "authenticated" state. (It is not a server in the sense of accepting
 * connections on some socket.)
 */
public class SparkSaslServer implements SaslEncryptionBackend {
  private static final Logger logger = LoggerFactory.getLogger(SparkSaslServer.class);

  /**
   * This is passed as the server name when creating the sasl client/server.
   * This could be changed to be configurable in the future.
   */
  static final String DEFAULT_REALM = "default";

  /**
   * The authentication mechanism used here is DIGEST-MD5. This could be changed to be
   * configurable in the future.
   */
  static final String DIGEST = "DIGEST-MD5";

  /**
   * Quality of protection value that includes encryption.
   */
  static final String QOP_AUTH_CONF = "auth-conf";

  /**
   * Quality of protection value that does not include encryption.
   */
  static final String QOP_AUTH = "auth";

  /** Identifier for a certain secret key within the secretKeyHolder. */
  private final String secretKeyId;
  private final SecretKeyHolder secretKeyHolder;
  private SaslServer saslServer;

  public SparkSaslServer(
      String secretKeyId,
      SecretKeyHolder secretKeyHolder,
      boolean alwaysEncrypt) {
    this.secretKeyId = secretKeyId;
    this.secretKeyHolder = secretKeyHolder;

    // Sasl.QOP is a comma-separated list of supported values. The value that allows encryption
    // is listed first since it's preferred over the non-encrypted one (if the client also
    // lists both in the request).
    String qop = alwaysEncrypt ? QOP_AUTH_CONF : String.format("%s,%s", QOP_AUTH_CONF, QOP_AUTH);
    Map<String, String> saslProps = ImmutableMap.<String, String>builder()
      .put(Sasl.SERVER_AUTH, "true")
      .put(Sasl.QOP, qop)
      .build();
    try {
      this.saslServer = Sasl.createSaslServer(DIGEST, null, DEFAULT_REALM, saslProps,
        new DigestCallbackHandler());
    } catch (SaslException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Determines whether the authentication exchange has completed successfully.
   */
  public synchronized boolean isComplete() {
    return saslServer != null && saslServer.isComplete();
  }

  /** Returns the value of a negotiated property. */
  public Object getNegotiatedProperty(String name) {
    return saslServer.getNegotiatedProperty(name);
  }

  /**
   * Used to respond to server SASL tokens.
   * @param token Server's SASL token
   * @return response to send back to the server.
   */
  public synchronized byte[] response(byte[] token) {
    try {
      return saslServer != null ? saslServer.evaluateResponse(token) : new byte[0];
    } catch (SaslException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the
   * SaslServer might be using.
   */
  @Override
  public synchronized void dispose() {
    if (saslServer != null) {
      try {
        saslServer.dispose();
      } catch (SaslException e) {
        // ignore
      } finally {
        saslServer = null;
      }
    }
  }

  @Override
  public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
    return saslServer.wrap(data, offset, len);
  }

  @Override
  public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
    return saslServer.unwrap(data, offset, len);
  }

  /**
   * Negotiate with peer for extended options, such as using AES cipher.
   * @param message is message receive from peer which may contains communication parameters.
   * @param callback is rpc callback.
   * @param conf contains transport configuration.
   * @return Object which represent the result of negotiate.
   */
  public Object negotiate(ByteBuffer message, RpcResponseCallback callback, TransportConf conf)
    throws SaslException {
    AesCipher cipher;

    // Receive initial option from client
    AesCipherOption cipherOption = AesCipherOption.decode(Unpooled.wrappedBuffer(message));
    String transformation = AesCipher.TRANSFORM;
    Properties properties = new Properties();

    try {
      // Generate key and iv
      if (conf.saslEncryptionAesCipherKeySizeBits() % 8 != 0) {
        throw new IllegalArgumentException("The AES cipher key size in bits should be a multiple " +
          "of byte");
      }

      int keyLen = conf.saslEncryptionAesCipherKeySizeBits() / 8;
      int paramLen = CryptoCipherFactory.getCryptoCipher(transformation,properties).getBlockSize();
      byte[] inKey = new byte[keyLen];
      byte[] outKey = new byte[keyLen];
      byte[] inIv = new byte[paramLen];
      byte[] outIv = new byte[paramLen];

      // Get the 'CryptoRandom' instance.
      CryptoRandom random = CryptoRandomFactory.getCryptoRandom(properties);
      random.nextBytes(inKey);
      random.nextBytes(outKey);
      random.nextBytes(inIv);
      random.nextBytes(outIv);

      // Update cipher option for client. The key is encrypted.
      cipherOption.setParameters(wrap(inKey, 0, inKey.length), inIv,
        wrap(outKey, 0, outKey.length), outIv);

      // Enable AES on saslServer
      cipher = new AesCipher(properties, inKey, outKey, inIv, outIv);

      // Send cipher option to client
      ByteBuf buf = Unpooled.buffer(cipherOption.encodedLength());
      cipherOption.encode(buf);
      callback.onSuccess(buf.nioBuffer());
    } catch (Exception e) {
      logger.error("AES negotiation exception: ", e);
      throw Throwables.propagate(e);
    }

    return cipher;
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler for SASL DIGEST-MD5 mechanism.
   */
  private class DigestCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          logger.trace("SASL server callback: setting username");
          NameCallback nc = (NameCallback) callback;
          nc.setName(encodeIdentifier(secretKeyHolder.getSaslUser(secretKeyId)));
        } else if (callback instanceof PasswordCallback) {
          logger.trace("SASL server callback: setting password");
          PasswordCallback pc = (PasswordCallback) callback;
          pc.setPassword(encodePassword(secretKeyHolder.getSecretKey(secretKeyId)));
        } else if (callback instanceof RealmCallback) {
          logger.trace("SASL server callback: setting realm");
          RealmCallback rc = (RealmCallback) callback;
          rc.setText(rc.getDefaultText());
        } else if (callback instanceof AuthorizeCallback) {
          AuthorizeCallback ac = (AuthorizeCallback) callback;
          String authId = ac.getAuthenticationID();
          String authzId = ac.getAuthorizationID();
          ac.setAuthorized(authId.equals(authzId));
          if (ac.isAuthorized()) {
            ac.setAuthorizedID(authzId);
          }
          logger.debug("SASL Authorization complete, authorized set to {}", ac.isAuthorized());
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
    }
  }

  /* Encode a byte[] identifier as a Base64-encoded string. */
  public static String encodeIdentifier(String identifier) {
    Preconditions.checkNotNull(identifier, "User cannot be null if SASL is enabled");
    return Base64.encode(Unpooled.wrappedBuffer(identifier.getBytes(StandardCharsets.UTF_8)))
      .toString(StandardCharsets.UTF_8);
  }

  /** Encode a password as a base64-encoded char[] array. */
  public static char[] encodePassword(String password) {
    Preconditions.checkNotNull(password, "Password cannot be null if SASL is enabled");
    return Base64.encode(Unpooled.wrappedBuffer(password.getBytes(StandardCharsets.UTF_8)))
      .toString(StandardCharsets.UTF_8).toCharArray();
  }
}
