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
import java.io.IOException;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SASL Server for Spark which simply keeps track of the state of a single SASL session, from the
 * initial state to the "authenticated" state. (It is not a server in the sense of accepting
 * connections on some socket.)
 */
public class SparkSaslServer {
  private final Logger logger = LoggerFactory.getLogger(SparkSaslServer.class);

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
   * The quality of protection is just "auth". This means that we are doing
   * authentication only, we are not supporting integrity or privacy protection of the
   * communication channel after authentication. This could be changed to be configurable
   * in the future.
   */
  static final Map<String, String> SASL_PROPS = ImmutableMap.<String, String>builder()
    .put(Sasl.QOP, "auth")
    .put(Sasl.SERVER_AUTH, "true")
    .build();

  /** Identifier for a certain secret key within the secretKeyHolder. */
  private final String secretKeyId;
  private final SecretKeyHolder secretKeyHolder;
  private SaslServer saslServer;

  public SparkSaslServer(String secretKeyId, SecretKeyHolder secretKeyHolder) {
    this.secretKeyId = secretKeyId;
    this.secretKeyHolder = secretKeyHolder;
    try {
      this.saslServer = Sasl.createSaslServer(DIGEST, null, DEFAULT_REALM, SASL_PROPS,
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
    return BaseEncoding.base64().encode(identifier.getBytes(Charsets.UTF_8));
  }

  /** Encode a password as a base64-encoded char[] array. */
  public static char[] encodePassword(String password) {
    Preconditions.checkNotNull(password, "Password cannot be null if SASL is enabled");
    return BaseEncoding.base64().encode(password.getBytes(Charsets.UTF_8)).toCharArray();
  }
}
