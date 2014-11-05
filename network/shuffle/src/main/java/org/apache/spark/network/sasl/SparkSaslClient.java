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
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.network.sasl.SparkSaslServer.*;

/**
 * A SASL Client for Spark which simply keeps track of the state of a single SASL session, from the
 * initial state to the "authenticated" state. This client initializes the protocol via a
 * firstToken, which is then followed by a set of challenges and responses.
 */
public class SparkSaslClient {
  private final Logger logger = LoggerFactory.getLogger(SparkSaslClient.class);

  private final String secretKeyId;
  private final SecretKeyHolder secretKeyHolder;
  private SaslClient saslClient;

  public SparkSaslClient(String secretKeyId, SecretKeyHolder secretKeyHolder) {
    this.secretKeyId = secretKeyId;
    this.secretKeyHolder = secretKeyHolder;
    try {
      this.saslClient = Sasl.createSaslClient(new String[] { DIGEST }, null, null, DEFAULT_REALM,
        SASL_PROPS, new ClientCallbackHandler());
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

  /** Determines whether the authentication exchange has completed. */
  public synchronized boolean isComplete() {
    return saslClient != null && saslClient.isComplete();
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
          logger.info("Realm callback");
        } else if (callback instanceof RealmChoiceCallback) {
          // ignore (?)
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
    }
  }
}
