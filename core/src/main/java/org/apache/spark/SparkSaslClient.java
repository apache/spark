/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslClient;

/**
 * Implements SASL Client logic for Spark
 * Some of the code borrowed from Giraph and Hadoop
 */
public class SparkSaslClient {
  /** Class logger */
  private static Logger LOG = LoggerFactory.getLogger(SparkSaslClient.class);

  /**
   * Used to respond to server's counterpart, SaslServer with SASL tokens
   * represented as byte arrays.
   */
  private SaslClient saslClient;

  /**
   * Create a SaslClient for authentication with BSP servers.
   */
  public SparkSaslClient(SecurityManager securityMgr) {
    try {
      saslClient = Sasl.createSaslClient(new String[] { SparkSaslServer.DIGEST },
          null, null, SparkSaslServer.SASL_DEFAULT_REALM,
          SparkSaslServer.SASL_PROPS, new SparkSaslClientCallbackHandler(securityMgr));
    } catch (IOException e) {
      LOG.error("SaslClient: Could not create SaslClient");
      saslClient = null;
    }
  }

  /**
   * Used to initiate SASL handshake with server.
   * @return response to challenge if needed 
   * @throws IOException
   */
  public byte[] firstToken() throws SaslException {
    byte[] saslToken = new byte[0];
    if (saslClient.hasInitialResponse()) {
      LOG.debug("has initial response");
      saslToken = saslClient.evaluateChallenge(saslToken);
    }
    return saslToken;
  }

  /**
   * Determines whether the authentication exchange has completed.
   */
  public boolean isComplete() {
    return saslClient.isComplete();
  }

  /**
   * Respond to server's SASL token.
   * @param saslTokenMessage contains server's SASL token
   * @return client's response SASL token
   */
  public byte[] saslResponse(byte[] saslTokenMessage) throws SaslException {
    try {
      byte[] retval = saslClient.evaluateChallenge(saslTokenMessage);
      return retval;
    } catch (SaslException e) {
      LOG.error("saslResponse: Failed to respond to SASL server's token:", e);
      throw e;
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the 
   * SaslClient might be using.
   */
  public void dispose() throws SaslException {
    if (saslClient != null) {
      try {
        saslClient.dispose();
        saslClient = null;
      } catch (SaslException ignored) {
      }
    }
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler
   * that works with share secrets.
   */
  private static class SparkSaslClientCallbackHandler implements CallbackHandler {
    private final String userName;
    private final char[] userPassword;

    /**
     * Constructor
     */
    public SparkSaslClientCallbackHandler(SecurityManager securityMgr) {
      this.userName = SparkSaslServer.
        encodeIdentifier(securityMgr.getSaslUser().getBytes());
      String secretKey = securityMgr.getSecretKey() ; 
      String passwd = (secretKey != null) ? secretKey : "";
      this.userPassword = SparkSaslServer.encodePassword(passwd.getBytes());
    }

    /**
     * Implementation used to respond to SASL tokens from server.
     *
     * @param callbacks objects that indicate what credential information the
     *                  server's SaslServer requires from the client.
     * @throws UnsupportedCallbackException
     */
    public void handle(Callback[] callbacks)
      throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "handle: Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("handle: SASL client callback: setting username: " +
              userName);
        }
        nc.setName(userName);
      }
      if (pc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("handle: SASL client callback: setting userPassword");
        }
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("handle: SASL client callback: setting realm: " +
              rc.getDefaultText());
        }
        rc.setText(rc.getDefaultText());
      }
    }
  }
}
