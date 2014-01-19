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

import org.apache.commons.net.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

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

/**
 * Encapsulates SASL server logic for Server
 */
public class SparkSaslServer {
  /** Logger */
  private static Logger LOG = LoggerFactory.getLogger("SparkSaslServer.class");

  /**
   * Actual SASL work done by this object from javax.security.sasl.
   * Initialized below in constructor.
   */
  private SaslServer saslServer;

  public static final String SASL_DEFAULT_REALM = "default";
  public static final String DIGEST = "DIGEST-MD5";
  public static final Map<String, String> SASL_PROPS = 
        new TreeMap<String, String>();

  /**
   * Constructor
   */
  public SparkSaslServer(SecurityManager securityMgr) {
    try {
      SASL_PROPS.put(Sasl.QOP, "auth");
      SASL_PROPS.put(Sasl.SERVER_AUTH, "true");
      saslServer = Sasl.createSaslServer(DIGEST, null, SASL_DEFAULT_REALM, SASL_PROPS,
        new SaslDigestCallbackHandler(securityMgr));
    } catch (SaslException e) {
      LOG.error("SparkSaslServer: Could not create SaslServer: " + e);
      saslServer = null;
    }
  }

  /**
   * Determines whether the authentication exchange has completed.
   */
  public boolean isComplete() {
    return saslServer.isComplete();
  }

  /**
   * Used to respond to server SASL tokens.
   *
   * @param token Server's SASL token
   * @return response to send back to the server.
   */
  public byte[] response(byte[] token) throws SaslException {
    try {
      byte[] retval = saslServer.evaluateResponse(token);
      if (LOG.isDebugEnabled()) {
        LOG.debug("response: Response token length: " + retval.length);
      }
      return retval;
    } catch (SaslException e) {
      LOG.error("Response: Failed to evaluate client token of length: " +
        token.length + " : " + e);
      throw e;
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the 
   * SaslServer might be using.
   */
  public void dispose() throws SaslException {
    if (saslServer != null) {
      try {
        saslServer.dispose();
        saslServer = null;
      } catch (SaslException ignored) {
      }
    }
  }

  /**
   * Encode a byte[] identifier as a Base64-encoded string.
   *
   * @param identifier identifier to encode
   * @return Base64-encoded string
   */
  static String encodeIdentifier(byte[] identifier) {
    return new String(Base64.encodeBase64(identifier));
  }

  /**
   * Encode a password as a base64-encoded char[] array.
   * @param password as a byte array.
   * @return password as a char array.
   */
  static char[] encodePassword(byte[] password) {
    return new String(Base64.encodeBase64(password)).toCharArray();
  }

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  public static class SaslDigestCallbackHandler implements CallbackHandler {

    private SecurityManager securityManager; 

    /**
     * Constructor
     */
    public SaslDigestCallbackHandler(SecurityManager securityMgr) {
      this.securityManager = securityMgr;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      LOG.debug("in the sasl server callback handler");
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback,
              "handle: Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
      if (pc != null) {
        char[] password =
          encodePassword(securityManager.getSecretKey().getBytes());
        pc.setPassword(password);
      }

      if (ac != null) {
        String authid = ac.getAuthenticationID();
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          LOG.debug("set auth to true");
          ac.setAuthorized(true);
        } else {
          LOG.debug("set auth to false");
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          LOG.debug("sasl server is authorized");
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
}
