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
package org.apache.hive.service.auth;

import java.io.IOException;
import java.security.Provider;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.hive.service.auth.AuthenticationProviderFactory.AuthMethods;

/**
 * Sun JDK only provides a PLAIN client and no server. This class implements the Plain SASL server
 * conforming to RFC #4616 (http://www.ietf.org/rfc/rfc4616.txt).
 */
public class PlainSaslServer implements SaslServer {

  public static final String PLAIN_METHOD = "PLAIN";
  private String user;
  private final CallbackHandler handler;

  PlainSaslServer(CallbackHandler handler, String authMethodStr) throws SaslException {
    this.handler = handler;
    AuthMethods.getValidAuthMethod(authMethodStr);
  }

  @Override
  public String getMechanismName() {
    return PLAIN_METHOD;
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    try {
      // parse the response
      // message   = [authzid] UTF8NUL authcid UTF8NUL passwd'

      Deque<String> tokenList = new ArrayDeque<String>();
      StringBuilder messageToken = new StringBuilder();
      for (byte b : response) {
        if (b == 0) {
          tokenList.addLast(messageToken.toString());
          messageToken = new StringBuilder();
        } else {
          messageToken.append((char) b);
        }
      }
      tokenList.addLast(messageToken.toString());

      // validate response
      if (tokenList.size() < 2 || tokenList.size() > 3) {
        throw new SaslException("Invalid message format");
      }
      String passwd = tokenList.removeLast();
      user = tokenList.removeLast();
      // optional authzid
      String authzId;
      if (tokenList.isEmpty()) {
        authzId = user;
      } else {
        authzId = tokenList.removeLast();
      }
      if (user == null || user.isEmpty()) {
        throw new SaslException("No user name provided");
      }
      if (passwd == null || passwd.isEmpty()) {
        throw new SaslException("No password name provided");
      }

      NameCallback nameCallback = new NameCallback("User");
      nameCallback.setName(user);
      PasswordCallback pcCallback = new PasswordCallback("Password", false);
      pcCallback.setPassword(passwd.toCharArray());
      AuthorizeCallback acCallback = new AuthorizeCallback(user, authzId);

      Callback[] cbList = {nameCallback, pcCallback, acCallback};
      handler.handle(cbList);
      if (!acCallback.isAuthorized()) {
        throw new SaslException("Authentication failed");
      }
    } catch (IllegalStateException eL) {
      throw new SaslException("Invalid message format", eL);
    } catch (IOException eI) {
      throw new SaslException("Error validating the login", eI);
    } catch (UnsupportedCallbackException eU) {
      throw new SaslException("Error validating the login", eU);
    }
    return null;
  }

  @Override
  public boolean isComplete() {
    return user != null;
  }

  @Override
  public String getAuthorizationID() {
    return user;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    return null;
  }

  @Override
  public void dispose() {}

  public static class SaslPlainServerFactory implements SaslServerFactory {

    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
      Map<String, ?> props, CallbackHandler cbh) {
      if (PLAIN_METHOD.equals(mechanism)) {
        try {
          return new PlainSaslServer(cbh, protocol);
        } catch (SaslException e) {
          /* This is to fulfill the contract of the interface which states that an exception shall
             be thrown when a SaslServer cannot be created due to an error but null should be
             returned when a Server can't be created due to the parameters supplied. And the only
             thing PlainSaslServer can fail on is a non-supported authentication mechanism.
             That's why we return null instead of throwing the Exception */
          return null;
        }
      }
      return null;
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return new String[] {PLAIN_METHOD};
    }
  }

  public static class SaslPlainProvider extends Provider {

    public SaslPlainProvider() {
      super("HiveSaslPlain", 1.0, "Hive Plain SASL provider");
      put("SaslServerFactory.PLAIN", SaslPlainServerFactory.class.getName());
    }
  }
}
