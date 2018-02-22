/**
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
package org.apache.hive.service.auth;

import javax.security.sasl.AuthenticationException;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * This class helps select a {@link PasswdAuthenticationProvider} for a given {@code AuthMethod}.
 */
public final class AuthenticationProviderFactory {

  public enum AuthMethods {
    LDAP("LDAP"),
    PAM("PAM"),
    CUSTOM("CUSTOM"),
    NONE("NONE");

    private final String authMethod;

    AuthMethods(String authMethod) {
      this.authMethod = authMethod;
    }

    public String getAuthMethod() {
      return authMethod;
    }

    public static AuthMethods getValidAuthMethod(String authMethodStr)
      throws AuthenticationException {
      for (AuthMethods auth : AuthMethods.values()) {
        if (authMethodStr.equals(auth.getAuthMethod())) {
          return auth;
        }
      }
      throw new AuthenticationException("Not a valid authentication method");
    }
  }

  private AuthenticationProviderFactory() {
  }

  public static PasswdAuthenticationProvider getAuthenticationProvider(AuthMethods authMethod)
    throws AuthenticationException {
    return getAuthenticationProvider(authMethod, new HiveConf());
  }
  public static PasswdAuthenticationProvider getAuthenticationProvider(AuthMethods authMethod, HiveConf conf)
    throws AuthenticationException {
    if (authMethod == AuthMethods.LDAP) {
      return new LdapAuthenticationProviderImpl(conf);
    } else if (authMethod == AuthMethods.PAM) {
      return new PamAuthenticationProviderImpl(conf);
    } else if (authMethod == AuthMethods.CUSTOM) {
      return new CustomAuthenticationProviderImpl(conf);
    } else if (authMethod == AuthMethods.NONE) {
      return new AnonymousAuthenticationProviderImpl();
    } else {
      throw new AuthenticationException("Unsupported authentication method");
    }
  }
}
