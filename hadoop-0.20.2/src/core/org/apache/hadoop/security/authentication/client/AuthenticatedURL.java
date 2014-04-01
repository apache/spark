/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.client;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * The {@link AuthenticatedURL} class enables the use of the JDK {@link URL} class
 * against HTTP endpoints protected with the {@link AuthenticationFilter}.
 * <p/>
 * The authentication mechanisms supported by default are Hadoop Simple  authentication
 * (also known as pseudo authentication) and Kerberos SPNEGO authentication.
 * <p/>
 * Additional authentication mechanisms can be supported via {@link Authenticator} implementations.
 * <p/>
 * The default {@link Authenticator} is the {@link KerberosAuthenticator} class which supports
 * automatic fallback from Kerberos SPNEGO to Hadoop Simple authentication.
 * <p/>
 * <code>AuthenticatedURL</code> instances are not thread-safe.
 * <p/>
 * The usage pattern of the {@link AuthenticatedURL} is:
 * <p/>
 * <pre>
 *
 * // establishing an initial connection
 *
 * URL url = new URL("http://foo:8080/bar");
 * AuthenticatedURL.Token token = new AuthenticatedURL.Token();
 * AuthenticatedURL aUrl = new AuthenticatedURL();
 * HttpURLConnection conn = new AuthenticatedURL(url, token).openConnection();
 * ....
 * // use the 'conn' instance
 * ....
 *
 * // establishing a follow up connection using a token from the previous connection
 *
 * HttpURLConnection conn = new AuthenticatedURL(url, token).openConnection();
 * ....
 * // use the 'conn' instance
 * ....
 *
 * </pre>
 */
public class AuthenticatedURL {

  /**
   * Name of the HTTP cookie used for the authentication token between the client and the server.
   */
  public static final String AUTH_COOKIE = "hadoop.auth";

  private static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";

  /**
   * Client side authentication token.
   */
  public static class Token {

    private String token;

    /**
     * Creates a token.
     */
    public Token() {
    }

    /**
     * Creates a token using an existing string representation of the token.
     *
     * @param tokenStr string representation of the tokenStr.
     */
    public Token(String tokenStr) {
      if (tokenStr == null) {
        throw new IllegalArgumentException("tokenStr cannot be null");
      }
      set(tokenStr);
    }

    /**
     * Returns if a token from the server has been set.
     *
     * @return if a token from the server has been set.
     */
    public boolean isSet() {
      return token != null;
    }

    /**
     * Sets a token.
     *
     * @param tokenStr string representation of the tokenStr.
     */
    void set(String tokenStr) {
      token = tokenStr;
    }

    /**
     * Returns the string representation of the token.
     *
     * @return the string representation of the token.
     */
    @Override
    public String toString() {
      return token;
    }

    /**
     * Return the hashcode for the token.
     *
     * @return the hashcode for the token.
     */
    @Override
    public int hashCode() {
      return (token != null) ? token.hashCode() : 0;
    }

    /**
     * Return if two token instances are equal.
     *
     * @param o the other token instance.
     *
     * @return if this instance and the other instance are equal.
     */
    @Override
    public boolean equals(Object o) {
      boolean eq = false;
      if (o instanceof Token) {
        Token other = (Token) o;
        eq = (token == null && other.token == null) || (token != null && this.token.equals(other.token));
      }
      return eq;
    }
  }

  private static Class<? extends Authenticator> DEFAULT_AUTHENTICATOR = KerberosAuthenticator.class;

  /**
   * Sets the default {@link Authenticator} class to use when an {@link AuthenticatedURL} instance
   * is created without specifying an authenticator.
   *
   * @param authenticator the authenticator class to use as default.
   */
  public static void setDefaultAuthenticator(Class<? extends Authenticator> authenticator) {
    DEFAULT_AUTHENTICATOR = authenticator;
  }

  /**
   * Returns the default {@link Authenticator} class to use when an {@link AuthenticatedURL} instance
   * is created without specifying an authenticator.
   *
   * @return the authenticator class to use as default.
   */
  public static Class<? extends Authenticator> getDefaultAuthenticator() {
    return DEFAULT_AUTHENTICATOR;
  }

  private Authenticator authenticator;

  /**
   * Creates an {@link AuthenticatedURL}.
   */
  public AuthenticatedURL() {
    this(null);
  }

  /**
   * Creates an <code>AuthenticatedURL</code>.
   *
   * @param authenticator the {@link Authenticator} instance to use, if <code>null</code> a {@link
   * KerberosAuthenticator} is used.
   */
  public AuthenticatedURL(Authenticator authenticator) {
    try {
      this.authenticator = (authenticator != null) ? authenticator : DEFAULT_AUTHENTICATOR.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Returns an authenticated {@link HttpURLConnection}.
   *
   * @param url the URL to connect to. Only HTTP/S URLs are supported.
   * @param token the authentication token being used for the user.
   *
   * @return an authenticated {@link HttpURLConnection}.
   *
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  public HttpURLConnection openConnection(URL url, Token token) throws IOException, AuthenticationException {
    if (url == null) {
      throw new IllegalArgumentException("url cannot be NULL");
    }
    if (!url.getProtocol().equalsIgnoreCase("http") && !url.getProtocol().equalsIgnoreCase("https")) {
      throw new IllegalArgumentException("url must be for a HTTP or HTTPS resource");
    }
    if (token == null) {
      throw new IllegalArgumentException("token cannot be NULL");
    }
    authenticator.authenticate(url, token);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    injectToken(conn, token);
    return conn;
  }

  /**
   * Helper method that injects an authentication token to send with a connection.
   *
   * @param conn connection to inject the authentication token into.
   * @param token authentication token to inject.
   */
  public static void injectToken(HttpURLConnection conn, Token token) {
    String t = token.token;
    if (t != null) {
      if (!t.startsWith("\"")) {
        t = "\"" + t + "\"";
      }
      conn.addRequestProperty("Cookie", AUTH_COOKIE_EQ + t);
    }
  }

  /**
   * Helper method that extracts an authentication token received from a connection.
   * <p/>
   * This method is used by {@link Authenticator} implementations.
   *
   * @param conn connection to extract the authentication token from.
   * @param token the authentication token.
   *
   * @throws IOException if an IO error occurred.
   * @throws AuthenticationException if an authentication exception occurred.
   */
  public static void extractToken(HttpURLConnection conn, Token token) throws IOException, AuthenticationException {
    if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
      Map<String, List<String>> headers = conn.getHeaderFields();
      List<String> cookies = headers.get("Set-Cookie");
      if (cookies != null) {
        for (String cookie : cookies) {
          if (cookie.startsWith(AUTH_COOKIE_EQ)) {
            String value = cookie.substring(AUTH_COOKIE_EQ.length());
            int separator = value.indexOf(";");
            if (separator > -1) {
              value = value.substring(0, separator);
            }
            if (value.length() > 0) {
              token.set(value);
            }
          }
        }
      }
    } else {
      throw new AuthenticationException("Authentication failed, status: " + conn.getResponseCode() +
                                        ", message: " + conn.getResponseMessage());
    }
  }

}
