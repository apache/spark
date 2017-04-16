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
package org.apache.hadoop.security.authentication.server;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * The {@link AuthenticationToken} contains information about an authenticated HTTP client and doubles
 * as the {@link Principal} to be returned by authenticated {@link HttpServletRequest}s
 * <p/>
 * The token can be serialized/deserialized to and from a string as it is sent and received in HTTP client
 * responses and requests as a HTTP cookie (this is done by the {@link AuthenticationFilter}).
 */
public class AuthenticationToken implements Principal {

  /**
   * Constant that identifies an anonymous request.
   */
  public static final AuthenticationToken ANONYMOUS = new AuthenticationToken();

  private static final String ATTR_SEPARATOR = "&";
  private static final String USER_NAME = "u";
  private static final String PRINCIPAL = "p";
  private static final String EXPIRES = "e";
  private static final String TYPE = "t";

  private final static Set<String> ATTRIBUTES =
    new HashSet<String>(Arrays.asList(USER_NAME, PRINCIPAL, EXPIRES, TYPE));

  private String userName;
  private String principal;
  private String type;
  private long expires;
  private String token;

  private AuthenticationToken() {
    userName = null;
    principal = null;
    type = null;
    expires = -1;
    token = "ANONYMOUS";
    generateToken();
  }

  private static final String ILLEGAL_ARG_MSG = " is NULL, empty or contains a '" + ATTR_SEPARATOR + "'";

  /**
   * Creates an authentication token.
   *
   * @param userName user name.
   * @param principal principal (commonly matches the user name, with Kerberos is the full/long principal
   * name while the userName is the short name).
   * @param type the authentication mechanism name.
   * (<code>System.currentTimeMillis() + validityPeriod</code>).
   */
  public AuthenticationToken(String userName, String principal, String type) {
    checkForIllegalArgument(userName, "userName");
    checkForIllegalArgument(principal, "principal");
    checkForIllegalArgument(type, "type");
    this.userName = userName;
    this.principal = principal;
    this.type = type;
    this.expires = -1;
  }
  
  /**
   * Check if the provided value is invalid. Throw an error if it is invalid, NOP otherwise.
   * 
   * @param value the value to check.
   * @param name the parameter name to use in an error message if the value is invalid.
   */
  private static void checkForIllegalArgument(String value, String name) {
    if (value == null || value.length() == 0 || value.contains(ATTR_SEPARATOR)) {
      throw new IllegalArgumentException(name + ILLEGAL_ARG_MSG);
    }
  }

  /**
   * Sets the expiration of the token.
   *
   * @param expires expiration time of the token in milliseconds since the epoch.
   */
  public void setExpires(long expires) {
    if (this != AuthenticationToken.ANONYMOUS) {
      this.expires = expires;
      generateToken();
    }
  }

  /**
   * Generates the token.
   */
  private void generateToken() {
    StringBuffer sb = new StringBuffer();
    sb.append(USER_NAME).append("=").append(userName).append(ATTR_SEPARATOR);
    sb.append(PRINCIPAL).append("=").append(principal).append(ATTR_SEPARATOR);
    sb.append(TYPE).append("=").append(type).append(ATTR_SEPARATOR);
    sb.append(EXPIRES).append("=").append(expires);
    token = sb.toString();
  }

  /**
   * Returns the user name.
   *
   * @return the user name.
   */
  public String getUserName() {
    return userName;
  }

  /**
   * Returns the principal name (this method name comes from the JDK {@link Principal} interface).
   *
   * @return the principal name.
   */
  @Override
  public String getName() {
    return principal;
  }

  /**
   * Returns the authentication mechanism of the token.
   *
   * @return the authentication mechanism of the token.
   */
  public String getType() {
    return type;
  }

  /**
   * Returns the expiration time of the token.
   *
   * @return the expiration time of the token, in milliseconds since Epoc.
   */
  public long getExpires() {
    return expires;
  }

  /**
   * Returns if the token has expired.
   *
   * @return if the token has expired.
   */
  public boolean isExpired() {
    return expires != -1 && System.currentTimeMillis() > expires;
  }

  /**
   * Returns the string representation of the token.
   * <p/>
   * This string representation is parseable by the {@link #parse} method.
   *
   * @return the string representation of the token.
   */
  @Override
  public String toString() {
    return token;
  }

  /**
   * Parses a string into an authentication token.
   *
   * @param tokenStr string representation of a token.
   *
   * @return the parsed authentication token.
   *
   * @throws AuthenticationException thrown if the string representation could not be parsed into
   * an authentication token.
   */
  public static AuthenticationToken parse(String tokenStr) throws AuthenticationException {
    Map<String, String> map = split(tokenStr);
    if (!map.keySet().equals(ATTRIBUTES)) {
      throw new AuthenticationException("Invalid token string, missing attributes");
    }
    long expires = Long.parseLong(map.get(EXPIRES));
    AuthenticationToken token = new AuthenticationToken(map.get(USER_NAME), map.get(PRINCIPAL), map.get(TYPE));
    token.setExpires(expires);
    return token;
  }

  /**
   * Splits the string representation of a token into attributes pairs.
   *
   * @param tokenStr string representation of a token.
   *
   * @return a map with the attribute pairs of the token.
   *
   * @throws AuthenticationException thrown if the string representation of the token could not be broken into
   * attribute pairs.
   */
  private static Map<String, String> split(String tokenStr) throws AuthenticationException {
    Map<String, String> map = new HashMap<String, String>();
    StringTokenizer st = new StringTokenizer(tokenStr, ATTR_SEPARATOR);
    while (st.hasMoreTokens()) {
      String part = st.nextToken();
      int separator = part.indexOf('=');
      if (separator == -1) {
        throw new AuthenticationException("Invalid authentication token");
      }
      String key = part.substring(0, separator);
      String value = part.substring(separator + 1);
      map.put(key, value);
    }
    return map;
  }

}
