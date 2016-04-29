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

package org.apache.hive.service.cli.thrift;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.NewCookie;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.auth.AuthenticationProviderFactory;
import org.apache.hive.service.auth.AuthenticationProviderFactory.AuthMethods;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.HttpAuthUtils;
import org.apache.hive.service.auth.HttpAuthenticationException;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.CookieSigner;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 *
 * ThriftHttpServlet
 *
 */
public class ThriftHttpServlet extends TServlet {

  private static final long serialVersionUID = 1L;
  public static final Log LOG = LogFactory.getLog(ThriftHttpServlet.class.getName());
  private final String authType;
  private final UserGroupInformation serviceUGI;
  private final UserGroupInformation httpUGI;
  private HiveConf hiveConf = new HiveConf();

  // Class members for cookie based authentication.
  private CookieSigner signer;
  public static final String AUTH_COOKIE = "hive.server2.auth";
  private static final Random RAN = new Random();
  private boolean isCookieAuthEnabled;
  private String cookieDomain;
  private String cookiePath;
  private int cookieMaxAge;
  private boolean isCookieSecure;
  private boolean isHttpOnlyCookie;

  public ThriftHttpServlet(TProcessor processor, TProtocolFactory protocolFactory,
      String authType, UserGroupInformation serviceUGI, UserGroupInformation httpUGI) {
    super(processor, protocolFactory);
    this.authType = authType;
    this.serviceUGI = serviceUGI;
    this.httpUGI = httpUGI;
    this.isCookieAuthEnabled = hiveConf.getBoolVar(
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_AUTH_ENABLED);
    // Initialize the cookie based authentication related variables.
    if (isCookieAuthEnabled) {
      // Generate the signer with secret.
      String secret = Long.toString(RAN.nextLong());
      LOG.debug("Using the random number as the secret for cookie generation " + secret);
      this.signer = new CookieSigner(secret.getBytes());
      this.cookieMaxAge = (int) hiveConf.getTimeVar(
        ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_MAX_AGE, TimeUnit.SECONDS);
      this.cookieDomain = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_DOMAIN);
      this.cookiePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_PATH);
      this.isCookieSecure = hiveConf.getBoolVar(
        ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_IS_SECURE);
      this.isHttpOnlyCookie = hiveConf.getBoolVar(
        ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_IS_HTTPONLY);
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String clientUserName = null;
    String clientIpAddress;
    boolean requireNewCookie = false;

    try {
      // If the cookie based authentication is already enabled, parse the
      // request and validate the request cookies.
      if (isCookieAuthEnabled) {
        clientUserName = validateCookie(request);
        requireNewCookie = (clientUserName == null);
        if (requireNewCookie) {
          LOG.info("Could not validate cookie sent, will try to generate a new cookie");
        }
      }
      // If the cookie based authentication is not enabled or the request does
      // not have a valid cookie, use the kerberos or password based authentication
      // depending on the server setup.
      if (clientUserName == null) {
        // For a kerberos setup
        if (isKerberosAuthMode(authType)) {
          clientUserName = doKerberosAuth(request);
        }
        // For password based authentication
        else {
          clientUserName = doPasswdAuth(request, authType);
        }
      }
      LOG.debug("Client username: " + clientUserName);

      // Set the thread local username to be used for doAs if true
      SessionManager.setUserName(clientUserName);

      // find proxy user if any from query param
      String doAsQueryParam = getDoAsQueryParam(request.getQueryString());
      if (doAsQueryParam != null) {
        SessionManager.setProxyUserName(doAsQueryParam);
      }

      clientIpAddress = request.getRemoteAddr();
      LOG.debug("Client IP Address: " + clientIpAddress);
      // Set the thread local ip address
      SessionManager.setIpAddress(clientIpAddress);
      // Generate new cookie and add it to the response
      if (requireNewCookie &&
          !authType.equalsIgnoreCase(HiveAuthFactory.AuthTypes.NOSASL.toString())) {
        String cookieToken = HttpAuthUtils.createCookieToken(clientUserName);
        Cookie hs2Cookie = createCookie(signer.signCookie(cookieToken));

        if (isHttpOnlyCookie) {
          response.setHeader("SET-COOKIE", getHttpOnlyCookieHeader(hs2Cookie));
        } else {
          response.addCookie(hs2Cookie);
        }
        LOG.info("Cookie added for clientUserName " + clientUserName);
      }
      super.doPost(request, response);
    }
    catch (HttpAuthenticationException e) {
      LOG.error("Error: ", e);
      // Send a 401 to the client
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      if(isKerberosAuthMode(authType)) {
        response.addHeader(HttpAuthUtils.WWW_AUTHENTICATE, HttpAuthUtils.NEGOTIATE);
      }
      response.getWriter().println("Authentication Error: " + e.getMessage());
    }
    finally {
      // Clear the thread locals
      SessionManager.clearUserName();
      SessionManager.clearIpAddress();
      SessionManager.clearProxyUserName();
    }
  }

  /**
   * Retrieves the client name from cookieString. If the cookie does not
   * correspond to a valid client, the function returns null.
   * @param cookies HTTP Request cookies.
   * @return Client Username if cookieString has a HS2 Generated cookie that is currently valid.
   * Else, returns null.
   */
  private String getClientNameFromCookie(Cookie[] cookies) {
    // Current Cookie Name, Current Cookie Value
    String currName, currValue;

    // Following is the main loop which iterates through all the cookies send by the client.
    // The HS2 generated cookies are of the format hive.server2.auth=<value>
    // A cookie which is identified as a hiveserver2 generated cookie is validated
    // by calling signer.verifyAndExtract(). If the validation passes, send the
    // username for which the cookie is validated to the caller. If no client side
    // cookie passes the validation, return null to the caller.
    for (Cookie currCookie : cookies) {
      // Get the cookie name
      currName = currCookie.getName();
      if (!currName.equals(AUTH_COOKIE)) {
        // Not a HS2 generated cookie, continue.
        continue;
      }
      // If we reached here, we have match for HS2 generated cookie
      currValue = currCookie.getValue();
      // Validate the value.
      currValue = signer.verifyAndExtract(currValue);
      // Retrieve the user name, do the final validation step.
      if (currValue != null) {
        String userName = HttpAuthUtils.getUserNameFromCookieToken(currValue);

        if (userName == null) {
          LOG.warn("Invalid cookie token " + currValue);
          continue;
        }
        //We have found a valid cookie in the client request.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Validated the cookie for user " + userName);
        }
        return userName;
      }
    }
    // No valid HS2 generated cookies found, return null
    return null;
  }

  /**
   * Convert cookie array to human readable cookie string
   * @param cookies Cookie Array
   * @return String containing all the cookies separated by a newline character.
   * Each cookie is of the format [key]=[value]
   */
  private String toCookieStr(Cookie[] cookies) {
	String cookieStr = "";

	for (Cookie c : cookies) {
     cookieStr += c.getName() + "=" + c.getValue() + " ;\n";
    }
    return cookieStr;
  }

  /**
   * Validate the request cookie. This function iterates over the request cookie headers
   * and finds a cookie that represents a valid client/server session. If it finds one, it
   * returns the client name associated with the session. Else, it returns null.
   * @param request The HTTP Servlet Request send by the client
   * @return Client Username if the request has valid HS2 cookie, else returns null
   * @throws UnsupportedEncodingException
   */
  private String validateCookie(HttpServletRequest request) throws UnsupportedEncodingException {
    // Find all the valid cookies associated with the request.
    Cookie[] cookies = request.getCookies();

    if (cookies == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No valid cookies associated with the request " + request);
      }
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received cookies: " + toCookieStr(cookies));
    }
    return getClientNameFromCookie(cookies);
  }

  /**
   * Generate a server side cookie given the cookie value as the input.
   * @param str Input string token.
   * @return The generated cookie.
   * @throws UnsupportedEncodingException
   */
  private Cookie createCookie(String str) throws UnsupportedEncodingException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cookie name = " + AUTH_COOKIE + " value = " + str);
    }
    Cookie cookie = new Cookie(AUTH_COOKIE, str);

    cookie.setMaxAge(cookieMaxAge);
    if (cookieDomain != null) {
      cookie.setDomain(cookieDomain);
    }
    if (cookiePath != null) {
      cookie.setPath(cookiePath);
    }
    cookie.setSecure(isCookieSecure);
    return cookie;
  }

  /**
   * Generate httponly cookie from HS2 cookie
   * @param cookie HS2 generated cookie
   * @return The httponly cookie
   */
  private static String getHttpOnlyCookieHeader(Cookie cookie) {
    NewCookie newCookie = new NewCookie(cookie.getName(), cookie.getValue(),
      cookie.getPath(), cookie.getDomain(), cookie.getVersion(),
      cookie.getComment(), cookie.getMaxAge(), cookie.getSecure());
    return newCookie + "; HttpOnly";
  }

  /**
   * Do the LDAP/PAM authentication
   * @param request
   * @param authType
   * @throws HttpAuthenticationException
   */
  private String doPasswdAuth(HttpServletRequest request, String authType)
      throws HttpAuthenticationException {
    String userName = getUsername(request, authType);
    // No-op when authType is NOSASL
    if (!authType.equalsIgnoreCase(HiveAuthFactory.AuthTypes.NOSASL.toString())) {
      try {
        AuthMethods authMethod = AuthMethods.getValidAuthMethod(authType);
        PasswdAuthenticationProvider provider =
            AuthenticationProviderFactory.getAuthenticationProvider(authMethod);
        provider.Authenticate(userName, getPassword(request, authType));

      } catch (Exception e) {
        throw new HttpAuthenticationException(e);
      }
    }
    return userName;
  }

  /**
   * Do the GSS-API kerberos authentication.
   * We already have a logged in subject in the form of serviceUGI,
   * which GSS-API will extract information from.
   * In case of a SPNego request we use the httpUGI,
   * for the authenticating service tickets.
   * @param request
   * @return
   * @throws HttpAuthenticationException
   */
  private String doKerberosAuth(HttpServletRequest request)
      throws HttpAuthenticationException {
    // Try authenticating with the http/_HOST principal
    if (httpUGI != null) {
      try {
        return httpUGI.doAs(new HttpKerberosServerAction(request, httpUGI));
      } catch (Exception e) {
        LOG.info("Failed to authenticate with http/_HOST kerberos principal, " +
            "trying with hive/_HOST kerberos principal");
      }
    }
    // Now try with hive/_HOST principal
    try {
      return serviceUGI.doAs(new HttpKerberosServerAction(request, serviceUGI));
    } catch (Exception e) {
      LOG.error("Failed to authenticate with hive/_HOST kerberos principal");
      throw new HttpAuthenticationException(e);
    }

  }

  class HttpKerberosServerAction implements PrivilegedExceptionAction<String> {
    HttpServletRequest request;
    UserGroupInformation serviceUGI;

    HttpKerberosServerAction(HttpServletRequest request,
        UserGroupInformation serviceUGI) {
      this.request = request;
      this.serviceUGI = serviceUGI;
    }

    @Override
    public String run() throws HttpAuthenticationException {
      // Get own Kerberos credentials for accepting connection
      GSSManager manager = GSSManager.getInstance();
      GSSContext gssContext = null;
      String serverPrincipal = getPrincipalWithoutRealm(
          serviceUGI.getUserName());
      try {
        // This Oid for Kerberos GSS-API mechanism.
        Oid kerberosMechOid = new Oid("1.2.840.113554.1.2.2");
        // Oid for SPNego GSS-API mechanism.
        Oid spnegoMechOid = new Oid("1.3.6.1.5.5.2");
        // Oid for kerberos principal name
        Oid krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1");

        // GSS name for server
        GSSName serverName = manager.createName(serverPrincipal, krb5PrincipalOid);

        // GSS credentials for server
        GSSCredential serverCreds = manager.createCredential(serverName,
            GSSCredential.DEFAULT_LIFETIME,
            new Oid[]{kerberosMechOid, spnegoMechOid},
            GSSCredential.ACCEPT_ONLY);

        // Create a GSS context
        gssContext = manager.createContext(serverCreds);
        // Get service ticket from the authorization header
        String serviceTicketBase64 = getAuthHeader(request, authType);
        byte[] inToken = Base64.decodeBase64(serviceTicketBase64.getBytes());
        gssContext.acceptSecContext(inToken, 0, inToken.length);
        // Authenticate or deny based on its context completion
        if (!gssContext.isEstablished()) {
          throw new HttpAuthenticationException("Kerberos authentication failed: " +
              "unable to establish context with the service ticket " +
              "provided by the client.");
        }
        else {
          return getPrincipalWithoutRealmAndHost(gssContext.getSrcName().toString());
        }
      }
      catch (GSSException e) {
        throw new HttpAuthenticationException("Kerberos authentication failed: ", e);
      }
      finally {
        if (gssContext != null) {
          try {
            gssContext.dispose();
          } catch (GSSException e) {
            // No-op
          }
        }
      }
    }

    private String getPrincipalWithoutRealm(String fullPrincipal)
        throws HttpAuthenticationException {
      KerberosNameShim fullKerberosName;
      try {
        fullKerberosName = ShimLoader.getHadoopShims().getKerberosNameShim(fullPrincipal);
      } catch (IOException e) {
        throw new HttpAuthenticationException(e);
      }
      String serviceName = fullKerberosName.getServiceName();
      String hostName = fullKerberosName.getHostName();
      String principalWithoutRealm = serviceName;
      if (hostName != null) {
        principalWithoutRealm = serviceName + "/" + hostName;
      }
      return principalWithoutRealm;
    }

    private String getPrincipalWithoutRealmAndHost(String fullPrincipal)
        throws HttpAuthenticationException {
      KerberosNameShim fullKerberosName;
      try {
        fullKerberosName = ShimLoader.getHadoopShims().getKerberosNameShim(fullPrincipal);
        return fullKerberosName.getShortName();
      } catch (IOException e) {
        throw new HttpAuthenticationException(e);
      }
    }
  }

  private String getUsername(HttpServletRequest request, String authType)
      throws HttpAuthenticationException {
    String creds[] = getAuthHeaderTokens(request, authType);
    // Username must be present
    if (creds[0] == null || creds[0].isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client does not contain username.");
    }
    return creds[0];
  }

  private String getPassword(HttpServletRequest request, String authType)
      throws HttpAuthenticationException {
    String creds[] = getAuthHeaderTokens(request, authType);
    // Password must be present
    if (creds[1] == null || creds[1].isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client does not contain username.");
    }
    return creds[1];
  }

  private String[] getAuthHeaderTokens(HttpServletRequest request,
      String authType) throws HttpAuthenticationException {
    String authHeaderBase64 = getAuthHeader(request, authType);
    String authHeaderString = StringUtils.newStringUtf8(
        Base64.decodeBase64(authHeaderBase64.getBytes()));
    String[] creds = authHeaderString.split(":");
    return creds;
  }

  /**
   * Returns the base64 encoded auth header payload
   * @param request
   * @param authType
   * @return
   * @throws HttpAuthenticationException
   */
  private String getAuthHeader(HttpServletRequest request, String authType)
      throws HttpAuthenticationException {
    String authHeader = request.getHeader(HttpAuthUtils.AUTHORIZATION);
    // Each http request must have an Authorization header
    if (authHeader == null || authHeader.isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client is empty.");
    }

    String authHeaderBase64String;
    int beginIndex;
    if (isKerberosAuthMode(authType)) {
      beginIndex = (HttpAuthUtils.NEGOTIATE + " ").length();
    }
    else {
      beginIndex = (HttpAuthUtils.BASIC + " ").length();
    }
    authHeaderBase64String = authHeader.substring(beginIndex);
    // Authorization header must have a payload
    if (authHeaderBase64String == null || authHeaderBase64String.isEmpty()) {
      throw new HttpAuthenticationException("Authorization header received " +
          "from the client does not contain any data.");
    }
    return authHeaderBase64String;
  }

  private boolean isKerberosAuthMode(String authType) {
    return authType.equalsIgnoreCase(HiveAuthFactory.AuthTypes.KERBEROS.toString());
  }

  private static String getDoAsQueryParam(String queryString) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("URL query string:" + queryString);
    }
    if (queryString == null) {
      return null;
    }
    Map<String, String[]> params = javax.servlet.http.HttpUtils.parseQueryString( queryString );
    Set<String> keySet = params.keySet();
    for (String key: keySet) {
      if (key.equalsIgnoreCase("doAs")) {
        return params.get(key)[0];
      }
    }
    return null;
  }

}


