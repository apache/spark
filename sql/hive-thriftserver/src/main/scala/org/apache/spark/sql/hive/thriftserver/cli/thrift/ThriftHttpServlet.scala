/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli.thrift

import java.io.{IOException, UnsupportedEncodingException}
import java.security.PrivilegedExceptionAction
import java.util
import java.util.concurrent.TimeUnit
import javax.servlet.ServletException
import javax.servlet.http.{Cookie, HttpServletRequest, HttpServletResponse}
import javax.ws.rs.core.NewCookie

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.commons.codec.binary.{Base64, StringUtils}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.shims.{HadoopShims, ShimLoader}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.server.TServlet
import org.ietf.jgss._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.CookieSigner
import org.apache.spark.sql.hive.thriftserver.auth._
import org.apache.spark.sql.hive.thriftserver.cli.session.SessionManager

class ThriftHttpServlet(processor: TProcessor,
                        protocolFactory: TProtocolFactory,
                        authType: String,
                        serviceUGI: UserGroupInformation,
                        httpUGI: UserGroupInformation)
  extends TServlet(processor, protocolFactory) with Logging {
  private val hiveConf: HiveConf = new HiveConf
  // Class members for cookie based authentication.
  private var signer: CookieSigner = null
  val AUTH_COOKIE = "hive.server2.auth"
  private val RAN = new util.Random
  private val isCookieAuthEnabled: Boolean =
    hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_AUTH_ENABLED)

  private var cookieDomain: String = null
  private var cookiePath: String = null
  private var cookieMaxAge: Int = 0
  private var isCookieSecure: Boolean = false
  private var isHttpOnlyCookie: Boolean = false

  try {
    // Initialize the cookie based authentication related variables.
    if (isCookieAuthEnabled) {
      // Generate the signer with secret.
      val secret = RAN.nextLong.toString
      logDebug("Using the random number as the secret for cookie generation " + secret)
      this.signer = new CookieSigner(secret.getBytes)
      this.cookieMaxAge =
        hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_MAX_AGE,
          TimeUnit.SECONDS).toInt
      this.cookieDomain = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_DOMAIN)
      this.cookiePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_PATH)
      this.isCookieSecure = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_IS_SECURE)
      this.isHttpOnlyCookie =
        hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_IS_HTTPONLY)
    }
  } catch {
    case NonFatal(e) =>
      logError("Error initializing ThriftHttpServlet.", e)
  }

  @throws[ServletException]
  @throws[IOException]
  override protected def doPost(request: HttpServletRequest,
                                response: HttpServletResponse): Unit = {
    var clientUserName: String = null
    var clientIpAddress: String = null
    var requireNewCookie: Boolean = false
    try { // If the cookie based authentication is already enabled, parse the
      // request and validate the request cookies.
      if (isCookieAuthEnabled) {
        clientUserName = validateCookie(request)
        requireNewCookie = clientUserName == null
        if (requireNewCookie) {
          logInfo("Could not validate cookie sent, will try to generate a new cookie")
        }
      }
      // If the cookie based authentication is not enabled or the request does
      // not have a valid cookie, use the kerberos or password based authentication
      // depending on the server setup.
      if (clientUserName == null) { // For a kerberos setup
        if (isKerberosAuthMode(authType)) clientUserName = doKerberosAuth(request)
        else { // For password based authentication
          clientUserName = doPasswdAuth(request, authType)
        }
      }
      logDebug("Client username: " + clientUserName)
      // Set the thread local username to be used for doAs if true
      SessionManager.setUserName(clientUserName)
      // find proxy user if any from query param
      val doAsQueryParam: String = getDoAsQueryParam(request.getQueryString)
      if (doAsQueryParam != null) {
        SessionManager.setProxyUserName(doAsQueryParam)
      }
      clientIpAddress = request.getRemoteAddr
      logDebug("Client IP Address: " + clientIpAddress)
      // Set the thread local ip address
      SessionManager.setIpAddress(clientIpAddress)
      // Generate new cookie and add it to the response
      if (requireNewCookie && !authType.equalsIgnoreCase(NOSASL.toString)) {
        val cookieToken: String = HttpAuthUtils.createCookieToken(clientUserName)
        val hs2Cookie: Cookie = createCookie(signer.signCookie(cookieToken))
        if (isHttpOnlyCookie) {
          response.setHeader("SET-COOKIE", getHttpOnlyCookieHeader(hs2Cookie))
        } else {
          response.addCookie(hs2Cookie)
        }
        logInfo("Cookie added for clientUserName " + clientUserName)
      }
      super.doPost(request, response)
    } catch {
      case e: HttpAuthenticationException =>
        logError("Error: ", e)
        // Send a 401 to the client
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
        if (isKerberosAuthMode(authType)) {
          response.addHeader(HttpAuthUtils.WWW_AUTHENTICATE, HttpAuthUtils.NEGOTIATE)
        }
        // scalastyle:off
        response.getWriter.println("Authentication Error: " + e.getMessage)
      // scalastyle:on
    } finally {
      // Clear the thread locals
      SessionManager.clearUserName
      SessionManager.clearIpAddress
      SessionManager.clearProxyUserName
    }
  }

  /**
   * Retrieves the client name from cookieString. If the cookie does not
   * correspond to a valid client, the function returns null.
   *
   * @param cookies HTTP Request cookies.
   * @return Client Username if cookieString has a HS2 Generated cookie that is currently valid.
   *         Else, returns null.
   */
  private def getClientNameFromCookie(cookies: Array[Cookie]): String = {
    // Current Cookie Name, Current Cookie Value
    var currName: String = null
    var currValue: String = null
    // Following is the main loop which iterates through all the cookies send by the client.
    // The HS2 generated cookies are of the format hive.server2.auth=<value>
    // A cookie which is identified as a hiveserver2 generated cookie is validated
    // by calling signer.verifyAndExtract(). If the validation passes, send the
    // username for which the cookie is validated to the caller. If no client side
    // cookie passes the validation, return null to the caller.
    for (currCookie <- cookies) { // Get the cookie name
      currName = currCookie.getName
      if (currName == AUTH_COOKIE) {
        // If we reached here, we have match for HS2 generated cookie
        currValue = currCookie.getValue
        // Validate the value.
        currValue = signer.verifyAndExtract(currValue)
        // Retrieve the user name, do the final validation step.
        if (currValue != null) {
          val userName: String = HttpAuthUtils.getUserNameFromCookieToken(currValue)
          if (userName == null) {
            logWarning("Invalid cookie token " + currValue)
          } else {
            // We have found a valid cookie in the client request.
            logDebug("Validated the cookie for user " + userName)
            return userName
          }
        }
      }
    }
    // No valid HS2 generated cookies found, return null
    null
  }

  /**
   * Convert cookie array to human readable cookie string
   *
   * @param cookies Cookie Array
   * @return String containing all the cookies separated by a newline character.
   *         Each cookie is of the format [key]=[value]
   */
  private def toCookieStr(cookies: Array[Cookie]): String = {
    var cookieStr: String = ""
    for (c <- cookies) {
      cookieStr += c.getName + "=" + c.getValue + " ;\n"
    }
    cookieStr
  }

  /**
   * Validate the request cookie. This function iterates over the request cookie headers
   * and finds a cookie that represents a valid client/server session. If it finds one, it
   * returns the client name associated with the session. Else, it returns null.
   *
   * @param request The HTTP Servlet Request send by the client
   * @return Client Username if the request has valid HS2 cookie, else returns null
   * @throws UnsupportedEncodingException
   */
  @throws[UnsupportedEncodingException]
  private def validateCookie(request: HttpServletRequest): String = {
    // Find all the valid cookies associated with the request.
    val cookies: Array[Cookie] = request.getCookies
    if (cookies == null) {
      logDebug("No valid cookies associated with the request " + request)
      return null
    }
    logDebug("Received cookies: " + toCookieStr(cookies))
    getClientNameFromCookie(cookies)
  }

  /**
   * Generate a server side cookie given the cookie value as the input.
   *
   * @param str Input string token.
   * @return The generated cookie.
   * @throws UnsupportedEncodingException
   */
  @throws[UnsupportedEncodingException]
  private def createCookie(str: String): Cookie = {
    logDebug("Cookie name = " + AUTH_COOKIE + " value = " + str)
    val cookie: Cookie = new Cookie(AUTH_COOKIE, str)
    cookie.setMaxAge(cookieMaxAge)
    if (cookieDomain != null) {
      cookie.setDomain(cookieDomain)
    }
    if (cookiePath != null) {
      cookie.setPath(cookiePath)
    }
    cookie.setSecure(isCookieSecure)
    cookie
  }

  /**
   * Generate httponly cookie from HS2 cookie
   *
   * @param cookie HS2 generated cookie
   * @return The httponly cookie
   */
  private def getHttpOnlyCookieHeader(cookie: Cookie): String = {
    val newCookie: NewCookie =
      new NewCookie(cookie.getName,
        cookie.getValue,
        cookie.getPath,
        cookie.getDomain,
        cookie.getVersion,
        cookie.getComment,
        cookie.getMaxAge,
        cookie.getSecure)
    newCookie + "; HttpOnly"
  }

  /**
   * Do the LDAP/PAM authentication
   *
   * @param request
   * @param authType
   * @throws HttpAuthenticationException
   */
  @throws[HttpAuthenticationException]
  private def doPasswdAuth(request: HttpServletRequest, authType: String): String = {
    val userName: String = getUsername(request, authType)
    // No-op when authType is NOSASL
    if (!authType.equalsIgnoreCase(NOSASL.toString)) {
      try {
        val authMethod: AuthMethods =
          AuthMethods.getValidAuthMethod(authType)
        val provider: PasswdAuthenticationProvider =
          AuthenticationProviderFactory.getAuthenticationProvider(authMethod)
        provider.Authenticate(userName, getPassword(request, authType))
      } catch {
        case e: Exception =>
          throw new HttpAuthenticationException(e)
      }
    }
    userName
  }

  /**
   * Do the GSS-API kerberos authentication.
   * We already have a logged in subject in the form of serviceUGI,
   * which GSS-API will extract information from.
   * In case of a SPNego request we use the httpUGI,
   * for the authenticating service tickets.
   *
   * @param request
   * @return
   * @throws HttpAuthenticationException
   */
  @throws[HttpAuthenticationException]
  private def doKerberosAuth(request: HttpServletRequest): String = {
    // Try authenticating with the http/_HOST principal
    if (httpUGI != null) {
      try
        return httpUGI.doAs(new HttpKerberosServerAction(request, httpUGI))
      catch {
        case e: Exception =>
          logInfo("Failed to authenticate with http/_HOST kerberos principal, " +
            "trying with hive/_HOST kerberos principal")
      }
    }
    // Now try with hive/_HOST principal
    try {
      serviceUGI.doAs(new HttpKerberosServerAction(request, serviceUGI))
    } catch {
      case e: Exception =>
        logError("Failed to authenticate with hive/_HOST kerberos principal")
        throw new HttpAuthenticationException(e)
    }
  }

  private[thrift] class HttpKerberosServerAction(var request: HttpServletRequest,
                                                 var serviceUGI: UserGroupInformation)
    extends PrivilegedExceptionAction[String] {
    @throws[HttpAuthenticationException]
    override def run: String = { // Get own Kerberos credentials for accepting connection
      val manager: GSSManager = GSSManager.getInstance
      var gssContext: GSSContext = null
      val serverPrincipal: String = getPrincipalWithoutRealm(serviceUGI.getUserName)
      try { // This Oid for Kerberos GSS-API mechanism.
        val kerberosMechOid: Oid = new Oid("1.2.840.113554.1.2.2")
        // Oid for SPNego GSS-API mechanism.
        val spnegoMechOid: Oid = new Oid("1.3.6.1.5.5.2")
        // Oid for kerberos principal name
        val krb5PrincipalOid: Oid = new Oid("1.2.840.113554.1.2.2.1")
        // GSS name for server
        val serverName: GSSName = manager.createName(serverPrincipal, krb5PrincipalOid)
        // GSS credentials for server
        val serverCreds: GSSCredential = manager.createCredential(serverName,
          GSSCredential.DEFAULT_LIFETIME,
          Array[Oid](kerberosMechOid,
            spnegoMechOid),
          GSSCredential.ACCEPT_ONLY)
        // Create a GSS context
        gssContext = manager.createContext(serverCreds)
        // Get service ticket from the authorization header
        val serviceTicketBase64: String = getAuthHeader(request, authType)
        val inToken: Array[Byte] = Base64.decodeBase64(serviceTicketBase64.getBytes)
        gssContext.acceptSecContext(inToken, 0, inToken.length)
        // Authenticate or deny based on its context completion
        if (!gssContext.isEstablished) {
          throw new HttpAuthenticationException("Kerberos authentication failed: " +
            "unable to establish context with the service ticket " + "provided by the client.")
        }
        else getPrincipalWithoutRealmAndHost(gssContext.getSrcName.toString)
      } catch {
        case e: GSSException =>
          throw new HttpAuthenticationException("Kerberos authentication failed: ", e)
      } finally if (gssContext != null) try
        gssContext.dispose()
      catch {
        case e: GSSException =>

        // No-op
      }
    }

    @throws[HttpAuthenticationException]
    private def getPrincipalWithoutRealm(fullPrincipal: String): String = {
      var fullKerberosName: HadoopShims.KerberosNameShim = null
      try
        fullKerberosName = ShimLoader.getHadoopShims.getKerberosNameShim(fullPrincipal)
      catch {
        case e: IOException =>
          throw new HttpAuthenticationException(e)
      }
      val serviceName: String = fullKerberosName.getServiceName
      val hostName: String = fullKerberosName.getHostName
      var principalWithoutRealm: String = serviceName
      if (hostName != null) principalWithoutRealm = serviceName + "/" + hostName
      principalWithoutRealm
    }

    @throws[HttpAuthenticationException]
    private def getPrincipalWithoutRealmAndHost(fullPrincipal: String): String = {
      var fullKerberosName: HadoopShims.KerberosNameShim = null
      try {
        fullKerberosName = ShimLoader.getHadoopShims.getKerberosNameShim(fullPrincipal)
        fullKerberosName.getShortName
      } catch {
        case e: IOException =>
          throw new HttpAuthenticationException(e)
      }
    }
  }

  @throws[HttpAuthenticationException]
  private def getUsername(request: HttpServletRequest, authType: String): String = {
    val creds: Array[String] = getAuthHeaderTokens(request, authType)
    // Username must be present
    if (creds(0) == null || creds(0).isEmpty) {
      throw new HttpAuthenticationException("Authorization header received " +
        "from the client does not contain username.")
    }
    creds(0)
  }

  @throws[HttpAuthenticationException]
  private def getPassword(request: HttpServletRequest, authType: String): String = {
    val creds: Array[String] = getAuthHeaderTokens(request, authType)
    // Password must be present
    if (creds(1) == null || creds(1).isEmpty) {
      throw new HttpAuthenticationException("Authorization header received " +
        "from the client does not contain username.")
    }
    creds(1)
  }

  @throws[HttpAuthenticationException]
  private def getAuthHeaderTokens(request: HttpServletRequest, authType: String): Array[String] = {
    val authHeaderBase64: String = getAuthHeader(request, authType)
    val authHeaderString: String =
      StringUtils.newStringUtf8(Base64.decodeBase64(authHeaderBase64.getBytes))
    val creds: Array[String] = authHeaderString.split(":")
    creds
  }

  /**
   * Returns the base64 encoded auth header payload
   *
   * @param request
   * @param authType
   * @return
   * @throws HttpAuthenticationException
   */
  @throws[HttpAuthenticationException]
  private def getAuthHeader(request: HttpServletRequest, authType: String): String = {
    val authHeader: String = request.getHeader(HttpAuthUtils.AUTHORIZATION)
    // Each http request must have an Authorization header
    if (authHeader == null || authHeader.isEmpty) {
      throw new HttpAuthenticationException("Authorization header received " +
        "from the client is empty.")
    }
    var authHeaderBase64String: String = null
    var beginIndex: Int = 0
    if (isKerberosAuthMode(authType)) {
      beginIndex = (HttpAuthUtils.NEGOTIATE + " ").length
    } else {
      beginIndex = (HttpAuthUtils.BASIC + " ").length
    }
    authHeaderBase64String = authHeader.substring(beginIndex)
    // Authorization header must have a payload
    if (authHeaderBase64String == null || authHeaderBase64String.isEmpty) {
      throw new HttpAuthenticationException("Authorization header received " +
        "from the client does not contain any data.")
    }
    authHeaderBase64String
  }

  private def isKerberosAuthMode(authType: String): Boolean =
    authType.equalsIgnoreCase(KERBEROS.toString)

  private def getDoAsQueryParam(queryString: String): String = {
    logDebug("URL query string:" + queryString)
    if (queryString == null) {
      return null
    }
    val params: util.Hashtable[String, Array[String]] =
      javax.servlet.http.HttpUtils.parseQueryString(queryString)
    val keySet: Seq[String] = params.keySet.asScala.toSeq
    keySet.foreach(key => {
      if (key.equalsIgnoreCase("doAs")) {
        return params.get(key)(0)
      }
    })
    null
  }
}
