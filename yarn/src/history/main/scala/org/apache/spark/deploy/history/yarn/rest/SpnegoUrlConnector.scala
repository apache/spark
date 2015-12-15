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

package org.apache.spark.deploy.history.yarn.rest

import java.io.{FileNotFoundException, IOException}
import java.net.{HttpURLConnection, NoRouteToHostException, URL, URLConnection}
import javax.net.ssl.HttpsURLConnection
import javax.servlet.http.HttpServletResponse

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authentication.client.{AuthenticatedURL, AuthenticationException, Authenticator, ConnectionConfigurator, KerberosAuthenticator, PseudoAuthenticator}
import org.apache.hadoop.security.ssl.SSLFactory
import org.apache.hadoop.security.token.delegation.web.{DelegationTokenAuthenticatedURL, DelegationTokenAuthenticator, PseudoDelegationTokenAuthenticator}
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token

import org.apache.spark.Logging

/**
 * SPNEGO-backed URL connection factory for Java net, and hence Jersey.
 * Based on WebHFDS client code in
 * `org.apache.hadoop.hdfs.web.URLConnectionFactory`
 */
private[spark] class SpnegoUrlConnector(
  connConfigurator: ConnectionConfigurator,
  delegationToken: DelegationTokenAuthenticatedURL.Token) extends Logging {

  val secure = UserGroupInformation.isSecurityEnabled
  var authToken = new AuthenticatedURL.Token

  // choose an authenticator based on security settings
  val delegationAuthenticator: DelegationTokenAuthenticator =
    if (secure)  {
      new LoggingKerberosDelegationTokenAuthenticator()
    } else {
      new PseudoDelegationTokenAuthenticator()
    }

  init()

  /**
   * Initialization operation sets up authentication
   */
  private def init(): Unit = {
    logDebug(s"using $delegationAuthenticator for authentication")
    delegationAuthenticator.setConnectionConfigurator(connConfigurator)
  }

  /**
   * Opens a URL
   *
   * @param url URL to open
   * @param isSpnego whether the url should be authenticated via SPNEGO
   * @return URLConnection
   * @throws IOException IO problems
   * @throws AuthenticationException authentication failure
   */

  def openConnection(url: URL, isSpnego: Boolean): URLConnection = {
    require(connConfigurator != null)
    require(url.getPort != 0, "no port")
    if (isSpnego) {
      logDebug(s"open AuthenticatedURL connection $url")
      UserGroupInformation.getCurrentUser.checkTGTAndReloginFromKeytab()
      val authToken = new AuthenticatedURL.Token
      val authurl = new AuthenticatedURL(KerberosUgiAuthenticator, connConfigurator)
      authurl.openConnection(url, authToken)
    } else {
      logDebug(s"open URL connection $url")
      val connection = url.openConnection()
      connection match {
        case connection1: HttpURLConnection =>
          connConfigurator.configure(connection1)
        case _ =>
      }
      connection
    }
  }

  /**
   * Opens a URL authenticated as the current user.
   * A TGT check and login is done first, so the
   * caller will always be logged in before the operation takes place
   *
   * @param url URL to open
   * @return URLConnection
   * @throws IOException problems.
   */
  def getHttpURLConnection(url: URL): HttpURLConnection = {
    val callerUGI = UserGroupInformation.getCurrentUser
    callerUGI.checkTGTAndReloginFromKeytab()
    openConnection(url, callerUGI)
  }

  /**
   * Open a connection as the specific user.
   *
   * @param url URL to open
   * @param callerUGI identity of the caller
   * @return the open connection
   */
  private def openConnection(url: URL, callerUGI: UserGroupInformation): HttpURLConnection = {
    // special sanity check for 0.0.0.0, as the entire TCP stack forgot to do this.
    if (url.getHost == "0.0.0.0") {
      throw new NoRouteToHostException(s"Cannot connect to a host with no address: $url")
    }
    val conn = callerUGI.doAs(new PrivilegedFunction(
      () => {
        try {
          new AuthenticatedURL(KerberosUgiAuthenticator, connConfigurator)
              .openConnection(url, authToken)
        } catch {
          case ex: AuthenticationException =>
            // auth failure
            throw new UnauthorizedRequestException(url.toString,
              s"Authentication failure as $callerUGI against $url: $ex", ex)
          case other: Throwable =>
            // anything else is rethrown
            throw other
        }
      }))
    conn.setUseCaches(false)
    conn.setInstanceFollowRedirects(true)
    conn
  }

  /**
   * Get the current delegation token.
   * @return the current delegation token
   */
  def getDelegationToken: Token = {
    this.synchronized {
      delegationToken
    }
  }

  /**
   * Get the auth token.
   * @return the current auth token
   */
  def getAuthToken: AuthenticatedURL.Token = {
    this.synchronized {
      authToken
    }
  }

  /**
   * Reset the auth tokens.
   */
  def resetAuthTokens(): Unit = {
    logInfo("Resetting the auth tokens")
    this.synchronized {
      authToken = new AuthenticatedURL.Token
      delegationToken.setDelegationToken(null)
    }
  }

  /**
   * Execute an HTTP operation
   * @param verb request method
   * @param url URL
   * @param payload payload, default is `null`
   * @param payloadContentType content type. Required if payload != null
   * @return the response
   */
  def execHttpOperation(
    verb: String,
    url: URL,
    payload: Array[Byte] = null,
    payloadContentType: String = ""): HttpOperationResponse = {
    var conn: HttpURLConnection = null
    val outcome = new HttpOperationResponse()
    var resultCode = 0
    var body: Array[Byte] = null
    logDebug(s"$verb $url spnego=$secure")
    val hasData = payload != null
    try {
      conn = getHttpURLConnection(url)
      conn.setRequestMethod(verb)
      conn.setDoOutput(hasData)
      if (hasData) {
        require(payloadContentType != null, "no content type")
        conn.setRequestProperty("Content-Type", payloadContentType)
      }
      // connection
      conn.connect()
      if (hasData) {
        // submit any data
        val output = conn.getOutputStream
        try {
          IOUtils.write(payload, output)
        } finally {
          output.close()
        }
      }
      resultCode = conn.getResponseCode
      outcome.responseCode = resultCode
      outcome.responseMessage = conn.getResponseMessage
      outcome.lastModified = conn.getLastModified
      outcome.contentType = conn.getContentType

      var stream = conn.getErrorStream
      if (stream == null) {
        stream = conn.getInputStream
      }
      if (stream != null) {
        outcome.data = IOUtils.toByteArray(stream)
      } else {
        log.debug("No body in response")
      }
      // now check to see if the response indicated that the delegation token had expired.
    } finally {
      if (conn != null) {
        conn.disconnect()
      }
    }
    if (SpnegoUrlConnector.delegationTokensExpired(outcome)) {
      logInfo(s"Delegation token may have expired")
      resetAuthTokens()
      throw new UnauthorizedRequestException(url.toString,
        s"Authentication failure: ${outcome.responseLine}")
    }
    SpnegoUrlConnector.uprateFaults(verb, url.toString, outcome)
    outcome
  }

}


/**
 * Use UserGroupInformation as a fallback authenticator
 * if the server does not use Kerberos SPNEGO HTTP authentication.
 */
private object KerberosUgiAuthenticator extends KerberosAuthenticator {

  private object UgiAuthenticator extends PseudoAuthenticator {
    protected override def getUserName: String = {
      try {
        UserGroupInformation.getLoginUser.getUserName
      } catch {
        case e: IOException => {
          throw new SecurityException("Failed to obtain current username", e)
        }
      }
    }
  }

  protected override def getFallBackAuthenticator: Authenticator = {
    UgiAuthenticator
  }
}

/**
 * Default Connection Configurator: simply sets the socket timeout
 */
private object DefaultConnectionConfigurator extends ConnectionConfigurator {
  override def configure(conn: HttpURLConnection): HttpURLConnection = {
    SpnegoUrlConnector.setTimeouts(conn, SpnegoUrlConnector.DEFAULT_SOCKET_TIMEOUT)
    conn
  }
}

private[spark] object SpnegoUrlConnector extends Logging {

  /**
   * Timeout for socket connects and reads
   */
  val DEFAULT_SOCKET_TIMEOUT: Int = 1 * 60 * 1000

  /**
   * Sets timeout parameters on the given URLConnection.
   *
   * @param connection URLConnection to set
   * @param socketTimeout the connection and read timeout of the connection.
   */
  def setTimeouts(connection: URLConnection, socketTimeout: Int) {
    connection.setConnectTimeout(socketTimeout)
    connection.setReadTimeout(socketTimeout)
  }

  /**
   * Construct a new URLConnectionFactory based on the configuration.
   *
   * @param conf configuration
   * @param token delegation token
   * @return a new instance
   */
  def newInstance(conf: Configuration,
      token: DelegationTokenAuthenticatedURL.Token): SpnegoUrlConnector = {
    var conn: ConnectionConfigurator = null
    try {
      conn = newSslConnConfigurator(DEFAULT_SOCKET_TIMEOUT, conf)
    } catch {
      case e: Exception => {
        logInfo("Cannot load customized SSL related configuration." +
            " Fallback to system-generic settings.")
        logDebug("Exception details", e)
        conn = DefaultConnectionConfigurator
      }
    }
    new SpnegoUrlConnector(conn, token)
  }

  /**
   * Create a new ConnectionConfigurator for SSL connections.
   *
   * This uses `org.apache.hadoop.security.ssl.SSLFactory` to build an
   * SSL factory based on information provided in the Hadoop configuration
   * object passed in, including the hostname verifier specified
   * in the configuration options.
   *
   * @param timeout timeout in millis
   * @param conf configuration
   * @return a [[SSLConnConfigurator]] instance
   */
  def newSslConnConfigurator(timeout: Int, conf: Configuration): ConnectionConfigurator = {
    val factory = new SSLFactory(SSLFactory.Mode.CLIENT, conf)
    factory.init()
    new SSLConnConfigurator(timeout, factory )
  }

  /**
   * The SSL connection configurator
   * @param timeout connection timeout
   * @param factory socket factory
   */
  private[rest] class SSLConnConfigurator(timeout: Int, factory: SSLFactory)
      extends ConnectionConfigurator {
    val sf = factory.createSSLSocketFactory
    val hv = factory.getHostnameVerifier

    @throws(classOf[IOException])
    def configure(conn: HttpURLConnection): HttpURLConnection = {
      conn match {
        case c: HttpsURLConnection =>
          c.setSSLSocketFactory(sf)
          c.setHostnameVerifier(hv)
        case _ =>
      }
      setTimeouts(conn, timeout)
      conn
    }
  }

  /**
   * Uprate error codes 400 and up into exceptions, which are then thrown
   *
   * 1. 404 is converted to a `FileNotFoundException`
   * 2. 401 and 404 to `UnauthorizedRequestException`
   * 3. All others above 400: `IOException`
   * 4. Any error code under 400 is not considered a failure; this function will return normally.
   *
   * @param verb HTTP Verb used
   * @param url URL as string
   * @param response response from the request
   * @throws IOException if the result code was 400 or higher
   */
  @throws(classOf[IOException])
  def uprateFaults(verb: String, url: String, response: HttpOperationResponse): Unit = {
    val resultCode: Int = response.responseCode
    val body: Array[Byte] = response.data
    val errorText = s"$verb $url"
    resultCode match {

        // 401 & 403
      case HttpServletResponse.SC_UNAUTHORIZED | HttpServletResponse.SC_FORBIDDEN =>
        throw new UnauthorizedRequestException(url, errorText)

      case 404 =>
        throw new FileNotFoundException(errorText)

      case resultCode: Int if resultCode >= 400 =>
        val bodyText = if (body != null && body.length > 0) {
          new String(body)
        } else {
          ""
        }
        val message = s"$errorText failed with exit code $resultCode, body length" +
            s" ${bodyText.length }\n${bodyText }"
        logError(message)
        throw new HttpRequestException(resultCode, verb, url, message, bodyText)

      case _ =>
        // success
    }
  }

  /** Error text received warning anonymous requests are forbidden */
  val ANONYMOUS_REQUESTS_DISALLOWED = "Anonymous requests are disallowed"

  /** Error text received on an invalid signature */
  val INVALID_SIGNATURE = "Invalid signature"

  /**
   * Look for specific error codes or messages as a cue triggering token renewal.
   * Looking for specific text in exception codes is awful, but it is what other
   * Hadoop SPNEGO client libraries do.
   *
   * @param response response to analyse
   * @return true if this is a 40x exception that may be addressed by renewing
   *         the delegation tokens
   */
  def delegationTokensExpired(response: HttpOperationResponse): Boolean = {
    val message = response.responseMessage
    response.responseCode match {
      case HttpURLConnection.HTTP_UNAUTHORIZED =>
        true
      case HttpURLConnection.HTTP_FORBIDDEN
        if message == ANONYMOUS_REQUESTS_DISALLOWED || message.contains(INVALID_SIGNATURE) =>
          true
      case _ =>
        false
    }
  }

}
