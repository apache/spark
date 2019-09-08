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

package org.apache.spark.sql.hive.thriftserver.auth

import java.security.{AccessController, PrivilegedExceptionAction}
import java.util
import java.util.{Random, StringTokenizer}
import javax.security.auth.Subject

import org.apache.commons.codec.binary.Base64
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.http.protocol.{BasicHttpContext, HttpContext}
import org.ietf.jgss.{GSSContext, GSSManager, Oid}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * Utility functions for HTTP mode authentication.
 */
object HttpAuthUtils {
  val WWW_AUTHENTICATE = "WWW-Authenticate"
  val AUTHORIZATION = "Authorization"
  val BASIC = "Basic"
  val NEGOTIATE = "Negotiate"
  private val LOG = LogFactory.getLog(classOf[HttpAuthUtils])
  private val COOKIE_ATTR_SEPARATOR = "&"
  private val COOKIE_CLIENT_USER_NAME = "cu"
  private val COOKIE_CLIENT_RAND_NUMBER = "rn"
  private val COOKIE_KEY_VALUE_SEPARATOR = "="
  private val COOKIE_ATTRIBUTES =
    new util.HashSet[String](util.Arrays.asList(COOKIE_CLIENT_USER_NAME, COOKIE_CLIENT_RAND_NUMBER))

  /**
   * @return Stringified Base64 encoded kerberosAuthHeader on success
   * @throws Exception
   */
  @throws[Exception]
  def getKerberosServiceTicket(principal: String,
                               host: String,
                               serverHttpUrl: String,
                               assumeSubject: Boolean): String = {
    val serverPrincipal = ShimLoader.getHadoopThriftAuthBridge.getServerPrincipal(principal, host)
    if (assumeSubject) { // With this option, we're assuming that the external application,
      // using the JDBC driver has done a JAAS kerberos login already
      val context = AccessController.getContext
      val subject = Subject.getSubject(context)
      if (subject == null) throw new Exception("The Subject is not set")
      Subject.doAs(subject,
        new HttpAuthUtils.HttpKerberosClientAction(serverPrincipal, serverHttpUrl))
    }
    else { // JAAS login from ticket cache to setup the client UserGroupInformation
      val clientUGI = ShimLoader.getHadoopThriftAuthBridge.getCurrentUGIWithConf("kerberos")
      clientUGI.doAs(new HttpAuthUtils.HttpKerberosClientAction(serverPrincipal, serverHttpUrl))
    }
  }

  /**
   * Creates and returns a HS2 cookie token.
   *
   * @param clientUserName Client User name.
   * @return An unsigned cookie token generated from input parameters.
   *         The final cookie generated is of the following format :
   *         { @code cu=<username>&rn=<randomNumber>&s=<cookieSignature>}
   */
  def createCookieToken(clientUserName: String): String = {
    val sb = new StringBuffer
    sb.append(COOKIE_CLIENT_USER_NAME)
      .append(COOKIE_KEY_VALUE_SEPARATOR)
      .append(clientUserName).append(COOKIE_ATTR_SEPARATOR)
    sb.append(COOKIE_CLIENT_RAND_NUMBER)
      .append(COOKIE_KEY_VALUE_SEPARATOR)
      .append(new Random(System.currentTimeMillis).nextLong)
    sb.toString
  }

  /**
   * Parses a cookie token to retrieve client user name.
   *
   * @param tokenStr Token String.
   * @return A valid user name if input is of valid format, else returns null.
   */
  def getUserNameFromCookieToken(tokenStr: String): String = {
    val map = splitCookieToken(tokenStr)
    if (!(map.keySet == COOKIE_ATTRIBUTES)) {
      LOG.error("Invalid token with missing attributes " + tokenStr)
      return null
    }
    map.get(COOKIE_CLIENT_USER_NAME)
  }

  /**
   * Splits the cookie token into attributes pairs.
   *
   * @param str input token.
   * @return a map with the attribute pairs of the token if the input is valid.
   *         Else, returns null.
   */
  private def splitCookieToken(tokenStr: String): util.Map[String, String] = {
    val map = new util.HashMap[String, String]
    val st = new StringTokenizer(tokenStr, COOKIE_ATTR_SEPARATOR)
    while ( {
      st.hasMoreTokens
    }) {
      val part = st.nextToken
      val separator = part.indexOf(COOKIE_KEY_VALUE_SEPARATOR)
      if (separator == -1) {
        LOG.error("Invalid token string " + tokenStr)
        return null
      }
      val key = part.substring(0, separator)
      val value = part.substring(separator + 1)
      map.put(key, value)
    }
    map
  }

  /**
   * We'll create an instance of this class within a doAs block so that the client's TGT credentials
   * can be read from the Subject
   */
  object HttpKerberosClientAction {
    val HTTP_RESPONSE = "HTTP_RESPONSE"
    val SERVER_HTTP_URL = "SERVER_HTTP_URL"
  }

  class HttpKerberosClientAction(val serverPrincipal: String,
                                 val serverHttpUrl: String)
    extends PrivilegedExceptionAction[String] {
    base64codec = new Base64(0)
    httpContext = new BasicHttpContext
    httpContext.setAttribute(HttpKerberosClientAction.SERVER_HTTP_URL, serverHttpUrl)
    final private var base64codec: Base64 = null
    final private var httpContext: HttpContext = null

    @throws[Exception]
    override def run: String = { // This Oid for Kerberos GSS-API mechanism.
      val mechOid = new Oid("1.2.840.113554.1.2.2")
      // Oid for kerberos principal name
      val krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1")
      val manager = GSSManager.getInstance
      // GSS name for server
      val serverName = manager.createName(serverPrincipal, krb5PrincipalOid)
      // Create a GSSContext for authentication with the service.
      // We're passing client credentials as null since we want them to be read from the Subject.
      val gssContext = manager.createContext(serverName, mechOid, null, GSSContext.DEFAULT_LIFETIME)
      gssContext.requestMutualAuth(false)
      // Establish context
      val inToken = new Array[Byte](0)
      val outToken = gssContext.initSecContext(inToken, 0, inToken.length)
      gssContext.dispose()
      // Base64 encoded and stringified token for server
      new String(base64codec.encode(outToken))
    }
  }

}

final class HttpAuthUtils private() {
  throw new UnsupportedOperationException("Can't initialize class")
}
