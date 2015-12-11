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

import java.net.URL

import org.apache.hadoop.security.authentication.client.AuthenticatedURL
import org.apache.hadoop.security.authentication.client.AuthenticatedURL.{Token => AuthToken}
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator

import org.apache.spark.Logging

/**
 * Extend the Kerberos Delegation Token Authenticator with some logging.
 */
private[spark] class LoggingKerberosDelegationTokenAuthenticator
  extends KerberosDelegationTokenAuthenticator with Logging {

  override def authenticate(url: URL, token: AuthToken): Unit = {
    val orig = tokenToString(token)
    super.authenticate(url, token)
    val updated = tokenToString(token)
    if (updated != orig) {
      logInfo(s"Token updated against $url")
      logDebug(s"New Token: $updated")
    }
  }

  /**
   * Gets a token to a string. If the token value is null, the string "(unset)" is returned.
   *
   * In contrast, `Token.toString()` returns `null`
   * @param token token to stringify
   * @return a string value for logging
   */
  def tokenToString(token: AuthenticatedURL.Token): String = {
    if (token.isSet) token.toString else "(unset)"
  }

  /**
   * Low level token renewal operation
   */
  override def renewDelegationToken(
    url: URL,
    token: AuthToken,
    dToken: Token[AbstractDelegationTokenIdentifier],
    doAsUser: String): Long = {

    val orig = dToken.toString
    val user = if (doAsUser != null) s"user=$doAsUser" else ""
    logInfo(s"Renewing token against $url $user")
    val result = super.renewDelegationToken(url, token, dToken, doAsUser)
    val updated = dToken.toString
    if (updated != orig) {
      logInfo(s"Delegation Token updated against $url")
      logDebug(s"New Token: $updated")
    }
    result
  }
}
