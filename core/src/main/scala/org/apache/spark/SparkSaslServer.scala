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

package org.apache.spark

import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.sasl.AuthorizeCallback
import javax.security.sasl.RealmCallback
import javax.security.sasl.Sasl
import javax.security.sasl.SaslException
import javax.security.sasl.SaslServer
import scala.collection.JavaConversions.mapAsJavaMap
import org.apache.commons.net.util.Base64

/**
 * Encapsulates SASL server logic
 */
private[spark] class SparkSaslServer(securityMgr: SecurityManager) extends Logging {

  /**
   * Actual SASL work done by this object from javax.security.sasl.
   */
  private var saslServer: SaslServer = Sasl.createSaslServer(SparkSaslServer.DIGEST, null,
    SparkSaslServer.SASL_DEFAULT_REALM, SparkSaslServer.SASL_PROPS,
    new SparkSaslDigestCallbackHandler(securityMgr))

  /**
   * Determines whether the authentication exchange has completed.
   * @return true is complete, otherwise false
   */
  def isComplete(): Boolean = {
    synchronized {
      if (saslServer != null) saslServer.isComplete() else false
    }
  }

  /**
   * Used to respond to server SASL tokens.
   * @param token Server's SASL token
   * @return response to send back to the server.
   */
  def response(token: Array[Byte]): Array[Byte] = {
    synchronized {
      if (saslServer != null) saslServer.evaluateResponse(token) else new Array[Byte](0)
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the
   * SaslServer might be using.
   */
  def dispose() {
    synchronized {
      if (saslServer != null) {
        try {
          saslServer.dispose()
        } catch {
          case e: SaslException => // ignore
        } finally {
          saslServer = null
        }
      }
    }
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler
   * for SASL DIGEST-MD5 mechanism
   */
  private class SparkSaslDigestCallbackHandler(securityMgr: SecurityManager)
    extends CallbackHandler {

    private val userName: String =
      SparkSaslServer.encodeIdentifier(securityMgr.getSaslUser().getBytes("utf-8"))

    override def handle(callbacks: Array[Callback]) {
      logDebug("In the sasl server callback handler")
      callbacks foreach {
        case nc: NameCallback => {
          logDebug("handle: SASL server callback: setting username")
          nc.setName(userName)
        }
        case pc: PasswordCallback => {
          logDebug("handle: SASL server callback: setting userPassword")
          val password: Array[Char] =
            SparkSaslServer.encodePassword(securityMgr.getSecretKey().getBytes("utf-8"))
          pc.setPassword(password)
        }
        case rc: RealmCallback => {
          logDebug("handle: SASL server callback: setting realm: " + rc.getDefaultText())
          rc.setText(rc.getDefaultText())
        }
        case ac: AuthorizeCallback => {
          val authid = ac.getAuthenticationID()
          val authzid = ac.getAuthorizationID()
          if (authid.equals(authzid)) {
            logDebug("set auth to true")
            ac.setAuthorized(true)
          } else {
            logDebug("set auth to false")
            ac.setAuthorized(false)
          }
          if (ac.isAuthorized()) {
            logDebug("sasl server is authorized")
            ac.setAuthorizedID(authzid)
          }
        }
        case cb: Callback => throw
          new UnsupportedCallbackException(cb, "handle: Unrecognized SASL DIGEST-MD5 Callback")
      }
    }
  }
}

private[spark] object SparkSaslServer {

  /**
   * This is passed as the server name when creating the sasl client/server.
   * This could be changed to be configurable in the future.
   */
  val  SASL_DEFAULT_REALM = "default"

  /**
   * The authentication mechanism used here is DIGEST-MD5. This could be changed to be
   * configurable in the future.
   */
  val DIGEST = "DIGEST-MD5"

  /**
   * The quality of protection is just "auth". This means that we are doing
   * authentication only, we are not supporting integrity or privacy protection of the
   * communication channel after authentication. This could be changed to be configurable
   * in the future.
   */
  val SASL_PROPS = Map(Sasl.QOP -> "auth", Sasl.SERVER_AUTH ->"true")

  /**
   * Encode a byte[] identifier as a Base64-encoded string.
   *
   * @param identifier identifier to encode
   * @return Base64-encoded string
   */
  def encodeIdentifier(identifier: Array[Byte]): String = {
    new String(Base64.encodeBase64(identifier), "utf-8")
  }

  /**
   * Encode a password as a base64-encoded char[] array.
   * @param password as a byte array.
   * @return password as a char array.
   */
  def encodePassword(password: Array[Byte]): Array[Char] = {
    new String(Base64.encodeBase64(password), "utf-8").toCharArray()
  }
}

