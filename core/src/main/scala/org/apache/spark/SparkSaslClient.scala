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
import javax.security.sasl.RealmCallback
import javax.security.sasl.RealmChoiceCallback
import javax.security.sasl.Sasl
import javax.security.sasl.SaslClient
import javax.security.sasl.SaslException

import scala.collection.JavaConversions.mapAsJavaMap

import com.google.common.base.Charsets.UTF_8

/**
 * Implements SASL Client logic for Spark
 */
private[spark] class SparkSaslClient(securityMgr: SecurityManager)  extends Logging {

  /**
   * Used to respond to server's counterpart, SaslServer with SASL tokens
   * represented as byte arrays.
   *
   * The authentication mechanism used here is DIGEST-MD5. This could be changed to be
   * configurable in the future.
   */
  private var saslClient: SaslClient = Sasl.createSaslClient(Array[String](SparkSaslServer.DIGEST),
    null, null, SparkSaslServer.SASL_DEFAULT_REALM, SparkSaslServer.SASL_PROPS,
    new SparkSaslClientCallbackHandler(securityMgr))

  /**
   * Used to initiate SASL handshake with server.
   * @return response to challenge if needed
   */
  def firstToken(): Array[Byte] = {
    synchronized {
      val saslToken: Array[Byte] =
        if (saslClient != null && saslClient.hasInitialResponse()) {
          logDebug("has initial response")
          saslClient.evaluateChallenge(new Array[Byte](0))
        } else {
          new Array[Byte](0)
        }
      saslToken
    }
  }

  /**
   * Determines whether the authentication exchange has completed.
   * @return true is complete, otherwise false
   */
  def isComplete(): Boolean = {
    synchronized {
      if (saslClient != null) saslClient.isComplete() else false
    }
  }

  /**
   * Respond to server's SASL token.
   * @param saslTokenMessage contains server's SASL token
   * @return client's response SASL token
   */
  def saslResponse(saslTokenMessage: Array[Byte]): Array[Byte] = {
    synchronized {
      if (saslClient != null) saslClient.evaluateChallenge(saslTokenMessage) else new Array[Byte](0)
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the
   * SaslClient might be using.
   */
  def dispose() {
    synchronized {
      if (saslClient != null) {
        try {
          saslClient.dispose()
        } catch {
          case e: SaslException => // ignored
        } finally {
          saslClient = null
        }
      }
    }
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler
   * that works with share secrets.
   */
  private class SparkSaslClientCallbackHandler(securityMgr: SecurityManager) extends
    CallbackHandler {

    private val userName: String =
      SparkSaslServer.encodeIdentifier(securityMgr.getSaslUser().getBytes(UTF_8))
    private val secretKey = securityMgr.getSecretKey()
    private val userPassword: Array[Char] = SparkSaslServer.encodePassword(
        if (secretKey != null) secretKey.getBytes(UTF_8) else "".getBytes(UTF_8))

    /**
     * Implementation used to respond to SASL request from the server.
     *
     * @param callbacks objects that indicate what credential information the
     *                  server's SaslServer requires from the client.
     */
    override def handle(callbacks: Array[Callback]) {
      logDebug("in the sasl client callback handler")
      callbacks foreach {
        case  nc: NameCallback => {
          logDebug("handle: SASL client callback: setting username: " + userName)
          nc.setName(userName)
        }
        case pc: PasswordCallback => {
          logDebug("handle: SASL client callback: setting userPassword")
          pc.setPassword(userPassword)
        }
        case rc: RealmCallback => {
          logDebug("handle: SASL client callback: setting realm: " + rc.getDefaultText())
          rc.setText(rc.getDefaultText())
        }
        case cb: RealmChoiceCallback => {}
        case cb: Callback => throw
          new UnsupportedCallbackException(cb, "handle: Unrecognized SASL client callback")
      }
    }
  }
}
