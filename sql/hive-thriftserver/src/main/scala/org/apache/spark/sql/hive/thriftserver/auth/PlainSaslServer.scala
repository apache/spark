/*
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

package org.apache.spark.sql.hive.thriftserver.auth

import java.io.IOException
import java.security.Provider
import java.util
import javax.security.auth.callback._
import javax.security.sasl.{AuthorizeCallback, SaslException, SaslServer, SaslServerFactory}

/**
 * Sun JDK only provides a PLAIN client and no server. This class implements the Plain SASL server
 * conforming to RFC #4616 (http://www.ietf.org/rfc/rfc4616.txt).
 */
class PlainSaslServer extends SaslServer {

  private var _user: String = null
  private var _handler: CallbackHandler = null

  def this(handler: CallbackHandler, authMethodStr: String) {
    this()
    _handler = handler
    AuthMethods.getValidAuthMethod(authMethodStr)
  }

  override def getMechanismName: String = PlainSaslServer.PLAIN_METHOD

  @throws[SaslException]
  override def evaluateResponse(response: Array[Byte]): Array[Byte] = {
    try {
      // parse the response
      // message   = [authzid] UTF8NUL authcid UTF8NUL passwd'
      val tokenList = new util.ArrayDeque[String]
      var messageToken = new StringBuilder
      for (b <- response) {
        if (b == 0) {
          tokenList.addLast(messageToken.toString)
          messageToken = new StringBuilder
        }
        else messageToken.append(b.toChar)
      }
      tokenList.addLast(messageToken.toString)
      // validate response
      if (tokenList.size < 2 || tokenList.size > 3) {
        throw new SaslException("Invalid message format")
      }
      val passwd = tokenList.removeLast
      _user = tokenList.removeLast
      // optional authzid
      var authzId: String = null
      if (tokenList.isEmpty) {
        authzId = _user
      } else {
        authzId = tokenList.removeLast
      }
      if (_user == null || _user.isEmpty) {
        throw new SaslException("No user name provided")
      }
      if (passwd == null || passwd.isEmpty) {
        throw new SaslException("No password name provided")
      }
      val nameCallback = new NameCallback("User")
      nameCallback.setName(_user)
      val pcCallback = new PasswordCallback("Password", false)
      pcCallback.setPassword(passwd.toCharArray)
      val acCallback = new AuthorizeCallback(_user, authzId)
      val cbList: Array[Callback] = Array(nameCallback, pcCallback, acCallback)

      _handler.handle(cbList)
      if (!acCallback.isAuthorized) throw new SaslException("Authentication failed")
    } catch {
      case eL: IllegalStateException =>
        throw new SaslException("Invalid message format", eL)
      case eI: IOException =>
        throw new SaslException("Error validating the login", eI)
      case eU: UnsupportedCallbackException =>
        throw new SaslException("Error validating the login", eU)
    }
    null
  }

  override def isComplete: Boolean = _user != null

  override def getAuthorizationID: String = _user


  override def getNegotiatedProperty(propName: String): AnyRef = null

  override def dispose(): Unit = {
  }

  override def unwrap(incoming: Array[Byte], offset: Int, len: Int): Array[Byte] = {
    throw new UnsupportedOperationException
  }

  override def wrap(outgoing: Array[Byte], offset: Int, len: Int): Array[Byte] = {
    throw new UnsupportedOperationException
  }
}

object PlainSaslServer {
  val PLAIN_METHOD: String = "PLAIN"

  class SaslPlainServerFactory extends SaslServerFactory {

    override def createSaslServer(mechanism: String,
                                  protocol: String,
                                  serverName: String,
                                  props: util.Map[String, _], cbh: CallbackHandler): SaslServer = {
      if (PLAIN_METHOD == mechanism) {
        try
          return new PlainSaslServer(cbh, protocol)
        catch {
          case e: SaslException =>
            /* This is to fulfill the contract of the interface which states that an exception shall
                         be thrown when a SaslServer cannot be created due to an error but null should be
                         returned when a Server can't be created due to the parameters supplied. And the only
                         thing PlainSaslServer can fail on is a non-supported authentication mechanism.
                         That's why we return null instead of throwing the Exception */
            return null
        }
      }
      null
    }

    override def getMechanismNames(props: util.Map[String, _]): Array[String] =
      Array[String](PLAIN_METHOD)

  }

  class SaslPlainProvider() extends Provider("HiveSaslPlain", 1.0, "Hive Plain SASL provider") {
    put("SaslServerFactory.PLAIN", classOf[SaslPlainServerFactory].getName)
  }

}