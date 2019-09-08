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

import java.io.IOException
import java.security.Security
import java.util
import javax.security.auth.callback._
import javax.security.auth.login.LoginException
import javax.security.sasl.{AuthenticationException, AuthorizeCallback, SaslException}

import org.apache.thrift.{TProcessor, TProcessorFactory}
import org.apache.thrift.transport.{TSaslClientTransport, TSaslServerTransport, TTransport, TTransportFactory}

import org.apache.spark.service.cli.thrift.TCLIService.Iface
import org.apache.spark.sql.hive.thriftserver.cli.thrift.ThriftCLIService


object PlainSaslHelper {

  // Register Plain SASL server provider
  try {
    Security.addProvider(new PlainSaslServer.SaslPlainProvider)
  } catch {
    case e: Exception =>
      throw new UnsupportedOperationException("Can't initialize class")
  }

  def getPlainProcessorFactory(service: ThriftCLIService): SQLPlainProcessorFactory = {
    new SQLPlainProcessorFactory(service)
  }

  @throws[LoginException]
  def getPlainTransportFactory(authTypeStr: String): TTransportFactory = {
    val saslFactory: TSaslServerTransport.Factory = new TSaslServerTransport.Factory()
    try
      saslFactory.addServerDefinition("PLAIN",
        authTypeStr,
        null,
        new util.HashMap[String, String](),
        new PlainServerCallbackHandler(authTypeStr))
    catch {
      case e: AuthenticationException =>
        throw new LoginException("Error setting callback handler" + e)
    }
    saslFactory
  }


  @throws[SaslException]
  def getPlainTransport(username: String,
                        password: String,
                        underlyingTransport: TTransport): TSaslClientTransport = {
    new TSaslClientTransport("PLAIN",
      null,
      null,
      null,
      new util.HashMap[String, String](),
      new PlainCallbackHandler(username, password),
      underlyingTransport)
  }
}


private class PlainServerCallbackHandler @throws[AuthenticationException]
(val authMethodStr: String) extends CallbackHandler {
  final private var authMethod = AuthMethods.getValidAuthMethod(authMethodStr)

  @throws[IOException]
  @throws[UnsupportedCallbackException]
  def handle(callbacks: Array[Callback]): Unit = {
    var username: String = null
    var password: String = null
    var ac: AuthorizeCallback = null
    for (callback <- callbacks) {
      if (callback.isInstanceOf[NameCallback]) {
        val nc = callback.asInstanceOf[NameCallback]
        username = nc.getName
      } else if (callback.isInstanceOf[PasswordCallback]) {
        val pc = callback.asInstanceOf[PasswordCallback]
        password = new String(pc.getPassword)
      } else if (callback.isInstanceOf[AuthorizeCallback]) {
        ac = callback.asInstanceOf[AuthorizeCallback]
      } else {
        throw new UnsupportedCallbackException(callback)
      }
    }
    val provider = AuthenticationProviderFactory.getAuthenticationProvider(authMethod)
    provider.Authenticate(username, password)
    if (ac != null) {
      ac.setAuthorized(true)
    }
  }
}


class SQLPlainProcessorFactory(val service: ThriftCLIService)
  extends TProcessorFactory(null) {
  override def getProcessor(trans: TTransport): TProcessor = {
    new TSetIpAddressProcessor[Iface](service)
  }
}

class PlainCallbackHandler(val username: String, val password: String) extends CallbackHandler {
  @throws[IOException]
  @throws[UnsupportedCallbackException]
  def handle(callbacks: Array[Callback]): Unit = {
    for (callback <- callbacks) {
      if (callback.isInstanceOf[NameCallback]) {
        val nameCallback = callback.asInstanceOf[NameCallback]
        nameCallback.setName(username)
      }
      else if (callback.isInstanceOf[PasswordCallback]) {
        val passCallback = callback.asInstanceOf[PasswordCallback]
        passCallback.setPassword(password.toCharArray)
      }
      else throw new UnsupportedCallbackException(callback)
    }
  }
}