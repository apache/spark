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
import java.lang.reflect.{Field, Method}
import java.net.{InetSocketAddress, UnknownHostException}
import java.util
import java.util.{Locale, Objects}
import javax.net.ssl.SSLServerSocket
import javax.security.auth.login.LoginException
import javax.security.sasl.Sasl

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.HiveMetaStore
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.hive.thrift.{DBTokenStore, HadoopThriftAuthBridge}
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.authorize.ProxyUsers
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.transport._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils
import org.apache.spark.sql.hive.thriftserver.cli.thrift.ThriftCLIService
import org.apache.spark.sql.hive.thriftserver.server.cli.SparkThriftServerSQLException


class HiveAuthFactory extends Logging {

  import HiveAuthFactory._

  private var saslServer: HadoopThriftAuthBridge.Server = null
  private var delegationTokenManager: HiveDelegationTokenManager = null
  private var authTypeStr: String = null
  private var transportMode: String = null
  private var conf: HiveConf = null


  def this(hiveConf: HiveConf) {
    this()
    this.conf = hiveConf
    transportMode = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
    authTypeStr = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION)

    // In http mode we use NOSASL as the default auth type
    if ("http".equalsIgnoreCase(transportMode)) {
      if (authTypeStr == null) {
        authTypeStr = NOSASL.getAuthName
      } else {
        if (authTypeStr == null) {
          authTypeStr = NONE.getAuthName
        }
        if (authTypeStr.equalsIgnoreCase(KERBEROS.getAuthName)) {
          val principal = conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL)
          val keytab = conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB)
          if (needUgiLogin(UserGroupInformation.getCurrentUser,
            SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keytab)) {
            saslServer = ShimLoader.getHadoopThriftAuthBridge.createServer(principal, keytab)
          } else { // Using the default constructor to avoid unnecessary UGI login.
            saslServer = new HadoopThriftAuthBridge.Server
          }

          delegationTokenManager = new HiveDelegationTokenManager()
          // start delegation token manager
          try { // rawStore is only necessary for DBTokenStore
            var rawStore: Any = null
            val tokenStoreClass =
              conf.getVar(HiveConf.ConfVars.METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS)
            if (tokenStoreClass == classOf[DBTokenStore].getName) {
              val baseHandler: HMSHandler =
                new HiveMetaStore.HMSHandler("new db based metaserver", conf, true)
              rawStore = baseHandler.getMS
            }
            delegationTokenManager
              .startDelegationTokenSecretManager(conf, rawStore, ServerMode.HIVESERVER2)
            ReflectionUtils.setSuperField(saslServer, "secretManager", delegationTokenManager)
          } catch {
            case e@(_: MetaException | _: IOException) =>
              throw new TTransportException("Failed to start token manager", e)
          }
        }
      }
    }
  }

  def getSaslProperties: util.HashMap[String, String] = {
    val saslProps = new util.HashMap[String, String];
    val saslQOP = SaslQOP.fromString(conf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP))
    saslProps.put(Sasl.QOP, saslQOP.toString)
    saslProps.put(Sasl.SERVER_AUTH, "true")
    saslProps
  }

  @throws[LoginException]
  def getAuthTransFactory: TTransportFactory = {
    var transportFactory: TTransportFactory = null
    if (authTypeStr.equalsIgnoreCase(KERBEROS.getAuthName)) {
      try {
        transportFactory = saslServer.createTransportFactory(getSaslProperties)
      } catch {
        case e: TTransportException =>
          throw new LoginException(e.getMessage)
      }
    } else if (authTypeStr.equalsIgnoreCase(NONE.getAuthName)) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr)
    } else if (authTypeStr.equalsIgnoreCase(LDAP.getAuthName)) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr)
    } else if (authTypeStr.equalsIgnoreCase(PAM.getAuthName)) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr)
    } else if (authTypeStr.equalsIgnoreCase(NOSASL.getAuthName)) {
      transportFactory = new TTransportFactory()
    } else if (authTypeStr.equalsIgnoreCase(CUSTOM.getAuthName)) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr)
    } else {
      throw new LoginException("Unsupported authentication type " + authTypeStr)
    }
    transportFactory
  }

  /**
   * Returns the thrift processor factory for HiveServer2 running in binary mode
   *
   * @param service
   * @return
   * @throws LoginException
   */
  @throws[LoginException]
  def getAuthProcFactory(service: ThriftCLIService): TProcessorFactory = {
    if (authTypeStr.equalsIgnoreCase(KERBEROS.getAuthName)) {
      KerberosSaslHelper.getKerberosProcessorFactory(saslServer, service)
    } else {
      PlainSaslHelper.getPlainProcessorFactory(service)
    }
  }

  def getRemoteUser: String = {
    if (saslServer == null) {
      null
    } else {
      saslServer.getRemoteUser
    }
  }

  def getIpAddress: String = {
    if (saslServer == null || saslServer.getRemoteAddress == null) {
      null
    } else {
      saslServer.getRemoteAddress.getHostAddress
    }
  }


  def getSocketTransport(host: String, port: Int, loginTimeout: Int): TSocket = {
    new TSocket(host, port, loginTimeout)
  }

  // retrieve delegation token for the given user
  @throws[SparkThriftServerSQLException]
  def getDelegationToken(owner: String, renewer: String, remoteAddr: String): String = {
    if (delegationTokenManager == null) {
      throw new SparkThriftServerSQLException("Delegation token only supported " +
        "over kerberos authentication", "08S01")
    }
    try {
      val tokenStr =
        delegationTokenManager.getDelegationTokenWithService(
          owner,
          renewer,
          HS2_CLIENT_TOKEN,
          remoteAddr)
      if (tokenStr == null || tokenStr.isEmpty) {
        throw new SparkThriftServerSQLException("Received empty retrieving " +
          "delegation token for user " + owner, "08S01")
      }
      tokenStr
    } catch {
      case e: IOException =>
        throw new SparkThriftServerSQLException("Error retrieving " +
          "delegation token for user " + owner, "08S01", e)
      case e: InterruptedException =>
        throw new SparkThriftServerSQLException("delegation token retrieval interrupted",
          "08S01", e)
    }
  }

  // cancel given delegation token
  @throws[SparkThriftServerSQLException]
  def cancelDelegationToken(delegationToken: String): Unit = {
    if (delegationTokenManager == null) {
      throw new SparkThriftServerSQLException("Delegation token only supported " +
        "over kerberos authentication", "08S01")
    }
    try {
      delegationTokenManager.cancelDelegationToken(delegationToken)
    } catch {
      case e: IOException =>
        throw new SparkThriftServerSQLException("Error canceling delegation token " +
          delegationToken, "08S01", e)
    }
  }

  @throws[SparkThriftServerSQLException]
  def renewDelegationToken(delegationToken: String): Unit = {
    if (delegationTokenManager == null) {
      throw new SparkThriftServerSQLException("Delegation token only supported " +
        "over kerberos authentication", "08S01")
    }
    try {
      delegationTokenManager.renewDelegationToken(delegationToken)
    } catch {
      case e: IOException =>
        throw new SparkThriftServerSQLException("Error renewing delegation token " +
          delegationToken, "08S01", e)
    }
  }

  @throws[SparkThriftServerSQLException]
  def getUserFromToken(delegationToken: String): String = {
    if (delegationTokenManager == null) {
      throw new SparkThriftServerSQLException("Delegation token only supported over" +
        " kerberos authentication", "08S01")
    }
    try {
      delegationTokenManager.getUserFromToken(delegationToken)
    } catch {
      case e: IOException =>
        throw new SparkThriftServerSQLException("Error extracting user from delegation token " +
          delegationToken, "08S01", e)
    }
  }


}

object HiveAuthFactory extends Logging {
  val HS2_PROXY_USER = "hive.server2.proxy.user"
  val HS2_CLIENT_TOKEN = "hiveserver2ClientToken"

  private var keytabFile: Field = null
  private var getKeytab: Method = null

  try {
    val clz = classOf[UserGroupInformation]
    try {
      keytabFile = clz.getDeclaredField("keytabFile")
      keytabFile.setAccessible(true);
    } catch {
      case nfe: NoSuchFieldError =>
        logDebug("Cannot find private field \"keytabFile\" in class: " +
          classOf[UserGroupInformation].getCanonicalName, nfe)
        keytabFile = null
    }

    try {
      getKeytab = clz.getDeclaredMethod("getKeytab")
      getKeytab.setAccessible(true);
    } catch {
      case nme: NoSuchMethodError =>
        logDebug("Cannot find private method \"getKeytab\" in class:" +
          classOf[UserGroupInformation].getCanonicalName, nme
        )
        getKeytab = null
    }
  } catch {
    case e: Throwable =>
  }

  @throws[SparkThriftServerSQLException]
  def verifyProxyAccess(realUser: String,
                        proxyUser: String,
                        ipAddress: String,
                        hiveConf: HiveConf): Unit = {
    try {
      var sessionUgi: UserGroupInformation = null
      if (UserGroupInformation.isSecurityEnabled) {
        val kerbName = ShimLoader.getHadoopShims.getKerberosNameShim(realUser)
        sessionUgi =
          UserGroupInformation.createProxyUser(kerbName.getServiceName,
            UserGroupInformation.getLoginUser)
      } else {
        sessionUgi = UserGroupInformation.createRemoteUser(realUser)
      }
      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ProxyUsers.refreshSuperUserGroupsConfiguration(hiveConf)
        ProxyUsers.authorize(UserGroupInformation.createProxyUser(proxyUser, sessionUgi),
          ipAddress, hiveConf)
      }
    } catch {
      case e: IOException =>
        throw new SparkThriftServerSQLException("Failed to validate proxy privilege of " +
          realUser + " for " + proxyUser, "08S01", e)
    }
  }

  @throws[TTransportException]
  def getSSLSocket(host: String, port: Int, loginTimeout: Int): TTransport = {
    TSSLTransportFactory.getClientSocket(host, port, loginTimeout)
  }

  @throws[TTransportException]
  def getSSLSocket(host: String,
                   port: Int,
                   loginTimeout: Int,
                   trustStorePath: String,
                   trustStorePassWord: String): TTransport = {
    val params = new TSSLTransportFactory.TSSLTransportParameters()
    params.setTrustStore(trustStorePath, trustStorePassWord)
    params.requireClientAuth(true)
    TSSLTransportFactory.getClientSocket(host, port, loginTimeout, params)
  }

  @throws[TTransportException]
  def getServerSocket(hiveHost: String, portNum: Int): TServerSocket = {
    var serverAddress: InetSocketAddress = null
    if (hiveHost == null || hiveHost.isEmpty) { // Wildcard bind
      serverAddress = new InetSocketAddress(portNum)
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum)
    }
    new TServerSocket(serverAddress)
  }

  @throws[TTransportException]
  @throws[UnknownHostException]
  def getServerSSLSocket(hiveHost: String,
                         portNum: Int,
                         keyStorePath: String,
                         keyStorePassWord: String,
                         sslVersionBlacklist: util.List[String]): TServerSocket = {
    val params: TSSLTransportFactory.TSSLTransportParameters =
      new TSSLTransportFactory.TSSLTransportParameters()
    params.setKeyStore(keyStorePath, keyStorePassWord)
    var serverAddress: InetSocketAddress = null
    if (hiveHost == null || hiveHost.isEmpty) {
      serverAddress = new InetSocketAddress(portNum)
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum)
    }
    val thriftServerSocket =
      TSSLTransportFactory.getServerSocket(portNum, 0, serverAddress.getAddress, params)
    if (thriftServerSocket.getServerSocket.isInstanceOf[SSLServerSocket]) {
      val sslVersionBlacklistLocal = new util.ArrayList[String]
      for (sslVersion <- sslVersionBlacklist.asScala) {
        sslVersionBlacklistLocal.add(sslVersion.trim.toLowerCase(Locale.ROOT))
      }
      val sslServerSocket = thriftServerSocket.getServerSocket.asInstanceOf[SSLServerSocket]
      val enabledProtocols = new util.ArrayList[String]
      for (protocol <- sslServerSocket.getEnabledProtocols) {
        if (sslVersionBlacklistLocal.contains(protocol.toLowerCase(Locale.ROOT))) {
          logDebug("Disabling SSL Protocol: " + protocol)
        } else {
          enabledProtocols.add(protocol)
        }
      }
      sslServerSocket.setEnabledProtocols(enabledProtocols.toArray(new Array[String](0)))
      logInfo("SSL Server Socket Enabled Protocols: " +
        sslServerSocket.getEnabledProtocols.mkString(","))
    }
    thriftServerSocket
  }

  // Perform kerberos login using the hadoop shim API if the configuration is available
  @throws[IOException]
  def loginFromKeytab(hiveConf: HiveConf): Unit = {
    val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL)
    val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB)
    if (principal.isEmpty || keyTabFile.isEmpty) {
      throw new IOException("HiveServer2 Kerberos principal or keytab is not correctly configured")
    } else {
      UserGroupInformation
        .loginUserFromKeytab(SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile)
    }
  }

  // Perform SPNEGO login using the hadoop shim API if the configuration is available
  @throws[IOException]
  def loginFromSpnegoKeytabAndReturnUGI(hiveConf: HiveConf): UserGroupInformation = {
    val principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL)
    val keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB)
    if (principal.isEmpty || keyTabFile.isEmpty) {
      throw new IOException("HiveServer2 SPNEGO principal or keytab is not correctly configured")
    } else {
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile)
    }
  }

  def needUgiLogin(ugi: UserGroupInformation, principal: String, keytab: String): Boolean = {
    null == ugi ||
      !ugi.hasKerberosCredentials ||
      !(ugi.getUserName == principal) ||
      !Objects.equals(keytab, getKeytabFromUgi)
  }

  private def getKeytabFromUgi: String = classOf[UserGroupInformation].synchronized {
    try {
      if (keytabFile != null) {
        keytabFile.get(null).asInstanceOf[String]
      } else if (getKeytab != null) {
        getKeytab.invoke(UserGroupInformation.getCurrentUser).asInstanceOf[String]
      } else {
        null
      }
    } catch {
      case e: Exception =>
        logDebug("Fail to get keytabFile path via reflection", e)
        null
    }

  }
}
