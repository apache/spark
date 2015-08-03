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

import java.net.{Authenticator, PasswordAuthentication}
import java.security.KeyStore
import java.security.cert.X509Certificate
import javax.net.ssl._

import com.google.common.io.Files
import org.apache.hadoop.io.Text

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.network.sasl.SecretKeyHolder
import org.apache.spark.util.Utils

/**
 * Spark class responsible for security.
 *
 * In general this class should be instantiated by the SparkEnv and most components
 * should access it from that. There are some cases where the SparkEnv hasn't been
 * initialized yet and this class must be instantiated directly.
 *
 * Spark currently supports authentication via a shared secret.
 * Authentication can be configured to be on via the 'spark.authenticate' configuration
 * parameter. This parameter controls whether the Spark communication protocols do
 * authentication using the shared secret. This authentication is a basic handshake to
 * make sure both sides have the same shared secret and are allowed to communicate.
 * If the shared secret is not identical they will not be allowed to communicate.
 *
 * The Spark UI can also be secured by using javax servlet filters. A user may want to
 * secure the UI if it has data that other users should not be allowed to see. The javax
 * servlet filter specified by the user can authenticate the user and then once the user
 * is logged in, Spark can compare that user versus the view acls to make sure they are
 * authorized to view the UI. The configs 'spark.acls.enable' and 'spark.ui.view.acls'
 * control the behavior of the acls. Note that the person who started the application
 * always has view access to the UI.
 *
 * Spark has a set of modify acls (`spark.modify.acls`) that controls which users have permission
 * to  modify a single application. This would include things like killing the application. By
 * default the person who started the application has modify access. For modify access through
 * the UI, you must have a filter that does authentication in place for the modify acls to work
 * properly.
 *
 * Spark also has a set of admin acls (`spark.admin.acls`) which is a set of users/administrators
 * who always have permission to view or modify the Spark application.
 *
 * Starting from version 1.3, Spark has partial support for encrypted connections with SSL.
 *
 * At this point spark has multiple communication protocols that need to be secured and
 * different underlying mechanisms are used depending on the protocol:
 *
 *  - Akka -> The only option here is to use the Akka Remote secure-cookie functionality.
 *            Akka remoting allows you to specify a secure cookie that will be exchanged
 *            and ensured to be identical in the connection handshake between the client
 *            and the server. If they are not identical then the client will be refused
 *            to connect to the server. There is no control of the underlying
 *            authentication mechanism so its not clear if the password is passed in
 *            plaintext or uses DIGEST-MD5 or some other mechanism.
 *
 *            Akka also has an option to turn on SSL, this option is currently supported (see
 *            the details below).
 *
 *  - HTTP for broadcast and file server (via HttpServer) ->  Spark currently uses Jetty
 *            for the HttpServer. Jetty supports multiple authentication mechanisms -
 *            Basic, Digest, Form, Spengo, etc. It also supports multiple different login
 *            services - Hash, JAAS, Spnego, JDBC, etc.  Spark currently uses the HashLoginService
 *            to authenticate using DIGEST-MD5 via a single user and the shared secret.
 *            Since we are using DIGEST-MD5, the shared secret is not passed on the wire
 *            in plaintext.
 *
 *            We currently support SSL (https) for this communication protocol (see the details
 *            below).
 *
 *            The Spark HttpServer installs the HashLoginServer and configures it to DIGEST-MD5.
 *            Any clients must specify the user and password. There is a default
 *            Authenticator installed in the SecurityManager to how it does the authentication
 *            and in this case gets the user name and password from the request.
 *
 *  - BlockTransferService -> The Spark BlockTransferServices uses java nio to asynchronously
 *            exchange messages.  For this we use the Java SASL
 *            (Simple Authentication and Security Layer) API and again use DIGEST-MD5
 *            as the authentication mechanism. This means the shared secret is not passed
 *            over the wire in plaintext.
 *            Note that SASL is pluggable as to what mechanism it uses.  We currently use
 *            DIGEST-MD5 but this could be changed to use Kerberos or other in the future.
 *            Spark currently supports "auth" for the quality of protection, which means
 *            the connection does not support integrity or privacy protection (encryption)
 *            after authentication. SASL also supports "auth-int" and "auth-conf" which
 *            SPARK could support in the future to allow the user to specify the quality
 *            of protection they want. If we support those, the messages will also have to
 *            be wrapped and unwrapped via the SaslServer/SaslClient.wrap/unwrap API's.
 *
 *            Since the NioBlockTransferService does asynchronous messages passing, the SASL
 *            authentication is a bit more complex. A ConnectionManager can be both a client
 *            and a Server, so for a particular connection it has to determine what to do.
 *            A ConnectionId was added to be able to track connections and is used to
 *            match up incoming messages with connections waiting for authentication.
 *            The ConnectionManager tracks all the sendingConnections using the ConnectionId,
 *            waits for the response from the server, and does the handshake before sending
 *            the real message.
 *
 *            The NettyBlockTransferService ensures that SASL authentication is performed
 *            synchronously prior to any other communication on a connection. This is done in
 *            SaslClientBootstrap on the client side and SaslRpcHandler on the server side.
 *
 *  - HTTP for the Spark UI -> the UI was changed to use servlets so that javax servlet filters
 *            can be used. Yarn requires a specific AmIpFilter be installed for security to work
 *            properly. For non-Yarn deployments, users can write a filter to go through their
 *            organization's normal login service. If an authentication filter is in place then the
 *            SparkUI can be configured to check the logged in user against the list of users who
 *            have view acls to see if that user is authorized.
 *            The filters can also be used for many different purposes. For instance filters
 *            could be used for logging, encryption, or compression.
 *
 *  The exact mechanisms used to generate/distribute the shared secret are deployment-specific.
 *
 *  For Yarn deployments, the secret is automatically generated using the Akka remote
 *  Crypt.generateSecureCookie() API. The secret is placed in the Hadoop UGI which gets passed
 *  around via the Hadoop RPC mechanism. Hadoop RPC can be configured to support different levels
 *  of protection. See the Hadoop documentation for more details. Each Spark application on Yarn
 *  gets a different shared secret. On Yarn, the Spark UI gets configured to use the Hadoop Yarn
 *  AmIpFilter which requires the user to go through the ResourceManager Proxy. That Proxy is there
 *  to reduce the possibility of web based attacks through YARN. Hadoop can be configured to use
 *  filters to do authentication. That authentication then happens via the ResourceManager Proxy
 *  and Spark will use that to do authorization against the view acls.
 *
 *  For other Spark deployments, the shared secret must be specified via the
 *  spark.authenticate.secret config.
 *  All the nodes (Master and Workers) and the applications need to have the same shared secret.
 *  This again is not ideal as one user could potentially affect another users application.
 *  This should be enhanced in the future to provide better protection.
 *  If the UI needs to be secure, the user needs to install a javax servlet filter to do the
 *  authentication. Spark will then use that user to compare against the view acls to do
 *  authorization. If not filter is in place the user is generally null and no authorization
 *  can take place.
 *
 *  When authentication is being used, encryption can also be enabled by setting the option
 *  spark.authenticate.enableSaslEncryption to true. This is only supported by communication
 *  channels that use the network-common library, and can be used as an alternative to SSL in those
 *  cases.
 *
 *  SSL can be used for encryption for certain communication channels. The user can configure the
 *  default SSL settings which will be used for all the supported communication protocols unless
 *  they are overwritten by protocol specific settings. This way the user can easily provide the
 *  common settings for all the protocols without disabling the ability to configure each one
 *  individually.
 *
 *  All the SSL settings like `spark.ssl.xxx` where `xxx` is a particular configuration property,
 *  denote the global configuration for all the supported protocols. In order to override the global
 *  configuration for the particular protocol, the properties must be overwritten in the
 *  protocol-specific namespace. Use `spark.ssl.yyy.xxx` settings to overwrite the global
 *  configuration for particular protocol denoted by `yyy`. Currently `yyy` can be either `akka` for
 *  Akka based connections or `fs` for broadcast and file server.
 *
 *  Refer to [[org.apache.spark.SSLOptions]] documentation for the list of
 *  options that can be specified.
 *
 *  SecurityManager initializes SSLOptions objects for different protocols separately. SSLOptions
 *  object parses Spark configuration at a given namespace and builds the common representation
 *  of SSL settings. SSLOptions is then used to provide protocol-specific configuration like
 *  TypeSafe configuration for Akka or SSLContextFactory for Jetty.
 *
 *  SSL must be configured on each node and configured for each component involved in
 *  communication using the particular protocol. In YARN clusters, the key-store can be prepared on
 *  the client side then distributed and used by the executors as the part of the application
 *  (YARN allows the user to deploy files before the application is started).
 *  In standalone deployment, the user needs to provide key-stores and configuration
 *  options for master and workers. In this mode, the user may allow the executors to use the SSL
 *  settings inherited from the worker which spawned that executor. It can be accomplished by
 *  setting `spark.ssl.useNodeLocalConf` to `true`.
 */

private[spark] class SecurityManager(sparkConf: SparkConf)
  extends Logging with SecretKeyHolder {

  // key used to store the spark secret in the Hadoop UGI
  private val sparkSecretLookupKey = "sparkCookie"

  private val authOn = sparkConf.getBoolean(SecurityManager.SPARK_AUTH_CONF, false)
  // keep spark.ui.acls.enable for backwards compatibility with 1.0
  private var aclsOn =
    sparkConf.getBoolean("spark.acls.enable", sparkConf.getBoolean("spark.ui.acls.enable", false))

  // admin acls should be set before view or modify acls
  private var adminAcls: Set[String] =
    stringToSet(sparkConf.get("spark.admin.acls", ""))

  private var viewAcls: Set[String] = _

  // list of users who have permission to modify the application. This should
  // apply to both UI and CLI for things like killing the application.
  private var modifyAcls: Set[String] = _

  // always add the current user and SPARK_USER to the viewAcls
  private val defaultAclUsers = Set[String](System.getProperty("user.name", ""),
    Utils.getCurrentUserName())

  setViewAcls(defaultAclUsers, sparkConf.get("spark.ui.view.acls", ""))
  setModifyAcls(defaultAclUsers, sparkConf.get("spark.modify.acls", ""))

  private val secretKey = generateSecretKey()
  logInfo("SecurityManager: authentication " + (if (authOn) "enabled" else "disabled") +
    "; ui acls " + (if (aclsOn) "enabled" else "disabled") +
    "; users with view permissions: " + viewAcls.toString() +
    "; users with modify permissions: " + modifyAcls.toString())

  // Set our own authenticator to properly negotiate user/password for HTTP connections.
  // This is needed by the HTTP client fetching from the HttpServer. Put here so its
  // only set once.
  if (authOn) {
    Authenticator.setDefault(
      new Authenticator() {
        override def getPasswordAuthentication(): PasswordAuthentication = {
          var passAuth: PasswordAuthentication = null
          val userInfo = getRequestingURL().getUserInfo()
          if (userInfo != null) {
            val  parts = userInfo.split(":", 2)
            passAuth = new PasswordAuthentication(parts(0), parts(1).toCharArray())
          }
          return passAuth
        }
      }
    )
  }

  // the default SSL configuration - it will be used by all communication layers unless overwritten
  private val defaultSSLOptions = SSLOptions.parse(sparkConf, "spark.ssl", defaults = None)

  // SSL configuration for different communication layers - they can override the default
  // configuration at a specified namespace. The namespace *must* start with spark.ssl.
  val fileServerSSLOptions = SSLOptions.parse(sparkConf, "spark.ssl.fs", Some(defaultSSLOptions))
  val akkaSSLOptions = SSLOptions.parse(sparkConf, "spark.ssl.akka", Some(defaultSSLOptions))

  logDebug(s"SSLConfiguration for file server: $fileServerSSLOptions")
  logDebug(s"SSLConfiguration for Akka: $akkaSSLOptions")

  val (sslSocketFactory, hostnameVerifier) = if (fileServerSSLOptions.enabled) {
    val trustStoreManagers =
      for (trustStore <- fileServerSSLOptions.trustStore) yield {
        val input = Files.asByteSource(fileServerSSLOptions.trustStore.get).openStream()

        try {
          val ks = KeyStore.getInstance(KeyStore.getDefaultType)
          ks.load(input, fileServerSSLOptions.trustStorePassword.get.toCharArray)

          val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
          tmf.init(ks)
          tmf.getTrustManagers
        } finally {
          input.close()
        }
      }

    lazy val credulousTrustStoreManagers = Array({
      logWarning("Using 'accept-all' trust manager for SSL connections.")
      new X509TrustManager {
        override def getAcceptedIssuers: Array[X509Certificate] = null

        override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) {}

        override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) {}
      }: TrustManager
    })

    val sslContext = SSLContext.getInstance(fileServerSSLOptions.protocol.getOrElse("Default"))
    sslContext.init(null, trustStoreManagers.getOrElse(credulousTrustStoreManagers), null)

    val hostVerifier = new HostnameVerifier {
      override def verify(s: String, sslSession: SSLSession): Boolean = true
    }

    (Some(sslContext.getSocketFactory), Some(hostVerifier))
  } else {
    (None, None)
  }

  /**
   * Split a comma separated String, filter out any empty items, and return a Set of strings
   */
  private def stringToSet(list: String): Set[String] = {
    list.split(',').map(_.trim).filter(!_.isEmpty).toSet
  }

  /**
   * Admin acls should be set before the view or modify acls.  If you modify the admin
   * acls you should also set the view and modify acls again to pick up the changes.
   */
  def setViewAcls(defaultUsers: Set[String], allowedUsers: String) {
    viewAcls = (adminAcls ++ defaultUsers ++ stringToSet(allowedUsers))
    logInfo("Changing view acls to: " + viewAcls.mkString(","))
  }

  def setViewAcls(defaultUser: String, allowedUsers: String) {
    setViewAcls(Set[String](defaultUser), allowedUsers)
  }

  def getViewAcls: String = viewAcls.mkString(",")

  /**
   * Admin acls should be set before the view or modify acls.  If you modify the admin
   * acls you should also set the view and modify acls again to pick up the changes.
   */
  def setModifyAcls(defaultUsers: Set[String], allowedUsers: String) {
    modifyAcls = (adminAcls ++ defaultUsers ++ stringToSet(allowedUsers))
    logInfo("Changing modify acls to: " + modifyAcls.mkString(","))
  }

  def getModifyAcls: String = modifyAcls.mkString(",")

  /**
   * Admin acls should be set before the view or modify acls.  If you modify the admin
   * acls you should also set the view and modify acls again to pick up the changes.
   */
  def setAdminAcls(adminUsers: String) {
    adminAcls = stringToSet(adminUsers)
    logInfo("Changing admin acls to: " + adminAcls.mkString(","))
  }

  def setAcls(aclSetting: Boolean) {
    aclsOn = aclSetting
    logInfo("Changing acls enabled to: " + aclsOn)
  }

  /**
   * Generates or looks up the secret key.
   *
   * The way the key is stored depends on the Spark deployment mode. Yarn
   * uses the Hadoop UGI.
   *
   * For non-Yarn deployments, If the config variable is not set
   * we throw an exception.
   */
  private def generateSecretKey(): String = {
    if (!isAuthenticationEnabled) return null
    // first check to see if the secret is already set, else generate a new one if on yarn
    val sCookie = if (SparkHadoopUtil.get.isYarnMode) {
      val secretKey = SparkHadoopUtil.get.getSecretKeyFromUserCredentials(sparkSecretLookupKey)
      if (secretKey != null) {
        logDebug("in yarn mode, getting secret from credentials")
        return new Text(secretKey).toString
      } else {
        logDebug("getSecretKey: yarn mode, secret key from credentials is null")
      }
      val cookie = akka.util.Crypt.generateSecureCookie
      // if we generated the secret then we must be the first so lets set it so t
      // gets used by everyone else
      SparkHadoopUtil.get.addSecretKeyToUserCredentials(sparkSecretLookupKey, cookie)
      logInfo("adding secret to credentials in yarn mode")
      cookie
    } else {
      // user must have set spark.authenticate.secret config
      // For Master/Worker, auth secret is in conf; for Executors, it is in env variable
      sys.env.get(SecurityManager.ENV_AUTH_SECRET)
        .orElse(sparkConf.getOption(SecurityManager.SPARK_AUTH_SECRET_CONF)) match {
        case Some(value) => value
        case None => throw new Exception("Error: a secret key must be specified via the " +
          SecurityManager.SPARK_AUTH_SECRET_CONF + " config")
      }
    }
    sCookie
  }

  /**
   * Check to see if Acls for the UI are enabled
   * @return true if UI authentication is enabled, otherwise false
   */
  def aclsEnabled(): Boolean = aclsOn

  /**
   * Checks the given user against the view acl list to see if they have
   * authorization to view the UI. If the UI acls are disabled
   * via spark.acls.enable, all users have view access. If the user is null
   * it is assumed authentication is off and all users have access.
   *
   * @param user to see if is authorized
   * @return true is the user has permission, otherwise false
   */
  def checkUIViewPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " viewAcls=" +
      viewAcls.mkString(","))
    !aclsEnabled || user == null || viewAcls.contains(user)
  }

  /**
   * Checks the given user against the modify acl list to see if they have
   * authorization to modify the application. If the UI acls are disabled
   * via spark.acls.enable, all users have modify access. If the user is null
   * it is assumed authentication isn't turned on and all users have access.
   *
   * @param user to see if is authorized
   * @return true is the user has permission, otherwise false
   */
  def checkModifyPermissions(user: String): Boolean = {
    logDebug("user=" + user + " aclsEnabled=" + aclsEnabled() + " modifyAcls=" +
      modifyAcls.mkString(","))
    !aclsEnabled || user == null || modifyAcls.contains(user)
  }


  /**
   * Check to see if authentication for the Spark communication protocols is enabled
   * @return true if authentication is enabled, otherwise false
   */
  def isAuthenticationEnabled(): Boolean = authOn

  /**
   * Checks whether SASL encryption should be enabled.
   * @return Whether to enable SASL encryption when connecting to services that support it.
   */
  def isSaslEncryptionEnabled(): Boolean = {
    sparkConf.getBoolean("spark.authenticate.enableSaslEncryption", false)
  }

  /**
   * Gets the user used for authenticating HTTP connections.
   * For now use a single hardcoded user.
   * @return the HTTP user as a String
   */
  def getHttpUser(): String = "sparkHttpUser"

  /**
   * Gets the user used for authenticating SASL connections.
   * For now use a single hardcoded user.
   * @return the SASL user as a String
   */
  def getSaslUser(): String = "sparkSaslUser"

  /**
   * Gets the secret key.
   * @return the secret key as a String if authentication is enabled, otherwise returns null
   */
  def getSecretKey(): String = secretKey

  // Default SecurityManager only has a single secret key, so ignore appId.
  override def getSaslUser(appId: String): String = getSaslUser()
  override def getSecretKey(appId: String): String = getSecretKey()
}

private[spark] object SecurityManager {

  val SPARK_AUTH_CONF: String = "spark.authenticate"
  val SPARK_AUTH_SECRET_CONF: String = "spark.authenticate.secret"
  // This is used to set auth secret to an executor's env variable. It should have the same
  // value as SPARK_AUTH_SECERET_CONF set in SparkConf
  val ENV_AUTH_SECRET = "_SPARK_AUTH_SECRET"
}
