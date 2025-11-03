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

import java.io.File
import java.security.NoSuchAlgorithmException
import java.util.HashMap
import java.util.Map
import javax.net.ssl.SSLContext

import org.apache.hadoop.conf.Configuration
import org.eclipse.jetty.util.ssl.SslContextFactory

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ConfigProvider
import org.apache.spark.network.util.MapConfigProvider

/**
 * SSLOptions class is a common container for SSL configuration options. It offers methods to
 * generate specific objects to configure SSL for different communication protocols.
 *
 * SSLOptions is intended to provide the maximum common set of SSL settings, which are supported
 * by the protocol, which it can generate the configuration for.
 *
 * @param namespace           the configuration namespace
 * @param enabled             enables or disables SSL; if it is set to false, the rest of the
 *                            settings are disregarded
 * @param port                the port where to bind the SSL server; if not defined, it will be
 *                            based on the non-SSL port for the same service.
 * @param keyStore            a path to the key-store file
 * @param keyStorePassword    a password to access the key-store file
 * @param privateKey          a PKCS#8 private key file in PEM format
 * @param privateKeyPassword  a password to access the privateKey file
 * @param keyPassword         a password to access the private key in the key-store
 * @param keyStoreType        the type of the key-store
 * @param needClientAuth      set true if SSL needs client authentication
 * @param certChain           an X.509 certificate chain file in PEM format
 * @param trustStore          a path to the trust-store file
 * @param trustStorePassword  a password to access the trust-store file
 * @param trustStoreType      the type of the trust-store
 * @param trustStoreReloadingEnabled enables or disables using a trust-store that reloads
 *                                   its configuration when the trust-store file on disk changes
 * @param trustStoreReloadIntervalMs the interval, in milliseconds,
 *                                 when the trust-store will reload its configuration
 * @param openSslEnabled      enables or disables using an OpenSSL implementation (if available),
 *                            requires certChain and keyFile arguments
 * @param protocol            SSL protocol (remember that SSLv3 was compromised) supported by Java
 * @param enabledAlgorithms   a set of encryption algorithms that may be used
 */
private[spark] case class SSLOptions(
    namespace: Option[String] = None,
    enabled: Boolean = false,
    port: Option[Int] = None,
    keyStore: Option[File] = None,
    keyStorePassword: Option[String] = None,
    privateKey: Option[File] = None,
    keyPassword: Option[String] = None,
    keyStoreType: Option[String] = None,
    needClientAuth: Boolean = false,
    certChain: Option[File] = None,
    trustStore: Option[File] = None,
    trustStorePassword: Option[String] = None,
    trustStoreType: Option[String] = None,
    trustStoreReloadingEnabled: Boolean = false,
    trustStoreReloadIntervalMs: Int = 10000,
    openSslEnabled: Boolean = false,
    protocol: Option[String] = None,
    enabledAlgorithms: Set[String] = Set.empty,
    privateKeyPassword: Option[String] = None)
    extends Logging {

  /**
   * Creates a Jetty SSL context factory according to the SSL settings represented by this object.
   */
  def createJettySslContextFactoryServer(): Option[SslContextFactory.Server] = {
    if (enabled) {
      val sslContextFactory = new SslContextFactory.Server()

      keyStore.foreach(file => sslContextFactory.setKeyStorePath(file.getAbsolutePath))
      keyStorePassword.foreach(sslContextFactory.setKeyStorePassword)
      keyPassword.foreach(sslContextFactory.setKeyManagerPassword)
      keyStoreType.foreach(sslContextFactory.setKeyStoreType)
      if (needClientAuth) {
        trustStore.foreach(file => sslContextFactory.setTrustStorePath(file.getAbsolutePath))
        trustStorePassword.foreach(sslContextFactory.setTrustStorePassword)
        trustStoreType.foreach(sslContextFactory.setTrustStoreType)
        /*
         * Need to pass needClientAuth flag to jetty for Jetty server to authenticate
         * client certificates. This would help enable mTLS authentication.
         */
        sslContextFactory.setNeedClientAuth(needClientAuth)

      }
      protocol.foreach(sslContextFactory.setProtocol)
      if (supportedAlgorithms.nonEmpty) {
        sslContextFactory.setIncludeCipherSuites(supportedAlgorithms.toSeq: _*)
      }

      Some(sslContextFactory)
    } else {
      None
    }
  }

  /*
   * The supportedAlgorithms set is a subset of the enabledAlgorithms that
   * are supported by the current Java security provider for this protocol.
   */
  private val supportedAlgorithms: Set[String] = if (enabledAlgorithms.isEmpty) {
    Set.empty
  } else {
    var context: SSLContext = null
    if (protocol.isEmpty) {
      logDebug("No SSL protocol specified")
      context = SSLContext.getDefault
    } else {
      try {
        context = SSLContext.getInstance(protocol.get)
        /* The set of supported algorithms does not depend upon the keys, trust, or
         rng, although they will influence which algorithms are eventually used. */
        context.init(null, null, null)
      } catch {
        case nsa: NoSuchAlgorithmException =>
          logDebug(s"No support for requested SSL protocol ${protocol.get}")
          context = SSLContext.getDefault
      }
    }

    val providerAlgorithms = context.getServerSocketFactory.getSupportedCipherSuites.toSet

    // Log which algorithms we are discarding
    (enabledAlgorithms &~ providerAlgorithms).foreach { cipher =>
      logDebug(s"Discarding unsupported cipher $cipher")
    }

    val supported = enabledAlgorithms & providerAlgorithms
    require(supported.nonEmpty || sys.env.contains("SPARK_TESTING"),
      "SSLContext does not support any of the enabled algorithms: " +
        enabledAlgorithms.mkString(","))
    supported
  }

  def createConfigProvider(conf: SparkConf): ConfigProvider = {
    val nsp = namespace.getOrElse("spark.ssl")
    val confMap: Map[String, String] = new HashMap[String, String]
    conf.getAll.foreach(tuple => confMap.put(tuple._1, tuple._2))
    confMap.put(s"$nsp.enabled", enabled.toString)
    confMap.put(s"$nsp.trustStoreReloadingEnabled", trustStoreReloadingEnabled.toString)
    confMap.put(s"$nsp.openSslEnabled", openSslEnabled.toString)
    confMap.put(s"$nsp.trustStoreReloadIntervalMs", trustStoreReloadIntervalMs.toString)
    keyStore.map(_.getAbsolutePath).foreach(confMap.put(s"$nsp.keyStore", _))
    keyStorePassword.foreach(confMap.put(s"$nsp.keyStorePassword", _))
    privateKey.map(_.getAbsolutePath).foreach(confMap.put(s"$nsp.privateKey", _))
    keyPassword.foreach(confMap.put(s"$nsp.keyPassword", _))
    certChain.map(_.getAbsolutePath).foreach(confMap.put(s"$nsp.certChain", _))
    trustStore.map(_.getAbsolutePath).foreach(confMap.put(s"$nsp.trustStore", _))
    trustStorePassword.foreach(confMap.put(s"$nsp.trustStorePassword", _))
    protocol.foreach(confMap.put(s"$nsp.protocol", _))
    confMap.put(s"$nsp.enabledAlgorithms", enabledAlgorithms.mkString(","))
    privateKeyPassword.foreach(confMap.put(s"$nsp.privateKeyPassword", _))

    new MapConfigProvider(confMap)
  }

  /** Returns a string representation of this SSLOptions with all the passwords masked. */
  override def toString: String = s"SSLOptions{enabled=$enabled, port=$port, " +
      s"keyStore=$keyStore, keyStorePassword=${keyStorePassword.map(_ => "xxx")}, " +
      s"privateKey=$privateKey, keyPassword=${keyPassword.map(_ => "xxx")}, " +
      s"privateKeyPassword=${privateKeyPassword.map(_ => "xxx")}, keyStoreType=$keyStoreType, " +
      s"needClientAuth=$needClientAuth, certChain=$certChain, trustStore=$trustStore, " +
      s"trustStorePassword=${trustStorePassword.map(_ => "xxx")}, " +
      s"trustStoreReloadIntervalMs=$trustStoreReloadIntervalMs, " +
      s"trustStoreReloadingEnabled=$trustStoreReloadingEnabled, openSSLEnabled=$openSslEnabled, " +
      s"protocol=$protocol, enabledAlgorithms=$enabledAlgorithms}"
}

private[spark] object SSLOptions extends Logging {

  /**
   * Resolves SSLOptions settings from a given Spark configuration object at a given namespace.
   *
   * The following settings are allowed:
   * $ - `[ns].enabled` - `true` or `false`, to enable or disable SSL respectively
   * $ - `[ns].port` - the port where to bind the SSL server
   * $ - `[ns].keyStore` - a path to the key-store file; can be relative to the current directory
   * $ - `[ns].keyStorePassword` - a password to the key-store file
   * $ - `[ns].privateKey` - a PKCS#8 private key file in PEM format
   * $ - `[ns].privateKeyPassword` - a password for the above key
   * $ - `[ns].keyPassword` - a password to the private key in the key store
   * $ - `[ns].keyStoreType` - the type of the key-store
   * $ - `[ns].needClientAuth` - whether SSL needs client authentication
   * $ - `[ns].certChain` - an X.509 certificate chain file in PEM format
   * $ - `[ns].trustStore` - a path to the trust-store file; can be relative to the current
   *                         directory
   * $ - `[ns].trustStorePassword` - a password to the trust-store file
   * $ - `[ns].trustStoreType` - the type of trust-store
   * $ - `[ns].trustStoreReloadingEnabled` - enables or disables using a trust-store
   * that reloads its configuration when the trust-store file on disk changes
   * $ - `[ns].trustStoreReloadIntervalMs` - the interval, in milliseconds, the
   * trust-store will reload its configuration
   * $ - `[ns].openSslEnabled` - enables or disables using an OpenSSL implementation
   * (if available on host system), requires certChain and keyFile arguments
   * $ - `[ns].protocol` - a protocol name supported by a particular Java version
   * $ - `[ns].enabledAlgorithms` - a comma separated list of ciphers
   *
   * For a list of protocols and ciphers supported by particular Java versions, you may go to
   * <a href="https://blogs.oracle.com/java-platform-group/entry/diagnosing_tls_ssl_and_https">
   * Oracle blog page</a>.
   *
   * You can optionally specify the default configuration. If you do, for each setting which is
   * missing in SparkConf, the corresponding setting is used from the default configuration.
   *
   * @param conf Spark configuration object where the settings are collected from
   * @param hadoopConf Hadoop configuration to get settings
   * @param ns the namespace name
   * @param defaults the default configuration
   * @return [[org.apache.spark.SSLOptions]] object
   */
  def parse(
      conf: SparkConf,
      hadoopConf: Configuration,
      ns: String,
      defaults: Option[SSLOptions] = None): SSLOptions = {

    // RPC does not inherit the default enabled setting due to backwards compatibility reasons
    val enabledDefault = if (ns == "spark.ssl.rpc") {
      false
    } else {
      defaults.exists(_.enabled)
    }

    val enabled = conf.getBoolean(s"$ns.enabled", defaultValue = enabledDefault)
    if (!enabled) {
      return new SSLOptions()
    }
    val port = conf.getWithSubstitution(s"$ns.port").map(_.toInt)
    port.foreach { p =>
      require(p >= 0, "Port number must be a non-negative value.")
    }

    val keyStore = conf.getWithSubstitution(s"$ns.keyStore").map(new File(_))
        .orElse(defaults.flatMap(_.keyStore))

    val keyStorePassword = conf.getWithSubstitution(s"$ns.keyStorePassword")
        .orElse(Option(hadoopConf.getPassword(s"$ns.keyStorePassword")).map(new String(_)))
        .orElse(Option(conf.getenv(ENV_RPC_SSL_KEY_STORE_PASSWORD)).filter(_.trim.nonEmpty))
        .orElse(defaults.flatMap(_.keyStorePassword))

    val privateKey = conf.getOption(s"$ns.privateKey").map(new File(_))
        .orElse(defaults.flatMap(_.privateKey))

    val privateKeyPassword = conf.getWithSubstitution(s"$ns.privateKeyPassword")
      .orElse(Option(conf.getenv(ENV_RPC_SSL_PRIVATE_KEY_PASSWORD)).filter(_.trim.nonEmpty))
      .orElse(defaults.flatMap(_.privateKeyPassword))

    val keyPassword = conf.getWithSubstitution(s"$ns.keyPassword")
        .orElse(Option(hadoopConf.getPassword(s"$ns.keyPassword")).map(new String(_)))
        .orElse(Option(conf.getenv(ENV_RPC_SSL_KEY_PASSWORD)).filter(_.trim.nonEmpty))
        .orElse(defaults.flatMap(_.keyPassword))

    val keyStoreType = conf.getWithSubstitution(s"$ns.keyStoreType")
        .orElse(defaults.flatMap(_.keyStoreType))

    val certChain = conf.getOption(s"$ns.certChain").map(new File(_))
        .orElse(defaults.flatMap(_.certChain))

    val needClientAuth =
      conf.getBoolean(s"$ns.needClientAuth", defaultValue = defaults.exists(_.needClientAuth))

    val trustStore = conf.getWithSubstitution(s"$ns.trustStore").map(new File(_))
        .orElse(defaults.flatMap(_.trustStore))

    val trustStorePassword = conf.getWithSubstitution(s"$ns.trustStorePassword")
        .orElse(Option(hadoopConf.getPassword(s"$ns.trustStorePassword")).map(new String(_)))
        .orElse(Option(conf.getenv(ENV_RPC_SSL_TRUST_STORE_PASSWORD)).filter(_.trim.nonEmpty))
        .orElse(defaults.flatMap(_.trustStorePassword))

    val trustStoreType = conf.getWithSubstitution(s"$ns.trustStoreType")
        .orElse(defaults.flatMap(_.trustStoreType))

    val trustStoreReloadingEnabled = conf.getBoolean(s"$ns.trustStoreReloadingEnabled",
        defaultValue = defaults.exists(_.trustStoreReloadingEnabled))

    val trustStoreReloadIntervalMs = conf.getInt(s"$ns.trustStoreReloadIntervalMs",
      defaultValue = defaults.map(_.trustStoreReloadIntervalMs).getOrElse(10000))

    val openSslEnabled = conf.getBoolean(s"$ns.openSslEnabled",
        defaultValue = defaults.exists(_.openSslEnabled))

    val protocol = conf.getWithSubstitution(s"$ns.protocol")
        .orElse(defaults.flatMap(_.protocol))

    val enabledAlgorithms = conf.getWithSubstitution(s"$ns.enabledAlgorithms")
        .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet)
        .orElse(defaults.map(_.enabledAlgorithms))
        .getOrElse(Set.empty)

    new SSLOptions(
      Some(ns),
      enabled,
      port,
      keyStore,
      keyStorePassword,
      privateKey,
      keyPassword,
      keyStoreType,
      needClientAuth,
      certChain,
      trustStore,
      trustStorePassword,
      trustStoreType,
      trustStoreReloadingEnabled,
      trustStoreReloadIntervalMs,
      openSslEnabled,
      protocol,
      enabledAlgorithms,
      privateKeyPassword)
  }

  // Config names and environment variables for propagating SSL passwords
  val SPARK_RPC_SSL_KEY_PASSWORD_CONF = "spark.ssl.rpc.keyPassword"
  val SPARK_RPC_SSL_PRIVATE_KEY_PASSWORD_CONF = "spark.ssl.rpc.privateKeyPassword"
  val SPARK_RPC_SSL_KEY_STORE_PASSWORD_CONF = "spark.ssl.rpc.keyStorePassword"
  val SPARK_RPC_SSL_TRUST_STORE_PASSWORD_CONF = "spark.ssl.rpc.trustStorePassword"
  val SPARK_RPC_SSL_PASSWORD_FIELDS: Seq[String] = Seq(
    SPARK_RPC_SSL_KEY_PASSWORD_CONF,
    SPARK_RPC_SSL_PRIVATE_KEY_PASSWORD_CONF,
    SPARK_RPC_SSL_KEY_STORE_PASSWORD_CONF,
    SPARK_RPC_SSL_TRUST_STORE_PASSWORD_CONF
  )

  val ENV_RPC_SSL_KEY_PASSWORD = "_SPARK_SSL_RPC_KEY_PASSWORD"
  val ENV_RPC_SSL_PRIVATE_KEY_PASSWORD = "_SPARK_SSL_RPC_PRIVATE_KEY_PASSWORD"
  val ENV_RPC_SSL_KEY_STORE_PASSWORD = "_SPARK_SSL_RPC_KEY_STORE_PASSWORD"
  val ENV_RPC_SSL_TRUST_STORE_PASSWORD = "_SPARK_SSL_RPC_TRUST_STORE_PASSWORD"
  val SPARK_RPC_SSL_PASSWORD_ENVS: Seq[String] = Seq(
    ENV_RPC_SSL_KEY_PASSWORD,
    ENV_RPC_SSL_PRIVATE_KEY_PASSWORD,
    ENV_RPC_SSL_KEY_STORE_PASSWORD,
    ENV_RPC_SSL_TRUST_STORE_PASSWORD
  )
}

