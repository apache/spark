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
import javax.net.ssl.SSLContext

import org.eclipse.jetty.util.ssl.SslContextFactory

import org.apache.spark.internal.Logging

/**
 * SSLOptions class is a common container for SSL configuration options. It offers methods to
 * generate specific objects to configure SSL for different communication protocols.
 *
 * SSLOptions is intended to provide the maximum common set of SSL settings, which are supported
 * by the protocol, which it can generate the configuration for.
 *
 * @param enabled             enables or disables SSL; if it is set to false, the rest of the
 *                            settings are disregarded
 * @param port                the port where to bind the SSL server; if not defined, it will be
 *                            based on the non-SSL port for the same service.
 * @param keyStore            a path to the key-store file
 * @param keyStorePassword    a password to access the key-store file
 * @param keyPassword         a password to access the private key in the key-store
 * @param keyStoreType        the type of the key-store
 * @param needClientAuth      set true if SSL needs client authentication
 * @param trustStore          a path to the trust-store file
 * @param trustStorePassword  a password to access the trust-store file
 * @param trustStoreType      the type of the trust-store
 * @param protocol            SSL protocol (remember that SSLv3 was compromised) supported by Java
 * @param enabledAlgorithms   a set of encryption algorithms that may be used
 */
private[spark] case class SSLOptions(
    enabled: Boolean = false,
    port: Option[Int] = None,
    keyStore: Option[File] = None,
    keyStorePassword: Option[String] = None,
    keyPassword: Option[String] = None,
    keyStoreType: Option[String] = None,
    needClientAuth: Boolean = false,
    trustStore: Option[File] = None,
    trustStorePassword: Option[String] = None,
    trustStoreType: Option[String] = None,
    protocol: Option[String] = None,
    enabledAlgorithms: Set[String] = Set.empty)
    extends Logging {

  /**
   * Creates a Jetty SSL context factory according to the SSL settings represented by this object.
   */
  def createJettySslContextFactory(): Option[SslContextFactory] = {
    if (enabled) {
      val sslContextFactory = new SslContextFactory()

      keyStore.foreach(file => sslContextFactory.setKeyStorePath(file.getAbsolutePath))
      keyStorePassword.foreach(sslContextFactory.setKeyStorePassword)
      keyPassword.foreach(sslContextFactory.setKeyManagerPassword)
      keyStoreType.foreach(sslContextFactory.setKeyStoreType)
      if (needClientAuth) {
        trustStore.foreach(file => sslContextFactory.setTrustStorePath(file.getAbsolutePath))
        trustStorePassword.foreach(sslContextFactory.setTrustStorePassword)
        trustStoreType.foreach(sslContextFactory.setTrustStoreType)
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
    Set()
  } else {
    var context: SSLContext = null
    try {
      context = SSLContext.getInstance(protocol.orNull)
      /* The set of supported algorithms does not depend upon the keys, trust, or
         rng, although they will influence which algorithms are eventually used. */
      context.init(null, null, null)
    } catch {
      case npe: NullPointerException =>
        logDebug("No SSL protocol specified")
        context = SSLContext.getDefault
      case nsa: NoSuchAlgorithmException =>
        logDebug(s"No support for requested SSL protocol ${protocol.get}")
        context = SSLContext.getDefault
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

  /** Returns a string representation of this SSLOptions with all the passwords masked. */
  override def toString: String = s"SSLOptions{enabled=$enabled, " +
      s"keyStore=$keyStore, keyStorePassword=${keyStorePassword.map(_ => "xxx")}, " +
      s"trustStore=$trustStore, trustStorePassword=${trustStorePassword.map(_ => "xxx")}, " +
      s"protocol=$protocol, enabledAlgorithms=$enabledAlgorithms}"

}

private[spark] object SSLOptions extends Logging {

  /**
   * Resolves SSLOptions settings from a given Spark configuration object at a given namespace.
   *
   * The following settings are allowed:
   * $ - `[ns].enabled` - `true` or `false`, to enable or disable SSL respectively
   * $ - `[ns].keyStore` - a path to the key-store file; can be relative to the current directory
   * $ - `[ns].keyStorePassword` - a password to the key-store file
   * $ - `[ns].keyPassword` - a password to the private key
   * $ - `[ns].keyStoreType` - the type of the key-store
   * $ - `[ns].needClientAuth` - whether SSL needs client authentication
   * $ - `[ns].trustStore` - a path to the trust-store file; can be relative to the current
   *                         directory
   * $ - `[ns].trustStorePassword` - a password to the trust-store file
   * $ - `[ns].trustStoreType` - the type of trust-store
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
   * @param ns the namespace name
   * @param defaults the default configuration
   * @return [[org.apache.spark.SSLOptions]] object
   */
  def parse(conf: SparkConf, ns: String, defaults: Option[SSLOptions] = None): SSLOptions = {
    val enabled = conf.getBoolean(s"$ns.enabled", defaultValue = defaults.exists(_.enabled))

    val port = conf.getOption(s"$ns.port").map(_.toInt)
    port.foreach { p =>
      require(p >= 0, "Port number must be a non-negative value.")
    }

    val keyStore = conf.getOption(s"$ns.keyStore").map(new File(_))
        .orElse(defaults.flatMap(_.keyStore))

    val keyStorePassword = conf.getOption(s"$ns.keyStorePassword")
        .orElse(defaults.flatMap(_.keyStorePassword))

    val keyPassword = conf.getOption(s"$ns.keyPassword")
        .orElse(defaults.flatMap(_.keyPassword))

    val keyStoreType = conf.getOption(s"$ns.keyStoreType")
        .orElse(defaults.flatMap(_.keyStoreType))

    val needClientAuth =
      conf.getBoolean(s"$ns.needClientAuth", defaultValue = defaults.exists(_.needClientAuth))

    val trustStore = conf.getOption(s"$ns.trustStore").map(new File(_))
        .orElse(defaults.flatMap(_.trustStore))

    val trustStorePassword = conf.getOption(s"$ns.trustStorePassword")
        .orElse(defaults.flatMap(_.trustStorePassword))

    val trustStoreType = conf.getOption(s"$ns.trustStoreType")
        .orElse(defaults.flatMap(_.trustStoreType))

    val protocol = conf.getOption(s"$ns.protocol")
        .orElse(defaults.flatMap(_.protocol))

    val enabledAlgorithms = conf.getOption(s"$ns.enabledAlgorithms")
        .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet)
        .orElse(defaults.map(_.enabledAlgorithms))
        .getOrElse(Set.empty)

    new SSLOptions(
      enabled,
      port,
      keyStore,
      keyStorePassword,
      keyPassword,
      keyStoreType,
      needClientAuth,
      trustStore,
      trustStorePassword,
      trustStoreType,
      protocol,
      enabledAlgorithms)
  }

}

