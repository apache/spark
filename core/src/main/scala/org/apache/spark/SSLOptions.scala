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

import scala.collection.JavaConverters._

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import org.apache.spark.network.util.ConfigProvider
import org.apache.spark.network.util.ssl.SSLFactory

import org.eclipse.jetty.util.ssl.SslContextFactory

/**
 * SSLOptions class is a common container for SSL configuration options. It offers methods to
 * generate specific objects to configure SSL for different communication protocols.
 *
 * SSLOptions is intended to provide the maximum common set of SSL settings, which are supported
 * by the protocol, which it can generate the configuration for. Since Akka doesn't support client
 * authentication with SSL, SSLOptions cannot support it either.
 *
 * @param nameSpace the configuration namespace
 * @param enabled enables or disables SSL;
 *                if it is set to false, the rest of the settings are disregarded
 * @param keyStore a path to the key-store file
 * @param keyStorePassword a password to access the key-store file
 * @param privateKey a PKCS#8 private key file in PEM format
 * @param keyPassword a password to access the private key in the key-store
 * @param certChain an X.509 certificate chain file in PEM format
 * @param trustStore a path to the trust-store file
 * @param trustStorePassword a password to access the trust-store file
 * @param trustStoreReloadingEnabled enables or disables using a trust-store that that
 * reloads its configuration when the trust-store file on disk changes
 * @param trustStoreReloadInterval the interval, in milliseconds,
 * when the trust-store will reload its configuration
 * @param openSslEnabled enables or disables using an OpenSSL implementation
 * (if available on host system), requires certChain and keyFile arguments
 * @param protocol SSL protocol (remember that SSLv3 was compromised) supported by Java
 * @param enabledAlgorithms a set of encryption algorithms that may be used
 */
private[spark] case class SSLOptions(
    nameSpace: Option[String] = None,
    enabled: Boolean = false,
    keyStore: Option[File] = None,
    keyStorePassword: Option[String] = None,
    privateKey: Option[File] = None,
    keyPassword: Option[String] = None,
    certChain: Option[File] = None,
    trustStore: Option[File] = None,
    trustStorePassword: Option[String] = None,
    trustStoreReloadingEnabled: Boolean = false,
    trustStoreReloadInterval: Int = 10000,
    openSslEnabled: Boolean = false,
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
      trustStore.foreach(file => sslContextFactory.setTrustStore(file.getAbsolutePath))
      keyStorePassword.foreach(sslContextFactory.setKeyStorePassword)
      trustStorePassword.foreach(sslContextFactory.setTrustStorePassword)
      keyPassword.foreach(sslContextFactory.setKeyManagerPassword)
      protocol.foreach(sslContextFactory.setProtocol)
      sslContextFactory.setIncludeCipherSuites(supportedAlgorithms.toSeq: _*)

      Some(sslContextFactory)
    } else {
      None
    }
  }

  /**
   *
   * @return
   */
  def createSSLFactory: Option[SSLFactory] = {
    if (enabled) {
      Some(new SSLFactory.Builder()
        .requestedProtocol(protocol.getOrElse(null))
        .requestedCiphers(enabledAlgorithms.toArray)
        .keyStore(keyStore.getOrElse(null), keyStorePassword.getOrElse(null))
        .privateKey(privateKey.getOrElse(null))
        .keyPassword(keyPassword.getOrElse(null))
        .certChain(certChain.getOrElse(null))
        .trustStore(
          trustStore.getOrElse(null),
          trustStorePassword.getOrElse(null),
          trustStoreReloadingEnabled,
          trustStoreReloadInterval)
        .build())
    } else {
      None
    }
  }

  /**
   * Creates an Akka configuration object which contains all the SSL settings represented by this
   * object. It can be used then to compose the ultimate Akka configuration.
   */
  def createAkkaConfig: Option[Config] = {
    if (enabled) {
      Some(ConfigFactory.empty()
        .withValue("akka.remote.netty.tcp.security.key-store",
          ConfigValueFactory.fromAnyRef(keyStore.map(_.getAbsolutePath).getOrElse("")))
        .withValue("akka.remote.netty.tcp.security.key-store-password",
          ConfigValueFactory.fromAnyRef(keyStorePassword.getOrElse("")))
        .withValue("akka.remote.netty.tcp.security.trust-store",
          ConfigValueFactory.fromAnyRef(trustStore.map(_.getAbsolutePath).getOrElse("")))
        .withValue("akka.remote.netty.tcp.security.trust-store-password",
          ConfigValueFactory.fromAnyRef(trustStorePassword.getOrElse("")))
        .withValue("akka.remote.netty.tcp.security.key-password",
          ConfigValueFactory.fromAnyRef(keyPassword.getOrElse("")))
        .withValue("akka.remote.netty.tcp.security.random-number-generator",
          ConfigValueFactory.fromAnyRef(""))
        .withValue("akka.remote.netty.tcp.security.protocol",
          ConfigValueFactory.fromAnyRef(protocol.getOrElse("")))
        .withValue("akka.remote.netty.tcp.security.enabled-algorithms",
          ConfigValueFactory.fromIterable(supportedAlgorithms.asJava))
        .withValue("akka.remote.netty.tcp.enable-ssl",
          ConfigValueFactory.fromAnyRef(true)))
    } else {
      None
    }
  }

  /*
   * The supportedAlgorithms set is a subset of the enabledAlgorithms that
   * are supported by the current Java security provider for this protocol.
   */
  private val supportedAlgorithms: Set[String] = {
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

    enabledAlgorithms & providerAlgorithms
  }

  /**
   * Simple implicit helper class to allow for string interpolation
   * pattern matching...
   * @param sc
   */
  implicit private class NsContext (val sc: StringContext) {
    object ns {
      def apply (args : Any*) : String = sc.s (args : _*)
      def unapplySeq (s : String) : Option[Seq[String]] = {
        val regexp = sc.parts.mkString ("(.+)").r
        regexp.unapplySeq(s)
      }
    }
  }

  /**
   *
   * @return
   */
  def createConfigProvider(conf: SparkConf): ConfigProvider = {
    val nsp = nameSpace.getOrElse("spark.ssl")
    new ConfigProvider() {
      override def get(name: String): String = conf.get(name)
      override def getBoolean(name: String, defaultValue: Boolean): Boolean = {
        name match {
          case ns"$nsp.enabled" => enabled
          case ns"$nsp.trustStoreReloadingEnabled" => trustStoreReloadingEnabled
          case ns"$nsp.openSslEnabled" => openSslEnabled
          case _ => conf.getBoolean(name, defaultValue)
        }
      }

      override def getInt(name: String, defaultValue: Int): Int = {
        name match {
          case ns"$nsp.trustStoreReloadInterval" => trustStoreReloadInterval
          case _ => conf.getInt(name, defaultValue)
        }
      }

      override def get(name: String, defaultValue: String): String = {
        name match {
          case ns"$nsp.keyStore" => keyStore.fold(defaultValue)(_.getAbsolutePath)
          case ns"$nsp.keyStorePassword" => keyStorePassword.getOrElse(defaultValue)
          case ns"$nsp.privateKey" => privateKey.fold(defaultValue)(_.getAbsolutePath)
          case ns"$nsp.keyPassword" => keyPassword.getOrElse(defaultValue)
          case ns"$nsp.certChain" => certChain.fold(defaultValue)(_.getAbsolutePath)
          case ns"$nsp.trustStore" => trustStore.fold(defaultValue)(_.getAbsolutePath)
          case ns"$nsp.trustStorePassword" => trustStorePassword.getOrElse(defaultValue)
          case ns"$nsp.protocol" => protocol.getOrElse(defaultValue)
          case ns"$nsp.enabledAlgorithms" => enabledAlgorithms.mkString(",")
          case _ => conf.get(name, defaultValue)
        }
      }
    }
  }

  /** Returns a string representation of this SSLOptions with all the passwords masked. */
  override def toString: String = s"SSLOptions{enabled=$enabled, " +
    s"keyStore=$keyStore, keyStorePassword=${keyStorePassword.map(_ => "xxx")}, " +
    s"keyFile=$privateKey, certChain=$certChain, trustStore=$trustStore, " +
    s"trustStorePassword=${trustStorePassword.map(_ => "xxx")}, " +
    s"openSslEnabled=$openSslEnabled, trustStoreReloadingEnabled=$trustStoreReloadingEnabled, " +
    s"protocol=$protocol, enabledAlgorithms=$enabledAlgorithms}"

}

private[spark] object SSLOptions extends Logging {

  /** Resolves SSLOptions settings from a given Spark configuration object at a given namespace.
    *
    * The following settings are allowed:
    * $ - `[ns].enabled` - `true` or `false`, to enable or disable SSL respectively
    * $ - `[ns].keyStore` - a path to the key-store file; can be relative
    * to the current directory
    * $ - `[ns].keyStorePassword` - a password to the key-store file
    * $ - `[ns].privateKey` - a PKCS#8 private key file in PEM format
    * $ - `[ns].keyPassword` - a password to the private key
    * $ - `[ns].certChain` - an X.509 certificate chain file in PEM format
    * $ - `[ns].trustStore` - a path to the trust-store file; can be relative
    * to the current directory
    * $ - `[ns].trustStorePassword` - a password to the trust-store file
    * $ - `[ns].trustStoreReloadingEnabled` - enables or disables using a trust-store
    * that that reloads its configuration when the trust-store file on disk changes
    * $ - `[ns].trustStoreReloadInterval` - the interval, in milliseconds, the
    * trust-store will reload its configuration
    * $ - `[ns].openSslEnabled` - enables or disables using an OpenSSL implementation
    * (if available on host system), requires certChain and keyFile arguments
    * $ - `[ns].protocol` - a protocol name supported by a particular Java version
    * $ - `[ns].enabledAlgorithms` - a comma separated list of ciphers
    *
    * For a list of protocols and ciphers supported by particular Java versions, you may go to
    * [[https://blogs.oracle.com/java-platform-group/entry/diagnosing_tls_ssl_and_https Oracle
    * blog page]].
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

    val keyStore = conf.getOption(s"$ns.keyStore").map(new File(_))
        .orElse(defaults.flatMap(_.keyStore))

    val keyStorePassword = conf.getOption(s"$ns.keyStorePassword")
        .orElse(defaults.flatMap(_.keyStorePassword))

    val privateKey = conf.getOption(s"$ns.privateKey").map(new File(_))
        .orElse(defaults.flatMap(_.privateKey))

    val keyPassword = conf.getOption(s"$ns.keyPassword")
        .orElse(defaults.flatMap(_.keyPassword))

    val certChain = conf.getOption(s"$ns.certChain").map(new File(_))
        .orElse(defaults.flatMap(_.certChain))

    val trustStore = conf.getOption(s"$ns.trustStore").map(new File(_))
        .orElse(defaults.flatMap(_.trustStore))

    val trustStorePassword = conf.getOption(s"$ns.trustStorePassword")
        .orElse(defaults.flatMap(_.trustStorePassword))

    val trustStoreReloadingEnabled = conf.getBoolean(s"$ns.trustStoreReloadingEnabled", false)

    val trustStoreReloadInterval = conf.getInt(s"$ns.trustStoreReloadInterval", 10000)

    val openSslEnabled = conf.getBoolean(s"$ns.openSslEnabled", false)

    val protocol = conf.getOption(s"$ns.protocol")
        .orElse(defaults.flatMap(_.protocol))

    val enabledAlgorithms = conf.getOption(s"$ns.enabledAlgorithms")
        .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet)
        .orElse(defaults.map(_.enabledAlgorithms))
        .getOrElse(Set.empty)

    new SSLOptions(
      Some(ns),
      enabled,
      keyStore,
      keyStorePassword,
      privateKey,
      keyPassword,
      certChain,
      trustStore,
      trustStorePassword,
      trustStoreReloadingEnabled,
      trustStoreReloadInterval,
      openSslEnabled,
      protocol,
      enabledAlgorithms)
  }

}

