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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.eclipse.jetty.util.ssl.SslContextFactory

private[spark] case class SSLOptions(
    enabled: Boolean = false,
    keyStore: Option[File] = None,
    keyStorePassword: Option[String] = None,
    keyPassword: Option[String] = None,
    trustStore: Option[File] = None,
    trustStorePassword: Option[String] = None,
    protocol: Option[String] = None,
    enabledAlgorithms: Set[String] = Set.empty) {

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
      sslContextFactory.setIncludeCipherSuites(enabledAlgorithms.toSeq: _*)

      Some(sslContextFactory)
    } else {
      None
    }
  }

  /**
   * Creates an Akka configuration object which contains all the SSL settings represented by this
   * object. It can be used then to compose the ultimate Akka configuration.
   */
  def createAkkaConfig: Option[Config] = {
    import scala.collection.JavaConversions._
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
          ConfigValueFactory.fromIterable(enabledAlgorithms.toSeq))
        .withValue("akka.remote.netty.tcp.enable-ssl",
          ConfigValueFactory.fromAnyRef(true)))
    } else {
      None
    }
  }

  override def toString: String = s"SSLOptions{enabled=$enabled, " +
      s"keyStore=$keyStore, keyStorePassword=${keyStorePassword.map(_ => "xxx")}, " +
      s"trustStore=$trustStore, trustStorePassword=${trustStorePassword.map(_ => "xxx")}, " +
      s"protocol=$protocol, enabledAlgorithms=$enabledAlgorithms}"

}

private[spark] object SSLOptions extends Logging {

  /**
   * Resolves SSLOptions settings from a given Spark configuration object at a given namespace.
   * The parent directory of that location is used as a base directory to resolve relative paths
   * to keystore and truststore.
   */
  def parse(conf: SparkConf, ns: String, defaults: Option[SSLOptions] = None): SSLOptions = {
    val enabled = conf.getBoolean(s"$ns.enabled", defaultValue = defaults.exists(_.enabled))

    val keyStore = conf.getOption(s"$ns.keyStore").map(new File(_))
        .orElse(defaults.flatMap(_.keyStore))

    val keyStorePassword = conf.getOption(s"$ns.keyStorePassword")
        .orElse(defaults.flatMap(_.keyStorePassword))

    val keyPassword = conf.getOption(s"$ns.keyPassword")
        .orElse(defaults.flatMap(_.keyPassword))

    val trustStore = conf.getOption(s"$ns.trustStore").map(new File(_))
        .orElse(defaults.flatMap(_.trustStore))

    val trustStorePassword = conf.getOption(s"$ns.trustStorePassword")
        .orElse(defaults.flatMap(_.trustStorePassword))

    val protocol = conf.getOption(s"$ns.protocol")
        .orElse(defaults.flatMap(_.protocol))

    val enabledAlgorithms = conf.getOption(s"$ns.enabledAlgorithms")
        .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet)
        .orElse(defaults.map(_.enabledAlgorithms))
        .getOrElse(Set.empty)

    new SSLOptions(
      enabled,
      keyStore,
      keyStorePassword,
      keyPassword,
      trustStore,
      trustStorePassword,
      protocol,
      enabledAlgorithms)
  }

}

