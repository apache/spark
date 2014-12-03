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

import java.io.{FileReader, File}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.eclipse.jetty.util.ssl.SslContextFactory

import scala.util.Try

case class SSLOptions(enabled: Boolean = false,
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

}

object SSLOptions extends Logging {

  /**
   * Resolves the SSL configuration file location by checking:
   * - SPARK_SSL_CONFIG_FILE env variable
   * - SPARK_CONF_DIR/ssl.conf
   * - SPARK_HOME/conf/ssl.conf
   */
  def defaultConfigFile: Option[File] = {
    val specifiedFile = Option(System.getenv("SPARK_SSL_CONFIG_FILE")).map(new File(_))
    val sparkConfDir = Option(System.getenv("SPARK_CONF_DIR")).map(new File(_))
    val sparkHomeConfDir = Option(System.getenv("SPARK_HOME"))
      .map(new File(_, "conf"))
    val defaultFile = (sparkConfDir orElse sparkHomeConfDir).map(new File(_, "ssl.conf"))

    specifiedFile orElse defaultFile
  }

  /**
   * Loads the given properties file with failover to empty Properties object.
   */
  def load(configFile: File): Properties = {
    logInfo(s"Loading SSL configuration from $configFile")
    try {
      val props = new Properties()
      val reader = new FileReader(configFile)
      try {
        props.load(reader)
        props.put("sslConfigurationFileLocation", configFile.getAbsolutePath)
        props
      } finally {
        reader.close()
      }
    } catch {
      case ex: Throwable =>
        logWarning(s"The SSL configuration file ${configFile.getAbsolutePath} " +
          s"could not be loaded. The underlying exception was: ${ex.getMessage}")
        new Properties
    }
  }

  /**
   * Resolves SSLOptions settings from a given Spark configuration object at a given namespace.
   * If SSL settings were loaded from the configuration file, ``sslConfigurationFileLocation``
   * property is present in the Spark configuration. The parent directory of that location is used
   * as a base directory to resolve relative paths to keystore and truststore.
   */
  def parse(conf: SparkConf, ns: String): SSLOptions = {
    val parentDir = conf.getOption("sslConfigurationFileLocation").map(new File(_).getParentFile)
      .getOrElse(new File(".")).toPath

    val enabled = conf.getBoolean(s"$ns.enabled", defaultValue = false)
    val keyStore = Try(conf.get(s"$ns.keyStore")).toOption.map(parentDir.resolve(_).toFile)
    val keyStorePassword = Try(conf.get(s"$ns.keyStorePassword")).toOption
    val keyPassword = Try(conf.get(s"$ns.keyPassword")).toOption
    val trustStore = Try(conf.get(s"$ns.trustStore")).toOption.map(parentDir.resolve(_).toFile)
    val trustStorePassword = Try(conf.get(s"$ns.trustStorePassword")).toOption
    val protocol = Try(conf.get(s"$ns.protocol")).toOption
    val enabledAlgorithms = Try(conf.get(s"$ns.enabledAlgorithms")).toOption
      .map(_.trim.dropWhile(_ == '[')
      .takeWhile(_ != ']')).map(_.split(",").map(_.trim).toSet)
      .getOrElse(Set.empty)

    new SSLOptions(enabled, keyStore, keyStorePassword, keyPassword, trustStore, trustStorePassword,
      protocol, enabledAlgorithms)
  }

  /**
   * Loads the SSL configuration file. If ``spark.ssl.configFile`` property is in the system
   * properties, it is assumed it contains the SSL configuration file location to be used.
   * Otherwise, it uses the location returned by [[SSLOptions.defaultConfigFile]].
   */
  def load(): Properties  = {
    val file = Option(System.getProperty("spark.ssl.configFile"))
      .map(new File(_)) orElse defaultConfigFile

    file.fold {
      logWarning("SSL configuration file not found. SSL will be disabled.")
      new Properties()
    } { file =>
      logInfo(s"Loading SSL configuration from ${file.getAbsolutePath}")
      load(file)
    }
  }

}

