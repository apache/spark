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
package org.apache.spark.deploy.rest.kubernetes.v2

import java.io.File
import java.security.SecureRandom

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.commons.lang3.RandomStringUtils

import org.apache.spark.{SecurityManager => SparkSecurityManager, SparkConf, SparkException, SSLOptions}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.rest.kubernetes.v1.PemsToKeyStoreConverter
import org.apache.spark.internal.Logging

private[spark] trait ResourceStagingServerSslOptionsProvider {
  def getSslOptions: SSLOptions
}

private[spark] class ResourceStagingServerSslOptionsProviderImpl(sparkConf: SparkConf)
    extends ResourceStagingServerSslOptionsProvider with Logging {

  private val SECURE_RANDOM = new SecureRandom()

  def getSslOptions: SSLOptions = {
    val baseSslOptions = new SparkSecurityManager(sparkConf)
      .getSSLOptions(RESOURCE_STAGING_SERVER_SSL_NAMESPACE)
    val maybeKeyPem = sparkConf.get(RESOURCE_STAGING_SERVER_KEY_PEM)
    val maybeServerCertPem = sparkConf.get(RESOURCE_STAGING_SERVER_CERT_PEM)
    val maybeKeyStorePasswordFile = sparkConf.get(RESOURCE_STAGING_SERVER_KEYSTORE_PASSWORD_FILE)
    val maybeKeyPasswordFile = sparkConf.get(RESOURCE_STAGING_SERVER_KEYSTORE_KEY_PASSWORD_FILE)
    val maybeClientCertPem = sparkConf.get(RESOURCE_STAGING_SERVER_CLIENT_CERT_PEM)

    logSslConfigurations(
      baseSslOptions,
      maybeKeyPem,
      maybeServerCertPem,
      maybeKeyStorePasswordFile,
      maybeKeyPasswordFile,
      maybeClientCertPem)

    requireNandDefined(baseSslOptions.keyStore, maybeKeyPem,
      "Shouldn't provide both key PEM and keyStore files for TLS.")
    requireNandDefined(baseSslOptions.keyStore, maybeServerCertPem,
      "Shouldn't provide both certificate PEM and keyStore files for TLS.")
    requireNandDefined(baseSslOptions.keyStorePassword, maybeKeyStorePasswordFile,
      "Shouldn't provide both the keyStore password value and the keyStore password file.")
    requireNandDefined(baseSslOptions.keyPassword, maybeKeyPasswordFile,
      "Shouldn't provide both the keyStore key password value and the keyStore key password file.")
    requireBothOrNeitherDefined(
      maybeKeyPem,
      maybeServerCertPem,
      "When providing a certificate PEM file, the key PEM file must also be provided.",
      "When providing a key PEM file, the certificate PEM file must also be provided.")
    requireNandDefined(baseSslOptions.trustStore, maybeClientCertPem,
      "Shouldn't provide both the trustStore and a client certificate PEM file.")

    val resolvedKeyStorePassword = baseSslOptions.keyStorePassword
      .orElse(maybeKeyStorePasswordFile.map { keyStorePasswordFile =>
        safeFileToString(keyStorePasswordFile, "KeyStore password file")
      })
      .orElse(maybeKeyPem.map { _ => randomPassword()})
    val resolvedKeyStoreKeyPassword = baseSslOptions.keyPassword
      .orElse(maybeKeyPasswordFile.map { keyPasswordFile =>
        safeFileToString(keyPasswordFile, "KeyStore key password file")
      })
      .orElse(maybeKeyPem.map { _ => randomPassword()})
    val resolvedKeyStore = baseSslOptions.keyStore.orElse {
      for {
        keyPem <- maybeKeyPem
        certPem <- maybeServerCertPem
        keyStorePassword <- resolvedKeyStorePassword
        keyPassword <- resolvedKeyStoreKeyPassword
      } yield {
        val keyPemFile = new File(keyPem)
        val certPemFile = new File(certPem)
        PemsToKeyStoreConverter.convertPemsToTempKeyStoreFile(
          keyPemFile,
          certPemFile,
          "key",
          keyStorePassword,
          keyPassword,
          baseSslOptions.keyStoreType)
      }
    }
    val resolvedTrustStorePassword = baseSslOptions.trustStorePassword
      .orElse(maybeClientCertPem.map( _ => "defaultTrustStorePassword"))
    val resolvedTrustStore = baseSslOptions.trustStore.orElse {
      for {
        clientCertPem <- maybeClientCertPem
        trustStorePassword <- resolvedTrustStorePassword
      } yield {
        val certPemFile = new File(clientCertPem)
        PemsToKeyStoreConverter.convertCertPemToTempTrustStoreFile(
          certPemFile,
          trustStorePassword,
          baseSslOptions.trustStoreType)
      }
    }
    baseSslOptions.copy(
      keyStore = resolvedKeyStore,
      keyStorePassword = resolvedKeyStorePassword,
      keyPassword = resolvedKeyStoreKeyPassword,
      trustStore = resolvedTrustStore)
  }

  private def logSslConfigurations(
      baseSslOptions: SSLOptions,
      maybeKeyPem: Option[String],
      maybeServerCertPem: Option[String],
      maybeKeyStorePasswordFile: Option[String],
      maybeKeyPasswordFile: Option[String],
      maybeClientCertPem: Option[String]) = {
    logDebug("The following SSL configurations were provided for the resource staging server:")
    logDebug(s"KeyStore File: ${baseSslOptions.keyStore.map(_.getAbsolutePath).getOrElse("N/A")}")
    logDebug("KeyStore Password: " +
      baseSslOptions.keyStorePassword.map(_ => "<present_but_redacted>").getOrElse("N/A"))
    logDebug(s"KeyStore Password File: ${maybeKeyStorePasswordFile.getOrElse("N/A")}")
    logDebug("Key Password: " +
      baseSslOptions.keyPassword.map(_ => "<present_but_redacted>").getOrElse("N/A"))
    logDebug(s"Key Password File: ${maybeKeyPasswordFile.getOrElse("N/A")}")
    logDebug(s"KeyStore Type: ${baseSslOptions.keyStoreType.getOrElse("N/A")}")
    logDebug(s"Key PEM: ${maybeKeyPem.getOrElse("N/A")}")
    logDebug(s"Server-side certificate PEM: ${maybeServerCertPem.getOrElse("N/A")}")
    logDebug(s"Client-side certificate PEM: ${maybeClientCertPem.getOrElse("N/A")}")
  }

  private def requireBothOrNeitherDefined(
      opt1: Option[_],
      opt2: Option[_],
      errMessageWhenFirstIsMissing: String,
      errMessageWhenSecondIsMissing: String): Unit = {
    requireSecondIfFirstIsDefined(opt1, opt2, errMessageWhenSecondIsMissing)
    requireSecondIfFirstIsDefined(opt2, opt1, errMessageWhenFirstIsMissing)
  }

  private def requireSecondIfFirstIsDefined(
      opt1: Option[_], opt2: Option[_], errMessageWhenSecondIsMissing: String): Unit = {
    opt1.foreach { _ =>
      require(opt2.isDefined, errMessageWhenSecondIsMissing)
    }
  }

  private def requireNandDefined(opt1: Option[_], opt2: Option[_], errMessage: String): Unit = {
    opt1.foreach { _ => require(opt2.isEmpty, errMessage) }
  }

  private def safeFileToString(filePath: String, fileType: String): String = {
    val file = new File(filePath)
    if (!file.isFile) {
      throw new SparkException(s"$fileType provided at ${file.getAbsolutePath} does not exist or"
        + s" is not a file.")
    }
    Files.toString(file, Charsets.UTF_8)
  }

  private def randomPassword(): String = {
    RandomStringUtils.random(1024, 0, Integer.MAX_VALUE, false, false, null, SECURE_RANDOM)
  }
}
