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
package org.apache.spark.deploy.kubernetes.submit.v1

import java.io.{File, FileInputStream}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{SSLContext, TrustManagerFactory, X509TrustManager}

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{EnvVar, EnvVarBuilder, Secret, Volume, VolumeBuilder, VolumeMount, VolumeMountBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

import org.apache.spark.{SecurityManager => SparkSecurityManager, SparkConf, SparkException, SSLOptions}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.KubernetesFileUtils
import org.apache.spark.deploy.rest.kubernetes.v1.PemsToKeyStoreConverter
import org.apache.spark.util.Utils

/**
 * Raw SSL configuration as the user specified in SparkConf for setting up the driver
 * submission server.
 */
private case class DriverSubmitSslConfigurationParameters(
    storeBasedSslOptions: SSLOptions,
    isKeyStoreLocalFile: Boolean,
    driverSubmitServerKeyPem: Option[File],
    isDriverSubmitKeyPemLocalFile: Boolean,
    driverSubmitServerCertPem: Option[File],
    isDriverSubmitServerCertPemLocalFile: Boolean,
    submissionClientCertPem: Option[File])

/**
 * Resolved from translating options provided in
 * {@link DriverSubmitSslConfigurationParameters} into Kubernetes volumes, environment variables
 * for the driver pod, Kubernetes secrets, client-side trust managers, and the client-side SSL
 * context. This is used for setting up the SSL connection for the submission server where the
 * application local dependencies and configuration is provided from.
 */
private[spark] case class DriverSubmitSslConfiguration(
    enabled: Boolean,
    sslPodEnvVars: Array[EnvVar],
    sslPodVolume: Option[Volume],
    sslPodVolumeMount: Option[VolumeMount],
    sslSecret: Option[Secret],
    driverSubmitClientTrustManager: Option[X509TrustManager],
    driverSubmitClientSslContext: SSLContext)

/**
 * Provides the SSL configuration for bootstrapping the driver pod to listen for the driver
 * submission over SSL, and then supply the client-side configuration for establishing the
 * SSL connection. This is done in two phases: first, interpreting the raw configuration
 * values from the SparkConf object; then second, converting the configuration parameters
 * into the appropriate Kubernetes constructs, namely the volume and volume mount to add to the
 * driver pod, and the secret to create at the API server; and finally, constructing the
 * client-side trust manager and SSL context for sending the local dependencies.
 */
private[spark] class DriverSubmitSslConfigurationProvider(
    sparkConf: SparkConf,
    kubernetesAppId: String,
    kubernetesClient: KubernetesClient,
    kubernetesResourceCleaner: KubernetesResourceCleaner) {
  private val SECURE_RANDOM = new SecureRandom()
  private val sslSecretsName = s"$SUBMISSION_SSL_SECRETS_PREFIX-$kubernetesAppId"
  private val sslSecretsDirectory = DRIVER_CONTAINER_SUBMISSION_SECRETS_BASE_DIR +
    s"/$kubernetesAppId-ssl"

  def getSslConfiguration(): DriverSubmitSslConfiguration = {
    val sslConfigurationParameters = parseSslConfigurationParameters()
    if (sslConfigurationParameters.storeBasedSslOptions.enabled) {
      val storeBasedSslOptions = sslConfigurationParameters.storeBasedSslOptions
      val keyStoreSecret = resolveFileToSecretMapping(
          sslConfigurationParameters.isKeyStoreLocalFile,
          SUBMISSION_SSL_KEYSTORE_SECRET_NAME,
          storeBasedSslOptions.keyStore,
          "KeyStore")
      val keyStorePathEnv = resolveFilePathEnv(
          sslConfigurationParameters.isKeyStoreLocalFile,
          ENV_SUBMISSION_KEYSTORE_FILE,
          SUBMISSION_SSL_KEYSTORE_SECRET_NAME,
          storeBasedSslOptions.keyStore)
      val storePasswordSecret = storeBasedSslOptions.keyStorePassword.map(password => {
        val passwordBase64 = BaseEncoding.base64().encode(password.getBytes(Charsets.UTF_8))
        (SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME, passwordBase64)
      }).toMap
      val storePasswordLocationEnv = storeBasedSslOptions.keyStorePassword.map(_ => {
        new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_PASSWORD_FILE)
          .withValue(s"$sslSecretsDirectory/$SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME")
          .build()
      })
      val storeKeyPasswordSecret = storeBasedSslOptions.keyPassword.map(password => {
        val passwordBase64 = BaseEncoding.base64().encode(password.getBytes(Charsets.UTF_8))
        (SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME, passwordBase64)
      }).toMap
      val storeKeyPasswordEnv = storeBasedSslOptions.keyPassword.map(_ => {
        new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_KEY_PASSWORD_FILE)
          .withValue(s"$sslSecretsDirectory/$SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME")
          .build()
      })
      val storeTypeEnv = storeBasedSslOptions.keyStoreType.map(storeType => {
        new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_TYPE)
          .withValue(storeType)
          .build()
      })
      val keyPemSecret = resolveFileToSecretMapping(
        sslConfigurationParameters.isDriverSubmitKeyPemLocalFile,
        secretName = SUBMISSION_SSL_KEY_PEM_SECRET_NAME,
        secretType = "Key pem",
        secretFile = sslConfigurationParameters.driverSubmitServerKeyPem)
      val keyPemLocationEnv = resolveFilePathEnv(
        sslConfigurationParameters.isDriverSubmitKeyPemLocalFile,
        envName = ENV_SUBMISSION_KEY_PEM_FILE,
        secretName = SUBMISSION_SSL_KEY_PEM_SECRET_NAME,
        maybeFile = sslConfigurationParameters.driverSubmitServerKeyPem)
      val certPemSecret = resolveFileToSecretMapping(
        sslConfigurationParameters.isDriverSubmitServerCertPemLocalFile,
        secretName = SUBMISSION_SSL_CERT_PEM_SECRET_NAME,
        secretType = "Cert pem",
        secretFile = sslConfigurationParameters.driverSubmitServerCertPem)
      val certPemLocationEnv = resolveFilePathEnv(
        sslConfigurationParameters.isDriverSubmitServerCertPemLocalFile,
        envName = ENV_SUBMISSION_CERT_PEM_FILE,
        secretName = SUBMISSION_SSL_CERT_PEM_SECRET_NAME,
        maybeFile = sslConfigurationParameters.driverSubmitServerCertPem)
      val useSslEnv = new EnvVarBuilder()
        .withName(ENV_SUBMISSION_USE_SSL)
        .withValue("true")
        .build()
      val sslVolume = new VolumeBuilder()
        .withName(SUBMISSION_SSL_SECRETS_VOLUME_NAME)
        .withNewSecret()
        .withSecretName(sslSecretsName)
        .endSecret()
        .build()
      val sslVolumeMount = new VolumeMountBuilder()
        .withName(SUBMISSION_SSL_SECRETS_VOLUME_NAME)
        .withReadOnly(true)
        .withMountPath(sslSecretsDirectory)
        .build()
      val allSecrets = keyStoreSecret ++
        storePasswordSecret ++
        storeKeyPasswordSecret ++
        keyPemSecret ++
        certPemSecret
      val sslSecret = kubernetesClient.secrets().createNew()
        .withNewMetadata()
        .withName(sslSecretsName)
        .endMetadata()
        .withData(allSecrets.asJava)
        .withType("Opaque")
        .done()
      kubernetesResourceCleaner.registerOrUpdateResource(sslSecret)
      val allSslEnvs = keyStorePathEnv ++
        storePasswordLocationEnv ++
        storeKeyPasswordEnv ++
        storeTypeEnv ++
        keyPemLocationEnv ++
        Array(useSslEnv) ++
        certPemLocationEnv
      val (driverSubmitClientTrustManager, driverSubmitClientSslContext) =
        buildSslConnectionConfiguration(sslConfigurationParameters)
      DriverSubmitSslConfiguration(
        true,
        allSslEnvs.toArray,
        Some(sslVolume),
        Some(sslVolumeMount),
        Some(sslSecret),
        driverSubmitClientTrustManager,
        driverSubmitClientSslContext)
    } else {
      DriverSubmitSslConfiguration(
        false,
        Array[EnvVar](),
        None,
        None,
        None,
        None,
        SSLContext.getDefault)
    }
  }

  private def resolveFilePathEnv(
      isLocal: Boolean,
      envName: String,
      secretName: String,
      maybeFile: Option[File]): Option[EnvVar] = {
    maybeFile.map(file => {
      val pemPath = if (isLocal) {
        s"$sslSecretsDirectory/$secretName"
      } else {
        file.getAbsolutePath
      }
      new EnvVarBuilder()
        .withName(envName)
        .withValue(pemPath)
        .build()
    })
  }

  private def resolveFileToSecretMapping(
      isLocal: Boolean,
      secretName: String,
      secretFile: Option[File],
      secretType: String): Map[String, String] = {
    secretFile.filter(_ => isLocal).map(file => {
      if (!file.isFile) {
        throw new SparkException(s"$secretType specified at ${file.getAbsolutePath} is not" +
          s" a file or does not exist.")
      }
      val keyStoreBytes = Files.toByteArray(file)
      (secretName, BaseEncoding.base64().encode(keyStoreBytes))
    }).toMap
  }

  private def parseSslConfigurationParameters(): DriverSubmitSslConfigurationParameters = {
    val maybeKeyStore = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_SSL_KEYSTORE)
    val maybeTrustStore = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_SSL_TRUSTSTORE)
    val maybeKeyPem = sparkConf.get(DRIVER_SUBMIT_SSL_KEY_PEM)
    val maybeDriverSubmitServerCertPem = sparkConf.get(DRIVER_SUBMIT_SSL_SERVER_CERT_PEM)
    val maybeDriverSubmitClientCertPem = sparkConf.get(DRIVER_SUBMIT_SSL_CLIENT_CERT_PEM)
    validatePemsDoNotConflictWithStores(
      maybeKeyStore,
      maybeTrustStore,
      maybeKeyPem,
      maybeDriverSubmitServerCertPem,
      maybeDriverSubmitClientCertPem)
    val resolvedSparkConf = sparkConf.clone()
    val (isLocalKeyStore, resolvedKeyStore) = resolveLocalFile(maybeKeyStore, "keyStore")
    resolvedKeyStore.foreach {
      resolvedSparkConf.set(KUBERNETES_DRIVER_SUBMIT_SSL_KEYSTORE, _)
    }
    val (isLocalDriverSubmitServerCertPem, resolvedDriverSubmitServerCertPem) =
      resolveLocalFile(maybeDriverSubmitServerCertPem, "server cert PEM")
    val (isLocalKeyPem, resolvedKeyPem) = resolveLocalFile(maybeKeyPem, "key PEM")
    maybeTrustStore.foreach { trustStore =>
      require(KubernetesFileUtils.isUriLocalFile(trustStore), s"Invalid trustStore URI" +
        s" $trustStore; trustStore URI for submit server must have no scheme, or scheme file://")
      resolvedSparkConf.set(KUBERNETES_DRIVER_SUBMIT_SSL_TRUSTSTORE,
        Utils.resolveURI(trustStore).getPath)
    }
    val driverSubmitClientCertPem = maybeDriverSubmitClientCertPem.map { driverSubmitClientCert =>
      require(KubernetesFileUtils.isUriLocalFile(driverSubmitClientCert),
        "Invalid client certificate PEM URI $driverSubmitClientCert: client certificate URI must" +
          " have no scheme, or scheme file://")
      Utils.resolveURI(driverSubmitClientCert).getPath
    }
    val securityManager = new SparkSecurityManager(resolvedSparkConf)
    val storeBasedSslOptions = securityManager.getSSLOptions(DRIVER_SUBMIT_SSL_NAMESPACE)
    DriverSubmitSslConfigurationParameters(
      storeBasedSslOptions,
      isLocalKeyStore,
      resolvedKeyPem.map(new File(_)),
      isLocalKeyPem,
      resolvedDriverSubmitServerCertPem.map(new File(_)),
      isLocalDriverSubmitServerCertPem,
      driverSubmitClientCertPem.map(new File(_)))
  }

  private def resolveLocalFile(file: Option[String],
      fileType: String): (Boolean, Option[String]) = {
    file.map { f =>
      require(isValidSslFileScheme(f), s"Invalid $fileType URI $f, $fileType URI" +
        s" for submit server must have scheme file:// or local:// (no scheme defaults to file://")
      val isLocal = KubernetesFileUtils.isUriLocalFile(f)
      (isLocal, Option.apply(Utils.resolveURI(f).getPath))
    }.getOrElse(false, None)
  }

  private def validatePemsDoNotConflictWithStores(
      maybeKeyStore: Option[String],
      maybeTrustStore: Option[String],
      maybeKeyPem: Option[String],
      maybeDriverSubmitServerCertPem: Option[String],
      maybeSubmitClientCertPem: Option[String]) = {
    maybeKeyPem.orElse(maybeDriverSubmitServerCertPem).foreach { _ =>
      require(maybeKeyStore.isEmpty,
        "Cannot specify server PEM files and key store files; must specify only one or the other.")
    }
    maybeKeyPem.foreach { _ =>
      require(maybeDriverSubmitServerCertPem.isDefined,
        "When specifying the key PEM file, the server certificate PEM file must also be provided.")
    }
    maybeDriverSubmitServerCertPem.foreach { _ =>
      require(maybeKeyPem.isDefined,
        "When specifying the server certificate PEM file, the key PEM file must also be provided.")
    }
    maybeTrustStore.foreach { _ =>
      require(maybeSubmitClientCertPem.isEmpty,
        "Cannot specify client cert file and truststore file; must specify only one or the other.")
    }
  }

  private def isValidSslFileScheme(rawUri: String): Boolean = {
    val resolvedScheme = Option.apply(Utils.resolveURI(rawUri).getScheme).getOrElse("file")
    resolvedScheme == "file" || resolvedScheme == "local"
  }

  private def buildSslConnectionConfiguration(
      sslConfigurationParameters: DriverSubmitSslConfigurationParameters)
      : (Option[X509TrustManager], SSLContext) = {
    val maybeTrustStore = sslConfigurationParameters.submissionClientCertPem.map { certPem =>
      PemsToKeyStoreConverter.convertCertPemToTrustStore(
        certPem,
        sslConfigurationParameters.storeBasedSslOptions.trustStoreType)
    }.orElse(sslConfigurationParameters.storeBasedSslOptions.trustStore.map { trustStoreFile =>
      if (!trustStoreFile.isFile) {
        throw new SparkException(s"TrustStore file at ${trustStoreFile.getAbsolutePath}" +
          s" does not exist or is not a file.")
      }
      val trustStore = KeyStore.getInstance(
        sslConfigurationParameters
          .storeBasedSslOptions
          .trustStoreType
          .getOrElse(KeyStore.getDefaultType))
      Utils.tryWithResource(new FileInputStream(trustStoreFile)) { trustStoreStream =>
        val trustStorePassword = sslConfigurationParameters
          .storeBasedSslOptions
          .trustStorePassword
          .map(_.toCharArray)
          .orNull
        trustStore.load(trustStoreStream, trustStorePassword)
      }
      trustStore
    })
    maybeTrustStore.map { trustStore =>
      val trustManagerFactory = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm)
      trustManagerFactory.init(trustStore)
      val trustManagers = trustManagerFactory.getTrustManagers
      val sslContext = SSLContext.getInstance("TLSv1.2")
      sslContext.init(null, trustManagers, SECURE_RANDOM)
      (Option.apply(trustManagers(0).asInstanceOf[X509TrustManager]), sslContext)
    }.getOrElse((Option.empty[X509TrustManager], SSLContext.getDefault))
  }
}
