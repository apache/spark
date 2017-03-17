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
package org.apache.spark.deploy.kubernetes

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{SSLContext, TrustManagerFactory, X509TrustManager}

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{EnvVar, EnvVarBuilder, Secret, Volume, VolumeBuilder, VolumeMount, VolumeMountBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SecurityManager => SparkSecurityManager, SparkConf, SparkException, SSLOptions}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.util.Utils

private[spark] case class SslConfiguration(
  sslOptions: SSLOptions,
  isKeyStoreLocalFile: Boolean,
  sslPodEnvVars: Array[EnvVar],
  sslPodVolumes: Array[Volume],
  sslPodVolumeMounts: Array[VolumeMount],
  sslSecrets: Array[Secret],
  driverSubmitClientTrustManager: Option[X509TrustManager],
  driverSubmitClientSslContext: SSLContext)

private[spark] class SslConfigurationProvider(
    sparkConf: SparkConf,
    kubernetesAppId: String,
    kubernetesClient: KubernetesClient,
    kubernetesResourceCleaner: KubernetesResourceCleaner) {
  private val SECURE_RANDOM = new SecureRandom()
  private val sslSecretsName = s"$SUBMISSION_SSL_SECRETS_PREFIX-$kubernetesAppId"
  private val sslSecretsDirectory = DRIVER_CONTAINER_SUBMISSION_SECRETS_BASE_DIR +
    s"/$kubernetesAppId-ssl"

  def getSslConfiguration(): SslConfiguration = {
    val (driverSubmitSslOptions, isKeyStoreLocalFile) = parseDriverSubmitSslOptions()
    if (driverSubmitSslOptions.enabled) {
      val sslSecretsMap = mutable.HashMap[String, String]()
      val sslEnvs = mutable.Buffer[EnvVar]()
      val secrets = mutable.Buffer[Secret]()
      driverSubmitSslOptions.keyStore.foreach(store => {
        val resolvedKeyStoreFile = if (isKeyStoreLocalFile) {
          if (!store.isFile) {
            throw new SparkException(s"KeyStore specified at $store is not a file or" +
              s" does not exist.")
          }
          val keyStoreBytes = Files.toByteArray(store)
          val keyStoreBase64 = BaseEncoding.base64().encode(keyStoreBytes)
          sslSecretsMap += (SUBMISSION_SSL_KEYSTORE_SECRET_NAME -> keyStoreBase64)
          s"$sslSecretsDirectory/$SUBMISSION_SSL_KEYSTORE_SECRET_NAME"
        } else {
          store.getAbsolutePath
        }
        sslEnvs += new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_FILE)
          .withValue(resolvedKeyStoreFile)
          .build()
      })
      driverSubmitSslOptions.keyStorePassword.foreach(password => {
        val passwordBase64 = BaseEncoding.base64().encode(password.getBytes(Charsets.UTF_8))
        sslSecretsMap += (SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME -> passwordBase64)
        sslEnvs += new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_PASSWORD_FILE)
          .withValue(s"$sslSecretsDirectory/$SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME")
          .build()
      })
      driverSubmitSslOptions.keyPassword.foreach(password => {
        val passwordBase64 = BaseEncoding.base64().encode(password.getBytes(Charsets.UTF_8))
        sslSecretsMap += (SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME -> passwordBase64)
        sslEnvs += new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_KEY_PASSWORD_FILE)
          .withValue(s"$sslSecretsDirectory/$SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME")
          .build()
      })
      driverSubmitSslOptions.keyStoreType.foreach(storeType => {
        sslEnvs += new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_TYPE)
          .withValue(storeType)
          .build()
      })
      sslEnvs += new EnvVarBuilder()
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
      val sslSecrets = kubernetesClient.secrets().createNew()
        .withNewMetadata()
        .withName(sslSecretsName)
        .endMetadata()
        .withData(sslSecretsMap.asJava)
        .withType("Opaque")
        .done()
      kubernetesResourceCleaner.registerOrUpdateResource(sslSecrets)
      secrets += sslSecrets
      val (driverSubmitClientTrustManager, driverSubmitClientSslContext) =
        buildSslConnectionConfiguration(driverSubmitSslOptions)
      SslConfiguration(
        driverSubmitSslOptions,
        isKeyStoreLocalFile,
        sslEnvs.toArray,
        Array(sslVolume),
        Array(sslVolumeMount),
        secrets.toArray,
        driverSubmitClientTrustManager,
        driverSubmitClientSslContext)
    } else {
      SslConfiguration(
        driverSubmitSslOptions,
        isKeyStoreLocalFile,
        Array[EnvVar](),
        Array[Volume](),
        Array[VolumeMount](),
        Array[Secret](),
        None,
        SSLContext.getDefault)
    }
  }

  private def parseDriverSubmitSslOptions(): (SSLOptions, Boolean) = {
    val maybeKeyStore = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_KEYSTORE)
    val resolvedSparkConf = sparkConf.clone()
    val (isLocalKeyStore, resolvedKeyStore) = maybeKeyStore.map(keyStore => {
      val keyStoreURI = Utils.resolveURI(keyStore)
      val isProvidedKeyStoreLocal = keyStoreURI.getScheme match {
        case "file" | null => true
        case "local" => false
        case _ => throw new SparkException(s"Invalid KeyStore URI $keyStore; keyStore URI" +
          " for submit server must have scheme file:// or local:// (no scheme defaults" +
          " to file://)")
      }
      (isProvidedKeyStoreLocal, Option.apply(keyStoreURI.getPath))
    }).getOrElse((false, Option.empty[String]))
    resolvedKeyStore.foreach {
      resolvedSparkConf.set(KUBERNETES_DRIVER_SUBMIT_KEYSTORE, _)
    }
    sparkConf.get(KUBERNETES_DRIVER_SUBMIT_TRUSTSTORE).foreach { trustStore =>
      val trustStoreURI = Utils.resolveURI(trustStore)
      trustStoreURI.getScheme match {
        case "file" | null =>
          resolvedSparkConf.set(KUBERNETES_DRIVER_SUBMIT_TRUSTSTORE, trustStoreURI.getPath)
        case _ => throw new SparkException(s"Invalid trustStore URI $trustStore; trustStore URI" +
          " for submit server must have no scheme, or scheme file://")
      }
    }
    val securityManager = new SparkSecurityManager(resolvedSparkConf)
    (securityManager.getSSLOptions(KUBERNETES_SUBMIT_SSL_NAMESPACE), isLocalKeyStore)
  }

  private def buildSslConnectionConfiguration(driverSubmitSslOptions: SSLOptions):
      (Option[X509TrustManager], SSLContext) = {
    driverSubmitSslOptions.trustStore.map(trustStoreFile => {
      val trustManagerFactory = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm)
      val trustStore = KeyStore.getInstance(
        driverSubmitSslOptions.trustStoreType.getOrElse(KeyStore.getDefaultType))
      if (!trustStoreFile.isFile) {
        throw new SparkException(s"TrustStore file at ${trustStoreFile.getAbsolutePath}" +
          s" does not exist or is not a file.")
      }
      Utils.tryWithResource(new FileInputStream(trustStoreFile)) { trustStoreStream =>
        driverSubmitSslOptions.trustStorePassword match {
          case Some(password) =>
            trustStore.load(trustStoreStream, password.toCharArray)
          case None => trustStore.load(trustStoreStream, null)
        }
      }
      trustManagerFactory.init(trustStore)
      val trustManagers = trustManagerFactory.getTrustManagers
      val sslContext = SSLContext.getInstance("TLSv1.2")
      sslContext.init(null, trustManagers, SECURE_RANDOM)
      (Option.apply(trustManagers(0).asInstanceOf[X509TrustManager]), sslContext)
    }).getOrElse((Option.empty[X509TrustManager], SSLContext.getDefault))
  }
}
