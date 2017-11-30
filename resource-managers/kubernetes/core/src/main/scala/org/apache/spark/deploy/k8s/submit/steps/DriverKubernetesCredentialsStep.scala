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
package org.apache.spark.deploy.k8s.submit.steps

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder, Secret, SecretBuilder}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec

/**
 * Mounts Kubernetes credentials into the driver pod. The driver will use such mounted credentials
 * to request executors.
 */
private[spark] class DriverKubernetesCredentialsStep(
    submissionSparkConf: SparkConf,
    kubernetesResourceNamePrefix: String) extends DriverConfigurationStep {

  private val maybeMountedOAuthTokenFile = submissionSparkConf.getOption(
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX")
  private val maybeMountedClientKeyFile = submissionSparkConf.getOption(
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX")
  private val maybeMountedClientCertFile = submissionSparkConf.getOption(
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX")
  private val maybeMountedCaCertFile = submissionSparkConf.getOption(
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX")
  private val driverServiceAccount = submissionSparkConf.get(KUBERNETES_SERVICE_ACCOUNT_NAME)

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val driverSparkConf = driverSpec.driverSparkConf.clone()

    val oauthTokenBase64 = submissionSparkConf
        .getOption(s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$OAUTH_TOKEN_CONF_SUFFIX")
        .map { token =>
          BaseEncoding.base64().encode(token.getBytes(StandardCharsets.UTF_8))
        }
    val caCertDataBase64 = safeFileConfToBase64(
        s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX",
        "Driver CA cert file provided at %s does not exist or is not a file.")
    val clientKeyDataBase64 = safeFileConfToBase64(
        s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX",
        "Driver client key file provided at %s does not exist or is not a file.")
    val clientCertDataBase64 = safeFileConfToBase64(
        s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX",
        "Driver client cert file provided at %s does not exist or is not a file.")

    val driverSparkConfWithCredentialsLocations = setDriverPodKubernetesCredentialLocations(
        driverSparkConf,
        oauthTokenBase64,
        caCertDataBase64,
        clientKeyDataBase64,
        clientCertDataBase64)

    val kubernetesCredentialsSecret = createCredentialsSecret(
        oauthTokenBase64,
        caCertDataBase64,
        clientKeyDataBase64,
        clientCertDataBase64)

    val driverPodWithMountedKubernetesCredentials = kubernetesCredentialsSecret.map { secret =>
      new PodBuilder(driverSpec.driverPod)
        .editOrNewSpec()
          .addNewVolume()
            .withName(DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
            .withNewSecret().withSecretName(secret.getMetadata.getName).endSecret()
            .endVolume()
          .endSpec()
        .build()
    }.getOrElse(
      driverServiceAccount.map { account =>
        new PodBuilder(driverSpec.driverPod)
          .editOrNewSpec()
          .withServiceAccount(account)
          .withServiceAccountName(account)
          .endSpec()
          .build()
      }.getOrElse(driverSpec.driverPod)
    )

    val driverContainerWithMountedSecretVolume = kubernetesCredentialsSecret.map { secret =>
      new ContainerBuilder(driverSpec.driverContainer)
        .addNewVolumeMount()
          .withName(DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
          .withMountPath(DRIVER_CREDENTIALS_SECRETS_BASE_DIR)
          .endVolumeMount()
        .build()
    }.getOrElse(driverSpec.driverContainer)

    driverSpec.copy(
      driverPod = driverPodWithMountedKubernetesCredentials,
      otherKubernetesResources =
        driverSpec.otherKubernetesResources ++ kubernetesCredentialsSecret.toSeq,
      driverSparkConf = driverSparkConfWithCredentialsLocations,
      driverContainer = driverContainerWithMountedSecretVolume)
  }

  private def createCredentialsSecret(
      driverOAuthTokenBase64: Option[String],
      driverCaCertDataBase64: Option[String],
      driverClientKeyDataBase64: Option[String],
      driverClientCertDataBase64: Option[String]): Option[Secret] = {
    val allSecretData =
      resolveSecretData(
        maybeMountedClientKeyFile,
        driverClientKeyDataBase64,
        DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME) ++
      resolveSecretData(
        maybeMountedClientCertFile,
        driverClientCertDataBase64,
        DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME) ++
      resolveSecretData(
        maybeMountedCaCertFile,
        driverCaCertDataBase64,
        DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME) ++
      resolveSecretData(
        maybeMountedOAuthTokenFile,
        driverOAuthTokenBase64,
        DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME)

    if (allSecretData.isEmpty) {
      None
    } else {
      Some(new SecretBuilder()
        .withNewMetadata()
          .withName(s"$kubernetesResourceNamePrefix-kubernetes-credentials")
          .endMetadata()
        .withData(allSecretData.asJava)
        .build())
    }
  }

  private def setDriverPodKubernetesCredentialLocations(
      driverSparkConf: SparkConf,
      driverOauthTokenBase64: Option[String],
      driverCaCertDataBase64: Option[String],
      driverClientKeyDataBase64: Option[String],
      driverClientCertDataBase64: Option[String]): SparkConf = {
    val resolvedMountedOAuthTokenFile = resolveSecretLocation(
      maybeMountedOAuthTokenFile,
      driverOauthTokenBase64,
      DRIVER_CREDENTIALS_OAUTH_TOKEN_PATH)
    val resolvedMountedClientKeyFile = resolveSecretLocation(
      maybeMountedClientKeyFile,
      driverClientKeyDataBase64,
      DRIVER_CREDENTIALS_CLIENT_KEY_PATH)
    val resolvedMountedClientCertFile = resolveSecretLocation(
      maybeMountedClientCertFile,
      driverClientCertDataBase64,
      DRIVER_CREDENTIALS_CLIENT_CERT_PATH)
    val resolvedMountedCaCertFile = resolveSecretLocation(
      maybeMountedCaCertFile,
      driverCaCertDataBase64,
      DRIVER_CREDENTIALS_CA_CERT_PATH)

    val sparkConfWithCredentialLocations = driverSparkConf
      .setOption(
        s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX",
        resolvedMountedCaCertFile)
      .setOption(
        s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX",
        resolvedMountedClientKeyFile)
      .setOption(
        s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX",
        resolvedMountedClientCertFile)
      .setOption(
        s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX",
        resolvedMountedOAuthTokenFile)

    // Redact all OAuth token values
    sparkConfWithCredentialLocations
      .getAll
      .filter(_._1.endsWith(OAUTH_TOKEN_CONF_SUFFIX)).map(_._1)
      .foreach {
        sparkConfWithCredentialLocations.set(_, "<present_but_redacted>")
      }
    sparkConfWithCredentialLocations
  }

  private def safeFileConfToBase64(
      conf: String,
      fileNotFoundFormatString: String): Option[String] = {
    submissionSparkConf.getOption(conf)
      .map(new File(_))
      .map { file =>
        require(file.isFile, String.format(fileNotFoundFormatString, file.getAbsolutePath))
        BaseEncoding.base64().encode(Files.toByteArray(file))
      }
  }

  private def resolveSecretLocation(
      mountedUserSpecified: Option[String],
      valueMountedFromSubmitter: Option[String],
      mountedCanonicalLocation: String): Option[String] = {
    mountedUserSpecified.orElse(valueMountedFromSubmitter.map( _ =>
      mountedCanonicalLocation
    ))
  }

  private def resolveSecretData(
      mountedUserSpecified: Option[String],
      valueMountedFromSubmitter: Option[String],
      secretName: String): Map[String, String] = {
    mountedUserSpecified.map { _ => Map.empty[String, String] }
      .getOrElse {
        valueMountedFromSubmitter.map { valueBase64 =>
          Map(secretName -> valueBase64)
        }.getOrElse(Map.empty[String, String])
      }
  }

  private implicit def augmentSparkConf(sparkConf: SparkConf): OptionSettableSparkConf = {
    new OptionSettableSparkConf(sparkConf)
  }
}

private class OptionSettableSparkConf(sparkConf: SparkConf) {
  def setOption(configEntry: String, option: Option[String]): SparkConf = {
    option.map( opt =>
      sparkConf.set(configEntry, opt)
    ).getOrElse(sparkConf)
  }
}
