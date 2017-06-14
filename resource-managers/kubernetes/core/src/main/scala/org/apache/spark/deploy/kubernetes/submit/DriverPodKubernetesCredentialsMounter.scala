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
package org.apache.spark.deploy.kubernetes.submit

import io.fabric8.kubernetes.api.model.{PodBuilder, Secret, SecretBuilder}
import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.KubernetesCredentials
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._

private[spark] trait DriverPodKubernetesCredentialsMounter {

  /**
   * Set fields on the Spark configuration that indicate where the driver pod is
   * to find its Kubernetes credentials for requesting executors.
   */
  def setDriverPodKubernetesCredentialLocations(sparkConf: SparkConf): SparkConf

  /**
   * Create the Kubernetes secret object that correspond to the driver's credentials
   * that have to be created and mounted into the driver pod. The single Secret
   * object contains all of the data entries for the driver pod's Kubernetes
   * credentials. Returns empty if no secrets are to be mounted.
   */
  def createCredentialsSecret(): Option[Secret]

  /**
   * Mount any Kubernetes credentials from the submitting machine's disk into the driver pod. The
   * secret that is passed in here should have been created from createCredentialsSecret so that
   * the implementation does not need to hold its state.
   */
  def mountDriverKubernetesCredentials(
    originalPodSpec: PodBuilder,
    driverContainerName: String,
    credentialsSecret: Option[Secret]): PodBuilder
}

private[spark] class DriverPodKubernetesCredentialsMounterImpl(
      kubernetesAppId: String,
      submitterLocalDriverPodKubernetesCredentials: KubernetesCredentials,
      maybeUserSpecifiedMountedClientKeyFile: Option[String],
      maybeUserSpecifiedMountedClientCertFile: Option[String],
      maybeUserSpecifiedMountedOAuthTokenFile: Option[String],
      maybeUserSpecifiedMountedCaCertFile: Option[String])
    extends DriverPodKubernetesCredentialsMounter {

  override def setDriverPodKubernetesCredentialLocations(sparkConf: SparkConf): SparkConf = {
    val resolvedMountedClientKeyFile = resolveSecretLocation(
        maybeUserSpecifiedMountedClientKeyFile,
        submitterLocalDriverPodKubernetesCredentials.clientKeyDataBase64,
        DRIVER_CREDENTIALS_CLIENT_KEY_PATH)
    val resolvedMountedClientCertFile = resolveSecretLocation(
        maybeUserSpecifiedMountedClientCertFile,
        submitterLocalDriverPodKubernetesCredentials.clientCertDataBase64,
        DRIVER_CREDENTIALS_CLIENT_CERT_PATH)
    val resolvedMountedCaCertFile = resolveSecretLocation(
        maybeUserSpecifiedMountedCaCertFile,
        submitterLocalDriverPodKubernetesCredentials.caCertDataBase64,
        DRIVER_CREDENTIALS_CA_CERT_PATH)
    val resolvedMountedOAuthTokenFile = resolveSecretLocation(
        maybeUserSpecifiedMountedOAuthTokenFile,
        submitterLocalDriverPodKubernetesCredentials.oauthTokenBase64,
        DRIVER_CREDENTIALS_OAUTH_TOKEN_PATH)
    val sparkConfWithCredentialLocations = sparkConf.clone()
      .setOption(
          s"$APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX",
          resolvedMountedCaCertFile)
      .setOption(
          s"$APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX",
          resolvedMountedClientKeyFile)
      .setOption(
          s"$APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX",
          resolvedMountedClientCertFile)
      .setOption(
          s"$APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX",
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

  override def createCredentialsSecret(): Option[Secret] = {
    val allSecretData =
      resolveSecretData(
        maybeUserSpecifiedMountedClientKeyFile,
        submitterLocalDriverPodKubernetesCredentials.clientKeyDataBase64,
        DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME) ++
      resolveSecretData(
        maybeUserSpecifiedMountedClientCertFile,
        submitterLocalDriverPodKubernetesCredentials.clientCertDataBase64,
        DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME) ++
      resolveSecretData(
        maybeUserSpecifiedMountedCaCertFile,
        submitterLocalDriverPodKubernetesCredentials.caCertDataBase64,
        DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME) ++
      resolveSecretData(
        maybeUserSpecifiedMountedOAuthTokenFile,
        submitterLocalDriverPodKubernetesCredentials.oauthTokenBase64,
        DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME)
    if (allSecretData.isEmpty) {
      None
    } else {
      Some(new SecretBuilder()
        .withNewMetadata().withName(s"$kubernetesAppId-kubernetes-credentials").endMetadata()
        .withData(allSecretData.asJava)
        .build())
    }
  }

  override def mountDriverKubernetesCredentials(
      originalPodSpec: PodBuilder,
      driverContainerName: String,
      credentialsSecret: Option[Secret]): PodBuilder = {
    credentialsSecret.map { secret =>
      originalPodSpec.editSpec()
        .addNewVolume()
          .withName(DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
          .withNewSecret().withSecretName(secret.getMetadata.getName).endSecret()
          .endVolume()
        .editMatchingContainer(new ContainerNameEqualityPredicate(driverContainerName))
          .addNewVolumeMount()
            .withName(DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
            .withMountPath(DRIVER_CREDENTIALS_SECRETS_BASE_DIR)
            .endVolumeMount()
          .endContainer()
        .endSpec()
    }.getOrElse(originalPodSpec)
  }

  private def resolveSecretLocation(
      mountedUserSpecified: Option[String],
      valueMountedFromSubmitter: Option[String],
      mountedCanonicalLocation: String): Option[String] = {
    mountedUserSpecified.orElse(valueMountedFromSubmitter.map( _ => {
      mountedCanonicalLocation
    }))
  }

  private def resolveSecretData(
      mountedUserSpecified: Option[String],
      valueMountedFromSubmitter: Option[String],
      secretName: String): Map[String, String] = {
    mountedUserSpecified.map { _ => Map.empty[String, String]}
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
    option.map( opt => {
      sparkConf.set(configEntry, opt)
    }).getOrElse(sparkConf)
  }
}
