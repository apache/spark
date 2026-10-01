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
package org.apache.spark.deploy.k8s.features

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder, Secret, SecretBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesUtils.{buildPodWithServiceAccount, podServiceAccount}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, CONFIGS, PREFIX, SERVICE_ACCOUNT_NAME, VALUE}

private[spark] class DriverKubernetesCredentialsFeatureStep(kubernetesConf: KubernetesConf)
  extends KubernetesFeatureConfigStep with Logging {

  private val maybeMountedOAuthTokenFile = kubernetesConf.getOption(
    s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX")
  private val maybeMountedClientKeyFile = kubernetesConf.getOption(
    s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX")
  private val maybeMountedClientCertFile = kubernetesConf.getOption(
    s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX")
  private val maybeMountedCaCertFile = kubernetesConf.getOption(
    s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX")
  private val driverServiceAccount = kubernetesConf.get(KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME)

  private val oauthTokenBase64 = kubernetesConf
    .getOption(s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$OAUTH_TOKEN_CONF_SUFFIX")
    .map { token =>
      Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8))
    }

  private val caCertDataBase64 = safeFileConfToBase64(
    s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX",
    "Driver CA cert file")
  private val clientKeyDataBase64 = safeFileConfToBase64(
    s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX",
    "Driver client key file")
  private val clientCertDataBase64 = safeFileConfToBase64(
    s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX",
    "Driver client cert file")

  private val submittedCredentialConfs = Seq(
    OAUTH_TOKEN_CONF_SUFFIX -> oauthTokenBase64,
    CA_CERT_FILE_CONF_SUFFIX -> caCertDataBase64,
    CLIENT_KEY_FILE_CONF_SUFFIX -> clientKeyDataBase64,
    CLIENT_CERT_FILE_CONF_SUFFIX -> clientCertDataBase64)
    .collect { case (suffix, credential) if credential.isDefined =>
      s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$suffix"
    }

  private val shouldMountSecret = submittedCredentialConfs.nonEmpty

  private val driverCredentialsSecretName =
    s"${kubernetesConf.resourceNamePrefix}-kubernetes-credentials"

  override def configurePod(pod: SparkPod): SparkPod = {
    if (!shouldMountSecret) {
      pod.copy(pod = buildPodWithServiceAccount(driverServiceAccount, pod).getOrElse(pod.pod))
    } else {
      // The credentials secret takes precedence over the driver service account: this branch never
      // applies the account, so warn that the pod keeps whatever its spec names, or the namespace
      // default. Stay quiet when the spec already names the same account.
      val specServiceAccount = podServiceAccount(pod)
      driverServiceAccount.filterNot(specServiceAccount.contains).foreach { account =>
        val keptAccount = specServiceAccount
          .map(name => log"the pod keeps ${MDC(SERVICE_ACCOUNT_NAME, name)}, named by its spec")
          .getOrElse(log"the pod falls back to the namespace's default account")
        logWarning(log"Not applying " +
          log"${MDC(CONFIG, KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME.key)}=${MDC(VALUE, account)} " +
          log"to the driver pod, because the driver credentials given by " +
          log"${MDC(CONFIGS, submittedCredentialConfs.mkString(", "))} take precedence: " +
          keptAccount + log". To have Spark apply that configuration anyway, put the credentials " +
          log"inside the driver pod and point the " +
          log"${MDC(PREFIX, KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX)}.* configurations at " +
          log"them instead, which does not mount a secret.")
      }
      val driverPodWithMountedKubernetesCredentials =
        new PodBuilder(pod.pod)
          .editOrNewSpec()
            .addNewVolume()
              .withName(DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
              .withNewSecret().withSecretName(driverCredentialsSecretName).endSecret()
              .endVolume()
          .endSpec()
          .build()

      val driverContainerWithMountedSecretVolume =
        new ContainerBuilder(pod.container)
          .addNewVolumeMount()
            .withName(DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
            .withMountPath(DRIVER_CREDENTIALS_SECRETS_BASE_DIR)
            .endVolumeMount()
          .build()
      SparkPod(driverPodWithMountedKubernetesCredentials, driverContainerWithMountedSecretVolume)
    }
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    val resolvedMountedOAuthTokenFile = resolveSecretLocation(
      maybeMountedOAuthTokenFile,
      oauthTokenBase64,
      DRIVER_CREDENTIALS_OAUTH_TOKEN_PATH)
    val resolvedMountedClientKeyFile = resolveSecretLocation(
      maybeMountedClientKeyFile,
      clientKeyDataBase64,
      DRIVER_CREDENTIALS_CLIENT_KEY_PATH)
    val resolvedMountedClientCertFile = resolveSecretLocation(
      maybeMountedClientCertFile,
      clientCertDataBase64,
      DRIVER_CREDENTIALS_CLIENT_CERT_PATH)
    val resolvedMountedCaCertFile = resolveSecretLocation(
      maybeMountedCaCertFile,
      caCertDataBase64,
      DRIVER_CREDENTIALS_CA_CERT_PATH)

    val redactedTokens = kubernetesConf.sparkConf.getAll
      .filter(_._1.endsWith(OAUTH_TOKEN_CONF_SUFFIX))
      .toMap
      .map { case (k, v) => (k, "<present_but_redacted>") }
    redactedTokens ++
      resolvedMountedCaCertFile.map { file =>
        Map(
          s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX" ->
            file)
      }.getOrElse(Map.empty) ++
      resolvedMountedClientKeyFile.map { file =>
        Map(
          s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX" ->
            file)
      }.getOrElse(Map.empty) ++
      resolvedMountedClientCertFile.map { file =>
        Map(
          s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX" ->
            file)
      }.getOrElse(Map.empty) ++
      resolvedMountedOAuthTokenFile.map { file =>
        Map(
          s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX" ->
          file)
      }.getOrElse(Map.empty)
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (shouldMountSecret) {
      Seq(createCredentialsSecret())
    } else {
      Seq.empty
    }
  }

  private def safeFileConfToBase64(conf: String, fileType: String): Option[String] = {
    kubernetesConf.getOption(conf)
      .map(new File(_))
      .map { file =>
        require(file.isFile, String.format("%s provided at %s does not exist or is not a file.",
          fileType, file.getAbsolutePath))
        Base64.getEncoder().encodeToString(Files.readAllBytes(file.toPath))
      }
  }

  /**
   * Resolve a Kubernetes secret data entry from an optional client credential used by the
   * driver to talk to the Kubernetes API server.
   *
   * @param userSpecifiedCredential the optional user-specified client credential.
   * @param secretName name of the Kubernetes secret storing the client credential.
   * @return a secret data entry in the form of a map from the secret name to the secret data,
   *         which may be empty if the user-specified credential is empty.
   */
  private def resolveSecretData(
    userSpecifiedCredential: Option[String],
    secretName: String): Map[String, String] = {
    userSpecifiedCredential.map { valueBase64 =>
      Map(secretName -> valueBase64)
    }.getOrElse(Map.empty[String, String])
  }

  private def resolveSecretLocation(
    mountedUserSpecified: Option[String],
    valueMountedFromSubmitter: Option[String],
    mountedCanonicalLocation: String): Option[String] = {
    mountedUserSpecified.orElse(valueMountedFromSubmitter.map { _ =>
      mountedCanonicalLocation
    })
  }

  private def createCredentialsSecret(): Secret = {
    val allSecretData =
      resolveSecretData(
        clientKeyDataBase64,
        DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME) ++
        resolveSecretData(
          clientCertDataBase64,
          DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME) ++
        resolveSecretData(
          caCertDataBase64,
          DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME) ++
        resolveSecretData(
          oauthTokenBase64,
          DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME)

    new SecretBuilder()
      .withNewMetadata()
        .withName(driverCredentialsSecretName)
        .endMetadata()
      .withImmutable(true)
      .withData(allSecretData.asJava)
      .build()
  }

}
