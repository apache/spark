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

import scala.jdk.CollectionConverters._

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.Secret

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.util.Utils

class DriverKubernetesCredentialsFeatureStepSuite extends SparkFunSuite {

  private val credentialsTempDirectory = Utils.createTempDir()
  private val BASE_DRIVER_POD = SparkPod.initialPod()

  test("Don't set any credentials") {
    val kubernetesConf = KubernetesTestConf.createDriverConf()
    val kubernetesCredentialsStep = new DriverKubernetesCredentialsFeatureStep(kubernetesConf)
    assert(kubernetesCredentialsStep.configurePod(BASE_DRIVER_POD) === BASE_DRIVER_POD)
    assert(kubernetesCredentialsStep.getAdditionalPodSystemProperties().isEmpty)
    assert(kubernetesCredentialsStep.getAdditionalKubernetesResources().isEmpty)
  }

  test("Only set credentials that are manually mounted.") {
    val submissionSparkConf = new SparkConf(false)
      .set(
        s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX",
        "/mnt/secrets/my-token.txt")
      .set(
        s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX",
        "/mnt/secrets/my-key.pem")
      .set(
        s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX",
        "/mnt/secrets/my-cert.pem")
      .set(
        s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX",
        "/mnt/secrets/my-ca.pem")
    val kubernetesConf = KubernetesTestConf.createDriverConf(sparkConf = submissionSparkConf)
    val kubernetesCredentialsStep = new DriverKubernetesCredentialsFeatureStep(kubernetesConf)
    assert(kubernetesCredentialsStep.configurePod(BASE_DRIVER_POD) === BASE_DRIVER_POD)
    assert(kubernetesCredentialsStep.getAdditionalKubernetesResources().isEmpty)
    val resolvedProperties = kubernetesCredentialsStep.getAdditionalPodSystemProperties()
    resolvedProperties.foreach { case (propKey, propValue) =>
      assert(submissionSparkConf.get(propKey) === propValue)
    }
  }

  test("Mount credentials from the submission client as a secret.") {
    val caCertFile = writeCredentials("ca.pem", "ca-cert")
    val clientKeyFile = writeCredentials("key.pem", "key")
    val clientCertFile = writeCredentials("cert.pem", "cert")
    val submissionSparkConf = new SparkConf(false)
      .set(
        s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$OAUTH_TOKEN_CONF_SUFFIX",
        "token")
      .set(
        s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX",
        clientKeyFile.getAbsolutePath)
      .set(
        s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX",
        clientCertFile.getAbsolutePath)
      .set(
        s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX",
        caCertFile.getAbsolutePath)
    val kubernetesConf = KubernetesTestConf.createDriverConf(sparkConf = submissionSparkConf)
    val kubernetesCredentialsStep = new DriverKubernetesCredentialsFeatureStep(kubernetesConf)
    val resolvedProperties = kubernetesCredentialsStep.getAdditionalPodSystemProperties()
    val expectedSparkConf = Map(
      s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$OAUTH_TOKEN_CONF_SUFFIX" -> "<present_but_redacted>",
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX" ->
        DRIVER_CREDENTIALS_OAUTH_TOKEN_PATH,
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX" ->
        DRIVER_CREDENTIALS_CLIENT_KEY_PATH,
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX" ->
        DRIVER_CREDENTIALS_CLIENT_CERT_PATH,
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX" ->
        DRIVER_CREDENTIALS_CA_CERT_PATH)
    assert(resolvedProperties === expectedSparkConf)
    assert(kubernetesCredentialsStep.getAdditionalKubernetesResources().size === 1)
    val credentialsSecret = kubernetesCredentialsStep
      .getAdditionalKubernetesResources()
      .head
      .asInstanceOf[Secret]
    assert(credentialsSecret.getMetadata.getName ===
      s"${kubernetesConf.resourceNamePrefix}-kubernetes-credentials")
    val decodedSecretData = credentialsSecret.getData.asScala.map { data =>
      (data._1, new String(BaseEncoding.base64().decode(data._2), Charsets.UTF_8))
    }
    val expectedSecretData = Map(
      DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME -> "ca-cert",
      DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME -> "token",
      DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME -> "key",
      DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME -> "cert")
    assert(decodedSecretData === expectedSecretData)
    val driverPod = kubernetesCredentialsStep.configurePod(BASE_DRIVER_POD)
    val driverPodVolumes = driverPod.pod.getSpec.getVolumes.asScala
    assert(driverPodVolumes.size === 1)
    assert(driverPodVolumes.head.getName === DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
    assert(driverPodVolumes.head.getSecret != null)
    assert(driverPodVolumes.head.getSecret.getSecretName === credentialsSecret.getMetadata.getName)
    val driverContainerVolumeMount = driverPod.container.getVolumeMounts.asScala
    assert(driverContainerVolumeMount.size === 1)
    assert(driverContainerVolumeMount.head.getName === DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
    assert(driverContainerVolumeMount.head.getMountPath === DRIVER_CREDENTIALS_SECRETS_BASE_DIR)
  }

  private def writeCredentials(credentialsFileName: String, credentialsContents: String): File = {
    val credentialsFile = new File(credentialsTempDirectory, credentialsFileName)
    Files.asCharSink(credentialsFile, Charsets.UTF_8).write(credentialsContents)
    credentialsFile
  }
}
