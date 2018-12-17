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

import scala.collection.JavaConverters._

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder, Secret}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec
import org.apache.spark.util.Utils

class DriverKubernetesCredentialsStepSuite extends SparkFunSuite with BeforeAndAfter {

  private val KUBERNETES_RESOURCE_NAME_PREFIX = "spark"
  private var credentialsTempDirectory: File = _
  private val BASE_DRIVER_SPEC = new KubernetesDriverSpec(
    driverPod = new PodBuilder().build(),
    driverContainer = new ContainerBuilder().build(),
    driverSparkConf = new SparkConf(false),
    otherKubernetesResources = Seq.empty[HasMetadata])

  before {
    credentialsTempDirectory = Utils.createTempDir()
  }

  after {
    credentialsTempDirectory.delete()
  }

  test("Don't set any credentials") {
    val kubernetesCredentialsStep = new DriverKubernetesCredentialsStep(
        new SparkConf(false), KUBERNETES_RESOURCE_NAME_PREFIX)
    val preparedDriverSpec = kubernetesCredentialsStep.configureDriver(BASE_DRIVER_SPEC)
    assert(preparedDriverSpec.driverPod === BASE_DRIVER_SPEC.driverPod)
    assert(preparedDriverSpec.driverContainer === BASE_DRIVER_SPEC.driverContainer)
    assert(preparedDriverSpec.otherKubernetesResources.isEmpty)
    assert(preparedDriverSpec.driverSparkConf.getAll.isEmpty)
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

    val kubernetesCredentialsStep = new DriverKubernetesCredentialsStep(
      submissionSparkConf, KUBERNETES_RESOURCE_NAME_PREFIX)
    val preparedDriverSpec = kubernetesCredentialsStep.configureDriver(BASE_DRIVER_SPEC)
    assert(preparedDriverSpec.driverPod === BASE_DRIVER_SPEC.driverPod)
    assert(preparedDriverSpec.driverContainer === BASE_DRIVER_SPEC.driverContainer)
    assert(preparedDriverSpec.otherKubernetesResources.isEmpty)
    assert(preparedDriverSpec.driverSparkConf.getAll.toMap === submissionSparkConf.getAll.toMap)
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
    val kubernetesCredentialsStep = new DriverKubernetesCredentialsStep(
      submissionSparkConf, KUBERNETES_RESOURCE_NAME_PREFIX)
    val preparedDriverSpec = kubernetesCredentialsStep.configureDriver(
      BASE_DRIVER_SPEC.copy(driverSparkConf = submissionSparkConf))
    val expectedSparkConf = Map(
      s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$OAUTH_TOKEN_CONF_SUFFIX" -> "<present_but_redacted>",
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX" ->
        DRIVER_CREDENTIALS_OAUTH_TOKEN_PATH,
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX" ->
        DRIVER_CREDENTIALS_CLIENT_KEY_PATH,
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX" ->
        DRIVER_CREDENTIALS_CLIENT_CERT_PATH,
      s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX" ->
        DRIVER_CREDENTIALS_CA_CERT_PATH,
      s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CLIENT_KEY_FILE_CONF_SUFFIX" ->
        clientKeyFile.getAbsolutePath,
      s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CLIENT_CERT_FILE_CONF_SUFFIX" ->
        clientCertFile.getAbsolutePath,
      s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX" ->
        caCertFile.getAbsolutePath)
    assert(preparedDriverSpec.driverSparkConf.getAll.toMap === expectedSparkConf)
    assert(preparedDriverSpec.otherKubernetesResources.size === 1)
    val credentialsSecret = preparedDriverSpec.otherKubernetesResources.head.asInstanceOf[Secret]
    assert(credentialsSecret.getMetadata.getName ===
      s"$KUBERNETES_RESOURCE_NAME_PREFIX-kubernetes-credentials")
    val decodedSecretData = credentialsSecret.getData.asScala.map { data =>
      (data._1, new String(BaseEncoding.base64().decode(data._2), Charsets.UTF_8))
    }
    val expectedSecretData = Map(
      DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME -> "ca-cert",
      DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME -> "token",
      DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME -> "key",
      DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME -> "cert")
    assert(decodedSecretData === expectedSecretData)
    val driverPodVolumes = preparedDriverSpec.driverPod.getSpec.getVolumes.asScala
    assert(driverPodVolumes.size === 1)
    assert(driverPodVolumes.head.getName === DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
    assert(driverPodVolumes.head.getSecret != null)
    assert(driverPodVolumes.head.getSecret.getSecretName === credentialsSecret.getMetadata.getName)
    val driverContainerVolumeMount = preparedDriverSpec.driverContainer.getVolumeMounts.asScala
    assert(driverContainerVolumeMount.size === 1)
    assert(driverContainerVolumeMount.head.getName === DRIVER_CREDENTIALS_SECRET_VOLUME_NAME)
    assert(driverContainerVolumeMount.head.getMountPath === DRIVER_CREDENTIALS_SECRETS_BASE_DIR)
  }

  private def writeCredentials(credentialsFileName: String, credentialsContents: String): File = {
    val credentialsFile = new File(credentialsTempDirectory, credentialsFileName)
    Files.write(credentialsContents, credentialsFile, Charsets.UTF_8)
    credentialsFile
  }
}
