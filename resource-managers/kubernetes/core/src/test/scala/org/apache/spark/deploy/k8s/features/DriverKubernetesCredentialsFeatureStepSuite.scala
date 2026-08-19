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

import io.fabric8.kubernetes.api.model.Secret
import org.apache.logging.log4j.Level

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
      (data._1, new String(Base64.getDecoder().decode(data._2), StandardCharsets.UTF_8))
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

  test("SPARK-58872: warn when driver credentials drop the driver service account") {
    val serviceAccountConf = KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME.key
    val caCertConf = s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX"
    val caCertFile = writeCredentials("sa-ca.pem", "ca-cert")
    val submissionSparkConf = new SparkConf(false)
      .set(serviceAccountConf, "spark")
      .set(caCertConf, caCertFile.getAbsolutePath)
    val kubernetesConf = KubernetesTestConf.createDriverConf(sparkConf = submissionSparkConf)
    val stepUnderTest = new DriverKubernetesCredentialsFeatureStep(kubernetesConf)
    val logAppender = new LogAppender
    val configuredPod = withLogAppenderReturning(logAppender) {
      stepUnderTest.configurePod(BASE_DRIVER_POD)
    }
    // The documented behavior the warning describes: the credentials win, so the account is
    // never applied.
    assert(configuredPod.pod.getSpec.getServiceAccount === null)
    assert(configuredPod.pod.getSpec.getServiceAccountName === null)
    val warnings = warningsFrom(logAppender)
    val named = warnings.filter(w => w.contains(serviceAccountConf) && w.contains(caCertConf))
    assert(named.size === 1, s"expected one warning naming both $serviceAccountConf and " +
      s"$caCertConf, got: $warnings")
    // Only the credentials actually submitted are named, so a message that lists all four fails.
    Seq(OAUTH_TOKEN_CONF_SUFFIX, CLIENT_KEY_FILE_CONF_SUFFIX, CLIENT_CERT_FILE_CONF_SUFFIX)
      .map(suffix => s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$suffix")
      .foreach(conf => assert(!named.head.contains(conf),
        s"warning names $conf, which was not set: ${named.head}"))

    // With the account alone there is nothing to mount, so it is applied and nothing is warned.
    val saOnlyConf = KubernetesTestConf.createDriverConf(
      sparkConf = new SparkConf(false).set(serviceAccountConf, "spark"))
    val saOnlyStep = new DriverKubernetesCredentialsFeatureStep(saOnlyConf)
    val saOnlyAppender = new LogAppender
    val saOnlyPod = withLogAppenderReturning(saOnlyAppender) {
      saOnlyStep.configurePod(BASE_DRIVER_POD)
    }
    assert(saOnlyPod.pod.getSpec.getServiceAccount === "spark")
    assert(saOnlyPod.pod.getSpec.getServiceAccountName === "spark")
    assert(!warningsFrom(saOnlyAppender).exists(_.contains(serviceAccountConf)))
  }

  private def warningsFrom(appender: LogAppender): Seq[String] =
    appender.loggingEvents
      .filter(_.getLevel === Level.WARN)
      .map(_.getMessage.getFormattedMessage)
      .toSeq

  /** `withLogAppender` returns Unit, so carry the block's value out of it. */
  private def withLogAppenderReturning[T](appender: LogAppender)(f: => T): T = {
    val stepLogger = classOf[DriverKubernetesCredentialsFeatureStep].getName
    var result: Option[T] = None
    withLogAppender(appender, loggerNames = Seq(stepLogger)) {
      result = Some(f)
    }
    result.get
  }

  private def writeCredentials(credentialsFileName: String, credentialsContents: String): File = {
    val credentialsFile = new File(credentialsTempDirectory, credentialsFileName)
    Files.writeString(credentialsFile.toPath, credentialsContents)
    credentialsFile
  }
}
