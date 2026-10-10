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

import io.fabric8.kubernetes.api.model.{PodBuilder, Secret}
import org.apache.logging.log4j.Level

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.util.Utils

class DriverKubernetesCredentialsFeatureStepSuite extends SparkFunSuite {

  private val credentialsTempDirectory = Utils.createTempDir()
  private val BASE_DRIVER_POD = SparkPod.initialPod()
  private val SERVICE_ACCOUNT_CONF = KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME.key
  private val STEP_LOGGER = classOf[DriverKubernetesCredentialsFeatureStep].getName

  test("Don't set any credentials") {
    val kubernetesConf = KubernetesTestConf.createDriverConf()
    val kubernetesCredentialsStep = new DriverKubernetesCredentialsFeatureStep(kubernetesConf)
    assert(kubernetesCredentialsStep.configurePod(BASE_DRIVER_POD) === BASE_DRIVER_POD)
    assert(kubernetesCredentialsStep.getAdditionalPodSystemProperties().isEmpty)
    assert(kubernetesCredentialsStep.getAdditionalPreKubernetesResources().isEmpty)
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
    assert(kubernetesCredentialsStep.getAdditionalPreKubernetesResources().isEmpty)
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
    assert(kubernetesCredentialsStep.getAdditionalPreKubernetesResources().size === 1)
    val credentialsSecret = kubernetesCredentialsStep
      .getAdditionalPreKubernetesResources()
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
    val caCertConf = s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX"
    val caCertFile = writeCredentials("sa-ca.pem", "ca-cert")
    val stepUnderTest = stepWith(
      SERVICE_ACCOUNT_CONF -> "spark", caCertConf -> caCertFile.getAbsolutePath)
    val logAppender = new LogAppender
    val configuredPod = withLogAppenderReturning(logAppender) {
      stepUnderTest.configurePod(BASE_DRIVER_POD)
    }
    // The documented behavior the warning describes: the credentials win, so the account is
    // never applied, and with no account on the spec the pod falls back to the namespace default.
    assert(configuredPod.pod.getSpec.getServiceAccount === null)
    assert(configuredPod.pod.getSpec.getServiceAccountName === null)
    val warnings = warningsFrom(logAppender)
    val named = warnings.filter(w => w.contains(SERVICE_ACCOUNT_CONF) && w.contains(caCertConf))
    assert(named.size === 1, s"expected one warning naming both $SERVICE_ACCOUNT_CONF and " +
      s"$caCertConf, got: $warnings")
    assert(named.head.contains("namespace's default"),
      s"warning does not say what the pod falls back to: ${named.head}")
    // Only the credentials actually submitted are named, so a message that lists all four fails.
    Seq(OAUTH_TOKEN_CONF_SUFFIX, CLIENT_KEY_FILE_CONF_SUFFIX, CLIENT_CERT_FILE_CONF_SUFFIX)
      .map(suffix => s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$suffix")
      .foreach(conf => assert(!named.head.contains(conf),
        s"warning names $conf, which was not set: ${named.head}"))
    // The way out of the conflict is worth spelling out, so require it stays in the message.
    assert(named.head.contains(s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.*"),
      s"warning does not point at the mounted configs: ${named.head}")

    // A template naming a different account does lose the configured one, so it must warn and say
    // which account the pod keeps. The second case pins `serviceAccountName` beating the alias.
    Seq(
      podWithAccount(serviceAccountName = Some("other")),
      podWithAccount(serviceAccount = Some("spark"), serviceAccountName = Some("other"))
    ).foreach { otherAccountPod =>
      val otherAppender = new LogAppender
      withLogAppender(otherAppender, loggerNames = Seq(STEP_LOGGER)) {
        stepUnderTest.configurePod(otherAccountPod)
      }
      val otherWarnings = warningsFrom(otherAppender).filter(_.contains(SERVICE_ACCOUNT_CONF))
      assert(otherWarnings.size === 1,
        s"expected one warning for a pod running as " +
          s"${otherAccountPod.pod.getSpec.getServiceAccountName}/" +
          s"${otherAccountPod.pod.getSpec.getServiceAccount}, got: $otherWarnings")
      assert(otherWarnings.head.contains("other"),
        s"warning does not name the account the pod keeps: ${otherWarnings.head}")
    }
  }

  test("SPARK-58872: stay quiet when the driver service account survives") {
    // With the account alone there is nothing to mount, so it is applied and nothing is warned.
    val saOnlyAppender = new LogAppender
    val saOnlyPod = withLogAppenderReturning(saOnlyAppender) {
      stepWith(SERVICE_ACCOUNT_CONF -> "spark").configurePod(BASE_DRIVER_POD)
    }
    assert(saOnlyPod.pod.getSpec.getServiceAccount === "spark")
    assert(saOnlyPod.pod.getSpec.getServiceAccountName === "spark")
    assert(warningsFrom(saOnlyAppender).isEmpty,
      s"nothing was dropped, so nothing to warn about: ${warningsFrom(saOnlyAppender)}")

    // The mounted configs are the escape hatch the message and the docs point at, so setting one
    // alongside the account must neither drop it nor warn. The mounted keys never feed
    // `shouldMountSecret`, so this takes the first branch and never reaches the guard.
    val mountedAppender = new LogAppender
    val mountedPod = withLogAppenderReturning(mountedAppender) {
      stepWith(SERVICE_ACCOUNT_CONF -> "spark",
        s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX" -> "/etc/ca.pem")
        .configurePod(BASE_DRIVER_POD)
    }
    assert(mountedPod.pod.getSpec.getServiceAccount === "spark")
    assert(mountedPod.pod.getSpec.getServiceAccountName === "spark")
    assert(warningsFrom(mountedAppender).isEmpty,
      s"mounted configs must not drop the account: ${warningsFrom(mountedAppender)}")

    // A pod template that already names the same account loses nothing, so there is nothing to say.
    // An explicitly empty `serviceAccountName` means unset, so Kubernetes copies the alias up.
    val caCertFile = writeCredentials("quiet-ca.pem", "ca-cert")
    val stepUnderTest = stepWith(SERVICE_ACCOUNT_CONF -> "spark",
      s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX" ->
        caCertFile.getAbsolutePath)
    Seq(
      podWithAccount(serviceAccountName = Some("spark")),
      podWithAccount(serviceAccount = Some("spark")),
      podWithAccount(serviceAccount = Some("spark"), serviceAccountName = Some("spark")),
      podWithAccount(serviceAccount = Some("spark"), serviceAccountName = Some(""))
    ).foreach { templatePod =>
      val sameAccountAppender = new LogAppender
      withLogAppender(sameAccountAppender, loggerNames = Seq(STEP_LOGGER)) {
        stepUnderTest.configurePod(templatePod)
      }
      assert(warningsFrom(sameAccountAppender).isEmpty,
        s"warned although the pod already runs as spark via " +
          s"${templatePod.pod.getSpec.getServiceAccountName}/" +
          s"${templatePod.pod.getSpec.getServiceAccount}: " +
          s"${warningsFrom(sameAccountAppender)}")
    }
  }

  private def stepWith(confs: (String, String)*): DriverKubernetesCredentialsFeatureStep = {
    val sparkConf = new SparkConf(false)
    confs.foreach { case (k, v) => sparkConf.set(k, v) }
    new DriverKubernetesCredentialsFeatureStep(
      KubernetesTestConf.createDriverConf(sparkConf = sparkConf))
  }

  /**
   * A driver pod whose spec names a service account, standing in for a user pod template. A
   * template is parsed with no API-server defaulting, so it can name either field on its own.
   */
  private def podWithAccount(
      serviceAccount: Option[String] = None,
      serviceAccountName: Option[String] = None): SparkPod = {
    val spec = new PodBuilder(BASE_DRIVER_POD.pod).editOrNewSpec()
    serviceAccount.foreach(spec.withServiceAccount(_))
    serviceAccountName.foreach(spec.withServiceAccountName(_))
    SparkPod(spec.endSpec().build(), BASE_DRIVER_POD.container)
  }

  private def warningsFrom(appender: LogAppender): Seq[String] =
    appender.loggingEvents
      .filter(_.getLevel === Level.WARN)
      .map(_.getMessage.getFormattedMessage)
      .toSeq

  /** `withLogAppender` returns Unit, so carry the block's value out of it. */
  private def withLogAppenderReturning[T](appender: LogAppender)(f: => T): T = {
    var result: Option[T] = None
    withLogAppender(appender, loggerNames = Seq(STEP_LOGGER)) {
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
