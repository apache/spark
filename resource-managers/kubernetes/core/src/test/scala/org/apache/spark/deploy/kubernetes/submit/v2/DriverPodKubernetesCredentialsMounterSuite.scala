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
package org.apache.spark.deploy.kubernetes.submit.v2

import io.fabric8.kubernetes.api.model.{PodBuilder, SecretBuilder}
import org.scalatest.prop.TableDrivenPropertyChecks
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.KubernetesCredentials

class DriverPodKubernetesCredentialsMounterSuite
    extends SparkFunSuite with TableDrivenPropertyChecks {

  private val CLIENT_KEY_DATA = "client-key-data"
  private val CLIENT_CERT_DATA = "client-cert-data"
  private val OAUTH_TOKEN_DATA = "oauth-token"
  private val CA_CERT_DATA = "ca-cert-data"
  private val SUBMITTER_LOCAL_DRIVER_KUBERNETES_CREDENTIALS = KubernetesCredentials(
    caCertDataBase64 = Some(CA_CERT_DATA),
    clientKeyDataBase64 = Some(CLIENT_KEY_DATA),
    clientCertDataBase64 = Some(CLIENT_CERT_DATA),
    oauthTokenBase64 = Some(OAUTH_TOKEN_DATA))
  private val APP_ID = "app-id"
  private val USER_SPECIFIED_CLIENT_KEY_FILE = Some("/var/data/client-key.pem")
  private val USER_SPECIFIED_CLIENT_CERT_FILE = Some("/var/data/client-cert.pem")
  private val USER_SPECIFIED_OAUTH_TOKEN_FILE = Some("/var/data/token.txt")
  private val USER_SPECIFIED_CA_CERT_FILE = Some("/var/data/ca.pem")

  // Different configurations of credentials mounters
  private val credentialsMounterWithPreMountedFiles =
    new DriverPodKubernetesCredentialsMounterImpl(
      kubernetesAppId = APP_ID,
      submitterLocalDriverPodKubernetesCredentials = SUBMITTER_LOCAL_DRIVER_KUBERNETES_CREDENTIALS,
      maybeUserSpecifiedMountedClientKeyFile = USER_SPECIFIED_CLIENT_KEY_FILE,
      maybeUserSpecifiedMountedClientCertFile = USER_SPECIFIED_CLIENT_CERT_FILE,
      maybeUserSpecifiedMountedOAuthTokenFile = USER_SPECIFIED_OAUTH_TOKEN_FILE,
      maybeUserSpecifiedMountedCaCertFile = USER_SPECIFIED_CA_CERT_FILE)
  private val credentialsMounterWithoutPreMountedFiles =
    new DriverPodKubernetesCredentialsMounterImpl(
      kubernetesAppId = APP_ID,
      submitterLocalDriverPodKubernetesCredentials = SUBMITTER_LOCAL_DRIVER_KUBERNETES_CREDENTIALS,
      maybeUserSpecifiedMountedClientKeyFile = None,
      maybeUserSpecifiedMountedClientCertFile = None,
      maybeUserSpecifiedMountedOAuthTokenFile = None,
      maybeUserSpecifiedMountedCaCertFile = None)
  private val credentialsMounterWithoutAnyDriverCredentials =
    new DriverPodKubernetesCredentialsMounterImpl(
      APP_ID, KubernetesCredentials(None, None, None, None), None, None, None, None)

  // Test matrices
  private val TEST_MATRIX_EXPECTED_SPARK_CONFS = Table(
      ("Credentials Mounter Implementation",
        "Expected client key file",
        "Expected client cert file",
        "Expected CA Cert file",
        "Expected OAuth Token File"),
      (credentialsMounterWithoutAnyDriverCredentials,
        None,
        None,
        None,
        None),
      (credentialsMounterWithoutPreMountedFiles,
        Some(DRIVER_CREDENTIALS_CLIENT_KEY_PATH),
        Some(DRIVER_CREDENTIALS_CLIENT_CERT_PATH),
        Some(DRIVER_CREDENTIALS_CA_CERT_PATH),
        Some(DRIVER_CREDENTIALS_OAUTH_TOKEN_PATH)),
      (credentialsMounterWithPreMountedFiles,
        USER_SPECIFIED_CLIENT_KEY_FILE,
        USER_SPECIFIED_CLIENT_CERT_FILE,
        USER_SPECIFIED_CA_CERT_FILE,
        USER_SPECIFIED_OAUTH_TOKEN_FILE))

  private val TEST_MATRIX_EXPECTED_CREDENTIALS_SECRET = Table(
      ("Credentials Mounter Implementation", "Expected Credentials Secret Data"),
      (credentialsMounterWithoutAnyDriverCredentials, None),
      (credentialsMounterWithoutPreMountedFiles,
        Some(KubernetesSecretNameAndData(
          data = Map[String, String](
            DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME -> CLIENT_KEY_DATA,
            DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME -> CLIENT_CERT_DATA,
            DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME -> CA_CERT_DATA,
            DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME -> OAUTH_TOKEN_DATA
          ),
          name = s"$APP_ID-kubernetes-credentials"))),
      (credentialsMounterWithPreMountedFiles, None))

  test("Credentials mounter should set the driver's Kubernetes credentials locations") {
    forAll(TEST_MATRIX_EXPECTED_SPARK_CONFS) {
      case (credentialsMounter,
           expectedClientKeyFile,
           expectedClientCertFile,
           expectedCaCertFile,
           expectedOAuthTokenFile) =>
        val baseSparkConf = new SparkConf()
        val resolvedSparkConf =
          credentialsMounter.setDriverPodKubernetesCredentialLocations(baseSparkConf)
        assert(resolvedSparkConf.get(KUBERNETES_DRIVER_MOUNTED_CLIENT_KEY_FILE) ===
            expectedClientKeyFile)
        assert(resolvedSparkConf.get(KUBERNETES_DRIVER_MOUNTED_CLIENT_CERT_FILE) ===
            expectedClientCertFile)
        assert(resolvedSparkConf.get(KUBERNETES_DRIVER_MOUNTED_CA_CERT_FILE) ===
            expectedCaCertFile)
        assert(resolvedSparkConf.get(KUBERNETES_DRIVER_MOUNTED_OAUTH_TOKEN) ===
            expectedOAuthTokenFile)
    }
  }

  test("Credentials mounter should create the correct credentials secret.") {
    forAll(TEST_MATRIX_EXPECTED_CREDENTIALS_SECRET) {
      case (credentialsMounter, expectedSecretNameAndData) =>
        val builtSecret = credentialsMounter.createCredentialsSecret()
        val secretNameAndData = builtSecret.map { secret =>
          KubernetesSecretNameAndData(secret.getMetadata.getName, secret.getData.asScala.toMap)
        }
        assert(secretNameAndData === expectedSecretNameAndData)
    }
  }

  test("When credentials secret is provided, driver pod should mount the secret volume.") {
    val credentialsSecret = new SecretBuilder()
      .withNewMetadata().withName("secret").endMetadata()
      .addToData("secretKey", "secretValue")
      .build()
    val originalPodSpec = new PodBuilder()
      .withNewMetadata().withName("pod").endMetadata()
      .withNewSpec()
        .addNewContainer()
          .withName("container")
          .endContainer()
        .endSpec()
    val podSpecWithMountedDriverKubernetesCredentials =
        credentialsMounterWithoutPreMountedFiles.mountDriverKubernetesCredentials(
          originalPodSpec, "container", Some(credentialsSecret)).build()
    val volumes = podSpecWithMountedDriverKubernetesCredentials.getSpec.getVolumes.asScala
    assert(volumes.exists(_.getName == DRIVER_CREDENTIALS_SECRET_VOLUME_NAME))
    volumes.find(_.getName == DRIVER_CREDENTIALS_SECRET_VOLUME_NAME).foreach { secretVolume =>
      assert(secretVolume.getSecret != null && secretVolume.getSecret.getSecretName == "secret")
    }
  }

  test("When credentials secret is absent, driver pod should not be changed.") {
    val originalPodSpec = new PodBuilder()
    val nonAdjustedPodSpec =
      credentialsMounterWithoutAnyDriverCredentials.mountDriverKubernetesCredentials(
        originalPodSpec, "driver", None)
    assert(nonAdjustedPodSpec === originalPodSpec)
  }
}

private case class KubernetesSecretNameAndData(name: String, data: Map[String, String])
