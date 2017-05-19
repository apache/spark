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
package org.apache.spark.deploy.kubernetes.integrationtest

import java.util.UUID

import io.fabric8.kubernetes.client.internal.readiness.Readiness
import org.scalatest.{BeforeAndAfter, DoNotDiscover}
import org.scalatest.concurrent.Eventually
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.SSLUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.integrationtest.backend.IntegrationTestBackend
import org.apache.spark.deploy.kubernetes.integrationtest.backend.minikube.Minikube
import org.apache.spark.deploy.kubernetes.integrationtest.constants.MINIKUBE_TEST_BACKEND
import org.apache.spark.deploy.kubernetes.submit.v2.{Client, KeyAndCertPem}
import org.apache.spark.launcher.SparkLauncher

@DoNotDiscover
private[spark] class KubernetesV2Suite(testBackend: IntegrationTestBackend)
  extends SparkFunSuite with BeforeAndAfter {

  private val APP_LOCATOR_LABEL = UUID.randomUUID().toString.replaceAll("-", "")
  private var kubernetesTestComponents: KubernetesTestComponents = _
  private var sparkConf: SparkConf = _
  private var resourceStagingServerLauncher: ResourceStagingServerLauncher = _
  private var staticAssetServerLauncher: StaticAssetServerLauncher = _

  override def beforeAll(): Unit = {
    kubernetesTestComponents = new KubernetesTestComponents(testBackend.getKubernetesClient)
    resourceStagingServerLauncher = new ResourceStagingServerLauncher(
      kubernetesTestComponents.kubernetesClient.inNamespace(kubernetesTestComponents.namespace))
    staticAssetServerLauncher = new StaticAssetServerLauncher(
      kubernetesTestComponents.kubernetesClient.inNamespace(kubernetesTestComponents.namespace))
  }

  before {
    sparkConf = kubernetesTestComponents.newSparkConf()
      .set(INIT_CONTAINER_DOCKER_IMAGE, s"spark-driver-init:latest")
      .set(DRIVER_DOCKER_IMAGE, s"spark-driver-v2:latest")
      .set(KUBERNETES_DRIVER_LABELS, s"spark-app-locator=$APP_LOCATOR_LABEL")
    kubernetesTestComponents.createNamespace()
  }

  after {
    kubernetesTestComponents.deleteNamespace()
  }

  test("Use submission v2.") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    launchStagingServer(SSLOptions(), None)
    runSparkPiAndVerifyCompletion(KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Enable SSL on the submission server") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    val keyStoreAndTrustStore = SSLUtils.generateKeyStoreTrustStorePair(
      ipAddress = Minikube.getMinikubeIp,
      keyStorePassword = "keyStore",
      keyPassword = "key",
      trustStorePassword = "trustStore")
    sparkConf.set(RESOURCE_STAGING_SERVER_SSL_ENABLED, true)
      .set("spark.ssl.kubernetes.resourceStagingServer.keyStore",
          keyStoreAndTrustStore.keyStore.getAbsolutePath)
      .set("spark.ssl.kubernetes.resourceStagingServer.trustStore",
          keyStoreAndTrustStore.trustStore.getAbsolutePath)
      .set("spark.ssl.kubernetes.resourceStagingServer.keyStorePassword", "keyStore")
      .set("spark.ssl.kubernetes.resourceStagingServer.keyPassword", "key")
      .set("spark.ssl.kubernetes.resourceStagingServer.trustStorePassword", "trustStore")
    launchStagingServer(SSLOptions(
      enabled = true,
      keyStore = Some(keyStoreAndTrustStore.keyStore),
      trustStore = Some(keyStoreAndTrustStore.trustStore),
      keyStorePassword = Some("keyStore"),
      keyPassword = Some("key"),
      trustStorePassword = Some("trustStore")),
      None)
    runSparkPiAndVerifyCompletion(KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Use container-local resources without the resource staging server") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    sparkConf.setJars(Seq(
      KubernetesSuite.CONTAINER_LOCAL_HELPER_JAR_PATH))
    runSparkPiAndVerifyCompletion(KubernetesSuite.CONTAINER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Dynamic executor scaling basic test") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    launchStagingServer(SSLOptions(), None)
    createShuffleServiceDaemonSet()

    sparkConf.setJars(Seq(KubernetesSuite.CONTAINER_LOCAL_HELPER_JAR_PATH))
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.kubernetes.shuffle.labels", "app=spark-shuffle-service")
    sparkConf.set("spark.kubernetes.shuffle.namespace", kubernetesTestComponents.namespace)
    sparkConf.set("spark.app.name", "group-by-test")
    runSparkGroupByTestAndVerifyCompletion(KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Use remote resources without the resource staging server.") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    val assetServerUri = staticAssetServerLauncher.launchStaticAssetServer()
    sparkConf.setJars(Seq(
      s"$assetServerUri/${KubernetesSuite.EXAMPLES_JAR_FILE.getName}",
      s"$assetServerUri/${KubernetesSuite.HELPER_JAR_FILE.getName}"
    ))
    runSparkPiAndVerifyCompletion(SparkLauncher.NO_RESOURCE)
  }

  test("Mix remote resources with submitted ones.") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    launchStagingServer(SSLOptions(), None)
    val assetServerUri = staticAssetServerLauncher.launchStaticAssetServer()
    sparkConf.setJars(Seq(
      KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      s"$assetServerUri/${KubernetesSuite.HELPER_JAR_FILE.getName}"
    ))
    runSparkPiAndVerifyCompletion(SparkLauncher.NO_RESOURCE)
  }

  test("Use key and certificate PEM files for TLS.") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    val keyAndCertificate = SSLUtils.generateKeyCertPemPair(Minikube.getMinikubeIp)
    launchStagingServer(
        SSLOptions(enabled = true),
        Some(keyAndCertificate))
    sparkConf.set(RESOURCE_STAGING_SERVER_SSL_ENABLED, true)
        .set(
            RESOURCE_STAGING_SERVER_CLIENT_CERT_PEM.key, keyAndCertificate.certPem.getAbsolutePath)
    runSparkPiAndVerifyCompletion(KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Use client key and client cert file when requesting executors") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    sparkConf.setJars(Seq(
        KubernetesSuite.CONTAINER_LOCAL_MAIN_APP_RESOURCE,
        KubernetesSuite.CONTAINER_LOCAL_HELPER_JAR_PATH))
    sparkConf.set(KUBERNETES_DRIVER_CLIENT_KEY_FILE,
        kubernetesTestComponents.clientConfig.getClientKeyFile)
    sparkConf.set(KUBERNETES_DRIVER_CLIENT_CERT_FILE,
        kubernetesTestComponents.clientConfig.getClientCertFile)
    sparkConf.set(KUBERNETES_DRIVER_CA_CERT_FILE,
        kubernetesTestComponents.clientConfig.getCaCertFile)
    runSparkPiAndVerifyCompletion(SparkLauncher.NO_RESOURCE)
  }

  private def launchStagingServer(
      resourceStagingServerSslOptions: SSLOptions, keyAndCertPem: Option[KeyAndCertPem]): Unit = {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    val resourceStagingServerPort = resourceStagingServerLauncher.launchStagingServer(
      resourceStagingServerSslOptions, keyAndCertPem)
    val resourceStagingServerUriScheme = if (resourceStagingServerSslOptions.enabled) {
      "https"
    } else {
      "http"
    }
    sparkConf.set(RESOURCE_STAGING_SERVER_URI,
      s"$resourceStagingServerUriScheme://" +
        s"${Minikube.getMinikubeIp}:$resourceStagingServerPort")
  }

  private def runSparkPiAndVerifyCompletion(appResource: String): Unit = {
    Client.run(sparkConf, appResource, KubernetesSuite.SPARK_PI_MAIN_CLASS, Array.empty[String])
    val driverPod = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", APP_LOCATOR_LABEL)
      .list()
      .getItems
      .get(0)
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      assert(kubernetesTestComponents.kubernetesClient
        .pods()
        .withName(driverPod.getMetadata.getName)
        .getLog
        .contains("Pi is roughly 3"), "The application did not compute the value of pi.")
    }
  }

  private def runSparkGroupByTestAndVerifyCompletion(appResource: String): Unit = {
    Client.run(
      sparkConf = sparkConf,
      appArgs = Array.empty[String],
      mainClass = KubernetesSuite.GROUP_BY_MAIN_CLASS,
      mainAppResource = appResource)
    val driverPod = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", APP_LOCATOR_LABEL)
      .list()
      .getItems
      .get(0)
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      assert(kubernetesTestComponents.kubernetesClient
        .pods()
        .withName(driverPod.getMetadata.getName)
        .getLog
        .contains("The Result is"), "The application did not complete.")
    }
  }

  private def createShuffleServiceDaemonSet(): Unit = {
    val ds = kubernetesTestComponents.kubernetesClient.extensions().daemonSets()
      .createNew()
        .withNewMetadata()
        .withName("shuffle")
      .endMetadata()
      .withNewSpec()
        .withNewTemplate()
          .withNewMetadata()
            .withLabels(Map("app" -> "spark-shuffle-service").asJava)
          .endMetadata()
          .withNewSpec()
            .addNewVolume()
              .withName("shuffle-dir")
              .withNewHostPath()
                .withPath("/tmp")
              .endHostPath()
            .endVolume()
            .addNewContainer()
              .withName("shuffle")
              .withImage("spark-shuffle:latest")
              .withImagePullPolicy("IfNotPresent")
              .addNewVolumeMount()
                .withName("shuffle-dir")
                .withMountPath("/tmp")
              .endVolumeMount()
            .endContainer()
          .endSpec()
        .endTemplate()
      .endSpec()
      .done()

    // wait for daemonset to become available.
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val pods = kubernetesTestComponents.kubernetesClient.pods()
        .withLabel("app", "spark-shuffle-service").list().getItems()

      if (pods.size() == 0 || Readiness.isReady(pods.get(0))) {
        throw KubernetesSuite.ShuffleNotReadyException()
      }
    }
  }
}
