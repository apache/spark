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

import java.io.File
import java.nio.file.Paths
import java.util.UUID

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.client.internal.readiness.Readiness
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.SSLUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.integrationtest.backend.IntegrationTestBackendFactory
import org.apache.spark.deploy.kubernetes.integrationtest.backend.minikube.Minikube
import org.apache.spark.deploy.kubernetes.integrationtest.constants.MINIKUBE_TEST_BACKEND
import org.apache.spark.deploy.kubernetes.submit.{Client, KeyAndCertPem}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.Utils

private[spark] class KubernetesSuite extends SparkFunSuite with BeforeAndAfter {
  import KubernetesSuite._
  private val testBackend = IntegrationTestBackendFactory.getTestBackend()
  private val APP_LOCATOR_LABEL = UUID.randomUUID().toString.replaceAll("-", "")
  private var kubernetesTestComponents: KubernetesTestComponents = _
  private var sparkConf: SparkConf = _
  private var resourceStagingServerLauncher: ResourceStagingServerLauncher = _
  private var staticAssetServerLauncher: StaticAssetServerLauncher = _

  override def beforeAll(): Unit = {
    testBackend.initialize()
    kubernetesTestComponents = new KubernetesTestComponents(testBackend.getKubernetesClient)
    resourceStagingServerLauncher = new ResourceStagingServerLauncher(
      kubernetesTestComponents.kubernetesClient.inNamespace(kubernetesTestComponents.namespace))
    staticAssetServerLauncher = new StaticAssetServerLauncher(
      kubernetesTestComponents.kubernetesClient.inNamespace(kubernetesTestComponents.namespace))
  }

  override def afterAll(): Unit = {
    testBackend.cleanUp()
  }

  before {
    sparkConf = kubernetesTestComponents.newSparkConf()
      .set(INIT_CONTAINER_DOCKER_IMAGE, s"spark-init:latest")
      .set(DRIVER_DOCKER_IMAGE, s"spark-driver:latest")
      .set(KUBERNETES_DRIVER_LABELS, s"spark-app-locator=$APP_LOCATOR_LABEL")
    kubernetesTestComponents.createNamespace()
  }

  after {
    kubernetesTestComponents.deleteNamespace()
  }

  test("Simple submission test with the resource staging server.") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    launchStagingServer(SSLOptions(), None)
    runSparkPiAndVerifyCompletion(SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Enable SSL on the resource staging server") {
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
    runSparkPiAndVerifyCompletion(SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Use container-local resources without the resource staging server") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    sparkConf.setJars(Seq(CONTAINER_LOCAL_HELPER_JAR_PATH))
    runSparkPiAndVerifyCompletion(CONTAINER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Dynamic executor scaling basic test") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    launchStagingServer(SSLOptions(), None)
    createShuffleServiceDaemonSet()

    sparkConf.setJars(Seq(CONTAINER_LOCAL_HELPER_JAR_PATH))
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.kubernetes.shuffle.labels", "app=spark-shuffle-service")
    sparkConf.set("spark.kubernetes.shuffle.namespace", kubernetesTestComponents.namespace)
    sparkConf.set("spark.app.name", "group-by-test")
    runSparkApplicationAndVerifyCompletion(
        SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
        GROUP_BY_MAIN_CLASS,
        "The Result is",
        Array.empty[String])
  }

  test("Use remote resources without the resource staging server.") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    val assetServerUri = staticAssetServerLauncher.launchStaticAssetServer()
    sparkConf.setJars(Seq(
      s"$assetServerUri/${EXAMPLES_JAR_FILE.getName}",
      s"$assetServerUri/${HELPER_JAR_FILE.getName}"
    ))
    runSparkPiAndVerifyCompletion(SparkLauncher.NO_RESOURCE)
  }

  test("Mix remote resources with submitted ones.") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    launchStagingServer(SSLOptions(), None)
    val assetServerUri = staticAssetServerLauncher.launchStaticAssetServer()
    sparkConf.setJars(Seq(
      SUBMITTER_LOCAL_MAIN_APP_RESOURCE, s"$assetServerUri/${HELPER_JAR_FILE.getName}"
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
    runSparkPiAndVerifyCompletion(SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Use client key and client cert file when requesting executors") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    sparkConf.setJars(Seq(
        CONTAINER_LOCAL_MAIN_APP_RESOURCE,
        CONTAINER_LOCAL_HELPER_JAR_PATH))
    sparkConf.set(KUBERNETES_DRIVER_CLIENT_KEY_FILE,
        kubernetesTestComponents.clientConfig.getClientKeyFile)
    sparkConf.set(KUBERNETES_DRIVER_CLIENT_CERT_FILE,
        kubernetesTestComponents.clientConfig.getClientCertFile)
    sparkConf.set(KUBERNETES_DRIVER_CA_CERT_FILE,
        kubernetesTestComponents.clientConfig.getCaCertFile)
    runSparkPiAndVerifyCompletion(SparkLauncher.NO_RESOURCE)
  }

  test("Added files should be placed in the driver's working directory.") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)
    val testExistenceFileTempDir = Utils.createTempDir(namePrefix = "test-existence-file-temp-dir")
    val testExistenceFile = new File(testExistenceFileTempDir, "input.txt")
    Files.write(TEST_EXISTENCE_FILE_CONTENTS, testExistenceFile, Charsets.UTF_8)
    launchStagingServer(SSLOptions(), None)
    sparkConf.set("spark.files", testExistenceFile.getAbsolutePath)
    runSparkApplicationAndVerifyCompletion(
        SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
        FILE_EXISTENCE_MAIN_CLASS,
        s"File found at /opt/spark/${testExistenceFile.getName} with correct contents.",
        Array(testExistenceFile.getName, TEST_EXISTENCE_FILE_CONTENTS))
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
    runSparkApplicationAndVerifyCompletion(
        appResource, SPARK_PI_MAIN_CLASS, "Pi is roughly 3", Array.empty[String])
  }

  private def runSparkApplicationAndVerifyCompletion(
      appResource: String,
      mainClass: String,
      expectedLogOnCompletion: String,
      appArgs: Array[String]): Unit = {
    Client.run(
      sparkConf = sparkConf,
      appArgs = appArgs,
      mainClass = mainClass,
      mainAppResource = appResource)
    val driverPod = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", APP_LOCATOR_LABEL)
      .list()
      .getItems
      .get(0)
    Eventually.eventually(TIMEOUT, INTERVAL) {
      assert(kubernetesTestComponents.kubernetesClient
        .pods()
        .withName(driverPod.getMetadata.getName)
        .getLog
        .contains(expectedLogOnCompletion), "The application did not complete.")
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
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val pods = kubernetesTestComponents.kubernetesClient.pods()
        .withLabel("app", "spark-shuffle-service").list().getItems

      if (pods.size() == 0 || !Readiness.isReady(pods.get(0))) {
        throw ShuffleNotReadyException
      }
    }
  }
}

private[spark] object KubernetesSuite {
  val EXAMPLES_JAR_FILE = Paths.get("target", "integration-tests-spark-jobs")
    .toFile
    .listFiles()(0)

  val HELPER_JAR_FILE = Paths.get("target", "integration-tests-spark-jobs-helpers")
    .toFile
    .listFiles()(0)
  val SUBMITTER_LOCAL_MAIN_APP_RESOURCE = s"file://${EXAMPLES_JAR_FILE.getAbsolutePath}"
  val CONTAINER_LOCAL_MAIN_APP_RESOURCE = s"local:///opt/spark/examples/" +
    s"integration-tests-jars/${EXAMPLES_JAR_FILE.getName}"
  val CONTAINER_LOCAL_HELPER_JAR_PATH = s"local:///opt/spark/examples/" +
    s"integration-tests-jars/${HELPER_JAR_FILE.getName}"

  val TIMEOUT = PatienceConfiguration.Timeout(Span(2, Minutes))
  val INTERVAL = PatienceConfiguration.Interval(Span(2, Seconds))
  val SPARK_PI_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.SparkPiWithInfiniteWait"
  val FILE_EXISTENCE_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.FileExistenceTest"
  val GROUP_BY_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.GroupByTest"
  val TEST_EXISTENCE_FILE_CONTENTS = "contents"

  case object ShuffleNotReadyException extends Exception
}
