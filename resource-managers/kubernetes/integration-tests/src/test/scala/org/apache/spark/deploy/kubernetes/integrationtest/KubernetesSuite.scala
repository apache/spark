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
import java.util.concurrent.TimeUnit

import com.google.common.base.Charsets
import com.google.common.collect.ImmutableList
import com.google.common.io.Files
import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{Config, KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.deploy.kubernetes.SSLUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.integrationtest.docker.SparkDockerImageBuilder
import org.apache.spark.deploy.kubernetes.integrationtest.minikube.Minikube
import org.apache.spark.deploy.kubernetes.integrationtest.restapis.SparkRestApiV1
import org.apache.spark.deploy.kubernetes.submit.v1.{Client, ExternalSuppliedUrisDriverServiceManager}
import org.apache.spark.status.api.v1.{ApplicationStatus, StageStatus}
import org.apache.spark.util.Utils

private[spark] class KubernetesSuite extends SparkFunSuite with BeforeAndAfter {

  private val EXAMPLES_JAR_FILE = Paths.get("target", "integration-tests-spark-jobs")
    .toFile
    .listFiles()(0)

  private val HELPER_JAR_FILE = Paths.get("target", "integration-tests-spark-jobs-helpers")
      .toFile
      .listFiles()(0)
  private val SUBMITTER_LOCAL_MAIN_APP_RESOURCE = s"file://${EXAMPLES_JAR_FILE.getAbsolutePath}"
  private val CONTAINER_LOCAL_MAIN_APP_RESOURCE = s"local:///opt/spark/examples/" +
    s"integration-tests-jars/${EXAMPLES_JAR_FILE.getName}"
  private val CONTAINER_LOCAL_HELPER_JAR_PATH = s"local:///opt/spark/examples/" +
    s"integration-tests-jars/${HELPER_JAR_FILE.getName}"

  private val TEST_EXISTENCE_FILE = Paths.get("test-data", "input.txt").toFile
  private val TEST_EXISTENCE_FILE_CONTENTS = Files.toString(TEST_EXISTENCE_FILE, Charsets.UTF_8)
  private val TIMEOUT = PatienceConfiguration.Timeout(Span(2, Minutes))
  private val INTERVAL = PatienceConfiguration.Interval(Span(2, Seconds))
  private val SPARK_PI_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.SparkPiWithInfiniteWait"
  private val FILE_EXISTENCE_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.FileExistenceTest"
  private val NAMESPACE = UUID.randomUUID().toString.replaceAll("-", "")
  private var minikubeKubernetesClient: KubernetesClient = _
  private var clientConfig: Config = _
  private var sparkConf: SparkConf = _

  override def beforeAll(): Unit = {
    Minikube.startMinikube()
    new SparkDockerImageBuilder(Minikube.getDockerEnv).buildSparkDockerImages()
    Minikube.getKubernetesClient.namespaces.createNew()
      .withNewMetadata()
        .withName(NAMESPACE)
        .endMetadata()
      .done()
    minikubeKubernetesClient = Minikube.getKubernetesClient.inNamespace(NAMESPACE)
    clientConfig = minikubeKubernetesClient.getConfiguration
  }

  before {
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val podsList = minikubeKubernetesClient.pods().list()
      assert(podsList == null
        || podsList.getItems == null
        || podsList.getItems.isEmpty
      )
      val servicesList = minikubeKubernetesClient.services().list()
      assert(servicesList == null
        || servicesList.getItems == null
        || servicesList.getItems.isEmpty)
    }
    sparkConf = new SparkConf(true)
      .setMaster(s"k8s://https://${Minikube.getMinikubeIp}:8443")
      .set(KUBERNETES_SUBMIT_CA_CERT_FILE, clientConfig.getCaCertFile)
      .set(KUBERNETES_SUBMIT_CLIENT_KEY_FILE, clientConfig.getClientKeyFile)
      .set(KUBERNETES_SUBMIT_CLIENT_CERT_FILE, clientConfig.getClientCertFile)
      .set(KUBERNETES_NAMESPACE, NAMESPACE)
      .set(DRIVER_DOCKER_IMAGE, "spark-driver:latest")
      .set(EXECUTOR_DOCKER_IMAGE, "spark-executor:latest")
      .setJars(Seq(HELPER_JAR_FILE.getAbsolutePath))
      .set("spark.executor.memory", "500m")
      .set("spark.executor.cores", "1")
      .set("spark.executors.instances", "1")
      .set("spark.app.name", "spark-pi")
      .set("spark.ui.enabled", "true")
      .set("spark.testing", "false")
      .set(WAIT_FOR_APP_COMPLETION, false)
  }

  after {
    val pods = minikubeKubernetesClient.pods().list().getItems.asScala
    pods.par.foreach(pod => {
      minikubeKubernetesClient
        .pods()
        .withName(pod.getMetadata.getName)
        .withGracePeriod(60)
        .delete
    })
    // spark-submit sets system properties so we have to clear them
    new SparkConf(true)
      .getAll.map(_._1)
      .filter(_ != "spark.docker.test.persistMinikube")
      .foreach { System.clearProperty }
  }

  override def afterAll(): Unit = {
    if (!System.getProperty("spark.docker.test.persistMinikube", "false").toBoolean) {
      Minikube.deleteMinikube()
    }
  }

  private def getSparkMetricsService(sparkBaseAppName: String): SparkRestApiV1 = {
    val serviceName = minikubeKubernetesClient.services()
      .withLabel("spark-app-name", sparkBaseAppName)
      .list()
      .getItems
      .get(0)
      .getMetadata
      .getName
    Minikube.getService[SparkRestApiV1](serviceName, NAMESPACE, "spark-ui-port")
  }

  private def expectationsForStaticAllocation(sparkMetricsService: SparkRestApiV1): Unit = {
    val apps = Eventually.eventually(TIMEOUT, INTERVAL) {
      val result = sparkMetricsService
        .getApplications(ImmutableList.of(ApplicationStatus.RUNNING, ApplicationStatus.COMPLETED))
      assert(result.size == 1
        && !result.head.id.equalsIgnoreCase("appid")
        && !result.head.id.equalsIgnoreCase("{appId}"))
      result
    }
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val result = sparkMetricsService.getExecutors(apps.head.id)
      assert(result.size == 2)
      assert(result.count(exec => exec.id != "driver") == 1)
      result
    }
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val result = sparkMetricsService.getStages(
        apps.head.id, Seq(StageStatus.COMPLETE).asJava)
      assert(result.size == 1)
      result
    }
  }

  test("Run a simple example") {
    new Client(
      sparkConf = sparkConf,
      mainClass = SPARK_PI_MAIN_CLASS,
      mainAppResource = SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
    val sparkMetricsService = getSparkMetricsService("spark-pi")
    expectationsForStaticAllocation(sparkMetricsService)
  }

  test("Run using spark-submit") {
    val args = Array(
      "--master", s"k8s://https://${Minikube.getMinikubeIp}:8443",
      "--deploy-mode", "cluster",
      "--kubernetes-namespace", NAMESPACE,
      "--name", "spark-pi",
      "--executor-memory", "512m",
      "--executor-cores", "1",
      "--num-executors", "1",
      "--jars", HELPER_JAR_FILE.getAbsolutePath,
      "--class", SPARK_PI_MAIN_CLASS,
      "--conf", "spark.ui.enabled=true",
      "--conf", "spark.testing=false",
      "--conf", s"${KUBERNETES_SUBMIT_CA_CERT_FILE.key}=${clientConfig.getCaCertFile}",
      "--conf", s"${KUBERNETES_SUBMIT_CLIENT_KEY_FILE.key}=${clientConfig.getClientKeyFile}",
      "--conf", s"${KUBERNETES_SUBMIT_CLIENT_CERT_FILE.key}=${clientConfig.getClientCertFile}",
      "--conf", s"${EXECUTOR_DOCKER_IMAGE.key}=spark-executor:latest",
      "--conf", s"${DRIVER_DOCKER_IMAGE.key}=spark-driver:latest",
      "--conf", s"${WAIT_FOR_APP_COMPLETION.key}=false",
      EXAMPLES_JAR_FILE.getAbsolutePath)
    SparkSubmit.main(args)
    val sparkMetricsService = getSparkMetricsService("spark-pi")
    expectationsForStaticAllocation(sparkMetricsService)
  }

  test("Run with the examples jar on the docker image") {
    sparkConf.setJars(Seq(CONTAINER_LOCAL_HELPER_JAR_PATH))
    new Client(
      sparkConf = sparkConf,
      mainClass = SPARK_PI_MAIN_CLASS,
      mainAppResource = CONTAINER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
    val sparkMetricsService = getSparkMetricsService("spark-pi")
    expectationsForStaticAllocation(sparkMetricsService)
  }

  test("Run with custom labels and annotations") {
    sparkConf.set(KUBERNETES_DRIVER_LABELS, "label1=label1value,label2=label2value")
    sparkConf.set(KUBERNETES_DRIVER_ANNOTATIONS, "annotation1=annotation1value," +
        "annotation2=annotation2value")
    new Client(
      sparkConf = sparkConf,
      mainClass = SPARK_PI_MAIN_CLASS,
      mainAppResource = SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
    val driverPodMetadata = minikubeKubernetesClient
      .pods
      .withLabel("spark-app-name", "spark-pi")
      .list()
      .getItems
      .get(0)
      .getMetadata
    val driverPodLabels = driverPodMetadata.getLabels
    // We can't match all of the selectors directly since one of the selectors is based on the
    // launch time.
    assert(driverPodLabels.size === 5, "Unexpected number of pod labels.")
    assert(driverPodLabels.get("spark-app-name") === "spark-pi", "Unexpected value for" +
      " spark-app-name label.")
    assert(driverPodLabels.get("spark-app-id").startsWith("spark-pi"), "Unexpected value for" +
      " spark-app-id label (should be prefixed with the app name).")
    assert(driverPodLabels.get("label1") === "label1value", "Unexpected value for label1")
    assert(driverPodLabels.get("label2") === "label2value", "Unexpected value for label2")
    val driverPodAnnotations = driverPodMetadata.getAnnotations
    assert(driverPodAnnotations.size === 2, "Unexpected number of pod annotations.")
    assert(driverPodAnnotations.get("annotation1") === "annotation1value",
      "Unexpected value for annotation1")
    assert(driverPodAnnotations.get("annotation2") === "annotation2value",
      "Unexpected value for annotation2")
  }

  test("Enable SSL on the driver submit server") {
    val (keyStoreFile, trustStoreFile) = SSLUtils.generateKeyStoreTrustStorePair(
      Minikube.getMinikubeIp,
      "changeit",
      "changeit",
      "changeit")
    sparkConf.set(KUBERNETES_DRIVER_SUBMIT_SSL_KEYSTORE, s"file://${keyStoreFile.getAbsolutePath}")
    sparkConf.set("spark.ssl.kubernetes.driversubmitserver.keyStorePassword", "changeit")
    sparkConf.set("spark.ssl.kubernetes.driversubmitserver.keyPassword", "changeit")
    sparkConf.set(KUBERNETES_DRIVER_SUBMIT_SSL_TRUSTSTORE,
      s"file://${trustStoreFile.getAbsolutePath}")
    sparkConf.set("spark.ssl.kubernetes.driversubmitserver.trustStorePassword", "changeit")
    sparkConf.set(DRIVER_SUBMIT_SSL_ENABLED, true)
    new Client(
      sparkConf = sparkConf,
      mainClass = SPARK_PI_MAIN_CLASS,
      mainAppResource = SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
  }

  test("Enable SSL on the driver submit server using PEM files") {
    val (keyPem, certPem) = SSLUtils.generateKeyCertPemPair(Minikube.getMinikubeIp)
    sparkConf.set(DRIVER_SUBMIT_SSL_KEY_PEM, s"file://${keyPem.getAbsolutePath}")
    sparkConf.set(DRIVER_SUBMIT_SSL_CLIENT_CERT_PEM, s"file://${certPem.getAbsolutePath}")
    sparkConf.set(DRIVER_SUBMIT_SSL_SERVER_CERT_PEM, s"file://${certPem.getAbsolutePath}")
    sparkConf.set(DRIVER_SUBMIT_SSL_ENABLED, true)
    new Client(
      sparkConf = sparkConf,
      mainClass = SPARK_PI_MAIN_CLASS,
      mainAppResource = SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
  }

  test("Added files should exist on the driver.") {
    sparkConf.set("spark.files", TEST_EXISTENCE_FILE.getAbsolutePath)
    sparkConf.setAppName("spark-file-existence-test")
    val podCompletedFuture = SettableFuture.create[Boolean]
    val watch = new Watcher[Pod] {
      override def eventReceived(action: Action, pod: Pod): Unit = {
        val containerStatuses = pod.getStatus.getContainerStatuses.asScala
        val allSuccessful = containerStatuses.nonEmpty && containerStatuses
          .forall(status => {
            status.getState.getTerminated != null && status.getState.getTerminated.getExitCode == 0
        })
        if (allSuccessful) {
          podCompletedFuture.set(true)
        } else {
          val failedContainers = containerStatuses.filter(container => {
            container.getState.getTerminated != null &&
              container.getState.getTerminated.getExitCode != 0
          })
          if (failedContainers.nonEmpty) {
            podCompletedFuture.setException(new SparkException(
              "One or more containers in the driver failed with a nonzero exit code."))
          }
        }
      }

      override def onClose(e: KubernetesClientException): Unit = {
        logWarning("Watch closed", e)
      }
    }
    Utils.tryWithResource(minikubeKubernetesClient
        .pods
        .withLabel("spark-app-name", "spark-file-existence-test")
        .watch(watch)) { _ =>
      new Client(
        sparkConf = sparkConf,
        mainClass = FILE_EXISTENCE_MAIN_CLASS,
        mainAppResource = CONTAINER_LOCAL_MAIN_APP_RESOURCE,
        appArgs = Array(TEST_EXISTENCE_FILE.getName, TEST_EXISTENCE_FILE_CONTENTS)).run()
      assert(podCompletedFuture.get(60, TimeUnit.SECONDS), "Failed to run driver pod")
      val driverPod = minikubeKubernetesClient
        .pods
        .withLabel("spark-app-name", "spark-file-existence-test")
        .list()
        .getItems
        .get(0)
      val podLog = minikubeKubernetesClient
        .pods
        .withName(driverPod.getMetadata.getName)
        .getLog
      assert(podLog.contains(s"File found at /opt/spark/${TEST_EXISTENCE_FILE.getName}" +
        s" with correct contents."), "Job did not find the file as expected.")
    }
  }

  test("Use external URI provider") {
    val externalUriProviderWatch = new ExternalUriProviderWatch(minikubeKubernetesClient)
    Utils.tryWithResource(minikubeKubernetesClient.services()
        .withLabel("spark-app-name", "spark-pi")
        .watch(externalUriProviderWatch)) { _ =>
      sparkConf.set(DRIVER_SERVICE_MANAGER_TYPE, ExternalSuppliedUrisDriverServiceManager.TYPE)
      new Client(
        sparkConf = sparkConf,
        mainClass = SPARK_PI_MAIN_CLASS,
        mainAppResource = SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
        appArgs = Array.empty[String]).run()
      val sparkMetricsService = getSparkMetricsService("spark-pi")
      expectationsForStaticAllocation(sparkMetricsService)
      assert(externalUriProviderWatch.annotationSet.get)
      val driverService = minikubeKubernetesClient
        .services()
        .withLabel("spark-app-name", "spark-pi")
        .list()
        .getItems
        .asScala(0)
      assert(driverService.getMetadata.getAnnotations.containsKey(ANNOTATION_PROVIDE_EXTERNAL_URI),
          "External URI request annotation was not set on the driver service.")
      // Unfortunately we can't check the correctness of the actual value of the URI, as it depends
      // on the driver submission port set on the driver service but we remove that port from the
      // service once the submission is complete.
      assert(driverService.getMetadata.getAnnotations.containsKey(ANNOTATION_RESOLVED_EXTERNAL_URI),
        "Resolved URI annotation not set on driver service.")
    }
  }

  test("Mount the Kubernetes credentials onto the driver pod") {
    sparkConf.set(KUBERNETES_DRIVER_CA_CERT_FILE, clientConfig.getCaCertFile)
    sparkConf.set(KUBERNETES_DRIVER_CLIENT_KEY_FILE, clientConfig.getClientKeyFile)
    sparkConf.set(KUBERNETES_DRIVER_CLIENT_CERT_FILE, clientConfig.getClientCertFile)
    new Client(
      sparkConf = sparkConf,
      mainClass = SPARK_PI_MAIN_CLASS,
      mainAppResource = SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
    val sparkMetricsService = getSparkMetricsService("spark-pi")
    expectationsForStaticAllocation(sparkMetricsService)
  }
}
