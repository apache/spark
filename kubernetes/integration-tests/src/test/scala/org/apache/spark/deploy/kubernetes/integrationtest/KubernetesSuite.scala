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
import com.google.common.collect.ImmutableList
import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{Pod, ReplicationController}
import io.fabric8.kubernetes.client.{Config, KubernetesClient}
import org.apache.commons.io.FileUtils
import org.apache.spark.deploy.kubernetes.shuffle.{StartKubernetesShuffleServiceArgumentsBuilder, StartKubernetesShuffleServiceArguments, StartKubernetesShuffleService}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}
import scala.collection.JavaConverters._

import org.apache.spark.deploy.kubernetes.{Client, ClientArguments}
import org.apache.spark.deploy.kubernetes.integrationtest.docker.SparkDockerImageBuilder
import org.apache.spark.deploy.kubernetes.integrationtest.minikube.Minikube
import org.apache.spark.deploy.kubernetes.integrationtest.restapis.SparkRestApiV1
import org.apache.spark.status.api.v1.{ApplicationStatus, StageStatus}
import org.apache.spark.SparkFunSuite

private[spark] class KubernetesSuite extends SparkFunSuite with BeforeAndAfter {

  private val EXAMPLES_JAR = Paths.get("target", "integration-tests-spark-jobs")
      .toFile
      .listFiles()(0)
      .getAbsolutePath

  private val TIMEOUT = PatienceConfiguration.Timeout(Span(2, Minutes))
  private val INTERVAL = PatienceConfiguration.Interval(Span(2, Seconds))
  private val MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.SparkPiWithInfiniteWait"
  private val NAMESPACE = UUID.randomUUID.toString.replaceAll("-", "")
  private val SHUFFLE_SERVICE_NAMESPACE = UUID.randomUUID.toString.replaceAll("-", "")
  private val SHUFFLE_SERVICE_NAME = "spark-shuffle-service"
  private var minikubeKubernetesClient: KubernetesClient = _
  private var clientConfig: Config = _

  override def beforeAll(): Unit = {
    Minikube.startMinikube()
    new SparkDockerImageBuilder(Minikube.getDockerEnv).buildSparkDockerImages()
    Minikube.getKubernetesClient.namespaces.createNew()
      .withNewMetadata()
        .withName(NAMESPACE)
        .endMetadata()
      .done()
    minikubeKubernetesClient = Minikube.getKubernetesClient.inNamespace(NAMESPACE)
    Minikube.getKubernetesClient.namespaces.createNew()
      .withNewMetadata()
        .withName(SHUFFLE_SERVICE_NAMESPACE)
        .endMetadata()
      .done()

    Minikube.executeMinikubeSsh("mkdir -p /tmp/spark-shuffles")
    clientConfig = minikubeKubernetesClient.getConfiguration
    val startShuffleArgs = new StartKubernetesShuffleServiceArgumentsBuilder()
      .copy(kubernetesMaster = Some(clientConfig.getMasterUrl))
      .copy(shuffleServiceNamespace = Some(SHUFFLE_SERVICE_NAMESPACE))
      .copy(shuffleServiceDaemonSetName = SHUFFLE_SERVICE_NAME)
      .copy(shuffleServiceDockerImage = "spark-shuffle-service:latest")
      .copy(shuffleServiceMemory = "512m")
      .copy(kubernetesCaCertFile = Some(clientConfig.getCaCertFile))
      .copy(kubernetesClientCertFile = Some(clientConfig.getClientCertFile))
      .copy(kubernetesClientKeyFile = Some(clientConfig.getClientKeyFile))
      .copy(shuffleHostPathDir = "/tmp/spark-shuffles")
      .build()
    new StartKubernetesShuffleService().run(startShuffleArgs)
  }

  before {
    Eventually.eventually(TIMEOUT, INTERVAL) {
      assert(minikubeKubernetesClient.pods().list().getItems.isEmpty)
      assert(minikubeKubernetesClient.services().list().getItems.isEmpty)
    }
  }

  after {
    minikubeKubernetesClient
      .services
      .delete(minikubeKubernetesClient.services().list().getItems)
    minikubeKubernetesClient
      .pods()
      .delete(minikubeKubernetesClient.pods().list().getItems)
  }

  override def afterAll(): Unit = {
    if (!System.getProperty("spark.docker.test.persistMinikube", "false").toBoolean) {
      Minikube.deleteMinikube()
    }
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
      assert(result.size == 3)
      assert(result.count(exec => exec.id != "driver") == 2)
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
    val args = ClientArguments.builder()
      .userMainClass(MAIN_CLASS)
      .addJar(s"file://$EXAMPLES_JAR")
      .addSparkConf("spark.master", "kubernetes")
      .addSparkConf("spark.driver.memory", "512m")
      .addSparkConf("spark.executor.memory", "512m")
      .addSparkConf("spark.executor.instances", "2")
      .addSparkConf("spark.executor.cores", "1")
      .kubernetesAppName("spark-pi")
      .driverDockerImage("spark-driver:latest")
      .executorDockerImage("spark-executor:latest")
      .kubernetesAppNamespace(NAMESPACE)
      .kubernetesMaster(s"https://${Minikube.getMinikubeIp}:8443")
      .kubernetesCaCertFile(clientConfig.getCaCertFile)
      .kubernetesClientCertFile(clientConfig.getClientCertFile)
      .kubernetesClientKeyFile(clientConfig.getClientKeyFile)
      .build()
    new Client(args).run()

    val sparkMetricsService = Minikube.getService[SparkRestApiV1](
    "spark-pi", NAMESPACE, "spark-ui-port")
    expectationsForStaticAllocation(sparkMetricsService)
  }

  test("Dynamic allocation mode") {
    val args = ClientArguments.builder()
      .userMainClass(MAIN_CLASS)
      .addJar(s"file://$EXAMPLES_JAR")
      .addSparkConf("spark.master", "kubernetes")
      .addSparkConf("spark.driver.memory", "512m")
      .addSparkConf("spark.executor.memory", "512m")
      .addSparkConf("spark.dynamicAllocation.enabled", "true")
      .addSparkConf("spark.dynamicAllocation.minExecutors", "0")
      .addSparkConf("spark.dynamicAllocation.initialExecutors", "0")
      .addSparkConf("spark.dynamicAllocation.maxExecutors", "1")
      .addSparkConf("spark.executor.cores", "1")
      .addSparkConf("spark.shuffle.service.enabled", "true")
      .addSparkConf("spark.dynamicAllocation.executorIdleTimeout", "20s")
      .addSparkConf("spark.memory.storageFraction", "1.0")
      .kubernetesAppName("spark-pi-dyn")
      .driverDockerImage("spark-driver:latest")
      .executorDockerImage("spark-executor:latest")
      .addSparkConf("spark.kubernetes.shuffle.service.daemonset.name", SHUFFLE_SERVICE_NAME)
      .addSparkConf("spark.kubernetes.shuffle.service.daemonset.namespace", SHUFFLE_SERVICE_NAMESPACE)
      .kubernetesAppNamespace(NAMESPACE)
      .kubernetesMaster(s"https://${Minikube.getMinikubeIp}:8443")
      .kubernetesCaCertFile(clientConfig.getCaCertFile)
      .kubernetesClientCertFile(clientConfig.getClientCertFile)
      .kubernetesClientKeyFile(clientConfig.getClientKeyFile)
      .build()
    new Client(args).run()

    val sparkMetricsService = Minikube.getService[SparkRestApiV1](
    "spark-pi-dyn", NAMESPACE, "spark-ui-port")
    expectationsForDynamicAllocation(sparkMetricsService)
  }

  test("Dynamic allocation pod management") {
    val args = ClientArguments.builder()
      .userMainClass(MAIN_CLASS)
      .addJar(s"file://$EXAMPLES_JAR")
      .addSparkConf("spark.master", "kubernetes")
      .addSparkConf("spark.driver.memory", "512m")
      .addSparkConf("spark.executor.memory", "512m")
      .addSparkConf("spark.dynamicAllocation.enabled", "true")
      .addSparkConf("spark.dynamicAllocation.minExecutors", "1")
      .addSparkConf("spark.dynamicAllocation.initialExecutors", "2")
      .addSparkConf("spark.dynamicAllocation.maxExecutors", "2")
      .addSparkConf("spark.executor.cores", "1")
      .addSparkConf("spark.shuffle.service.enabled", "true")
      .addSparkConf("spark.dynamicAllocation.executorIdleTimeout", "10s")
      .addSparkConf("spark.memory.storageFraction", "1.0")
      .kubernetesAppName("spark-pi-dyn")
      .driverDockerImage("spark-driver:latest")
      .executorDockerImage("spark-executor:latest")
      .addSparkConf("spark.kubernetes.shuffle.service.daemonset.name", SHUFFLE_SERVICE_NAME)
      .addSparkConf("spark.kubernetes.shuffle.service.daemonset.namespace", SHUFFLE_SERVICE_NAMESPACE)
      .kubernetesAppNamespace(NAMESPACE)
      .kubernetesMaster(s"https://${Minikube.getMinikubeIp}:8443")
      .kubernetesCaCertFile(clientConfig.getCaCertFile)
      .kubernetesClientCertFile(clientConfig.getClientCertFile)
      .kubernetesClientKeyFile(clientConfig.getClientKeyFile)
      .build()
    new Client(args).run()
    def getExecutorPods: Iterable[Pod] =
      minikubeKubernetesClient.pods()
        .list()
        .getItems
        .asScala
        .filter(pod => pod
          .getSpec
          .getContainers
          .asScala
          .count(container => container.getImage.equals("spark-executor:latest")) == 1)
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val executorPods = getExecutorPods
      assert(executorPods.size == 2)
      executorPods.foreach(pod => assert(pod.getStatus.getPhase == "Running"))
    }

    Eventually.eventually(TIMEOUT, INTERVAL) {
      val executorPods = getExecutorPods
      assert(executorPods.size == 1)
      executorPods.foreach(pod => assert(pod.getStatus.getPhase == "Running"))
    }
  }

  private def expectationsForDynamicAllocation(sparkMetricsService: SparkRestApiV1): Unit = {
    val apps = Eventually.eventually(TIMEOUT, INTERVAL) {
      val result = sparkMetricsService
        .getApplications(ImmutableList.of(ApplicationStatus.RUNNING, ApplicationStatus.COMPLETED))
      // Sometimes "appId" comes back as the app ID when the app is initializing.
      assert(result.size == 1
        && !result.head.id.equalsIgnoreCase("appid")
        && !result.head.id.equalsIgnoreCase("{appId}"))
      result
    }
    val executors = Eventually.eventually(TIMEOUT, INTERVAL) {
      val result = sparkMetricsService.getExecutors(apps.head.id)
      assert(result.count(exec => exec.id != "driver") == 1)
      assert(result.size == 2)
      result
    }
    val completedApp = Eventually.eventually(TIMEOUT, INTERVAL) {
      val result = sparkMetricsService.getStages(
        apps.head.id, Seq(StageStatus.COMPLETE).asJava)
      val allStages = sparkMetricsService.getStages(
        apps.head.id, Seq[StageStatus](
          StageStatus.ACTIVE,
          StageStatus.COMPLETE,
          StageStatus.FAILED,
          StageStatus.PENDING).asJava)
      assert(result.size == allStages.size)
      result
    }
    val noExecutorsAfterFinishedJob = Eventually.eventually(TIMEOUT, INTERVAL) {
      val result = sparkMetricsService.getExecutors(apps.head.id)
      assert(result.size == 1)
      assert(result.count(exec => exec.id != "driver") == 0)
      result
    }
  }

  test("Custom executor specification file") {
    val executorSpecificationYml =
      """
        |apiVersion: v1
        |kind: Pod
        |metadata:
        |  name: executor-and-nginx
        |  labels:
        |    app: executor-and-nginx
        |spec:
        |  containers:
        |  - name: nginx
        |    image: nginx:1.11.5-alpine
        |    ports:
        |    - containerPort: 80
        |  - name: executor
        |    image: spark-executor:latest
        |    imagePullPolicy: Never
        |
      """.stripMargin
    val writtenSpecFile = new File(Files.createTempDir(), "executor-replication-controller.yml")
    FileUtils.write(writtenSpecFile, executorSpecificationYml, Charsets.UTF_8)
    val args = ClientArguments.builder()
      .userMainClass(MAIN_CLASS)
      .addJar(s"file://$EXAMPLES_JAR")
      .addSparkConf("spark.master", "kubernetes")
      .addSparkConf("spark.driver.memory", "512m")
      .addSparkConf("spark.executor.memory", "512m")
      .addSparkConf("spark.dynamicAllocation.enabled", "true")
      .addSparkConf("spark.dynamicAllocation.minExecutors", "0")
      .addSparkConf("spark.dynamicAllocation.initialExecutors", "0")
      .addSparkConf("spark.dynamicAllocation.maxExecutors", "1")
      .addSparkConf("spark.executor.cores", "1")
      .addSparkConf("spark.shuffle.service.enabled", "true")
      .addSparkConf("spark.dynamicAllocation.executorIdleTimeout", "20s")
      .addSparkConf("spark.memory.storageFraction", "1.0")
      .kubernetesAppName("spark-pi-custom")
      .driverDockerImage("spark-driver:latest")
      .executorDockerImage("spark-executor:latest")
      .addSparkConf("spark.kubernetes.shuffle.service.daemonset.name", SHUFFLE_SERVICE_NAME)
      .addSparkConf("spark.kubernetes.shuffle.service.daemonset.namespace", SHUFFLE_SERVICE_NAMESPACE)
      .kubernetesAppNamespace(NAMESPACE)
      .kubernetesMaster(s"https://${Minikube.getMinikubeIp}:8443")
      .kubernetesCaCertFile(clientConfig.getCaCertFile)
      .kubernetesClientCertFile(clientConfig.getClientCertFile)
      .kubernetesClientKeyFile(clientConfig.getClientKeyFile)
      .customExecutorSpecFile(writtenSpecFile.getAbsolutePath)
      .customExecutorSpecContainerName("executor")
      .build()
    new Client(args).run()

    Eventually.eventually(TIMEOUT, INTERVAL) {
      val executorPods = minikubeKubernetesClient
        .pods()
        .list()
        .getItems
        .asScala
        .filter(_.getStatus.getContainerStatuses.asScala.exists(
          _.getImage == "spark-executor:latest"))
      assert(executorPods.nonEmpty)
      executorPods.foreach(pod => assert(
          pod.getStatus.getContainerStatuses.asScala.exists(_.getImage == "nginx:1.11.5-alpine")))
    }
  }

}
