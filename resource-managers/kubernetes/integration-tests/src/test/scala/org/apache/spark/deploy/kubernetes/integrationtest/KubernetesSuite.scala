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

import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.google.common.collect.ImmutableList
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
import org.apache.spark.deploy.kubernetes.Client
import org.apache.spark.deploy.kubernetes.integrationtest.docker.SparkDockerImageBuilder
import org.apache.spark.deploy.kubernetes.integrationtest.minikube.Minikube
import org.apache.spark.deploy.kubernetes.integrationtest.restapis.SparkRestApiV1
import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1.{ApplicationStatus, StageStatus}
import org.apache.spark.util.Utils

private[spark] class KubernetesSuite extends SparkFunSuite with BeforeAndAfter {

  private val EXAMPLES_JAR = Paths.get("target", "integration-tests-spark-jobs")
    .toFile
    .listFiles()(0)
    .getAbsolutePath

  private val HELPER_JAR = Paths.get("target", "integration-tests-spark-jobs-helpers")
      .toFile
      .listFiles()(0)
      .getAbsolutePath

  private val EXAMPLES_JAR_FILE_NAME = Paths.get("target", "docker", "driver", "examples", "jars")
    .toFile
    .listFiles()
    .toList
    .map(_.getName)
    .find(_.startsWith("spark-examples"))
    .getOrElse(throw new IllegalStateException("Expected to find spark-examples jar; was the" +
        " pre-integration-test phase run?"))

  private val TIMEOUT = PatienceConfiguration.Timeout(Span(2, Minutes))
  private val INTERVAL = PatienceConfiguration.Interval(Span(2, Seconds))
  private val MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.SparkPiWithInfiniteWait"
  private val NAMESPACE = UUID.randomUUID().toString.replaceAll("-", "")
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
    // We'll make assertions based on spark rest api, so we need to turn on
    // spark.ui.enabled explicitly since the scalatest-maven-plugin would set it
    // to false by default.
    val sparkConf = new SparkConf(true)
      .setMaster(s"k8s://https://${Minikube.getMinikubeIp}:8443")
      .set("spark.kubernetes.submit.caCertFile", clientConfig.getCaCertFile)
      .set("spark.kubernetes.submit.clientKeyFile", clientConfig.getClientKeyFile)
      .set("spark.kubernetes.submit.clientCertFile", clientConfig.getClientCertFile)
      .set("spark.kubernetes.namespace", NAMESPACE)
      .set("spark.kubernetes.driver.docker.image", "spark-driver:latest")
      .set("spark.kubernetes.executor.docker.image", "spark-executor:latest")
      .set("spark.kubernetes.driver.uploads.jars", HELPER_JAR)
      .set("spark.executor.memory", "500m")
      .set("spark.executor.cores", "1")
      .set("spark.executors.instances", "1")
      .set("spark.app.id", "spark-pi")
      .set("spark.ui.enabled", "true")
      .set("spark.testing", "false")
    val mainAppResource = s"file://$EXAMPLES_JAR"

    new Client(
      sparkConf = sparkConf,
      mainClass = MAIN_CLASS,
      mainAppResource = mainAppResource,
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
      "--upload-jars", HELPER_JAR,
      "--class", MAIN_CLASS,
      "--conf", "spark.ui.enabled=true",
      "--conf", "spark.testing=false",
      "--conf", s"spark.kubernetes.submit.caCertFile=${clientConfig.getCaCertFile}",
      "--conf", s"spark.kubernetes.submit.clientKeyFile=${clientConfig.getClientKeyFile}",
      "--conf", s"spark.kubernetes.submit.clientCertFile=${clientConfig.getClientCertFile}",
      "--conf", "spark.kubernetes.executor.docker.image=spark-executor:latest",
      "--conf", "spark.kubernetes.driver.docker.image=spark-driver:latest",
      EXAMPLES_JAR)
    SparkSubmit.main(args)
    val sparkMetricsService = getSparkMetricsService("spark-pi")
    expectationsForStaticAllocation(sparkMetricsService)
  }

  test("Run using spark-submit with the examples jar on the docker image") {
    val args = Array(
      "--master", s"k8s://${Minikube.getMinikubeIp}:8443",
      "--deploy-mode", "cluster",
      "--kubernetes-namespace", NAMESPACE,
      "--name", "spark-pi",
      "--executor-memory", "512m",
      "--executor-cores", "1",
      "--num-executors", "1",
      "--class", "org.apache.spark.examples.SparkPi",
      "--conf", s"spark.kubernetes.submit.caCertFile=${clientConfig.getCaCertFile}",
      "--conf", s"spark.kubernetes.submit.clientKeyFile=${clientConfig.getClientKeyFile}",
      "--conf", s"spark.kubernetes.submit.clientCertFile=${clientConfig.getClientCertFile}",
      "--conf", "spark.kubernetes.executor.docker.image=spark-executor:latest",
      "--conf", "spark.kubernetes.driver.docker.image=spark-driver:latest",
      s"container:///opt/spark/examples/jars/$EXAMPLES_JAR_FILE_NAME")
    val allContainersSucceeded = SettableFuture.create[Boolean]
    val watcher = new Watcher[Pod] {
      override def eventReceived(action: Action, pod: Pod): Unit = {
        if (action == Action.ERROR) {
          allContainersSucceeded.setException(
              new SparkException("The execution of the driver pod failed."))
        } else if (action == Action.MODIFIED &&
            pod.getStatus.getContainerStatuses.asScala.nonEmpty &&
            pod.getStatus
              .getContainerStatuses
              .asScala
              .forall(_.getState.getTerminated != null)) {
          allContainersSucceeded.set(
            pod.getStatus
              .getContainerStatuses
              .asScala
              .forall(_.getState.getTerminated.getExitCode == 0)
          )
        }
      }

      override def onClose(e: KubernetesClientException): Unit = {
        logError("Integration test pod watch closed", e)
      }
    }
    Utils.tryWithResource(
      minikubeKubernetesClient
        .pods
        .withLabel("spark-app-name", "spark-pi")
        .watch(watcher)) { _ =>
      SparkSubmit.main(args)
      assert(allContainersSucceeded.get(2, TimeUnit.MINUTES),
          "Some containers exited with a non-zero status.")
    }
    val driverPod = minikubeKubernetesClient.pods
      .withLabel("spark-app-name", "spark-pi")
      .list
      .getItems
      .get(0)
    val jobLog = minikubeKubernetesClient.pods.withName(driverPod.getMetadata.getName).getLog
    assert(jobLog.contains("Pi is roughly"), "Pi was not computed by the job...")
  }

  test("Run with custom labels") {
    val args = Array(
      "--master", s"k8s://https://${Minikube.getMinikubeIp}:8443",
      "--deploy-mode", "cluster",
      "--kubernetes-namespace", NAMESPACE,
      "--name", "spark-pi",
      "--executor-memory", "512m",
      "--executor-cores", "1",
      "--num-executors", "1",
      "--upload-jars", HELPER_JAR,
      "--class", MAIN_CLASS,
      "--conf", s"spark.kubernetes.submit.caCertFile=${clientConfig.getCaCertFile}",
      "--conf", s"spark.kubernetes.submit.clientKeyFile=${clientConfig.getClientKeyFile}",
      "--conf", s"spark.kubernetes.submit.clientCertFile=${clientConfig.getClientCertFile}",
      "--conf", "spark.kubernetes.executor.docker.image=spark-executor:latest",
      "--conf", "spark.kubernetes.driver.docker.image=spark-driver:latest",
      "--conf", "spark.kubernetes.driver.labels=label1=label1value,label2=label2value",
      EXAMPLES_JAR)
    SparkSubmit.main(args)
    val driverPodLabels = minikubeKubernetesClient
      .pods
      .withLabel("spark-app-name", "spark-pi")
      .list()
      .getItems
      .get(0)
      .getMetadata
      .getLabels
    // We can't match all of the selectors directly since one of the selectors is based on the
    // launch time.
    assert(driverPodLabels.size == 4, "Unexpected number of pod labels.")
    assert(driverPodLabels.containsKey("driver-launcher-selector"), "Expected driver launcher" +
      " selector label to be present.")
    assert(driverPodLabels.get("spark-app-name") == "spark-pi", "Unexpected value for" +
      " spark-app-name label.")
    assert(driverPodLabels.get("label1") == "label1value", "Unexpected value for label1")
    assert(driverPodLabels.get("label2") == "label2value", "Unexpected value for label2")
  }
}
