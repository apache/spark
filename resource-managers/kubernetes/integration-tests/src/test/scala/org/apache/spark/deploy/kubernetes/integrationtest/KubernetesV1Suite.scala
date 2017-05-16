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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.google.common.collect.ImmutableList
import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.scalatest.{BeforeAndAfter, DoNotDiscover}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.SSLUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.integrationtest.backend.IntegrationTestBackend
import org.apache.spark.deploy.kubernetes.integrationtest.backend.minikube.Minikube
import org.apache.spark.deploy.kubernetes.integrationtest.constants.{GCE_TEST_BACKEND, MINIKUBE_TEST_BACKEND}
import org.apache.spark.deploy.kubernetes.integrationtest.restapis.SparkRestApiV1
import org.apache.spark.deploy.kubernetes.submit.v1.{Client, ExternalSuppliedUrisDriverServiceManager}
import org.apache.spark.status.api.v1.{ApplicationStatus, StageStatus}
import org.apache.spark.util.Utils

@DoNotDiscover
private[spark] class KubernetesV1Suite(testBackend: IntegrationTestBackend)
  extends SparkFunSuite with BeforeAndAfter {

  private var kubernetesTestComponents: KubernetesTestComponents = _
  private var sparkConf: SparkConf = _

  override def beforeAll(): Unit = {
    kubernetesTestComponents = new KubernetesTestComponents(testBackend.getKubernetesClient)
    kubernetesTestComponents.createNamespace()
  }

  override def afterAll(): Unit = {
    kubernetesTestComponents.deleteNamespace()
  }

  before {
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val podsList = kubernetesTestComponents.kubernetesClient.pods().list()
      assert(podsList == null
        || podsList.getItems == null
        || podsList.getItems.isEmpty
      )
      val servicesList = kubernetesTestComponents.kubernetesClient.services().list()
      assert(servicesList == null
        || servicesList.getItems == null
        || servicesList.getItems.isEmpty)
    }
    sparkConf = kubernetesTestComponents.newSparkConf()
  }

  after {
    val pods = kubernetesTestComponents.kubernetesClient.pods().list().getItems.asScala
    pods.par.foreach(pod => {
      kubernetesTestComponents.kubernetesClient.pods()
        .withName(pod.getMetadata.getName)
        .withGracePeriod(60)
        .delete
    })
  }

  private def getSparkMetricsService(sparkBaseAppName: String): SparkRestApiV1 = {
    val serviceName = kubernetesTestComponents.kubernetesClient.services()
      .withLabel("spark-app-name", sparkBaseAppName)
      .list()
      .getItems
      .get(0)
      .getMetadata
      .getName
    kubernetesTestComponents.getService[SparkRestApiV1](serviceName,
      kubernetesTestComponents.namespace, "spark-ui-port")
  }

  private def expectationsForStaticAllocation(sparkMetricsService: SparkRestApiV1): Unit = {
    val apps = Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val result = sparkMetricsService
        .getApplications(ImmutableList.of(ApplicationStatus.RUNNING, ApplicationStatus.COMPLETED))
      assert(result.size == 1
        && !result.head.id.equalsIgnoreCase("appid")
        && !result.head.id.equalsIgnoreCase("{appId}"))
      result
    }
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val result = sparkMetricsService.getExecutors(apps.head.id)
      assert(result.size == 2)
      assert(result.count(exec => exec.id != "driver") == 1)
      result
    }
    Eventually.eventually(KubernetesSuite.TIMEOUT, KubernetesSuite.INTERVAL) {
      val result = sparkMetricsService.getStages(
        apps.head.id, Seq(StageStatus.COMPLETE).asJava)
      assert(result.size == 1)
      result
    }
  }

  test("Run a simple example") {
    new Client(
      sparkConf = sparkConf,
      mainClass = KubernetesSuite.SPARK_PI_MAIN_CLASS,
      mainAppResource = KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
    val sparkMetricsService = getSparkMetricsService("spark-pi")
    expectationsForStaticAllocation(sparkMetricsService)
  }

  test("Run with the examples jar on the docker image") {
    sparkConf.setJars(Seq(KubernetesSuite.CONTAINER_LOCAL_HELPER_JAR_PATH))
    new Client(
      sparkConf = sparkConf,
      mainClass = KubernetesSuite.SPARK_PI_MAIN_CLASS,
      mainAppResource = KubernetesSuite.CONTAINER_LOCAL_MAIN_APP_RESOURCE,
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
      mainClass = KubernetesSuite.SPARK_PI_MAIN_CLASS,
      mainAppResource = KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
    val driverPodMetadata = kubernetesTestComponents.kubernetesClient
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

  test("Run with driver pod name") {
    sparkConf.set(KUBERNETES_DRIVER_POD_NAME, "spark-pi")
    new Client(
      sparkConf = sparkConf,
      mainClass = KubernetesSuite.SPARK_PI_MAIN_CLASS,
      mainAppResource = KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
    val driverPodMetadata = kubernetesTestComponents.kubernetesClient
      .pods()
      .withName("spark-pi")
      .get()
      .getMetadata()
    val driverName = driverPodMetadata.getName
    assert(driverName === "spark-pi", "Unexpected driver pod name.")
  }

  test("Enable SSL on the driver submit server") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

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
      mainClass = KubernetesSuite.SPARK_PI_MAIN_CLASS,
      mainAppResource = KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
  }

  test("Enable SSL on the driver submit server using PEM files") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    val (keyPem, certPem) = SSLUtils.generateKeyCertPemPair(Minikube.getMinikubeIp)
    sparkConf.set(DRIVER_SUBMIT_SSL_KEY_PEM, s"file://${keyPem.getAbsolutePath}")
    sparkConf.set(DRIVER_SUBMIT_SSL_CLIENT_CERT_PEM, s"file://${certPem.getAbsolutePath}")
    sparkConf.set(DRIVER_SUBMIT_SSL_SERVER_CERT_PEM, s"file://${certPem.getAbsolutePath}")
    sparkConf.set(DRIVER_SUBMIT_SSL_ENABLED, true)
    new Client(
      sparkConf = sparkConf,
      mainClass = KubernetesSuite.SPARK_PI_MAIN_CLASS,
      mainAppResource = KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
  }

  test("Added files should exist on the driver.") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    sparkConf.set("spark.files", KubernetesSuite.TEST_EXISTENCE_FILE.getAbsolutePath)
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
    Utils.tryWithResource(kubernetesTestComponents.kubernetesClient
        .pods
        .withLabel("spark-app-name", "spark-file-existence-test")
        .watch(watch)) { _ =>
      new Client(
        sparkConf = sparkConf,
        mainClass = KubernetesSuite.FILE_EXISTENCE_MAIN_CLASS,
        mainAppResource = KubernetesSuite.CONTAINER_LOCAL_MAIN_APP_RESOURCE,
        appArgs = Array(KubernetesSuite.TEST_EXISTENCE_FILE.getName,
          KubernetesSuite.TEST_EXISTENCE_FILE_CONTENTS)).run()
      assert(podCompletedFuture.get(60, TimeUnit.SECONDS), "Failed to run driver pod")
      val driverPod = kubernetesTestComponents.kubernetesClient
        .pods
        .withLabel("spark-app-name", "spark-file-existence-test")
        .list()
        .getItems
        .get(0)
      val podLog = kubernetesTestComponents.kubernetesClient
        .pods
        .withName(driverPod.getMetadata.getName)
        .getLog
      assert(podLog.contains(s"File found at" +
        s" /opt/spark/${KubernetesSuite.TEST_EXISTENCE_FILE.getName} with correct contents."),
        "Job did not find the file as expected.")
    }
  }

  test("Use external URI provider") {
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    val externalUriProviderWatch =
      new ExternalUriProviderWatch(kubernetesTestComponents.kubernetesClient)
    Utils.tryWithResource(kubernetesTestComponents.kubernetesClient.services()
        .withLabel("spark-app-name", "spark-pi")
        .watch(externalUriProviderWatch)) { _ =>
      sparkConf.set(DRIVER_SERVICE_MANAGER_TYPE, ExternalSuppliedUrisDriverServiceManager.TYPE)
      new Client(
        sparkConf = sparkConf,
        mainClass = KubernetesSuite.SPARK_PI_MAIN_CLASS,
        mainAppResource = KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
        appArgs = Array.empty[String]).run()
      val sparkMetricsService = getSparkMetricsService("spark-pi")
      expectationsForStaticAllocation(sparkMetricsService)
      assert(externalUriProviderWatch.annotationSet.get)
      val driverService = kubernetesTestComponents.kubernetesClient
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
    assume(testBackend.name == MINIKUBE_TEST_BACKEND)

    sparkConf.set(KUBERNETES_DRIVER_CA_CERT_FILE,
      kubernetesTestComponents.clientConfig.getCaCertFile)
    sparkConf.set(KUBERNETES_DRIVER_CLIENT_KEY_FILE,
      kubernetesTestComponents.clientConfig.getClientKeyFile)
    sparkConf.set(KUBERNETES_DRIVER_CLIENT_CERT_FILE,
      kubernetesTestComponents.clientConfig.getClientCertFile)
    new Client(
      sparkConf = sparkConf,
      mainClass = KubernetesSuite.SPARK_PI_MAIN_CLASS,
      mainAppResource = KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE,
      appArgs = Array.empty[String]).run()
    val sparkMetricsService = getSparkMetricsService("spark-pi")
    expectationsForStaticAllocation(sparkMetricsService)
  }

}
