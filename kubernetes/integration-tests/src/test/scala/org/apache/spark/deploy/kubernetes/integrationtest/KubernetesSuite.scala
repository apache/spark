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

import com.google.common.collect.ImmutableList
import io.fabric8.kubernetes.client.{Config, KubernetesClient}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.deploy.kubernetes.Client
import org.apache.spark.deploy.kubernetes.integrationtest.docker.SparkDockerImageBuilder
import org.apache.spark.deploy.kubernetes.integrationtest.minikube.Minikube
import org.apache.spark.deploy.kubernetes.integrationtest.restapis.SparkRestApiV1
import org.apache.spark.status.api.v1.{ApplicationStatus, StageStatus}

private[spark] class KubernetesSuite extends SparkFunSuite with BeforeAndAfter {

  private val EXAMPLES_JAR = Paths.get("target", "integration-tests-spark-jobs")
      .toFile
      .listFiles()(0)
      .getAbsolutePath

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
      assert(minikubeKubernetesClient.pods().list().getItems.isEmpty)
      assert(minikubeKubernetesClient.services().list().getItems.isEmpty)
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
    val sparkConf = new SparkConf(true)
      .setMaster("kubernetes")
      .set("spark.kubernetes.master", s"https://${Minikube.getMinikubeIp}:8443")
      .set("spark.kubernetes.submit.caCertFile", clientConfig.getCaCertFile)
      .set("spark.kubernetes.submit.clientKeyFile", clientConfig.getClientKeyFile)
      .set("spark.kubernetes.submit.clientCertFile", clientConfig.getClientCertFile)
      .set("spark.kubernetes.namespace", NAMESPACE)
      .set("spark.kubernetes.driver.docker.image", "spark-driver:latest")
      .set("spark.kubernetes.executor.docker.image", "spark-executor:latest")
      .set("spark.executor.memory", "500m")
      .set("spark.executor.cores", "1")
      .set("spark.executors.instances", "1")
      .set("spark.app.id", "spark-pi")
    val mainAppResource = s"file://$EXAMPLES_JAR"

    new Client(
      sparkConf = sparkConf,
      mainClass = MAIN_CLASS,
      mainAppResource = mainAppResource,
      appArgs = Array.empty[String]).run()
    val sparkMetricsService = Minikube.getService[SparkRestApiV1](
      "spark-pi", NAMESPACE, "spark-ui-port")
    expectationsForStaticAllocation(sparkMetricsService)
  }

  test("Run using spark-submit") {
    val args = Array(
      "--master", "kubernetes",
      "--deploy-mode", "cluster",
      "--kubernetes-master", s"https://${Minikube.getMinikubeIp}:8443",
      "--kubernetes-namespace", NAMESPACE,
      "--name", "spark-pi",
      "--executor-memory", "512m",
      "--executor-cores", "1",
      "--num-executors", "1",
      "--class", MAIN_CLASS,
      "--conf", s"spark.kubernetes.submit.caCertFile=${clientConfig.getCaCertFile}",
      "--conf", s"spark.kubernetes.submit.clientKeyFile=${clientConfig.getClientKeyFile}",
      "--conf", s"spark.kubernetes.submit.clientCertFile=${clientConfig.getClientCertFile}",
      "--conf", "spark.kubernetes.executor.docker.image=spark-executor:latest",
      "--conf", "spark.kubernetes.driver.docker.image=spark-driver:latest",
      EXAMPLES_JAR)
    SparkSubmit.main(args)
    val sparkMetricsService = Minikube.getService[SparkRestApiV1](
      "spark-pi", NAMESPACE, "spark-ui-port")
    expectationsForStaticAllocation(sparkMetricsService)
  }
}
