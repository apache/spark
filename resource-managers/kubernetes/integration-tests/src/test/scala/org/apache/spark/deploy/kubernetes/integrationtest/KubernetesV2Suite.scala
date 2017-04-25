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

import org.scalatest.{BeforeAndAfter, DoNotDiscover}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkConf, SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.SSLUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.integrationtest.minikube.Minikube
import org.apache.spark.deploy.kubernetes.submit.v2.{MountedDependencyManagerProviderImpl, SubmissionKubernetesClientProviderImpl}

@DoNotDiscover
private[spark] class KubernetesV2Suite extends SparkFunSuite with BeforeAndAfter {

  private val APP_LOCATOR_LABEL = UUID.randomUUID().toString.replaceAll("-", "")
  private var kubernetesTestComponents: KubernetesTestComponents = _
  private var sparkConf: SparkConf = _
  private var resourceStagingServerLauncher: ResourceStagingServerLauncher = _

  override def beforeAll(): Unit = {
    kubernetesTestComponents = new KubernetesTestComponents
    resourceStagingServerLauncher = new ResourceStagingServerLauncher(
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
    launchStagingServer(SSLOptions())
    runSparkAppAndVerifyCompletion(KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Enable SSL on the submission server") {
    val (keyStore, trustStore) = SSLUtils.generateKeyStoreTrustStorePair(
      ipAddress = Minikube.getMinikubeIp,
      keyStorePassword = "keyStore",
      keyPassword = "key",
      trustStorePassword = "trustStore")
    sparkConf.set(RESOURCE_STAGING_SERVER_SSL_ENABLED, true)
      .set("spark.ssl.kubernetes.resourceStagingServer.keyStore", keyStore.getAbsolutePath)
      .set("spark.ssl.kubernetes.resourceStagingServer.trustStore", trustStore.getAbsolutePath)
      .set("spark.ssl.kubernetes.resourceStagingServer.keyStorePassword", "keyStore")
      .set("spark.ssl.kubernetes.resourceStagingServer.keyPassword", "key")
      .set("spark.ssl.kubernetes.resourceStagingServer.trustStorePassword", "trustStore")
    launchStagingServer(SSLOptions(
      enabled = true,
      keyStore = Some(keyStore),
      trustStore = Some(trustStore),
      keyStorePassword = Some("keyStore"),
      keyPassword = Some("key"),
      trustStorePassword = Some("trustStore")))
    runSparkAppAndVerifyCompletion(KubernetesSuite.SUBMITTER_LOCAL_MAIN_APP_RESOURCE)
  }

  test("Use container-local resources without the resource staging server") {
    sparkConf.setJars(Seq(
      KubernetesSuite.CONTAINER_LOCAL_MAIN_APP_RESOURCE,
      KubernetesSuite.CONTAINER_LOCAL_HELPER_JAR_PATH))
    runSparkAppAndVerifyCompletion(KubernetesSuite.CONTAINER_LOCAL_MAIN_APP_RESOURCE)
  }

  private def launchStagingServer(resourceStagingServerSslOptions: SSLOptions): Unit = {
    val resourceStagingServerPort = resourceStagingServerLauncher.launchStagingServer(
      resourceStagingServerSslOptions)
    val resourceStagingServerUriScheme = if (resourceStagingServerSslOptions.enabled) {
      "https"
    } else {
      "http"
    }
    sparkConf.set(RESOURCE_STAGING_SERVER_URI,
      s"$resourceStagingServerUriScheme://${Minikube.getMinikubeIp}:$resourceStagingServerPort")
  }

  private def runSparkAppAndVerifyCompletion(appResource: String): Unit = {
    val client = new org.apache.spark.deploy.kubernetes.submit.v2.Client(
      sparkConf = sparkConf,
      mainClass = KubernetesSuite.SPARK_PI_MAIN_CLASS,
      appArgs = Array.empty[String],
      mainAppResource = appResource,
      kubernetesClientProvider =
        new SubmissionKubernetesClientProviderImpl(sparkConf),
      mountedDependencyManagerProvider =
        new MountedDependencyManagerProviderImpl(sparkConf))
    client.run()
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
}
