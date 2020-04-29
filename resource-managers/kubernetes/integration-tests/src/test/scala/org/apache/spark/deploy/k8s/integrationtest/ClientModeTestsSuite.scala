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
package org.apache.spark.deploy.k8s.integrationtest

import org.scalatest.concurrent.Eventually
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{k8sTestTag, INTERVAL, TIMEOUT}

private[spark] trait ClientModeTestsSuite { k8sSuite: KubernetesSuite =>

  test("Run in client mode.", k8sTestTag) {
    val labels = Map("spark-app-selector" -> driverPodName)
    val driverPort = 7077
    val blockManagerPort = 10000
    val executorLabel = "spark-client-it"
    val driverService = testBackend
      .getKubernetesClient
      .services()
      .inNamespace(kubernetesTestComponents.namespace)
      .createNew()
        .withNewMetadata()
          .withName(s"$driverPodName-svc")
          .endMetadata()
        .withNewSpec()
          .withClusterIP("None")
          .withSelector(labels.asJava)
          .addNewPort()
            .withName("driver-port")
            .withPort(driverPort)
            .withNewTargetPort(driverPort)
            .endPort()
          .addNewPort()
            .withName("block-manager")
            .withPort(blockManagerPort)
            .withNewTargetPort(blockManagerPort)
            .endPort()
          .endSpec()
        .done()
    try {
      val driverPod = testBackend
        .getKubernetesClient
        .pods()
        .inNamespace(kubernetesTestComponents.namespace)
        .createNew()
          .withNewMetadata()
          .withName(driverPodName)
          .withLabels(labels.asJava)
          .endMetadata()
        .withNewSpec()
          .withServiceAccountName(kubernetesTestComponents.serviceAccountName)
          .withRestartPolicy("Never")
          .addNewContainer()
            .withName("spark-example")
            .withImage(image)
            .withImagePullPolicy("IfNotPresent")
            .addToArgs("/opt/spark/bin/run-example")
            .addToArgs("--master", s"k8s://https://kubernetes.default.svc")
            .addToArgs("--deploy-mode", "client")
            .addToArgs("--conf", s"spark.kubernetes.container.image=$image")
            .addToArgs(
              "--conf",
              s"spark.kubernetes.namespace=${kubernetesTestComponents.namespace}")
            .addToArgs("--conf", "spark.kubernetes.authenticate.oauthTokenFile=" +
              "/var/run/secrets/kubernetes.io/serviceaccount/token")
            .addToArgs("--conf", "spark.kubernetes.authenticate.caCertFile=" +
              "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
            .addToArgs("--conf", "spark.executor.memory=500m")
            .addToArgs("--conf", "spark.executor.cores=1")
            .addToArgs("--conf", "spark.executor.instances=2")
            .addToArgs("--conf", "spark.kubernetes.executor.deleteOnTermination=false")
            .addToArgs("--conf", s"spark.kubernetes.executor.label.$executorLabel=$executorLabel")
            .addToArgs("--conf",
              s"spark.driver.host=" +
                s"${driverService.getMetadata.getName}.${kubernetesTestComponents.namespace}.svc")
            .addToArgs("--conf", s"spark.driver.port=$driverPort")
            .addToArgs("--conf", s"spark.driver.blockManager.port=$blockManagerPort")
            .addToArgs("SparkPi")
            .addToArgs("10")
            .endContainer()
          .endSpec()
        .done()
      Eventually.eventually(TIMEOUT, INTERVAL) {
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(driverPodName)
          .getLog
          .contains("Pi is roughly 3"), "The application did not complete.")
      }

      val executors = kubernetesTestComponents
        .kubernetesClient
        .pods()
        .inNamespace(kubernetesTestComponents.namespace)
        .withLabel(executorLabel, executorLabel)
        .list()
        .getItems()
      assert(executors.size === 2)
      val prefixes = executors.asScala.map { pod =>
        val name = pod.getMetadata().getName()
        name.substring(0, name.lastIndexOf("-"))
      }.toSet
      assert(prefixes.size === 1, s"Executor prefixes did not match: $prefixes")
    } finally {
      // Have to delete the service manually since it doesn't have an owner reference
      kubernetesTestComponents
        .kubernetesClient
        .services()
        .inNamespace(kubernetesTestComponents.namespace)
        .delete(driverService)
      // Delete all executors, since the test explicitly asks them not to be deleted by the app.
      kubernetesTestComponents
        .kubernetesClient
        .pods()
        .inNamespace(kubernetesTestComponents.namespace)
        .withLabel(executorLabel, executorLabel)
        .delete()
    }
  }

}
