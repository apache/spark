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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.Quantity
import org.scalatest.concurrent.Eventually

import org.apache.spark.internal.config.PLUGINS

private[spark] trait ExecutorResizeSuite { k8sSuite: KubernetesSuite =>

  import ExecutorResizeSuite._
  import KubernetesSuite.{k8sTestTag, INTERVAL, SPARK_SQL_CLI_MAIN_CLASS, TIMEOUT}

  test("SPARK-60079: Resize executor memory in place with ExecutorResizePlugin", k8sTestTag) {
    // The resize does not depend on the scheduler, so skip the reruns in subclasses.
    assume(this.getClass.getSimpleName == "KubernetesSuite")
    assume(kubernetesTestComponents.kubernetesClient.hasApiGroup("metrics.k8s.io", true),
      "ExecutorResizePlugin requires the metrics.k8s.io API")
    sparkAppConf
      .set(PLUGINS.key, "ExecutorResizePlugin")
      .set("spark.kubernetes.executor.resizeInterval", "5s")
      // Even an idle executor exceeds this threshold.
      .set("spark.kubernetes.executor.resizeThreshold", "0.01")
      .set("spark.kubernetes.executor.resizeFactor", "0.1")
      // Lower than the first increased limit, so the first resize reaches the maximum.
      .set("spark.kubernetes.executor.resizeMaxMemory", "1536m")

    runSparkApplicationAndVerifyCompletion(
      appResource = containerLocalSparkDistroExamplesJar,
      mainClass = SPARK_SQL_CLI_MAIN_CLASS,
      expectedDriverLogOnCompletion = Seq(
        "Initialized driver component for plugin " +
          "org.apache.spark.scheduler.cluster.k8s.ExecutorResizePlugin",
        "Increase executor 1 container memory from",
        s"to $MAX_MEMORY as usage",
        s"Skip resizing executor 1 as container memory limit $MAX_MEMORY already reached " +
          s"the maximum $MAX_MEMORY"),
      // Keep the application running until the plugin resizes the executor.
      appArgs = Array("SELECT reflect('java.lang.Thread', 'sleep', 600000L)"),
      driverPodChecker = doBasicDriverPodCheck,
      executorPodChecker = doBasicExecutorPodCheck,
      isJVM = true)

    // The application is still running, so the executor pod is alive.
    val pods = kubernetesTestComponents.kubernetesClient
      .pods()
      .inNamespace(kubernetesTestComponents.namespace)
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "executor")
      .withLabel("spark-exec-id", "1")
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val pod = pods.list().getItems.get(0)
      val container = pod.getSpec.getContainers.asScala
        .find(_.getName == "spark-kubernetes-executor").get
      assert(toBytes(container.getResources.getLimits.get("memory")) === MAX_MEMORY)
      assert(toBytes(container.getResources.getRequests.get("memory")) === MAX_MEMORY)
      // The kubelet applies the new limit without restarting the container.
      val status = pod.getStatus.getContainerStatuses.asScala
        .find(_.getName == "spark-kubernetes-executor").get
      assert(toBytes(status.getResources.getLimits.get("memory")) === MAX_MEMORY)
      assert(status.getRestartCount === 0)
    }
  }

  private def toBytes(quantity: Quantity): Long = Quantity.getAmountInBytes(quantity).longValue()
}

private[spark] object ExecutorResizeSuite {
  val MAX_MEMORY: Long = 1536L * 1024 * 1024
}
