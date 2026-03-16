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
package org.apache.spark.scheduler.cluster.k8s

import java.util.{Map => JMap}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{PodBuilder, Quantity}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.base.PatchContext
import io.fabric8.kubernetes.client.dsl.base.PatchType

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.SparkKubernetesClientFactory
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{EXECUTOR_ID, MEMORY_SIZE}
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Spark plugin to monitor executor pod memory usage and increase the memory limit
 * if the usage exceeds a threshold.
 */
class ExecutorResizePlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new ExecutorResizeDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = null
}

class ExecutorResizeDriverPlugin extends DriverPlugin with Logging {
  private var sparkContext: SparkContext = _
  private var kubernetesClient: KubernetesClient = _

  private val periodicService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-resize-plugin")

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    val interval = Utils.timeStringAsSeconds(
      sc.conf.get(EXECUTOR_RESIZE_INTERVAL.key, "1m"))
    val threshold = sc.conf.getDouble(EXECUTOR_RESIZE_THRESHOLD.key, 0.9)
    val factor = sc.conf.getDouble(EXECUTOR_RESIZE_FACTOR.key, 0.1)
    val namespace = sc.conf.get(KUBERNETES_NAMESPACE)

    sparkContext = sc

    try {
      kubernetesClient = SparkKubernetesClientFactory.createKubernetesClient(
        sc.conf.get(KUBERNETES_DRIVER_MASTER_URL),
        Option(namespace),
        KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        SparkKubernetesClientFactory.ClientType.Driver,
        sc.conf,
        None)

      periodicService.scheduleAtFixedRate(() => {
        try {
          checkAndIncreaseMemory(namespace, threshold, factor)
        } catch {
          case e: Throwable => logError("Error in memory check thread", e)
        }
      }, interval, interval, TimeUnit.SECONDS)
    } catch {
      case e: Exception =>
        logError("Failed to initialize", e)
    }

    Map.empty[String, String].asJava
  }

  override def shutdown(): Unit = {
    periodicService.shutdown()
    if (kubernetesClient != null) {
      kubernetesClient.close()
    }
  }

  private def checkAndIncreaseMemory(namespace: String, threshold: Double, factor: Double): Unit = {
    val appId = sparkContext.applicationId

    // Get all running executor pods for this application
    val pods = kubernetesClient.pods()
      .inNamespace(namespace)
      .withLabel(SPARK_APP_ID_LABEL, appId)
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      .list()
      .getItems.asScala

    pods.filter(_.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL) != null).foreach { pod =>
      val execId = pod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL)
      try {
        val metrics = kubernetesClient.top().pods().metrics(namespace, pod.getMetadata.getName)

        val containerMetrics = metrics.getContainers.asScala
          .find(_.getName == DEFAULT_EXECUTOR_CONTAINER_NAME)
          .orElse(metrics.getContainers.asScala.headOption)

        containerMetrics.filter(_.getUsage.get("memory") != null).foreach { cm =>
          val usageQuantity = cm.getUsage.get("memory")
          val usage = Quantity.getAmountInBytes(usageQuantity).longValue()

          // Identify the Spark container in the Pod spec to get the limit
          val container = pod.getSpec.getContainers.asScala
            .find(_.getName == DEFAULT_EXECUTOR_CONTAINER_NAME)
            .orElse(pod.getSpec.getContainers.asScala.headOption)

          container.filter(c => c.getResources.getLimits != null &&
              c.getResources.getLimits.containsKey("memory")).foreach { c =>
            val limit = Quantity.getAmountInBytes(c.getResources.getLimits.get("memory"))
                .longValue()
            if (usage > limit * threshold) {
              val newLimit = (limit * (1.0 + factor)).toLong
              val newQuantity = new Quantity(newLimit.toString)

              logInfo(log"Increase executor ${MDC(EXECUTOR_ID, execId)} container memory " +
                log"from ${MDC(MEMORY_SIZE, limit)} to ${MDC(MEMORY_SIZE, newLimit)} " +
                log"as usage ${MDC(MEMORY_SIZE, usage)} exceeded threshold.")

              // Patch the pod to update both memory request and limit
              try {
                kubernetesClient.pods()
                  .inNamespace(namespace)
                  .withName(pod.getMetadata.getName)
                  .subresource("resize")
                  .patch(PatchContext.of(PatchType.STRATEGIC_MERGE), new PodBuilder()
                    .withNewMetadata()
                    .endMetadata()
                    .withNewSpec()
                    .addNewContainer()
                    .withName(c.getName)
                    .withNewResources()
                    .addToLimits("memory", newQuantity)
                    .addToRequests("memory", newQuantity)
                    .endResources()
                    .endContainer()
                    .endSpec()
                    .build())
              } catch {
                case e: Throwable =>
                  logInfo(log"Failed to update ${MDC(EXECUTOR_ID, execId)}", e)
              }
            } else {
              logDebug(log"Executor ${MDC(EXECUTOR_ID, execId)} limit " +
                log"${MDC(MEMORY_SIZE, limit)}, usage ${MDC(MEMORY_SIZE, usage)}")
            }
          }
        }
      } catch {
        case e: Throwable =>
          logDebug(log"Failed to handle ${MDC(EXECUTOR_ID, execId)}", e)
      }
    }
  }
}
