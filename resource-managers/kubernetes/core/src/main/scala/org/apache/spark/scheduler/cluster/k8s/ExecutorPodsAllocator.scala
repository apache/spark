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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.internal.Logging

private[spark] class ExecutorPodsAllocator(
    conf: SparkConf,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    eventQueue: ExecutorPodsEventQueue) extends Logging {

  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)

  private val totalExpectedExecutors = new AtomicInteger(0)

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)

  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)
    .getOrElse(throw new SparkException("Must specify the driver pod name"))

  private val driverPod = kubernetesClient.pods()
    .withName(kubernetesDriverPodName)
    .get()

  // Use sets of ids instead of counters to be able to handle duplicate events.

  // Executor IDs that have been requested from Kubernetes but are not running yet.
  private val pendingExecutors = mutable.Set.empty[Long]

  // We could use CoarseGrainedSchedulerBackend#totalRegisteredExecutors here for tallying the
  // executors that are running. But, here we choose instead to maintain all state within this
  // class from the persecptive of the k8s API. Therefore whether or not this scheduler loop
  // believes an executor is running is dictated by the K8s API rather than Spark's RPC events.
  // We may need to consider where these perspectives may differ and which perspective should
  // take precedence.
  private val runningExecutors = mutable.Set.empty[Long]

  def start(applicationId: String): Unit = {
    eventQueue.addSubscriber(podAllocationDelay) { updatedPods =>
      processUpdatedPodEvents(applicationId, updatedPods)
    }
  }

  def setTotalExpectedExecutors(total: Int): Unit = totalExpectedExecutors.set(total)

  private def processUpdatedPodEvents(applicationId: String, updatedPods: Seq[Pod]): Unit = {
    updatedPods.foreach { updatedPod =>
      val execId = updatedPod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL).toLong
      val phase = updatedPod.getStatus.getPhase.toLowerCase
      phase match {
        case "running" =>
          pendingExecutors -= execId
          runningExecutors += execId
        case "failed" | "succeeded" | "error" =>
          pendingExecutors -= execId
          runningExecutors -= execId
      }
    }

    val currentRunningExecutors = runningExecutors.size
    val currentTotalExpectedExecutors = totalExpectedExecutors.get
    if (pendingExecutors.isEmpty && currentRunningExecutors < currentTotalExpectedExecutors) {
      val numExecutorsToAllocate = math.min(
        currentTotalExpectedExecutors - currentRunningExecutors, podAllocationSize)
      logInfo(s"Going to request $numExecutorsToAllocate executors from Kubernetes.")
      val newExecutorIds = mutable.Buffer.empty[Long]
      val podsToAllocate = mutable.Buffer.empty[Pod]
      for ( _ <- 0 until numExecutorsToAllocate) {
        val newExecutorId = EXECUTOR_ID_COUNTER.incrementAndGet()
        val executorConf = KubernetesConf.createExecutorConf(
          conf,
          newExecutorId.toString,
          applicationId,
          driverPod)
        val executorPod = executorBuilder.buildFromFeatures(executorConf)
        val podWithAttachedContainer = new PodBuilder(executorPod.pod)
          .editOrNewSpec()
          .addToContainers(executorPod.container)
          .endSpec()
          .build()
        kubernetesClient.pods().create(podWithAttachedContainer)
        pendingExecutors += newExecutorId
      }
    } else if (currentRunningExecutors == currentTotalExpectedExecutors) {
      logDebug("Current number of running executors is equal to the number of requested" +
        " executors. Not scaling up further.")
    } else if (pendingExecutors.nonEmpty) {
      logInfo(s"Still waiting for ${pendingExecutors.size} executors to begin running before" +
        " requesting for more executors.")
    }
  }
}
