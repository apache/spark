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

import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.{Clock, Utils}

private[spark] class ExecutorPodsAllocator(
    conf: SparkConf,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock) extends Logging {

  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)

  private val totalExpectedExecutors = new AtomicInteger(0)

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)

  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)

  private val podCreationTimeout = math.max(podAllocationDelay * 5, 60000)

  private val namespace = conf.get(KUBERNETES_NAMESPACE)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)

  private val driverPod = kubernetesDriverPodName
    .map(name => Option(kubernetesClient.pods()
      .withName(name)
      .get())
      .getOrElse(throw new SparkException(
        s"No pod was found named $kubernetesDriverPodName in the cluster in the " +
          s"namespace $namespace (this was supposed to be the driver pod.).")))

  // Executor IDs that have been requested from Kubernetes but have not been detected in any
  // snapshot yet. Mapped to the timestamp when they were created.
  private val newlyCreatedExecutors = mutable.LinkedHashMap.empty[Long, Long]

  def start(applicationId: String): Unit = {
    snapshotsStore.addSubscriber(podAllocationDelay) {
      onNewSnapshots(applicationId, _)
    }
  }

  def setTotalExpectedExecutors(total: Int): Unit = totalExpectedExecutors.set(total)

  private def onNewSnapshots(applicationId: String, snapshots: Seq[ExecutorPodsSnapshot]): Unit = {
    newlyCreatedExecutors --= snapshots.flatMap(_.executorPods.keys)
    // For all executors we've created against the API but have not seen in a snapshot
    // yet - check the current time. If the current time has exceeded some threshold,
    // assume that the pod was either never created (the API server never properly
    // handled the creation request), or the API server created the pod but we missed
    // both the creation and deletion events. In either case, delete the missing pod
    // if possible, and mark such a pod to be rescheduled below.
    newlyCreatedExecutors.foreach { case (execId, timeCreated) =>
      val currentTime = clock.getTimeMillis()
      if (currentTime - timeCreated > podCreationTimeout) {
        logWarning(s"Executor with id $execId was not detected in the Kubernetes" +
          s" cluster after $podCreationTimeout milliseconds despite the fact that a" +
          " previous allocation attempt tried to create it. The executor may have been" +
          " deleted but the application missed the deletion event.")
        Utils.tryLogNonFatalError {
          kubernetesClient
            .pods()
            .withLabel(SPARK_APP_ID_LABEL, applicationId)
            .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
            .withLabel(SPARK_EXECUTOR_ID_LABEL, execId.toString)
            .delete()
        }
        newlyCreatedExecutors -= execId
      } else {
        logDebug(s"Executor with id $execId was not found in the Kubernetes cluster since it" +
          s" was created ${currentTime - timeCreated} milliseconds ago.")
      }
    }

    if (snapshots.nonEmpty) {
      // Only need to examine the cluster as of the latest snapshot, the "current" state, to see if
      // we need to allocate more executors or not.
      val latestSnapshot = snapshots.last
      val currentRunningExecutors = latestSnapshot.executorPods.values.count {
        case PodRunning(_) => true
        case _ => false
      }
      val currentPendingExecutors = latestSnapshot.executorPods.values.count {
        case PodPending(_) => true
        case _ => false
      }
      val currentTotalExpectedExecutors = totalExpectedExecutors.get
      logDebug(s"Currently have $currentRunningExecutors running executors and" +
        s" $currentPendingExecutors pending executors. $newlyCreatedExecutors executors" +
        s" have been requested but are pending appearance in the cluster.")
      if (newlyCreatedExecutors.isEmpty
        && currentPendingExecutors == 0
        && currentRunningExecutors < currentTotalExpectedExecutors) {
        val numExecutorsToAllocate = math.min(
          currentTotalExpectedExecutors - currentRunningExecutors, podAllocationSize)
        logInfo(s"Going to request $numExecutorsToAllocate executors from Kubernetes.")
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
          newlyCreatedExecutors(newExecutorId) = clock.getTimeMillis()
          logDebug(s"Requested executor with id $newExecutorId from Kubernetes.")
        }
      } else if (currentRunningExecutors >= currentTotalExpectedExecutors) {
        // TODO handle edge cases if we end up with more running executors than expected.
        logDebug("Current number of running executors is equal to the number of requested" +
          " executors. Not scaling up further.")
      } else if (newlyCreatedExecutors.nonEmpty || currentPendingExecutors != 0) {
        logDebug(s"Still waiting for ${newlyCreatedExecutors.size + currentPendingExecutors}" +
          s" executors to begin running before requesting for more executors. # of executors in" +
          s" pending status in the cluster: $currentPendingExecutors. # of executors that we have" +
          s" created but we have not observed as being present in the cluster yet:" +
          s" ${newlyCreatedExecutors.size}.")
      }
    }
  }
}
