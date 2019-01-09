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

import com.google.common.cache.Cache
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.KubernetesUtils._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorExited
import org.apache.spark.util.Utils

private[spark] class ExecutorPodsLifecycleManager(
    val conf: SparkConf,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    // Use a best-effort to track which executors have been removed already. It's not generally
    // job-breaking if we remove executors more than once but it's ideal if we make an attempt
    // to avoid doing so. Expire cache entries so that this data structure doesn't grow beyond
    // bounds.
    removedExecutorsCache: Cache[java.lang.Long, java.lang.Long]) extends Logging {

  import ExecutorPodsLifecycleManager._

  private val eventProcessingInterval = conf.get(KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL)

  private lazy val shouldDeleteExecutors = conf.get(KUBERNETES_DELETE_EXECUTORS)

  def start(schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    snapshotsStore.addSubscriber(eventProcessingInterval) {
      onNewSnapshots(schedulerBackend, _)
    }
  }

  private def onNewSnapshots(
      schedulerBackend: KubernetesClusterSchedulerBackend,
      snapshots: Seq[ExecutorPodsSnapshot]): Unit = {
    val execIdsRemovedInThisRound = mutable.HashSet.empty[Long]
    snapshots.foreach { snapshot =>
      snapshot.executorPods.foreach { case (execId, state) =>
        state match {
          case deleted@PodDeleted(_) =>
            logDebug(s"Snapshot reported deleted executor with id $execId," +
              s" pod name ${state.pod.getMetadata.getName}")
            removeExecutorFromSpark(schedulerBackend, deleted, execId)
            execIdsRemovedInThisRound += execId
          case failed@PodFailed(_) =>
            logDebug(s"Snapshot reported failed executor with id $execId," +
              s" pod name ${state.pod.getMetadata.getName}")
            onFinalNonDeletedState(failed, execId, schedulerBackend, execIdsRemovedInThisRound)
          case succeeded@PodSucceeded(_) =>
            logDebug(s"Snapshot reported succeeded executor with id $execId," +
              s" pod name ${state.pod.getMetadata.getName}. Note that succeeded executors are" +
              s" unusual unless Spark specifically informed the executor to exit.")
            onFinalNonDeletedState(succeeded, execId, schedulerBackend, execIdsRemovedInThisRound)
          case _ =>
        }
      }
    }

    // Reconcile the case where Spark claims to know about an executor but the corresponding pod
    // is missing from the cluster. This would occur if we miss a deletion event and the pod
    // transitions immediately from running io absent. We only need to check against the latest
    // snapshot for this, and we don't do this for executors in the deleted executors cache or
    // that we just removed in this round.
    if (snapshots.nonEmpty) {
      val latestSnapshot = snapshots.last
      (schedulerBackend.getExecutorIds().map(_.toLong).toSet
        -- latestSnapshot.executorPods.keySet
        -- execIdsRemovedInThisRound).foreach { missingExecutorId =>
        if (removedExecutorsCache.getIfPresent(missingExecutorId) == null) {
          val exitReasonMessage = s"The executor with ID $missingExecutorId was not found in the" +
            s" cluster but we didn't get a reason why. Marking the executor as failed. The" +
            s" executor may have been deleted but the driver missed the deletion event."
          logDebug(exitReasonMessage)
          val exitReason = ExecutorExited(
            UNKNOWN_EXIT_CODE,
            exitCausedByApp = false,
            exitReasonMessage)
          schedulerBackend.doRemoveExecutor(missingExecutorId.toString, exitReason)
          execIdsRemovedInThisRound += missingExecutorId
        }
      }
    }

    if (execIdsRemovedInThisRound.nonEmpty) {
      logDebug(s"Removed executors with ids ${execIdsRemovedInThisRound.mkString(",")}" +
        s" from Spark that were either found to be deleted or non-existent in the cluster.")
    }
  }

  private def onFinalNonDeletedState(
      podState: FinalPodState,
      execId: Long,
      schedulerBackend: KubernetesClusterSchedulerBackend,
      execIdsRemovedInRound: mutable.Set[Long]): Unit = {
    removeExecutorFromSpark(schedulerBackend, podState, execId)
    if (shouldDeleteExecutors) {
      removeExecutorFromK8s(podState.pod)
    }
    execIdsRemovedInRound += execId
  }

  private def removeExecutorFromK8s(updatedPod: Pod): Unit = {
    // If deletion failed on a previous try, we can try again if resync informs us the pod
    // is still around.
    // Delete as best attempt - duplicate deletes will throw an exception but the end state
    // of getting rid of the pod is what matters.
    Utils.tryLogNonFatalError {
      kubernetesClient
        .pods()
        .withName(updatedPod.getMetadata.getName)
        .delete()
    }
  }

  private def removeExecutorFromSpark(
      schedulerBackend: KubernetesClusterSchedulerBackend,
      podState: FinalPodState,
      execId: Long): Unit = {
    if (removedExecutorsCache.getIfPresent(execId) == null) {
      removedExecutorsCache.put(execId, execId)
      val exitReason = findExitReason(podState, execId)
      schedulerBackend.doRemoveExecutor(execId.toString, exitReason)
    }
  }

  private def findExitReason(podState: FinalPodState, execId: Long): ExecutorExited = {
    val exitCode = findExitCode(podState)
    val (exitCausedByApp, exitMessage) = podState match {
      case PodDeleted(_) =>
        (false, s"The executor with id $execId was deleted by a user or the framework.")
      case _ =>
        val msg = exitReasonMessage(podState, execId, exitCode)
        (true, msg)
    }
    ExecutorExited(exitCode, exitCausedByApp, exitMessage)
  }

  private def exitReasonMessage(podState: FinalPodState, execId: Long, exitCode: Int) = {
    val pod = podState.pod
    val reason = Option(pod.getStatus.getReason)
    val message = Option(pod.getStatus.getMessage)
    s"""
       |The executor with id $execId exited with exit code $exitCode.
       |The API gave the following brief reason: ${reason.getOrElse("N/A")}
       |The API gave the following message: ${message.getOrElse("N/A")}
       |The API gave the following container statuses:
       |
       |${containersDescription(pod)}
      """.stripMargin
  }

  private def findExitCode(podState: FinalPodState): Int = {
    podState.pod.getStatus.getContainerStatuses.asScala.find { containerStatus =>
      containerStatus.getState.getTerminated != null
    }.map { terminatedContainer =>
      terminatedContainer.getState.getTerminated.getExitCode.toInt
    }.getOrElse(UNKNOWN_EXIT_CODE)
  }
}

private object ExecutorPodsLifecycleManager {
  val UNKNOWN_EXIT_CODE = -1
}

