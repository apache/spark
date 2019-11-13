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
import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
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

  private lazy val shouldDeleteExecutors = conf.get(KUBERNETES_DELETE_EXECUTORS)

  // Keep track of which pods are inactive to avoid contacting the API server multiple times.
  // This set is cleaned up when a snapshot containing the updated pod is processed.
  private val inactivatedPods = mutable.HashSet.empty[Long]

  def start(schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    val eventProcessingInterval = conf.get(KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL)
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
          case _state if isPodInactive(_state.pod) =>
            inactivatedPods -= execId

          case deleted@PodDeleted(_) =>
            if (removeExecutorFromSpark(schedulerBackend, deleted, execId)) {
              execIdsRemovedInThisRound += execId
              logDebug(s"Snapshot reported deleted executor with id $execId," +
                s" pod name ${state.pod.getMetadata.getName}")
            }
            inactivatedPods -= execId

          case failed@PodFailed(_) =>
            val deleteFromK8s = !execIdsRemovedInThisRound.contains(execId)
            if (onFinalNonDeletedState(failed, execId, schedulerBackend, deleteFromK8s)) {
              execIdsRemovedInThisRound += execId
              logDebug(s"Snapshot reported failed executor with id $execId," +
                s" pod name ${state.pod.getMetadata.getName}")
            }

          case succeeded@PodSucceeded(_) =>
            val deleteFromK8s = !execIdsRemovedInThisRound.contains(execId)
            if (onFinalNonDeletedState(succeeded, execId, schedulerBackend, deleteFromK8s)) {
              execIdsRemovedInThisRound += execId
              if (schedulerBackend.isExecutorActive(execId.toString)) {
                logInfo(s"Snapshot reported succeeded executor with id $execId, " +
                  "even though the application has not requested for it to be removed.")
              } else {
                logDebug(s"Snapshot reported succeeded executor with id $execId," +
                  s" pod name ${state.pod.getMetadata.getName}.")
              }
            }

          case _ =>
        }
      }
    }

    // Clean up any pods from the inactive list that don't match any pods from the last snapshot.
    // This makes sure that we don't keep growing that set indefinitely, in case we end up missing
    // an update for some pod.
    if (inactivatedPods.nonEmpty && snapshots.nonEmpty) {
      inactivatedPods.retain(snapshots.last.executorPods.contains(_))
    }

    // Reconcile the case where Spark claims to know about an executor but the corresponding pod
    // is missing from the cluster. This would occur if we miss a deletion event and the pod
    // transitions immediately from running to absent. We only need to check against the latest
    // snapshot for this, and we don't do this for executors in the deleted executors cache or
    // that we just removed in this round.
    val lostExecutors = if (snapshots.nonEmpty) {
      schedulerBackend.getExecutorIds().map(_.toLong).toSet --
        snapshots.last.executorPods.keySet -- execIdsRemovedInThisRound
    } else {
      Nil
    }

    lostExecutors.foreach { lostId =>
      if (removedExecutorsCache.getIfPresent(lostId) == null) {
        val exitReasonMessage = s"The executor with ID $lostId was not found in the" +
          s" cluster but we didn't get a reason why. Marking the executor as failed. The" +
          s" executor may have been deleted but the driver missed the deletion event."
        logDebug(exitReasonMessage)
        val exitReason = ExecutorExited(
          UNKNOWN_EXIT_CODE,
          exitCausedByApp = false,
          exitReasonMessage)
        schedulerBackend.doRemoveExecutor(lostId.toString, exitReason)
      }
    }

    if (lostExecutors.nonEmpty) {
      logDebug(s"Removed executors with ids ${lostExecutors.mkString(",")}" +
        s" from Spark that were either found to be deleted or non-existent in the cluster.")
    }
  }

  private def onFinalNonDeletedState(
      podState: FinalPodState,
      execId: Long,
      schedulerBackend: KubernetesClusterSchedulerBackend,
      deleteFromK8s: Boolean): Boolean = {
    val deleted = removeExecutorFromSpark(schedulerBackend, podState, execId)
    if (deleteFromK8s) {
      removeExecutorFromK8s(execId, podState.pod)
    }
    deleted
  }

  private def removeExecutorFromK8s(execId: Long, updatedPod: Pod): Unit = {
    Utils.tryLogNonFatalError {
      if (shouldDeleteExecutors) {
        // If deletion failed on a previous try, we can try again if resync informs us the pod
        // is still around.
        // Delete as best attempt - duplicate deletes will throw an exception but the end state
        // of getting rid of the pod is what matters.
        kubernetesClient
          .pods()
          .withName(updatedPod.getMetadata.getName)
          .delete()
      } else if (!inactivatedPods.contains(execId) && !isPodInactive(updatedPod)) {
        // If the config is set to keep the executor  around, mark the pod as "inactive" so it
        // can be ignored in future updates from the API server.
        logDebug(s"Marking executor ${updatedPod.getMetadata.getName} as inactive since " +
          "deletion is disabled.")
        val inactivatedPod = new PodBuilder(updatedPod)
          .editMetadata()
            .addToLabels(Map(SPARK_EXECUTOR_INACTIVE_LABEL -> "true").asJava)
            .endMetadata()
          .build()

        kubernetesClient
          .pods()
          .withName(updatedPod.getMetadata.getName)
          .patch(inactivatedPod)

        inactivatedPods += execId
      }
    }
  }

  private def removeExecutorFromSpark(
      schedulerBackend: KubernetesClusterSchedulerBackend,
      podState: FinalPodState,
      execId: Long): Boolean = {
    if (removedExecutorsCache.getIfPresent(execId) == null) {
      removedExecutorsCache.put(execId, execId)
      val exitReason = findExitReason(podState, execId)
      schedulerBackend.doRemoveExecutor(execId.toString, exitReason)
      true
    } else {
      false
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

  private def isPodInactive(pod: Pod): Boolean = {
    pod.getMetadata.getLabels.get(SPARK_EXECUTOR_INACTIVE_LABEL) == "true"
  }
}

private object ExecutorPodsLifecycleManager {
  val UNKNOWN_EXIT_CODE = -1
}

