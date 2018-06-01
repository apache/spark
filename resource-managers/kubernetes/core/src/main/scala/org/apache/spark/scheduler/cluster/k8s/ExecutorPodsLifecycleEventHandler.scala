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

import com.google.common.cache.{Cache, CacheBuilder}
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.scheduler.ExecutorExited
import org.apache.spark.util.Utils

private[spark] class ExecutorPodsLifecycleEventHandler(
    conf: SparkConf,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    podsEventQueue: ExecutorPodsEventQueue,
    // Use a best-effort to track which executors have been removed already. It's not generally
    // job-breaking if we remove executors more than once but it's ideal if we make an attempt
    // to avoid doing so. Expire cache entries so that this data structure doesn't grow beyond
    // bounds.
    removedExecutorsCache: Cache[java.lang.Long, java.lang.Long]) {

  import ExecutorPodsLifecycleEventHandler._

  private val eventProcessingInterval = conf.get(KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL)

  def start(schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    podsEventQueue.addSubscriber(
      eventProcessingInterval,
      new ExecutorPodBatchSubscriber(
        processUpdatedPod(schedulerBackend),
        () => {}))
  }

  private def processUpdatedPod(
      schedulerBackend: KubernetesClusterSchedulerBackend)
      : PartialFunction[ExecutorPodState, Unit] = {
    case deleted @ PodDeleted(pod) =>
      removeExecutorFromSpark(schedulerBackend, pod, deleted.execId())
    case errorOrSucceeded @ (PodFailed(_) | PodSucceeded(_)) =>
      removeExecutorFromK8s(errorOrSucceeded.pod)
      removeExecutorFromSpark(schedulerBackend, errorOrSucceeded.pod, errorOrSucceeded.execId())
    case _ =>
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
      pod: Pod,
      execId: Long): Unit = {
    if (removedExecutorsCache.getIfPresent(execId) == null) {
      removedExecutorsCache.put(execId, execId)
      val exitReason = findExitReason(pod, execId)
      schedulerBackend.doRemoveExecutor(execId.toString, exitReason)
    }
  }

  private def findExitReason(pod: Pod, execId: Long): ExecutorExited = {
    val exitCode = findExitCode(pod)
    val (exitCausedByApp, exitMessage) = if (isDeleted(pod)) {
      (false, s"The executor with id $execId was deleted by a user or the framework.")
    } else {
      val msg = exitReasonMessage(pod, execId, exitCode)
      (true, msg)
    }
    ExecutorExited(exitCode, exitCausedByApp, exitMessage)
  }

  private def exitReasonMessage(pod: Pod, execId: Long, exitCode: Int) = {
    s"""
       |The executor with id $execId exited with exit code $exitCode.
       |The API gave the following brief reason: ${pod.getStatus.getReason}
       |The API gave the following message: ${pod.getStatus.getMessage}
       |The API gave the following container statuses:
       |
       |${pod.getStatus.getContainerStatuses.asScala.map(_.toString).mkString("\n===\n")}
      """.stripMargin
  }

  private def isDeleted(pod: Pod): Boolean = pod.getMetadata.getDeletionTimestamp != null

  private def findExitCode(pod: Pod): Int = {
    pod.getStatus.getContainerStatuses.asScala.find { containerStatus =>
      containerStatus.getState.getTerminated != null
    }.map { terminatedContainer =>
      terminatedContainer.getState.getTerminated.getExitCode.toInt
    }.getOrElse(UNKNOWN_EXIT_CODE)
  }
}

private object ExecutorPodsLifecycleEventHandler {
  val UNKNOWN_EXIT_CODE = -1
}

