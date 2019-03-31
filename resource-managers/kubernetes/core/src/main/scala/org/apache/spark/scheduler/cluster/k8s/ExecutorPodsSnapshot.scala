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

import java.util.Locale

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging

/**
 * An immutable view of the current executor pods that are running in the cluster.
 */
private[spark] case class ExecutorPodsSnapshot(executorPods: Map[Long, ExecutorPodState]) {

  import ExecutorPodsSnapshot._

  def withUpdate(updatedPod: Pod): ExecutorPodsSnapshot = {
    val newExecutorPods = executorPods ++ toStatesByExecutorId(Seq(updatedPod))
    new ExecutorPodsSnapshot(newExecutorPods)
  }
}

object ExecutorPodsSnapshot extends Logging {

  def apply(executorPods: Seq[Pod]): ExecutorPodsSnapshot = {
    ExecutorPodsSnapshot(toStatesByExecutorId(executorPods))
  }

  def apply(): ExecutorPodsSnapshot = ExecutorPodsSnapshot(Map.empty[Long, ExecutorPodState])

  private def toStatesByExecutorId(executorPods: Seq[Pod]): Map[Long, ExecutorPodState] = {
    executorPods.map { pod =>
      (pod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL).toLong, toState(pod))
    }.toMap
  }

  private def toState(pod: Pod): ExecutorPodState = {
    if (isDeleted(pod)) {
      PodDeleted(pod)
    } else {
      val phase = pod.getStatus.getPhase.toLowerCase(Locale.ROOT)
      phase match {
        case "pending" =>
          PodPending(pod)
        case "running" =>
          PodRunning(pod)
        case "failed" =>
          PodFailed(pod)
        case "succeeded" =>
          PodSucceeded(pod)
        case _ =>
          logWarning(s"Received unknown phase $phase for executor pod with name" +
            s" ${pod.getMetadata.getName} in namespace ${pod.getMetadata.getNamespace}")
          PodUnknown(pod)
      }
    }
  }

  private def isDeleted(pod: Pod): Boolean = pod.getMetadata.getDeletionTimestamp != null
}
