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
package org.apache.spark.deploy.k8s.submit

import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting, ContainerStatus, Job, Pod, Time}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

private[k8s] trait LoggingJobStatusWatcher extends Watcher[Job] {
  def awaitCompletion(): Unit
}

 /**
  * A monitor for the running Kubernetes pod of a Spark application. Status logging occurs on
  * every state change and also at an interval for liveness.
  *
  * @param appId application ID.
  * @param kubernetesClient kubernetes client.
  */
private[k8s] class LoggingJobStatusWatcherImpl(appId: String, kubernetesClient: KubernetesClient)
  extends LoggingJobStatusWatcher with Logging {

  private val jobCompletedFuture = new CountDownLatch(1)

  private var job : Option[Job] = None

  private def phase: String = job match {
    case Some(j) if j.getStatus.getConditions.isEmpty => "Running"
    case Some(j) => j.getStatus.getConditions.get(0).getType
    case _ => "unknown"
  }

  override def eventReceived(action: Action, job: Job): Unit = {
    this.job = Option(job)
    action match {
      case Action.DELETED | Action.ERROR =>
        closeWatch()

      case _ =>
        logLongStatus()
        if (hasCompleted()) {
          closeWatch()
        }
    }
  }

  override def onClose(e: KubernetesClientException): Unit = {
    logDebug(s"Stopping watching application $appId with last-observed phase $phase")
    closeWatch()
  }

  private def logLongStatus() = {
    logInfo("State changed, new state: " + job.map(formatPodState).getOrElse("unknown"))
  }

  private def hasCompleted(): Boolean = {
    phase == "Complete" || phase == "Failed"
  }

  private def closeWatch(): Unit = {
    jobCompletedFuture.countDown()
  }

  private def formatPodState(job: Job): String = {
    // Get driver pod from job
    val pod: Pod = findDriverPod(job)
    val details = Seq[(String, String)](
      // pod metadata
      ("pod name", pod.getMetadata.getName),
      ("namespace", pod.getMetadata.getNamespace),
      ("labels", pod.getMetadata.getLabels.asScala.mkString(", ")),
      ("pod uid", pod.getMetadata.getUid),
      ("creation time", formatTime(pod.getMetadata.getCreationTimestamp)),

      // spec details
      ("service account name", pod.getSpec.getServiceAccountName),
      ("volumes", pod.getSpec.getVolumes.asScala.map(_.getName).mkString(", ")),
      ("node name", pod.getSpec.getNodeName),

      // status
      ("start time", formatTime(pod.getStatus.getStartTime)),
      ("container images",
        pod.getStatus.getContainerStatuses
          .asScala
          .map(_.getImage)
          .mkString(", ")),
      ("phase", pod.getStatus.getPhase),
      ("status", pod.getStatus.getContainerStatuses.toString)
    )

    formatPairsBundle(details)
  }

  private def formatPairsBundle(pairs: Seq[(String, String)]) = {
    // Use more loggable format if value is null or empty
    pairs.map {
      case (k, v) => s"\n\t $k: ${Option(v).filter(_.nonEmpty).getOrElse("N/A")}"
    }.mkString("")
  }

  private def findDriverPod(job: Job): Pod = {
    val pods = kubernetesClient.pods()
      .withLabel("job-name", job.getMetadata.getName).list()
    pods.getItems.asScala.find(p =>
      p.getMetadata.getName.startsWith(job.getMetadata.getName)).get
  }

  override def awaitCompletion(): Unit = {
    jobCompletedFuture.await()
    job.foreach{ j =>
      val pod: Pod = findDriverPod(j)
      logInfo(s"Container final statuses:\n\n${containersDescription(pod)}"
      )}
  }

  private def containersDescription(p: Pod): String = {
    p.getStatus.getContainerStatuses.asScala.map { status =>
      Seq(
        ("Container name", status.getName),
        ("Container image", status.getImage)) ++
        containerStatusDescription(status)
    }.map(formatPairsBundle).mkString("\n\n")
  }

  private def containerStatusDescription(
      containerStatus: ContainerStatus): Seq[(String, String)] = {
    val state = containerStatus.getState
    Option(state.getRunning)
      .orElse(Option(state.getTerminated))
      .orElse(Option(state.getWaiting))
      .map {
        case running: ContainerStateRunning =>
          Seq(
            ("Container state", "Running"),
            ("Container started at", formatTime(running.getStartedAt)))
        case waiting: ContainerStateWaiting =>
          Seq(
            ("Container state", "Waiting"),
            ("Pending reason", waiting.getReason))
        case terminated: ContainerStateTerminated =>
          Seq(
            ("Container state", "Terminated"),
            ("Exit code", terminated.getExitCode.toString))
        case unknown =>
          throw new SparkException(s"Unexpected container status type ${unknown.getClass}.")
        }.getOrElse(Seq(("Container state", "N/A")))
  }

  private def formatTime(time: Time): String = {
    if (time != null) time.getTime else "N/A"
  }
}
