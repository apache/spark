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
package org.apache.spark.deploy.kubernetes.submit.v1

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

/**
 * A monitor for the running Kubernetes pod of a Spark application. Status logging occurs on
 * every state change and also at an interval for liveness.
 *
 * @param podCompletedFuture a CountDownLatch that is set to true when the watched pod finishes
 * @param appId
 * @param interval ms between each state request.  If set to 0 or a negative number, the periodic
 *                 logging will be disabled.
 */
private[kubernetes] class LoggingPodStatusWatcher(podCompletedFuture: CountDownLatch,
                                                  appId: String,
                                                  interval: Long)
    extends Watcher[Pod] with Logging {

  // start timer for periodic logging
  private val scheduler = Executors.newScheduledThreadPool(1)
  private val logRunnable: Runnable = new Runnable {
    override def run() = logShortStatus()
  }
  if (interval > 0) {
    scheduler.scheduleWithFixedDelay(logRunnable, 0, interval, TimeUnit.MILLISECONDS)
  }

  private var pod: Option[Pod] = Option.empty
  private def phase: String = pod.map(_.getStatus().getPhase()).getOrElse("unknown")
  private def status: String = pod.map(_.getStatus().getContainerStatuses().toString())
    .getOrElse("unknown")

  override def eventReceived(action: Action, pod: Pod): Unit = {
    this.pod = Option(pod)
    action match {
      case Action.DELETED =>
        closeWatch()

      case Action.ERROR =>
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

  private def logShortStatus() = {
    logInfo(s"Application status for $appId (phase: $phase)")
  }

  private def logLongStatus() = {
    logInfo("State changed, new state: " + pod.map(formatPodState(_)).getOrElse("unknown"))
  }

  private def hasCompleted(): Boolean = {
    phase == "Succeeded" || phase == "Failed"
  }

  private def closeWatch(): Unit = {
    podCompletedFuture.countDown()
    scheduler.shutdown()
  }

  private def formatPodState(pod: Pod): String = {

    val details = Seq[(String, String)](
      // pod metadata
      ("pod name", pod.getMetadata.getName()),
      ("namespace", pod.getMetadata.getNamespace()),
      ("labels", pod.getMetadata.getLabels().asScala.mkString(", ")),
      ("pod uid", pod.getMetadata.getUid),
      ("creation time", pod.getMetadata.getCreationTimestamp()),

      // spec details
      ("service account name", pod.getSpec.getServiceAccountName()),
      ("volumes", pod.getSpec.getVolumes().asScala.map(_.getName).mkString(", ")),
      ("node name", pod.getSpec.getNodeName()),

      // status
      ("start time", pod.getStatus.getStartTime),
      ("container images",
        pod.getStatus.getContainerStatuses()
            .asScala
            .map(_.getImage)
            .mkString(", ")),
      ("phase", pod.getStatus.getPhase()),
      ("status", pod.getStatus.getContainerStatuses().toString)
    )

    // Use more loggable format if value is null or empty
    details.map { case (k, v) =>
      val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
      s"\n\t $k: $newValue"
    }.mkString("")
  }
}
