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

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{Watcher, WatcherException}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.KubernetesDriverConf
import org.apache.spark.deploy.k8s.KubernetesUtils._
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}

private[k8s] trait LoggingPodStatusWatcher extends Watcher[Pod] {
  def watchOrStop(submissionId: String): Boolean
  def reset(): Unit
  def setLauncherBackend(lb: LauncherBackend): Unit
}

/**
 * A monitor for the running Kubernetes pod of a Spark application. Status logging occurs on
 * every state change and also at an interval for liveness.
 *
 * @param conf kubernetes driver conf.
 */
private[k8s] class LoggingPodStatusWatcherImpl(conf: KubernetesDriverConf)
  extends LoggingPodStatusWatcher with Logging {

  private val appId = conf.appId

  private var podCompleted = false

  private var resourceTooOldReceived = false

  private var pod = Option.empty[Pod]

  private def phase: String = pod.map(_.getStatus.getPhase).getOrElse("Unknown")

  private var lb = Option.empty[LauncherBackend]

  private var prevPhase = "Unknown"

  override def reset(): Unit = {
    resourceTooOldReceived = false
  }

  override def setLauncherBackend(lb: LauncherBackend): Unit = this.lb = Option(lb)

  override def eventReceived(action: Action, pod: Pod): Unit = {
    this.pod = Option(pod)
    action match {
      case Action.DELETED | Action.ERROR =>
        closeWatch()

      case _ =>
        if (phase != prevPhase) {
          lb.foreach(_.setState {
            phase match {
              case "Pending" => SparkAppHandle.State.SUBMITTED
              case "Running" => SparkAppHandle.State.RUNNING
              case "Succeeded" => SparkAppHandle.State.FINISHED
              case "Failed" => SparkAppHandle.State.FAILED
              case "Unknown" => SparkAppHandle.State.UNKNOWN
            }
          })
          prevPhase = phase
        }
        logLongStatus()
        if (hasCompleted()) {
          closeWatch()
        }
    }
  }

  override def onClose(e: WatcherException): Unit = {
    logDebug(s"Stopping watching application $appId with last-observed phase $phase")
    if(e != null && e.isHttpGone) {
      resourceTooOldReceived = true
      logDebug(s"Got HTTP Gone code, resource version changed in k8s api: $e")
    } else {
      closeWatch()
    }
  }

  override def onClose(): Unit = {
    logDebug(s"Stopping watching application $appId with last-observed phase $phase")
    closeWatch()
  }

  private def logLongStatus(): Unit = {
    logInfo("State changed, new state: " + pod.map(formatPodState).getOrElse("unknown"))
  }

  private def hasCompleted(): Boolean = {
    phase == "Succeeded" || phase == "Failed"
  }

  private def closeWatch(): Unit = synchronized {
    podCompleted = true
    lb.foreach(_.close())
    this.notifyAll()
  }

  override def watchOrStop(sId: String): Boolean = {
    logInfo(s"Waiting for application ${conf.appName} with application ID $appId " +
      s"and submission ID $sId to finish...")
    val interval = conf.get(REPORT_INTERVAL)
    synchronized {
      while (!podCompleted && !resourceTooOldReceived) {
        wait(interval)
        logInfo(s"Application status for $appId (phase: $phase)")
      }
    }

    if(podCompleted) {
      logInfo(
        pod.map { p => s"Container final statuses:\n\n${containersDescription(p)}" }
          .getOrElse("No containers were found in the driver pod."))
      logInfo(s"Application ${conf.appName} with application ID $appId " +
        s"and submission ID $sId finished")
    }
    podCompleted
  }
}
