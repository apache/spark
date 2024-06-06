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
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{APP_ID, APP_NAME, POD_PHASE, POD_STATE, STATUS, SUBMISSION_ID}

private[k8s] trait LoggingPodStatusWatcher extends Watcher[Pod] {
  def watchOrStop(submissionId: String): Boolean
  def reset(): Unit
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

  private def phase: String = pod.map(_.getStatus.getPhase).getOrElse("unknown")

  override def reset(): Unit = {
    resourceTooOldReceived = false
  }

  override def eventReceived(action: Action, pod: Pod): Unit = {
    this.pod = Option(pod)
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
    logInfo(log"State changed, new state: " +
      log"${MDC(POD_STATE, pod.map(formatPodState).getOrElse("unknown"))}")
  }

  private def hasCompleted(): Boolean = {
    phase == "Succeeded" || phase == "Failed"
  }

  private def closeWatch(): Unit = synchronized {
    podCompleted = true
    this.notifyAll()
  }

  override def watchOrStop(sId: String): Boolean = {
    logInfo(log"Waiting for application ${MDC(APP_NAME, conf.appName)}} with application ID " +
      log"${MDC(APP_ID, appId)} and submission ID ${MDC(SUBMISSION_ID, sId)} to finish...")
    val interval = conf.get(REPORT_INTERVAL)
    synchronized {
      while (!podCompleted && !resourceTooOldReceived) {
        wait(interval)
        logInfo(log"Application status for ${MDC(APP_ID, appId)} (phase: ${MDC(POD_PHASE, phase)})")
      }
    }

    if(podCompleted) {
      logInfo(
        pod.map { p => log"Container final statuses:\n\n${MDC(STATUS, containersDescription(p))}" }
          .getOrElse(log"No containers were found in the driver pod."))
      logInfo(log"Application ${MDC(APP_NAME, conf.appName)} with application ID " +
        log"${MDC(APP_ID, appId)} and submission ID ${MDC(SUBMISSION_ID, sId)} finished")
    }
    podCompleted
  }
}
