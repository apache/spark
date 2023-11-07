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
package org.apache.spark.deploy.k8s.integrationtest.backend.minikube

import io.fabric8.kubernetes.client.{Config, KubernetesClient, KubernetesClientBuilder}

import org.apache.spark.deploy.k8s.integrationtest.ProcessUtils
import org.apache.spark.internal.Logging

// TODO support windows
private[spark] object Minikube extends Logging {
  private val MINIKUBE_STARTUP_TIMEOUT_SECONDS = 60
  private val VERSION_PREFIX = "minikube version: "
  private val HOST_PREFIX = "host: "
  private val KUBELET_PREFIX = "kubelet: "
  private val APISERVER_PREFIX = "apiserver: "
  private val KUBECTL_PREFIX = "kubectl: "
  private val KUBECONFIG_PREFIX = "kubeconfig: "
  private val MINIKUBE_VM_PREFIX = "minikubeVM: "
  private val MINIKUBE_PREFIX = "minikube: "

  lazy val minikubeVersionString =
    executeMinikube(true, "version").find(_.contains(VERSION_PREFIX)).get

  def logVersion(): Unit =
    logInfo(minikubeVersionString)

  def getKubernetesClient: KubernetesClient = {
    // only the three-part version number is matched (the optional suffix like "-beta.0" is dropped)
    val versionArrayOpt = "\\d+\\.\\d+\\.\\d+".r
      .findFirstIn(minikubeVersionString.split(VERSION_PREFIX)(1))
      .map(_.split('.').map(_.toInt))

    versionArrayOpt match {
      case Some(Array(x, y, z)) =>
        if (Ordering.Tuple3[Int, Int, Int].lt((x, y, z), (1, 28, 0))) {
          assert(false, s"Unsupported Minikube version is detected: $minikubeVersionString." +
            "For integration testing Minikube version 1.28.0 or greater is expected.")
        }
      case _ =>
        assert(false, s"Unexpected version format detected in `$minikubeVersionString`." +
          "For minikube version a three-part version number is expected (the optional " +
          "non-numeric suffix is intentionally dropped)")
    }

    new KubernetesClientBuilder().withConfig(Config.autoConfigure("minikube")).build()
  }

  def getMinikubeStatus(): MinikubeStatus.Value = {
    val statusString = executeMinikube(true, "status")
    logInfo(s"Minikube status command output:\n$statusString")
    val hostString = statusString.find(_.contains(HOST_PREFIX))
    val kubeletString = statusString.find(_.contains(KUBELET_PREFIX))
    val apiserverString = statusString.find(_.contains(APISERVER_PREFIX))
    val kubectlString = statusString.find(_.contains(KUBECTL_PREFIX))
    val kubeconfigString = statusString.find(_.contains(KUBECONFIG_PREFIX))
    val hasConfigStatus = kubectlString.isDefined || kubeconfigString.isDefined

    if (hostString.isEmpty || kubeletString.isEmpty || apiserverString.isEmpty ||
        !hasConfigStatus) {
      MinikubeStatus.NONE
    } else {
      val status1 = hostString.get.split(HOST_PREFIX)(1)
      val status2 = kubeletString.get.split(KUBELET_PREFIX)(1)
      val status3 = apiserverString.get.split(APISERVER_PREFIX)(1)
      val isConfigured = if (kubectlString.isDefined) {
        val cfgStatus = kubectlString.get.split(KUBECTL_PREFIX)(1)
        cfgStatus.contains("Correctly Configured:")
      } else {
        kubeconfigString.get.split(KUBECONFIG_PREFIX)(1) == "Configured"
      }
      if (isConfigured) {
        val stats = List(status1, status2, status3)
          .map(MinikubeStatus.unapply)
          .map(_.getOrElse(throw new IllegalStateException(s"Unknown status $statusString")))
        if (stats.exists(_ != MinikubeStatus.RUNNING)) {
          MinikubeStatus.NONE
        } else {
          MinikubeStatus.RUNNING
        }
      } else {
        MinikubeStatus.NONE
      }
    }
  }

  def executeMinikube(logOutput: Boolean, action: String, args: String*): Seq[String] = {
    ProcessUtils.executeProcess(
      Array("bash", "-c", s"MINIKUBE_IN_STYLE=true minikube $action ${args.mkString(" ")}"),
      MINIKUBE_STARTUP_TIMEOUT_SECONDS, dumpOutput = logOutput).filter { x =>
      !x.contains("There is a newer version of minikube") &&
      !x.contains("https://github.com/kubernetes")
    }
  }

  def minikubeServiceAction(args: String*): String = {
    executeMinikube(true, "service", args: _*).head
  }
}

private[spark] object MinikubeStatus extends Enumeration {

  // The following states are listed according to
  // https://github.com/docker/machine/blob/master/libmachine/state/state.go.
  val STARTING = status("Starting")
  val RUNNING = status("Running")
  val PAUSED = status("Paused")
  val STOPPING = status("Stopping")
  val STOPPED = status("Stopped")
  val ERROR = status("Error")
  val TIMEOUT = status("Timeout")
  val SAVED = status("Saved")
  val NONE = status("")

  def status(value: String): Value = new Val(nextId, value)
  def unapply(s: String): Option[Value] = values.find(s == _.toString)
}
