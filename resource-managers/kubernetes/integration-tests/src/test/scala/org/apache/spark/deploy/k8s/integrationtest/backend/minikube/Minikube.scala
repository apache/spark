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

import java.nio.file.Paths

import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}

import org.apache.spark.deploy.k8s.integrationtest.ProcessUtils
import org.apache.spark.internal.Logging

// TODO support windows
private[spark] object Minikube extends Logging {
  private val MINIKUBE_STARTUP_TIMEOUT_SECONDS = 60
  private val HOST_PREFIX = "host:"
  private val KUBELET_PREFIX = "kubelet:"
  private val APISERVER_PREFIX = "apiserver:"
  private val KUBECTL_PREFIX = "kubectl:"
  private val MINIKUBE_VM_PREFIX = "minikubeVM: "
  private val MINIKUBE_PREFIX = "minikube: "
  private val MINIKUBE_PATH = ".minikube"

  def logVersion(): Unit = {
    logInfo(executeMinikube("version").mkString("\n"))
  }

  def getMinikubeIp: String = {
    val outputs = executeMinikube("ip")
      .filter(_.matches("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$"))
    assert(outputs.size == 1, "Unexpected amount of output from minikube ip")
    outputs.head
  }

  def getMinikubeStatus: MinikubeStatus.Value = {
    val statusString = executeMinikube("status")
    logInfo(s"Minikube status command output:\n$statusString")
    // up to minikube version v0.30.0 use this to check for minikube status
    val oldMinikube = statusString
      .filter(line => line.contains(MINIKUBE_VM_PREFIX) || line.contains(MINIKUBE_PREFIX))

    if (oldMinikube.isEmpty) {
      getIfNewMinikubeStatus(statusString)
    } else {
      val finalStatusString = oldMinikube
        .head
        .replaceFirst(MINIKUBE_VM_PREFIX, "")
        .replaceFirst(MINIKUBE_PREFIX, "")
      MinikubeStatus.unapply(finalStatusString)
        .getOrElse(throw new IllegalStateException(s"Unknown status $statusString"))
    }
  }

  def getKubernetesClient: DefaultKubernetesClient = {
    val kubernetesMaster = s"https://${getMinikubeIp}:8443"
    val userHome = System.getProperty("user.home")
    val kubernetesConf = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(kubernetesMaster)
      .withCaCertFile(
        Paths.get(userHome, MINIKUBE_PATH, "ca.crt").toFile.getAbsolutePath)
      .withClientCertFile(
        Paths.get(userHome, MINIKUBE_PATH, "apiserver.crt").toFile.getAbsolutePath)
      .withClientKeyFile(
        Paths.get(userHome, MINIKUBE_PATH, "apiserver.key").toFile.getAbsolutePath)
      .build()
    new DefaultKubernetesClient(kubernetesConf)
  }

  // Covers minikube status output after Minikube V0.30.
  private def getIfNewMinikubeStatus(statusString: Seq[String]): MinikubeStatus.Value = {
    val hostString = statusString.find(_.contains(s"$HOST_PREFIX "))
    val kubeletString = statusString.find(_.contains(s"$KUBELET_PREFIX "))
    val apiserverString = statusString.find(_.contains(s"$APISERVER_PREFIX "))
    val kubectlString = statusString.find(_.contains(s"$KUBECTL_PREFIX "))

    if (hostString.isEmpty || kubeletString.isEmpty
      || apiserverString.isEmpty || kubectlString.isEmpty) {
      MinikubeStatus.NONE
    } else {
      val status1 = hostString.get.replaceFirst(s"$HOST_PREFIX ", "")
      val status2 = kubeletString.get.replaceFirst(s"$KUBELET_PREFIX ", "")
      val status3 = apiserverString.get.replaceFirst(s"$APISERVER_PREFIX ", "")
      val status4 = kubectlString.get.replaceFirst(s"$KUBECTL_PREFIX ", "")
      if (!status4.contains("Correctly Configured:")) {
        MinikubeStatus.NONE
      } else {
        val stats = List(status1, status2, status3)
          .map(MinikubeStatus.unapply)
          .map(_.getOrElse(throw new IllegalStateException(s"Unknown status $statusString")))
        if (stats.exists(_ != MinikubeStatus.RUNNING)) {
          MinikubeStatus.NONE
        } else {
          MinikubeStatus.RUNNING
        }
      }
    }
  }

  private def executeMinikube(action: String, args: String*): Seq[String] = {
    ProcessUtils.executeProcess(
      Array("bash", "-c", s"minikube $action") ++ args, MINIKUBE_STARTUP_TIMEOUT_SECONDS)
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
