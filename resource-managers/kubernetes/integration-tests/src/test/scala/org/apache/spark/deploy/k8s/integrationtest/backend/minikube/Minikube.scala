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

import java.io.File
import java.nio.file.Paths

import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}

import org.apache.spark.deploy.k8s.integrationtest.ProcessUtils
import org.apache.spark.internal.Logging

// TODO support windows
private[spark] object Minikube extends Logging {

  private val MINIKUBE_STARTUP_TIMEOUT_SECONDS = 60

  def getMinikubeIp: String = {
    val outputs = executeMinikube("ip")
      .filter(_.matches("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$"))
    assert(outputs.size == 1, "Unexpected amount of output from minikube ip")
    outputs.head
  }

  def getMinikubeStatus: MinikubeStatus.Value = {
    val statusString = executeMinikube("status")
      .filter(line => line.contains("minikubeVM: ") || line.contains("minikube:"))
      .head
      .replaceFirst("minikubeVM: ", "")
      .replaceFirst("minikube: ", "")
    MinikubeStatus.unapply(statusString)
        .getOrElse(throw new IllegalStateException(s"Unknown status $statusString"))
  }

  def getKubernetesClient: DefaultKubernetesClient = {
    val kubernetesMaster = s"https://${getMinikubeIp}:8443"
    val userHome = System.getProperty("user.home")
    val kubernetesConf = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(kubernetesMaster)
      .withCaCertFile(Paths.get(userHome, ".minikube", "ca.crt").toFile.getAbsolutePath)
      .withClientCertFile(Paths.get(userHome, ".minikube", "apiserver.crt").toFile.getAbsolutePath)
      .withClientKeyFile(Paths.get(userHome, ".minikube", "apiserver.key").toFile.getAbsolutePath)
      .build()
    new DefaultKubernetesClient(kubernetesConf)
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
