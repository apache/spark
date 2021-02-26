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

import io.fabric8.kubernetes.client.{Config, ConfigBuilder, DefaultKubernetesClient}

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
  private val MINIKUBE_PATH = ".minikube"
  private val APIVERSION_PREFIX = "apiVersion: "
  private val SERVER_PREFIX = "server: "
  private val CA_PREFIX = "certificate-authority: "
  private val CLIENTCERT_PREFIX = "client-certificate: "
  private val CLIENTKEY_PREFIX = "client-key: "

  lazy val minikubeVersionString =
    executeMinikube("version").find(_.contains(VERSION_PREFIX)).get

  def logVersion(): Unit =
    logInfo(minikubeVersionString)

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
      val statusLine = oldMinikube.head
      val finalStatusString = if (statusLine.contains(MINIKUBE_VM_PREFIX)) {
        statusLine.split(MINIKUBE_VM_PREFIX)(1)
      } else {
        statusLine.split(MINIKUBE_PREFIX)(1)
      }
      MinikubeStatus.unapply(finalStatusString)
        .getOrElse(throw new IllegalStateException(s"Unknown status $statusString"))
    }
  }

  def getKubernetesClient: DefaultKubernetesClient = {
    // only the three-part version number is matched (the optional suffix like "-beta.0" is dropped)
    val versionArrayOpt = "\\d+\\.\\d+\\.\\d+".r
      .findFirstIn(minikubeVersionString.split(VERSION_PREFIX)(1))
      .map(_.split('.').map(_.toInt))

    assert(versionArrayOpt.isDefined && versionArrayOpt.get.size == 3,
      s"Unexpected version format detected in `$minikubeVersionString`." +
      "For minikube version a three-part version number is expected (the optional non-numeric " +
      "suffix is intentionally dropped)")

    val kubernetesConf = versionArrayOpt.get match {
      case Array(x, y, z) =>
        // comparing the versions as the kubectl command is only introduced in version v1.1.0:
        // https://github.com/kubernetes/minikube/blob/v1.1.0/CHANGELOG.md
        if (Ordering.Tuple3[Int, Int, Int].gteq((x, y, z), (1, 1, 0))) {
          kubectlBasedKubernetesClientConf
        } else {
          legacyKubernetesClientConf
        }
    }
    new DefaultKubernetesClient(kubernetesConf)
  }

  private def legacyKubernetesClientConf: Config = {
    val kubernetesMaster = s"https://${getMinikubeIp}:8443"
    val userHome = System.getProperty("user.home")
    buildKubernetesClientConf(
      "v1",
      kubernetesMaster,
      Paths.get(userHome, MINIKUBE_PATH, "ca.crt").toFile.getAbsolutePath,
      Paths.get(userHome, MINIKUBE_PATH, "apiserver.crt").toFile.getAbsolutePath,
      Paths.get(userHome, MINIKUBE_PATH, "apiserver.key").toFile.getAbsolutePath)
  }

  private def kubectlBasedKubernetesClientConf: Config = {
    val outputs = executeMinikube("kubectl config view")
    val apiVersionString = outputs.find(_.contains(APIVERSION_PREFIX))
    val serverString = outputs.find(_.contains(SERVER_PREFIX))
    val caString = outputs.find(_.contains(CA_PREFIX))
    val clientCertString = outputs.find(_.contains(CLIENTCERT_PREFIX))
    val clientKeyString = outputs.find(_.contains(CLIENTKEY_PREFIX))

    assert(!apiVersionString.isEmpty && !serverString.isEmpty && !caString.isEmpty &&
      !clientKeyString.isEmpty && !clientKeyString.isEmpty,
      "The output of 'minikube kubectl config view' does not contain all the neccesary attributes")

    buildKubernetesClientConf(
      apiVersionString.get.split(APIVERSION_PREFIX)(1),
      serverString.get.split(SERVER_PREFIX)(1),
      caString.get.split(CA_PREFIX)(1),
      clientCertString.get.split(CLIENTCERT_PREFIX)(1),
      clientKeyString.get.split(CLIENTKEY_PREFIX)(1))
  }

  private def buildKubernetesClientConf(apiVersion: String, masterUrl: String, caCertFile: String,
      clientCertFile: String, clientKeyFile: String): Config = {
    logInfo(s"building kubernetes config with apiVersion: $apiVersion, masterUrl: $masterUrl, " +
      s"caCertFile: $caCertFile, clientCertFile: $clientCertFile, clientKeyFile: $clientKeyFile")
    new ConfigBuilder()
      .withApiVersion(apiVersion)
      .withMasterUrl(masterUrl)
      .withCaCertFile(caCertFile)
      .withClientCertFile(clientCertFile)
      .withClientKeyFile(clientKeyFile)
      .build()
  }

  // Covers minikube status output after Minikube V0.30.
  private def getIfNewMinikubeStatus(statusString: Seq[String]): MinikubeStatus.Value = {
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

  def executeMinikube(action: String, args: String*): Seq[String] = {
    ProcessUtils.executeProcess(
      Array("bash", "-c", s"MINIKUBE_IN_STYLE=true minikube $action ${args.mkString(" ")}"),
      MINIKUBE_STARTUP_TIMEOUT_SECONDS).filter{x =>
      !x.contains("There is a newer version of minikube") &&
      !x.contains("https://github.com/kubernetes")
    }
  }

  def minikubeServiceAction(args: String*): String = {
    executeMinikube("service", args: _*).head
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
