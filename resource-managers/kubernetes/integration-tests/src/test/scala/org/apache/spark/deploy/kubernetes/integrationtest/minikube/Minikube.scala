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
package org.apache.spark.deploy.kubernetes.integrationtest.minikube

import java.io.{BufferedReader, InputStreamReader}
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import javax.net.ssl.X509TrustManager

import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}
import io.fabric8.kubernetes.client.internal.SSLUtils
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.deploy.rest.kubernetes.v1.HttpClientUtil
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

// TODO support windows
private[spark] object Minikube extends Logging {
  private val MINIKUBE_EXECUTABLE_DEST = if (Utils.isMac) {
    Paths.get("target", "minikube-bin", "darwin-amd64", "minikube").toFile
  } else if (Utils.isWindows) {
    throw new IllegalStateException("Executing Minikube based integration tests not yet " +
      " available on Windows.")
  } else {
    Paths.get("target", "minikube-bin", "linux-amd64", "minikube").toFile
  }

  private val EXPECTED_DOWNLOADED_MINIKUBE_MESSAGE = "Minikube is not downloaded, expected at " +
    s"${MINIKUBE_EXECUTABLE_DEST.getAbsolutePath}"

  private val MINIKUBE_STARTUP_TIMEOUT_SECONDS = 60

  def startMinikube(): Unit = synchronized {
    assert(MINIKUBE_EXECUTABLE_DEST.exists(), EXPECTED_DOWNLOADED_MINIKUBE_MESSAGE)
    if (getMinikubeStatus != MinikubeStatus.RUNNING) {
      executeMinikube("start", "--memory", "6000", "--cpus", "8")
    } else {
      logInfo("Minikube is already started.")
    }
  }

  def getMinikubeIp: String = synchronized {
    assert(MINIKUBE_EXECUTABLE_DEST.exists(), EXPECTED_DOWNLOADED_MINIKUBE_MESSAGE)
    val outputs = executeMinikube("ip")
      .filter(_.matches("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$"))
    assert(outputs.size == 1, "Unexpected amount of output from minikube ip")
    outputs.head
  }

  def getMinikubeStatus: MinikubeStatus.Value = synchronized {
    assert(MINIKUBE_EXECUTABLE_DEST.exists(), EXPECTED_DOWNLOADED_MINIKUBE_MESSAGE)
    val statusString = executeMinikube("status")
      .filter(_.contains("minikubeVM: "))
      .head
      .replaceFirst("minikubeVM: ", "")
    MinikubeStatus.unapply(statusString)
        .getOrElse(throw new IllegalStateException(s"Unknown status $statusString"))
  }

  def getDockerEnv: Map[String, String] = synchronized {
    assert(MINIKUBE_EXECUTABLE_DEST.exists(), EXPECTED_DOWNLOADED_MINIKUBE_MESSAGE)
    executeMinikube("docker-env", "--shell", "bash")
        .filter(_.startsWith("export"))
        .map(_.replaceFirst("export ", "").split('='))
        .map(arr => (arr(0), arr(1).replaceAllLiterally("\"", "")))
        .toMap
  }

  def deleteMinikube(): Unit = synchronized {
    assert(MINIKUBE_EXECUTABLE_DEST.exists, EXPECTED_DOWNLOADED_MINIKUBE_MESSAGE)
    if (getMinikubeStatus != MinikubeStatus.DOES_NOT_EXIST) {
      executeMinikube("delete")
    } else {
      logInfo("Minikube was already not running.")
    }
  }

  def getKubernetesClient: DefaultKubernetesClient = synchronized {
    val kubernetesMaster = s"https://$getMinikubeIp:8443"
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

  def getService[T: ClassTag](
      serviceName: String,
      namespace: String,
      servicePortName: String,
      servicePath: String = ""): T = synchronized {
    val kubernetesMaster = s"https://$getMinikubeIp:8443"
    val url = s"${
      Array[String](
        kubernetesMaster,
        "api", "v1", "proxy",
        "namespaces", namespace,
        "services", serviceName).mkString("/")}" +
      s":$servicePortName$servicePath"
    val userHome = System.getProperty("user.home")
    val kubernetesConf = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(kubernetesMaster)
      .withCaCertFile(Paths.get(userHome, ".minikube", "ca.crt").toFile.getAbsolutePath)
      .withClientCertFile(Paths.get(userHome, ".minikube", "apiserver.crt").toFile.getAbsolutePath)
      .withClientKeyFile(Paths.get(userHome, ".minikube", "apiserver.key").toFile.getAbsolutePath)
      .build()
    val sslContext = SSLUtils.sslContext(kubernetesConf)
    val trustManager = SSLUtils.trustManagers(kubernetesConf)(0).asInstanceOf[X509TrustManager]
    HttpClientUtil.createClient[T](Set(url), 5, sslContext.getSocketFactory, trustManager)
  }

  def executeMinikubeSsh(command: String): Unit = {
    executeMinikube("ssh", command)
  }

  private def executeMinikube(action: String, args: String*): Seq[String] = {
    if (!MINIKUBE_EXECUTABLE_DEST.canExecute) {
      if (!MINIKUBE_EXECUTABLE_DEST.setExecutable(true)) {
        throw new IllegalStateException("Failed to make the Minikube binary executable.")
      }
    }
    val fullCommand = Array(MINIKUBE_EXECUTABLE_DEST.getAbsolutePath, action) ++ args
    val pb = new ProcessBuilder().command(fullCommand: _*)
    pb.redirectErrorStream(true)
    val proc = pb.start()
    val outputLines = new ArrayBuffer[String]

    Utils.tryWithResource(new InputStreamReader(proc.getInputStream)) { procOutput =>
      Utils.tryWithResource(new BufferedReader(procOutput)) { (bufferedOutput: BufferedReader) =>
        var line: String = null
        do {
          line = bufferedOutput.readLine()
          if (line != null) {
            logInfo(line)
            outputLines += line
          }
        } while (line != null)
      }
    }
    assert(proc.waitFor(MINIKUBE_STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS),
      s"Timed out while executing $action on minikube.")
    assert(proc.exitValue == 0, s"Failed to execute minikube $action ${args.mkString(" ")}")
    outputLines.toSeq
  }
}

private[spark] object MinikubeStatus extends Enumeration {

  val RUNNING = status("Running")
  val STOPPED = status("Stopped")
  val DOES_NOT_EXIST = status("Does Not Exist")
  val SAVED = status("Saved")

  def status(value: String): Value = new Val(nextId, value)
  def unapply(s: String): Option[Value] = values.find(s == _.toString)
}
