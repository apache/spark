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

package org.apache.spark.deploy.k8s.integrationtest

import java.net._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import io.fabric8.kubernetes.api.model.Service
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Span}

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite._
import org.apache.spark.util

private[spark] trait StreamingCompatibilitySuite {

  k8sSuite: KubernetesSuite =>

  private def startSocketServer(): (String, Int, ServerSocket) = {
    val hostname = util.Utils.localHostName()
    val hostAddress: String = InetAddress.getByName(hostname).getHostAddress
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress(hostAddress, 0))
    val host = serverSocket.getInetAddress.getHostAddress
    val port = serverSocket.getLocalPort
    logInfo(s"Started test server socket at $host:$port")
    Future {
      while (!serverSocket.isClosed) {
        val socket: Socket = serverSocket.accept()
        logInfo(s"Received connection on $socket")
        for (i <- 1 to 10 ) {
          if (socket.isConnected && !serverSocket.isClosed) {
            socket.getOutputStream.write("spark-streaming-kube test.\n".getBytes())
            socket.getOutputStream.flush()
            Thread.sleep(100)
          }
        }
        socket.close()
      }
    }
    (host, port, serverSocket)
  }

  test("Run spark streaming in client mode.", k8sTestTag) {
    val (host, port, serverSocket) = startSocketServer()
    val (driverPort: Int, blockManagerPort: Int, driverService: Service) =
      driverServiceSetup(driverPodName)
    try {
      val driverPod = setupSparkStreamingPod("client", driverPodName)
        .addToArgs("--conf", s"spark.driver.host="
          + s"${driverService.getMetadata.getName}.${kubernetesTestComponents.namespace}.svc")
        .addToArgs("--conf", s"spark.driver.port=$driverPort")
        .addToArgs("--conf", s"spark.driver.blockManager.port=$blockManagerPort")
        .addToArgs("streaming.NetworkWordCount")
        .addToArgs(host, port.toString)
        .endContainer()
        .endSpec()
        .done()

      Eventually.eventually(TIMEOUT, INTERVAL) {
        assert(getRunLog(driverPodName)
          .contains("spark-streaming-kube"), "The application did not complete.")
      }
    } finally {
      // Have to delete the service manually since it doesn't have an owner reference
      kubernetesTestComponents
        .kubernetesClient
        .services()
        .inNamespace(kubernetesTestComponents.namespace)
        .delete(driverService)
      serverSocket.close()
    }
  }

  test("Run spark streaming in cluster mode.", k8sTestTag) {
    val (host, port, serverSocket) = startSocketServer()
    try {
      runSparkJVMCheckAndVerifyCompletion(
        mainClass = "org.apache.spark.examples.streaming.NetworkWordCount",
        appArgs = Array[String](host, port.toString),
        expectedJVMValue = Seq("spark-streaming-kube"))
    } finally {
      serverSocket.close()
    }
  }

  test("Run spark structured streaming in cluster mode.", k8sTestTag) {
    val (host, port, serverSocket) = startSocketServer()
    try {
      runSparkJVMCheckAndVerifyCompletion(
        mainClass = "org.apache.spark.examples.sql.streaming.StructuredNetworkWordCount",
        appArgs = Array[String](host, port.toString),
        expectedJVMValue = Seq("spark-streaming-kube"))
    } finally {
      serverSocket.close()
    }
  }

  test("Run spark structured streaming in client mode.", k8sTestTag) {
    val (host, port, serverSocket) = startSocketServer()
    val (driverPort: Int, blockManagerPort: Int, driverService: Service) =
      driverServiceSetup(driverPodName)
    try {
      val driverPod = setupSparkStreamingPod("client", driverPodName)
        .addToArgs("--conf",
          s"spark.driver.host=" +
            s"${driverService.getMetadata.getName}.${kubernetesTestComponents.namespace}.svc")
        .addToArgs("--conf", s"spark.driver.port=$driverPort")
        .addToArgs("--conf", s"spark.driver.blockManager.port=$blockManagerPort")
        .addToArgs("sql.streaming.StructuredNetworkWordCount")
        .addToArgs(host, port.toString)
        .endContainer()
        .endSpec()
        .done()

      val TIMEOUT = PatienceConfiguration.Timeout(Span(3, Minutes))
      Eventually.eventually(TIMEOUT, INTERVAL) {
        assert(getRunLog(driverPodName).contains("spark-streaming-kube"),
          "The application did not complete.")
      }
    }
    finally {
      // Have to delete the service manually since it doesn't have an owner reference
      kubernetesTestComponents
        .kubernetesClient
        .services()
        .inNamespace(kubernetesTestComponents.namespace)
        .delete(driverService)
      serverSocket.close()
    }
  }

  private def getRunLog(_driverPodName: String): String = kubernetesTestComponents.kubernetesClient
    .pods()
    .withName(_driverPodName)
    .getLog

  private def setupSparkStreamingPod(mode: String, _driverPodName: String) = {
    val labels = Map("spark-app-selector" -> _driverPodName)
    testBackend
      .getKubernetesClient
      .pods()
      .inNamespace(kubernetesTestComponents.namespace)
      .createNew()
      .withNewMetadata()
      .withName(_driverPodName)
      .withLabels(labels.asJava)
      .endMetadata()
      .withNewSpec()
      .withServiceAccountName(kubernetesTestComponents.serviceAccountName)
      .addNewContainer()
      .withName("spark-example")
      .withImage(image)
      .withImagePullPolicy("IfNotPresent")
      .withCommand("/opt/spark/bin/run-example")
      .addToArgs("--master", s"k8s://https://kubernetes.default.svc")
      .addToArgs("--deploy-mode", mode)
      .addToArgs("--conf", s"spark.kubernetes.container.image=$image")
      .addToArgs("--conf",
        s"spark.kubernetes.namespace=${kubernetesTestComponents.namespace}")
      .addToArgs("--conf", "spark.kubernetes.authenticate.oauthTokenFile=" +
        "/var/run/secrets/kubernetes.io/serviceaccount/token")
      .addToArgs("--conf", "spark.kubernetes.authenticate.caCertFile=" +
        "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
      .addToArgs("--conf", s"spark.kubernetes.driver.pod.name=${_driverPodName}")
      .addToArgs("--conf", "spark.executor.memory=500m")
      .addToArgs("--conf", "spark.executor.cores=2")
      .addToArgs("--conf", "spark.executor.instances=1")

  }

  private def driverServiceSetup(_driverPodName: String): (Int, Int, Service) = {
    val labels = Map("spark-app-selector" -> _driverPodName)
    val driverPort = 7077
    val blockManagerPort = 10000
    val driverService = testBackend
      .getKubernetesClient
      .services()
      .inNamespace(kubernetesTestComponents.namespace)
      .createNew()
      .withNewMetadata()
      .withName(s"${_driverPodName}-svc")
      .endMetadata()
      .withNewSpec()
      .withClusterIP("None")
      .withSelector(labels.asJava)
      .addNewPort()
      .withName("driver-port")
      .withPort(driverPort)
      .withNewTargetPort(driverPort)
      .endPort()
      .addNewPort()
      .withName("block-manager")
      .withPort(blockManagerPort)
      .withNewTargetPort(blockManagerPort)
      .endPort()
      .endSpec()
      .done()
    (driverPort, blockManagerPort, driverService)
  }

}
