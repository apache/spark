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
package org.apache.spark.deploy.kubernetes

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import java.util.concurrent.{Executors, TimeUnit}
import javax.net.ssl.X509TrustManager

import com.google.common.util.concurrent.{SettableFuture, ThreadFactoryBuilder}
import io.fabric8.kubernetes.api.model.{ContainerPort, ContainerPortBuilder, Pod, ServicePort, ServicePortBuilder}
import io.fabric8.kubernetes.client._
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.internal.SSLUtils
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.util.Success

import org.apache.spark.deploy.kubernetes.driverlauncher.{KubernetesDriverLauncherService, KubernetesSparkDriverConfiguration}
import org.apache.spark.deploy.kubernetes.httpclients.HttpClientUtil
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class Client(private val clientArgs: ClientArguments)
    extends Logging {
  import Client._
  private val namespace = clientArgs.kubernetesAppNamespace
  private val launchTime = System.currentTimeMillis
  private val kubernetesAppId = clientArgs.kubernetesAppName.getOrElse(s"spark-$launchTime")
  private val secretName = s"driver-launcher-service-secret-$kubernetesAppId"
  private val driverLauncherSelectorValue = s"driver-launcher-$launchTime"

  private val secretBytes = new Array[Byte](128)
  SECURE_RANDOM.nextBytes(secretBytes)
  private val secretBase64String = Base64.encodeBase64String(secretBytes)
  private implicit val retryableExecutionContext = ExecutionContext
    .fromExecutorService(
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("kubernetes-client-retryable-futures-%d")
        .setDaemon(true)
        .build()))

  private def retryableFuture[T]
      (times: Int, interval: Duration)
      (f: => Future[T])
      (implicit executionContext: ExecutionContext = retryableExecutionContext): Future[T] = {
    f recoverWith {
      case _ if times > 0 => {
        Thread.sleep(interval.toMillis)
        retryableFuture(times - 1, interval)(f)
      }
    }
  }

  private def retry[T]
      (times: Int, interval: Duration)
      (f: => T)
      (implicit executionContext: ExecutionContext = retryableExecutionContext): Future[T] = {
    retryableFuture(times, interval)(Future[T] { f })
  }

  def run(): Unit = {
    var k8ConfBuilder = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(clientArgs.kubernetesMaster)
      .withNamespace(namespace)
    clientArgs.kubernetesCaCertFile.foreach { f => k8ConfBuilder = k8ConfBuilder.withCaCertFile(f)}
    clientArgs.kubernetesClientKeyFile.foreach(file =>
      k8ConfBuilder = k8ConfBuilder.withClientKeyFile(file))
    clientArgs.kubernetesClientCertFile.foreach(file =>
        k8ConfBuilder = k8ConfBuilder.withClientCertFile(file))

    val k8ClientConfig = k8ConfBuilder.build
    Utils.tryWithResource(new DefaultKubernetesClient(k8ClientConfig))(kubernetesClient => {
      val secret = kubernetesClient.secrets().createNew()
        .withNewMetadata()
          .withName(secretName)
          .endMetadata()
        .withData(Map((DRIVER_LAUNCHER_SECRET_NAME, secretBase64String)).asJava)
        .withType("Opaque")
        .done()
      try {
        val driverConfiguration = buildSparkDriverConfiguration()
        val selectors = Map(DRIVER_LAUNCHER_SELECTOR_LABEL -> driverLauncherSelectorValue).asJava
        val uiPort = driverConfiguration
          .sparkConf
          .get("spark.ui.port")
          .map(_.toInt)
          .getOrElse(DEFAULT_UI_PORT)
        val (servicePorts, containerPorts) = configurePorts(uiPort)

        val service = kubernetesClient.services().createNew()
          .withNewMetadata()
            .withName(kubernetesAppId)
            .endMetadata()
          .withNewSpec()
            .withSelector(selectors)
            .withPorts(servicePorts.asJava)
            .endSpec()
          .done()

        val submitCompletedFuture = SettableFuture.create[Boolean]

        val podWatcher = new Watcher[Pod] {
          override def eventReceived(action: Action, t: Pod): Unit = {
            if ((action == Action.ADDED || action == Action.MODIFIED)
                && t.getStatus.getPhase == "Running"
                && !submitCompletedFuture.isDone) {
              t.getStatus
                .getContainerStatuses
                .asScala
                .find(status =>
                  status.getName == DRIVER_LAUNCHER_CONTAINER_NAME && status.getReady) match {
                case Some(status) =>
                  try {
                    val driverLauncher = getDriverLauncherService(k8ClientConfig)
                    val ping = retry(5, 5.seconds) {
                      driverLauncher.ping()
                    }
                    ping onFailure {
                      case t: Throwable =>
                        if (!submitCompletedFuture.isDone) {
                          submitCompletedFuture.setException(t)
                        }
                    }
                    val submitComplete = ping andThen {
                      case Success(_) =>
                        driverLauncher.submitApplication(secretBase64String, driverConfiguration)
                        submitCompletedFuture.set(true)
                    }
                    submitComplete onFailure {
                      case t: Throwable =>
                        if (!submitCompletedFuture.isDone) {
                          submitCompletedFuture.setException(t)
                        }
                    }
                  } catch {
                    case e: Throwable =>
                      if (!submitCompletedFuture.isDone) {
                        submitCompletedFuture.setException(e)
                        throw e
                      }
                  }
                case None =>
              }
            }
          }

          override def onClose(e: KubernetesClientException): Unit = {
            if (!submitCompletedFuture.isDone) {
              submitCompletedFuture.setException(e)
            }
          }
        }

        Utils.tryWithResource(kubernetesClient
          .pods()
          .withLabels(selectors)
          .watch(podWatcher)) { _ =>
          kubernetesClient.pods().createNew()
            .withNewMetadata()
              .withName(kubernetesAppId)
              .withLabels(selectors)
              .endMetadata()
            .withNewSpec()
              .addNewVolume()
                .withName(s"spark-app-secret-volume")
                .withNewSecret()
                  .withSecretName(secret.getMetadata.getName)
                  .endSecret()
                .endVolume
              .addNewContainer()
                .withName(DRIVER_LAUNCHER_CONTAINER_NAME)
                .withImage(clientArgs.driverDockerImage)
                .withImagePullPolicy("Never")
                .addNewVolumeMount()
                  .withName("spark-app-secret-volume")
                  .withReadOnly(true)
                  .withMountPath("/opt/spark/spark-app-secret")
                  .endVolumeMount()
                .addNewEnv()
                  .withName("SPARK_DRIVER_LAUNCHER_SERVICE_NAME")
                  .withValue(kubernetesAppId)
                  .endEnv()
                .addNewEnv()
                  .withName("SPARK_DRIVER_LAUNCHER_APP_SECRET_LOCATION")
                  .withValue(s"/opt/spark/spark-app-secret/$DRIVER_LAUNCHER_SECRET_NAME")
                  .endEnv()
                .addNewEnv()
                  .withName("SPARK_DRIVER_LAUNCHER_SERVER_PORT")
                  .withValue(DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT.toString)
                  .endEnv()
                .withPorts(containerPorts.asJava)
                .endContainer()
              .withRestartPolicy("OnFailure")
              .endSpec()
            .done()
          submitCompletedFuture.get(30, TimeUnit.SECONDS)
        }
    } finally {
        kubernetesClient.secrets().delete(secret)
      }
    })
  }

  private def getDriverLauncherService(k8ClientConfig: Config): KubernetesDriverLauncherService = {
    val url = s"${
      Array[String](
        clientArgs.kubernetesMaster,
        "api", "v1", "proxy",
        "namespaces", namespace,
        "services", kubernetesAppId).mkString("/")
    }" +
      s":$DRIVER_LAUNCHER_SERVICE_PORT_NAME/api"

    val sslContext = SSLUtils.sslContext(k8ClientConfig)
    val trustManager = SSLUtils.trustManagers(
      k8ClientConfig)(0).asInstanceOf[X509TrustManager]
    val driverLauncher = HttpClientUtil.createClient[KubernetesDriverLauncherService](
      url, sslContext.getSocketFactory, trustManager)
    driverLauncher
  }

  private def buildSparkDriverConfiguration(): KubernetesSparkDriverConfiguration = {
    // Set up Spark application configurations, classpath, and resource uploads
    val driverClassPathUris = clientArgs
      .driverExtraClassPath
      .map(uri => Utils.resolveURI(uri))
    val kubernetesSparkProperties = Map[String, String](
      "spark.master" -> "kubernetes",
      "spark.kubernetes.master" -> clientArgs.kubernetesMaster,
      "spark.kubernetes.executor.docker.image" -> clientArgs.executorDockerImage,
      "spark.kubernetes.namespace" -> clientArgs.kubernetesAppNamespace,
      "spark.kubernetes.app.id" -> kubernetesAppId,
      "spark.kubernetes.driver.service.name" -> kubernetesAppId,
      "spark.blockManager.port" -> BLOCKMANAGER_PORT.toString,
      "spark.driver.port" -> DRIVER_PORT.toString)
    val combinedSparkConf = clientArgs.sparkConf ++ kubernetesSparkProperties
    val jarUris = clientArgs.jars.map(uri => Utils.resolveURI(uri))

    val localJarsToUpload = jarUris.union(driverClassPathUris)
      .distinct
      .filter(uri => uri.getScheme == "file" || uri.getScheme == "local")
      .map(uri => {
        val jarRawContents = Files.readAllBytes(Paths.get(uri.getPath))
        val jarBase64Contents = Base64.encodeBase64String(jarRawContents)
        uri.toString -> jarBase64Contents
      }).toMap

    val customExecutorSpecBase64 = clientArgs.customExecutorSpecFile.map(filePath => {
      val file = new File(filePath)
      if (!file.isFile) {
        throw new IllegalArgumentException("Custom executor spec file was provided as " +
          s"$filePath, but no file is available at that path.")
      }
      Utils.tryWithResource(new FileInputStream(file)) { is =>
        Base64.encodeBase64String(IOUtils.toByteArray(is))
      }
    })
    KubernetesSparkDriverConfiguration(
      localJarsToUpload,
      driverClassPathUris.map(_.toString).toSeq,
      jarUris.map(_.toString).toSeq,
      combinedSparkConf,
      clientArgs.userArgs.toSeq,
      customExecutorSpecBase64,
      clientArgs.customExecutorSpecContainerName,
      clientArgs.userMainClass)
  }

  private def configurePorts(uiPort: Int): (Seq[ServicePort], Seq[ContainerPort]) = {
    val servicePorts = new ArrayBuffer[ServicePort]
    val containerPorts = new ArrayBuffer[ContainerPort]

    def addPortToServiceAndContainer(portName: String, portValue: Int): Unit = {
      servicePorts += new ServicePortBuilder()
        .withName(portName)
        .withPort(portValue)
        .withNewTargetPort(portValue)
        .build()
      containerPorts += new ContainerPortBuilder()
        .withContainerPort(portValue)
        .build()
    }

    addPortToServiceAndContainer(
      DRIVER_LAUNCHER_SERVICE_PORT_NAME,
      DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT)
    addPortToServiceAndContainer(
      DRIVER_PORT_NAME,
      DRIVER_PORT)
    addPortToServiceAndContainer(
      BLOCKMANAGER_PORT_NAME,
      BLOCKMANAGER_PORT)

    addPortToServiceAndContainer(UI_PORT_NAME, uiPort)
    clientArgs
      .additionalDriverPorts
      .foreach(port => addPortToServiceAndContainer(port._1, port._2))
    (servicePorts.toSeq, containerPorts.toSeq)
  }
}

private object Client {

  private val DRIVER_LAUNCHER_SECRET_NAME = "driver-launcher-secret"
  private val DRIVER_LAUNCHER_SELECTOR_LABEL = "driver-launcher-selector"
  private val APPLICATION_SECRET_ENV = "DRIVER_LAUNCHER_APPLICATION_SECRET"
  private val DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT = 7077
  private val DRIVER_PORT = 7078
  private val BLOCKMANAGER_PORT = 7079
  private val DEFAULT_UI_PORT = 4040
  private val UI_PORT_NAME = "spark-ui-port"
  private val DRIVER_LAUNCHER_SERVICE_PORT_NAME = "driver-launcher-port"
  private val DRIVER_PORT_NAME = "driver-port"
  private val BLOCKMANAGER_PORT_NAME = "block-manager-port"
  private val DRIVER_LAUNCHER_CONTAINER_NAME = "spark-kubernetes-driver-launcher"

  private val SECURE_RANDOM = new SecureRandom()

  def main(argStrings: Array[String]): Unit = {
    val args = ClientArguments.builder().fromArgsArray(argStrings).build()
    new Client(args).run()
  }
}
