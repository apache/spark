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

import java.io.File
import java.security.SecureRandom
import java.util.concurrent.{Executors, TimeUnit}
import javax.net.ssl.X509TrustManager

import com.google.common.io.Files
import com.google.common.util.concurrent.{SettableFuture, ThreadFactoryBuilder}
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{Config, ConfigBuilder, DefaultKubernetesClient, KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.internal.SSLUtils
import org.apache.commons.codec.binary.Base64
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.Success

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.deploy.rest.{AppResource, KubernetesCreateSubmissionRequest, RemoteAppResource, UploadedAppResource}
import org.apache.spark.deploy.rest.kubernetes._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class Client(
    sparkConf: SparkConf,
    mainClass: String,
    mainAppResource: String,
    appArgs: Array[String]) extends Logging {
  import Client._

  private val namespace = sparkConf.getOption("spark.kubernetes.namespace").getOrElse(
    throw new IllegalArgumentException("Namespace must be provided in spark.kubernetes.namespace"))
  private val master = sparkConf
    .getOption("spark.kubernetes.master")
    .getOrElse("Master must be provided in spark.kubernetes.master")

  private val launchTime = System.currentTimeMillis
  private val kubernetesAppId = sparkConf.getOption("spark.app.name")
      .orElse(sparkConf.getOption("spark.app.id"))
      .getOrElse(s"spark-$launchTime")

  private val secretName = s"spark-submission-server-secret-$kubernetesAppId"
  private val driverLauncherSelectorValue = s"driver-launcher-$launchTime"
  private val driverDockerImage = sparkConf.get(
    "spark.kubernetes.driver.docker.image", s"spark-driver:$SPARK_VERSION")
  private val uploadedDriverExtraClasspath = sparkConf
    .getOption("spark.kubernetes.driver.uploads.driverExtraClasspath")
  private val uploadedJars = sparkConf.getOption("spark.kubernetes.driver.uploads.jars")

  private val secretBytes = new Array[Byte](128)
  SECURE_RANDOM.nextBytes(secretBytes)
  private val secretBase64String = Base64.encodeBase64String(secretBytes)

  private implicit val retryableExecutionContext = ExecutionContext
    .fromExecutorService(
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("kubernetes-client-retryable-futures-%d")
        .setDaemon(true)
        .build()))

  def run(): Unit = {
    var k8ConfBuilder = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(master)
      .withNamespace(namespace)
    sparkConf.getOption("spark.kubernetes.submit.caCertFile").foreach {
      f => k8ConfBuilder = k8ConfBuilder.withCaCertFile(f)
    }
    sparkConf.getOption("spark.kubernetes.submit.clientKeyFile").foreach {
      f => k8ConfBuilder = k8ConfBuilder.withClientKeyFile(f)
    }
    sparkConf.getOption("spark.kubernetes.submit.clientCertFile").foreach {
      f => k8ConfBuilder = k8ConfBuilder.withClientCertFile(f)
    }

    val k8ClientConfig = k8ConfBuilder.build
    Utils.tryWithResource(new DefaultKubernetesClient(k8ClientConfig))(kubernetesClient => {
      val secret = kubernetesClient.secrets().createNew()
        .withNewMetadata()
        .withName(secretName)
        .endMetadata()
        .withData(Map((SUBMISSION_SERVER_SECRET_NAME, secretBase64String)).asJava)
        .withType("Opaque")
        .done()
      try {
        val selectors = Map(DRIVER_LAUNCHER_SELECTOR_LABEL -> driverLauncherSelectorValue).asJava
        val uiPort = sparkConf
          .getOption("spark.ui.port")
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
        sparkConf.set("spark.kubernetes.driver.service.name", service.getMetadata.getName)
        sparkConf.setIfMissing("spark.driver.port", DRIVER_PORT.toString)
        sparkConf.setIfMissing("spark.blockmanager.port", BLOCKMANAGER_PORT.toString)
        val submitRequest = buildSubmissionRequest()
        val submitCompletedFuture = SettableFuture.create[Boolean]
        val secretDirectory = s"/var/run/secrets/spark-submission/$kubernetesAppId"

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
                    val driverLauncher = getDriverLauncherService(
                      k8ClientConfig, master)
                    val ping = Retry.retry(5, 5.seconds) {
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
                        driverLauncher.create(submitRequest)
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

        def createDriverPod(unused: Watch): Unit = {
          kubernetesClient.pods().createNew()
            .withNewMetadata()
              .withName(kubernetesAppId)
              .withLabels(selectors)
              .endMetadata()
            .withNewSpec()
              .withRestartPolicy("OnFailure")
              .addNewVolume()
                .withName(s"spark-submission-secret-volume")
                  .withNewSecret()
                  .withSecretName(secret.getMetadata.getName)
                  .endSecret()
                .endVolume
              .addNewContainer()
                .withName(DRIVER_LAUNCHER_CONTAINER_NAME)
                .withImage(driverDockerImage)
                .withImagePullPolicy("IfNotPresent")
                .addNewVolumeMount()
                  .withName("spark-submission-secret-volume")
                  .withMountPath(secretDirectory)
                  .withReadOnly(true)
                  .endVolumeMount()
                .addNewEnv()
                  .withName("SPARK_SUBMISSION_SECRET_LOCATION")
                  .withValue(s"$secretDirectory/$SUBMISSION_SERVER_SECRET_NAME")
                  .endEnv()
                .addNewEnv()
                  .withName("SPARK_DRIVER_LAUNCHER_SERVER_PORT")
                  .withValue(DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT.toString)
                  .endEnv()
                .withPorts(containerPorts.asJava)
                .endContainer()
              .endSpec()
            .done()
          submitCompletedFuture.get(30, TimeUnit.SECONDS)
        }

        Utils.tryWithResource(kubernetesClient
          .pods()
          .withLabels(selectors)
          .watch(podWatcher)) { createDriverPod }
      } finally {
        kubernetesClient.secrets().delete(secret)
      }
    })
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
      sparkConf
        .getOption("spark.driver.port")
        .map(_.toInt)
        .getOrElse(DRIVER_PORT))
    addPortToServiceAndContainer(
      BLOCKMANAGER_PORT_NAME,
      sparkConf
        .getOption("spark.blockmanager.port")
        .map(_.toInt)
        .getOrElse(BLOCKMANAGER_PORT))

    addPortToServiceAndContainer(UI_PORT_NAME, uiPort)
    (servicePorts.toSeq, containerPorts.toSeq)
  }

  private def buildSubmissionRequest(): KubernetesCreateSubmissionRequest = {
    val appResourceUri = Utils.resolveURI(mainAppResource)
    val resolvedAppResource: AppResource = appResourceUri.getScheme match {
      case "file" | null =>
        val appFile = new File(appResourceUri.getPath)
        if (!appFile.isFile) {
          throw new IllegalStateException("Provided local file path does not exist" +
            s" or is not a file: ${appFile.getAbsolutePath}")
        }
        val fileBytes = Files.toByteArray(appFile)
        val fileBase64 = Base64.encodeBase64String(fileBytes)
        UploadedAppResource(resourceBase64Contents = fileBase64, name = appFile.getName)
      case other => RemoteAppResource(other)
    }

    val uploadDriverExtraClasspathBase64Contents = getFileContents(uploadedDriverExtraClasspath)
    val uploadJarsBase64Contents = getFileContents(uploadedJars)
    KubernetesCreateSubmissionRequest(
      appResource = resolvedAppResource,
      mainClass = mainClass,
      appArgs = appArgs,
      secret = secretBase64String,
      sparkProperties = sparkConf.getAll.toMap,
      uploadedDriverExtraClasspathBase64Contents = uploadDriverExtraClasspathBase64Contents,
      uploadedJarsBase64Contents = uploadJarsBase64Contents)
  }

  def getFileContents(maybeFilePaths: Option[String]): Array[(String, String)] = {
    maybeFilePaths
      .map(_.split(",").map(filePath => {
        val fileToUpload = new File(filePath)
        if (!fileToUpload.isFile) {
          throw new IllegalStateException("Provided file to upload for driver extra classpath" +
            s" does not exist or is not a file: $filePath")
        } else {
          val fileBytes = Files.toByteArray(fileToUpload)
          val fileBase64 = Base64.encodeBase64String(fileBytes)
          (fileToUpload.getName, fileBase64)
        }
      })).getOrElse(Array.empty[(String, String)])
  }

  private def getDriverLauncherService(
      k8ClientConfig: Config,
      kubernetesMaster: String): KubernetesSparkRestApi = {
    val url = s"${
      Array[String](
        kubernetesMaster,
        "api", "v1", "proxy",
        "namespaces", namespace,
        "services", kubernetesAppId).mkString("/")}" +
      s":$DRIVER_LAUNCHER_SERVICE_PORT_NAME/"

    val sslContext = SSLUtils.sslContext(k8ClientConfig)
    val trustManager = SSLUtils.trustManagers(
      k8ClientConfig)(0).asInstanceOf[X509TrustManager]
    HttpClientUtil.createClient[KubernetesSparkRestApi](
      uri = url,
      sslSocketFactory = sslContext.getSocketFactory,
      trustContext = trustManager)
  }
}

private object Client {

  private val SUBMISSION_SERVER_SECRET_NAME = "spark-submission-server-secret"
  private val DRIVER_LAUNCHER_SELECTOR_LABEL = "driver-launcher-selector"
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

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, s"Too few arguments. Usage: ${getClass.getName} <mainAppResource>" +
      s" <mainClass> [<application arguments>]")
    val mainAppResource = args(0)
    val mainClass = args(1)
    val appArgs = args.drop(2)
    val sparkConf = new SparkConf(true)
    new Client(
      mainAppResource = mainAppResource,
      mainClass = mainClass,
      sparkConf = sparkConf,
      appArgs = appArgs).run()
  }
}
