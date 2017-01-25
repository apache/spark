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
import java.util.concurrent.{Executors, TimeoutException, TimeUnit}
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

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkException}
import org.apache.spark.deploy.rest.{AppResource, ContainerAppResource, KubernetesCreateSubmissionRequest, RemoteAppResource, TarGzippedData, UploadedAppResource}
import org.apache.spark.deploy.rest.kubernetes._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class Client(
    sparkConf: SparkConf,
    mainClass: String,
    mainAppResource: String,
    appArgs: Array[String]) extends Logging {
  import Client._

  private val namespace = sparkConf.get("spark.kubernetes.namespace", "default")
  private val master = resolveK8sMaster(sparkConf.get("spark.master"))

  private val launchTime = System.currentTimeMillis
  private val appName = sparkConf.getOption("spark.app.name")
    .orElse(sparkConf.getOption("spark.app.id"))
    .getOrElse("spark")
  private val kubernetesAppId = s"$appName-$launchTime".toLowerCase.replaceAll("\\.", "-")
  private val secretName = s"spark-submission-server-secret-$kubernetesAppId"
  private val driverLauncherSelectorValue = s"driver-launcher-$launchTime"
  private val driverDockerImage = sparkConf.get(
    "spark.kubernetes.driver.docker.image", s"spark-driver:$SPARK_VERSION")
  private val uploadedJars = sparkConf.getOption("spark.kubernetes.driver.uploads.jars")
  private val driverLaunchTimeoutSecs = sparkConf.getTimeAsSeconds(
    "spark.kubernetes.driverLaunchTimeout", s"${DEFAULT_LAUNCH_TIMEOUT_SECONDS}s")

  private val secretBase64String = {
    val secretBytes = new Array[Byte](128)
    SECURE_RANDOM.nextBytes(secretBytes)
    Base64.encodeBase64String(secretBytes)
  }

  private val serviceAccount = sparkConf.get("spark.kubernetes.submit.serviceAccountName",
    "default")

  private val customLabels = sparkConf.get("spark.kubernetes.driver.labels", "")

  private implicit val retryableExecutionContext = ExecutionContext
    .fromExecutorService(
      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("kubernetes-client-retryable-futures-%d")
        .setDaemon(true)
        .build()))

  def run(): Unit = {
    val parsedCustomLabels = parseCustomLabels(customLabels)
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
        val resolvedSelectors = (Map(
            DRIVER_LAUNCHER_SELECTOR_LABEL -> driverLauncherSelectorValue,
            SPARK_APP_NAME_LABEL -> appName)
          ++ parsedCustomLabels).asJava
        val (servicePorts, containerPorts) = configurePorts()
        val service = kubernetesClient.services().createNew()
          .withNewMetadata()
            .withName(kubernetesAppId)
            .withLabels(Map(SPARK_APP_NAME_LABEL -> appName).asJava)
            .endMetadata()
          .withNewSpec()
            .withSelector(resolvedSelectors)
            .withPorts(servicePorts.asJava)
            .endSpec()
          .done()
        sparkConf.set("spark.kubernetes.driver.service.name", service.getMetadata.getName)
        sparkConf.setIfMissing("spark.driver.port", DEFAULT_DRIVER_PORT.toString)
        sparkConf.setIfMissing("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT.toString)
        val submitRequest = buildSubmissionRequest()
        val submitCompletedFuture = SettableFuture.create[Boolean]
        val secretDirectory = s"$SPARK_SUBMISSION_SECRET_BASE_DIR/$kubernetesAppId"

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
                case Some(_) =>
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
              .withLabels(resolvedSelectors)
              .endMetadata()
            .withNewSpec()
              .withRestartPolicy("OnFailure")
              .addNewVolume()
                .withName(s"spark-submission-secret-volume")
                  .withNewSecret()
                  .withSecretName(secret.getMetadata.getName)
                  .endSecret()
                .endVolume
              .withServiceAccount(serviceAccount)
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
          var submitSucceeded = false
          try {
            submitCompletedFuture.get(driverLaunchTimeoutSecs, TimeUnit.SECONDS)
            submitSucceeded = true
          } catch {
            case e: TimeoutException =>
              val driverPod = try {
                kubernetesClient.pods().withName(kubernetesAppId).get()
              } catch {
                case throwable: Throwable =>
                  logError(s"Timed out while waiting $driverLaunchTimeoutSecs seconds for the" +
                    " driver pod to start, but an error occurred while fetching the driver" +
                    " pod's details.", throwable)
                  throw new SparkException(s"Timed out while waiting $driverLaunchTimeoutSecs" +
                    " seconds for the driver pod to start. Unfortunately, in attempting to fetch" +
                    " the latest state of the pod, another error was thrown. Check the logs for" +
                    " the error that was thrown in looking up the driver pod.", e)
              }
              val topLevelMessage = s"The driver pod with name ${driverPod.getMetadata.getName}" +
                s" in namespace ${driverPod.getMetadata.getNamespace} was not ready in" +
                s" $driverLaunchTimeoutSecs seconds."
              val podStatusPhase = if (driverPod.getStatus.getPhase != null) {
                s"Latest phase from the pod is: ${driverPod.getStatus.getPhase}"
              } else {
                "The pod had no final phase."
              }
              val podStatusMessage = if (driverPod.getStatus.getMessage != null) {
                s"Latest message from the pod is: ${driverPod.getStatus.getMessage}"
              } else {
                "The pod had no final message."
              }
              val failedDriverContainerStatusString = driverPod.getStatus
                .getContainerStatuses
                .asScala
                .find(_.getName == DRIVER_LAUNCHER_CONTAINER_NAME)
                .map(status => {
                  val lastState = status.getState
                  if (lastState.getRunning != null) {
                    "Driver container last state: Running\n" +
                    s"Driver container started at: ${lastState.getRunning.getStartedAt}"
                  } else if (lastState.getWaiting != null) {
                    "Driver container last state: Waiting\n" +
                    s"Driver container wait reason: ${lastState.getWaiting.getReason}\n" +
                    s"Driver container message: ${lastState.getWaiting.getMessage}\n"
                  } else if (lastState.getTerminated != null) {
                    "Driver container last state: Terminated\n" +
                    s"Driver container started at: ${lastState.getTerminated.getStartedAt}\n" +
                    s"Driver container finished at: ${lastState.getTerminated.getFinishedAt}\n" +
                    s"Driver container exit reason: ${lastState.getTerminated.getReason}\n" +
                    s"Driver container exit code: ${lastState.getTerminated.getExitCode}\n" +
                    s"Driver container message: ${lastState.getTerminated.getMessage}"
                  } else {
                    "Driver container last state: Unknown"
                  }
                }).getOrElse("The driver container wasn't found in the pod; expected to find" +
                  s" container with name $DRIVER_LAUNCHER_CONTAINER_NAME")
              val finalErrorMessage = s"$topLevelMessage\n" +
                s"$podStatusPhase\n" +
                s"$podStatusMessage\n\n$failedDriverContainerStatusString"
              logError(finalErrorMessage, e)
              throw new SparkException(finalErrorMessage, e)
            } finally {
              if (!submitSucceeded) {
                try {
                  kubernetesClient.pods.withName(kubernetesAppId).delete
                } catch {
                  case throwable: Throwable =>
                    logError("Failed to delete driver pod after it failed to run.", throwable)
                }
              }
            }
          }

        Utils.tryWithResource(kubernetesClient
          .pods()
          .withLabels(resolvedSelectors)
          .watch(podWatcher)) { createDriverPod }
      } finally {
        kubernetesClient.secrets().delete(secret)
      }
    })
  }

  private def configurePorts(): (Seq[ServicePort], Seq[ContainerPort]) = {
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
        .getOrElse(DEFAULT_DRIVER_PORT))
    addPortToServiceAndContainer(
      BLOCKMANAGER_PORT_NAME,
      sparkConf
        .getOption("spark.blockmanager.port")
        .map(_.toInt)
        .getOrElse(DEFAULT_BLOCKMANAGER_PORT))

    addPortToServiceAndContainer(
      UI_PORT_NAME,
      sparkConf
        .getOption("spark.ui.port")
        .map(_.toInt)
        .getOrElse(DEFAULT_UI_PORT))
    (servicePorts, containerPorts)
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
      case "container" => ContainerAppResource(appResourceUri.getPath)
      case other => RemoteAppResource(other)
    }

    val uploadJarsBase64Contents = compressJars(uploadedJars)
    KubernetesCreateSubmissionRequest(
      appResource = resolvedAppResource,
      mainClass = mainClass,
      appArgs = appArgs,
      secret = secretBase64String,
      sparkProperties = sparkConf.getAll.toMap,
      uploadedJarsBase64Contents = uploadJarsBase64Contents)
  }

  private def compressJars(maybeFilePaths: Option[String]): Option[TarGzippedData] = {
    maybeFilePaths
      .map(_.split(","))
      .map(CompressionUtils.createTarGzip(_))
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

  private def parseCustomLabels(labels: String): Map[String, String] = {
    labels.split(",").map(_.trim).filterNot(_.isEmpty).map(label => {
      label.split("=", 2).toSeq match {
        case Seq(k, v) =>
          require(k != DRIVER_LAUNCHER_SELECTOR_LABEL, "Label with key" +
            s" $DRIVER_LAUNCHER_SELECTOR_LABEL cannot be used in" +
            " spark.kubernetes.driver.labels, as it is reserved for Spark's" +
            " internal configuration.")
          (k, v)
        case _ =>
          throw new SparkException("Custom labels set by spark.kubernetes.driver.labels" +
            " must be a comma-separated list of key-value pairs, with format <key>=<value>." +
            s" Got label: $label. All labels: $labels")
      }
    }).toMap
  }
}

private[spark] object Client extends Logging {

  private val SUBMISSION_SERVER_SECRET_NAME = "spark-submission-server-secret"
  private val DRIVER_LAUNCHER_SELECTOR_LABEL = "driver-launcher-selector"
  private val DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT = 7077
  private val DEFAULT_DRIVER_PORT = 7078
  private val DEFAULT_BLOCKMANAGER_PORT = 7079
  private val DEFAULT_UI_PORT = 4040
  private val UI_PORT_NAME = "spark-ui-port"
  private val DRIVER_LAUNCHER_SERVICE_PORT_NAME = "driver-launcher-port"
  private val DRIVER_PORT_NAME = "driver-port"
  private val BLOCKMANAGER_PORT_NAME = "block-manager-port"
  private val DRIVER_LAUNCHER_CONTAINER_NAME = "spark-kubernetes-driver-launcher"
  private val SECURE_RANDOM = new SecureRandom()
  private val SPARK_SUBMISSION_SECRET_BASE_DIR = "/var/run/secrets/spark-submission"
  private val DEFAULT_LAUNCH_TIMEOUT_SECONDS = 60
  private val SPARK_APP_NAME_LABEL = "spark-app-name"

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

  def resolveK8sMaster(rawMasterString: String): String = {
    if (!rawMasterString.startsWith("k8s://")) {
      throw new IllegalArgumentException("Master URL should start with k8s:// in Kubernetes mode.")
    }
    val masterWithoutK8sPrefix = rawMasterString.replaceFirst("k8s://", "")
    if (masterWithoutK8sPrefix.startsWith("http://")
        || masterWithoutK8sPrefix.startsWith("https://")) {
      masterWithoutK8sPrefix
    } else {
      val resolvedURL = s"https://$masterWithoutK8sPrefix"
      logDebug(s"No scheme specified for kubernetes master URL, so defaulting to https. Resolved" +
        s" URL is $resolvedURL")
      resolvedURL
    }
  }
}
