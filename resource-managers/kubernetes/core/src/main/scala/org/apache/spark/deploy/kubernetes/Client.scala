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
import java.security.{KeyStore, SecureRandom}
import java.util.concurrent.{Executors, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.{SSLContext, TrustManagerFactory, X509TrustManager}

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.google.common.util.concurrent.{SettableFuture, ThreadFactoryBuilder}
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.commons.codec.binary.Base64
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

import org.apache.spark.{SecurityManager, SPARK_VERSION => sparkVersion, SparkConf, SparkException, SSLOptions}
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
  private val secretDirectory = s"$SPARK_SUBMISSION_SECRET_BASE_DIR/$kubernetesAppId"
  private val sslSecretsDirectory = s"$SPARK_SUBMISSION_SECRET_BASE_DIR/$kubernetesAppId-ssl"
  private val sslSecretsName = s"spark-submission-server-ssl-$kubernetesAppId"
  private val driverLauncherSelectorValue = s"driver-launcher-$launchTime"
  private val driverDockerImage = sparkConf.get(
    "spark.kubernetes.driver.docker.image", s"spark-driver:$sparkVersion")
  private val uploadedJars = sparkConf.getOption("spark.kubernetes.driver.uploads.jars")
  private val uiPort = sparkConf.getInt("spark.ui.port", DEFAULT_UI_PORT)
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
    val (driverLaunchSslOptions, isKeyStoreLocalFile) = parseDriverLaunchSslOptions()
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
    Utils.tryWithResource(new DefaultKubernetesClient(k8ClientConfig)) { kubernetesClient =>
      val submitServerSecret = kubernetesClient.secrets().createNew()
        .withNewMetadata()
          .withName(secretName)
          .endMetadata()
        .withData(Map((SUBMISSION_SERVER_SECRET_NAME, secretBase64String)).asJava)
        .withType("Opaque")
        .done()
      val (sslEnvs, sslVolumes, sslVolumeMounts, sslSecrets) = configureSsl(kubernetesClient,
        driverLaunchSslOptions,
        isKeyStoreLocalFile)
      try {
        val driverKubernetesSelectors = (Map(
            DRIVER_LAUNCHER_SELECTOR_LABEL -> driverLauncherSelectorValue,
            SPARK_APP_NAME_LABEL -> appName)
          ++ parsedCustomLabels).asJava
        val containerPorts = buildContainerPorts()
        val submitCompletedFuture = SettableFuture.create[Boolean]
        val submitPending = new AtomicBoolean(false)
        val podWatcher = new DriverPodWatcher(
          submitCompletedFuture,
          submitPending,
          kubernetesClient,
          driverLaunchSslOptions,
          Array(submitServerSecret) ++ sslSecrets,
          driverKubernetesSelectors)
        Utils.tryWithResource(kubernetesClient
            .pods()
            .withLabels(driverKubernetesSelectors)
            .watch(podWatcher)) { _ =>
          kubernetesClient.pods().createNew()
            .withNewMetadata()
              .withName(kubernetesAppId)
              .withLabels(driverKubernetesSelectors)
              .endMetadata()
            .withNewSpec()
              .withRestartPolicy("OnFailure")
              .addNewVolume()
                .withName(s"spark-submission-secret-volume")
                .withNewSecret()
                  .withSecretName(submitServerSecret.getMetadata.getName)
                  .endSecret()
                .endVolume
              .addToVolumes(sslVolumes: _*)
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
                .addToVolumeMounts(sslVolumeMounts: _*)
                .addNewEnv()
                  .withName("SPARK_SUBMISSION_SECRET_LOCATION")
                  .withValue(s"$secretDirectory/$SUBMISSION_SERVER_SECRET_NAME")
                  .endEnv()
                .addNewEnv()
                  .withName("SPARK_DRIVER_LAUNCHER_SERVER_PORT")
                  .withValue(DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT.toString)
                  .endEnv()
                .addToEnv(sslEnvs: _*)
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
              val finalErrorMessage: String = buildSubmitFailedErrorMessage(kubernetesClient, e)
              logError(finalErrorMessage, e)
              throw new SparkException(finalErrorMessage, e)
          } finally {
            if (!submitSucceeded) {
              Utils.tryLogNonFatalError {
                kubernetesClient.pods.withName(kubernetesAppId).delete()
              }
            }
          }
        }
      } finally {
        Utils.tryLogNonFatalError {
          kubernetesClient.secrets().delete(submitServerSecret)
        }
        Utils.tryLogNonFatalError {
          kubernetesClient.secrets().delete(sslSecrets: _*)
        }
      }
    }
  }

  private def parseDriverLaunchSslOptions(): (SSLOptions, Boolean) = {
    val maybeKeyStore = sparkConf.getOption("spark.ssl.kubernetes.driverlaunch.keyStore")
    val resolvedSparkConf = sparkConf.clone()
    val (isLocalKeyStore, resolvedKeyStore) = maybeKeyStore.map(keyStore => {
      val keyStoreURI = Utils.resolveURI(keyStore)
      val isProvidedKeyStoreLocal = keyStoreURI.getScheme match {
        case "file" | null => true
        case "container" => false
        case _ => throw new SparkException(s"Invalid KeyStore URI $keyStore; keyStore URI" +
          " for submit server must have scheme file:// or container:// (no scheme defaults" +
          " to file://)")
      }
      (isProvidedKeyStoreLocal, Option.apply(keyStoreURI.getPath))
    }).getOrElse((true, Option.empty[String]))
    resolvedKeyStore.foreach {
      resolvedSparkConf.set("spark.ssl.kubernetes.driverlaunch.keyStore", _)
    }
    sparkConf.getOption("spark.ssl.kubernetes.driverlaunch.trustStore").foreach { trustStore =>
      val trustStoreURI = Utils.resolveURI(trustStore)
      trustStoreURI.getScheme match {
        case "file" | null =>
          resolvedSparkConf.set("spark.ssl.kubernetes.driverlaunch.trustStore",
            trustStoreURI.getPath)
        case _ => throw new SparkException(s"Invalid trustStore URI $trustStore; trustStore URI" +
          " for submit server must have no scheme, or scheme file://")
      }
    }
    val securityManager = new SecurityManager(resolvedSparkConf)
    (securityManager.getSSLOptions("kubernetes.driverlaunch"), isLocalKeyStore)
  }

  private def configureSsl(kubernetesClient: KubernetesClient, driverLaunchSslOptions: SSLOptions,
        isKeyStoreLocalFile: Boolean):
        (Array[EnvVar], Array[Volume], Array[VolumeMount], Array[Secret]) = {
    if (driverLaunchSslOptions.enabled) {
      val sslSecretsMap = mutable.HashMap[String, String]()
      val sslEnvs = mutable.Buffer[EnvVar]()
      val secrets = mutable.Buffer[Secret]()
      driverLaunchSslOptions.keyStore.foreach(store => {
        val resolvedKeyStoreFile = if (isKeyStoreLocalFile) {
          if (!store.isFile) {
            throw new SparkException(s"KeyStore specified at $store is not a file or" +
              s" does not exist.")
          }
          val keyStoreBytes = Files.toByteArray(store)
          val keyStoreBase64 = Base64.encodeBase64String(keyStoreBytes)
          sslSecretsMap += (SSL_KEYSTORE_SECRET_NAME -> keyStoreBase64)
          s"$sslSecretsDirectory/$SSL_KEYSTORE_SECRET_NAME"
        } else {
          store.getAbsolutePath
        }
        sslEnvs += new EnvVarBuilder()
          .withName("SPARK_SUBMISSION_KEYSTORE_FILE")
          .withValue(resolvedKeyStoreFile)
          .build()
      })
      driverLaunchSslOptions.keyStorePassword.foreach(password => {
        val passwordBase64 = Base64.encodeBase64String(password.getBytes(Charsets.UTF_8))
        sslSecretsMap += (SSL_KEYSTORE_PASSWORD_SECRET_NAME -> passwordBase64)
        sslEnvs += new EnvVarBuilder()
          .withName("SPARK_SUBMISSION_KEYSTORE_PASSWORD_FILE")
          .withValue(s"$sslSecretsDirectory/$SSL_KEYSTORE_PASSWORD_SECRET_NAME")
          .build()
      })
      driverLaunchSslOptions.keyPassword.foreach(password => {
        val passwordBase64 = Base64.encodeBase64String(password.getBytes(Charsets.UTF_8))
        sslSecretsMap += (SSL_KEY_PASSWORD_SECRET_NAME -> passwordBase64)
        sslEnvs += new EnvVarBuilder()
          .withName("SPARK_SUBMISSION_KEYSTORE_KEY_PASSWORD_FILE")
          .withValue(s"$sslSecretsDirectory/$SSL_KEY_PASSWORD_SECRET_NAME")
          .build()
      })
      driverLaunchSslOptions.keyStoreType.foreach(storeType => {
        sslEnvs += new EnvVarBuilder()
          .withName("SPARK_SUBMISSION_KEYSTORE_TYPE")
          .withValue(storeType)
          .build()
      })
      sslEnvs += new EnvVarBuilder()
        .withName("SPARK_SUBMISSION_USE_SSL")
        .withValue("true")
        .build()
      val sslSecrets = kubernetesClient.secrets().createNew()
        .withNewMetadata()
        .withName(sslSecretsName)
        .endMetadata()
        .withData(sslSecretsMap.asJava)
        .withType("Opaque")
        .done()
      secrets += sslSecrets
      val sslVolume = new VolumeBuilder()
        .withName("spark-submission-server-ssl-secrets")
        .withNewSecret()
          .withSecretName(sslSecrets.getMetadata.getName)
          .endSecret()
        .build()
      val sslVolumeMount = new VolumeMountBuilder()
        .withName("spark-submission-server-ssl-secrets")
        .withReadOnly(true)
        .withMountPath(sslSecretsDirectory)
        .build()
      (sslEnvs.toArray, Array(sslVolume), Array(sslVolumeMount), secrets.toArray)
    } else {
      (Array[EnvVar](), Array[Volume](), Array[VolumeMount](), Array[Secret]())
    }
  }

  private class DriverPodWatcher(
      submitCompletedFuture: SettableFuture[Boolean],
      submitPending: AtomicBoolean,
      kubernetesClient: KubernetesClient,
      driverLaunchSslOptions: SSLOptions,
      applicationSecrets: Array[Secret],
      driverKubernetesSelectors: java.util.Map[String, String]) extends Watcher[Pod] {
    override def eventReceived(action: Action, pod: Pod): Unit = {
      if ((action == Action.ADDED || action == Action.MODIFIED)
        && pod.getStatus.getPhase == "Running"
        && !submitCompletedFuture.isDone) {
        if (!submitPending.getAndSet(true)) {
          pod.getStatus
            .getContainerStatuses
            .asScala
            .find(status =>
              status.getName == DRIVER_LAUNCHER_CONTAINER_NAME && status.getReady) match {
            case Some(_) =>
              val ownerRefs = Seq(new OwnerReferenceBuilder()
                .withName(pod.getMetadata.getName)
                .withUid(pod.getMetadata.getUid)
                .withApiVersion(pod.getApiVersion)
                .withKind(pod.getKind)
                .withController(true)
                .build())

              applicationSecrets.foreach(secret => {
                secret.getMetadata.setOwnerReferences(ownerRefs.asJava)
                kubernetesClient.secrets().createOrReplace(secret)
              })

              val driverLauncherServicePort = new ServicePortBuilder()
                .withName(DRIVER_LAUNCHER_SERVICE_PORT_NAME)
                .withPort(DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT)
                .withNewTargetPort(DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT)
                .build()
              val service = kubernetesClient.services().createNew()
                .withNewMetadata()
                  .withName(kubernetesAppId)
                  .withLabels(driverKubernetesSelectors)
                  .withOwnerReferences(ownerRefs.asJava)
                  .endMetadata()
                .withNewSpec()
                  .withType("NodePort")
                  .withSelector(driverKubernetesSelectors)
                  .withPorts(driverLauncherServicePort)
                  .endSpec()
                .done()
              try {
                sparkConf.set("spark.kubernetes.driver.service.name",
                  service.getMetadata.getName)
                sparkConf.set("spark.kubernetes.driver.pod.name", kubernetesAppId)
                sparkConf.setIfMissing("spark.driver.port", DEFAULT_DRIVER_PORT.toString)
                sparkConf.setIfMissing("spark.blockmanager.port",
                  DEFAULT_BLOCKMANAGER_PORT.toString)
                val driverLauncher = buildDriverLauncherClient(kubernetesClient, service,
                    driverLaunchSslOptions)
                val ping = Retry.retry(5, 5.seconds) {
                  driverLauncher.ping()
                }
                ping onFailure {
                  case t: Throwable =>
                    submitCompletedFuture.setException(t)
                    kubernetesClient.services().delete(service)
                }
                val submitComplete = ping.flatMap { _ =>
                  Future {
                    sparkConf.set("spark.driver.host", pod.getStatus.getPodIP)
                    val submitRequest = buildSubmissionRequest()
                    driverLauncher.create(submitRequest)
                  }
                }
                submitComplete onFailure {
                  case t: Throwable =>
                    submitCompletedFuture.setException(t)
                    kubernetesClient.services().delete(service)
                }
                val adjustServicePort = submitComplete.flatMap { _ =>
                  Future {
                    // After submitting, adjust the service to only expose the Spark UI
                    val uiServicePort = new ServicePortBuilder()
                      .withName(UI_PORT_NAME)
                      .withPort(uiPort)
                      .withNewTargetPort(uiPort)
                      .build()
                    kubernetesClient.services().withName(kubernetesAppId).edit()
                      .editSpec()
                        .withType("ClusterIP")
                        .withPorts(uiServicePort)
                        .endSpec()
                      .done
                  }
                }
                adjustServicePort onSuccess {
                  case _ =>
                    submitCompletedFuture.set(true)
                }
                adjustServicePort onFailure {
                  case throwable: Throwable =>
                    submitCompletedFuture.setException(throwable)
                    kubernetesClient.services().delete(service)
                }
              } catch {
                case e: Throwable =>
                  submitCompletedFuture.setException(e)
                  Utils.tryLogNonFatalError({
                    kubernetesClient.services().delete(service)
                  })
                  throw e
              }
            case None =>
          }
        }
      }
    }

    override def onClose(e: KubernetesClientException): Unit = {
      if (!submitCompletedFuture.isDone) {
        submitCompletedFuture.setException(e)
      }
    }
  }

  private def buildSubmitFailedErrorMessage(
      kubernetesClient: DefaultKubernetesClient,
      e: TimeoutException): String = {
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
    s"$topLevelMessage\n" +
      s"$podStatusPhase\n" +
      s"$podStatusMessage\n\n$failedDriverContainerStatusString"
  }

  private def buildContainerPorts(): Seq[ContainerPort] = {
    Seq(sparkConf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT),
      sparkConf.getInt("spark.blockManager.port", DEFAULT_BLOCKMANAGER_PORT),
      DRIVER_LAUNCHER_SERVICE_INTERNAL_PORT,
      uiPort).map(new ContainerPortBuilder().withContainerPort(_).build())
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

  private def buildDriverLauncherClient(
      kubernetesClient: KubernetesClient,
      service: Service,
      driverLaunchSslOptions: SSLOptions): KubernetesSparkRestApi = {
    val servicePort = service
      .getSpec
      .getPorts
      .asScala
      .filter(_.getName == DRIVER_LAUNCHER_SERVICE_PORT_NAME)
      .head
      .getNodePort
    // NodePort is exposed on every node, so just pick one of them.
    // TODO be resilient to node failures and try all of them
    val node = kubernetesClient.nodes.list.getItems.asScala.head
    val nodeAddress = node.getStatus.getAddresses.asScala.head.getAddress
    val urlScheme = if (driverLaunchSslOptions.enabled) {
      "https"
    } else {
      logWarning("Submitting application details, application secret, and local" +
        " jars to the cluster over an insecure connection. You should configure SSL" +
        " to secure this step.")
      "http"
    }
    val (trustManager, sslContext): (X509TrustManager, SSLContext) =
      if (driverLaunchSslOptions.enabled) {
        buildSslConnectionConfiguration(driverLaunchSslOptions)
      } else {
        (null, SSLContext.getDefault)
      }
    val url = s"$urlScheme://$nodeAddress:$servicePort"
    HttpClientUtil.createClient[KubernetesSparkRestApi](
      url,
      sslSocketFactory = sslContext.getSocketFactory,
      trustContext = trustManager)
  }

  private def buildSslConnectionConfiguration(driverLaunchSslOptions: SSLOptions) = {
    driverLaunchSslOptions.trustStore.map(trustStoreFile => {
      val trustManagerFactory = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm)
      val trustStore = KeyStore.getInstance(
        driverLaunchSslOptions.trustStoreType.getOrElse(KeyStore.getDefaultType))
      if (!trustStoreFile.isFile) {
        throw new SparkException(s"TrustStore file at ${trustStoreFile.getAbsolutePath}" +
          s" does not exist or is not a file.")
      }
      Utils.tryWithResource(new FileInputStream(trustStoreFile)) { trustStoreStream =>
        driverLaunchSslOptions.trustStorePassword match {
          case Some(password) =>
            trustStore.load(trustStoreStream, password.toCharArray)
          case None => trustStore.load(trustStoreStream, null)
        }
      }
      trustManagerFactory.init(trustStore)
      val trustManagers = trustManagerFactory.getTrustManagers
      val sslContext = SSLContext.getInstance("TLSv1.2")
      sslContext.init(null, trustManagers, SECURE_RANDOM)
      (trustManagers(0).asInstanceOf[X509TrustManager], sslContext)
    }).getOrElse((null, SSLContext.getDefault))
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
  private val SSL_KEYSTORE_SECRET_NAME = "spark-submission-server-keystore"
  private val SSL_KEYSTORE_PASSWORD_SECRET_NAME = "spark-submission-server-keystore-password"
  private val SSL_KEY_PASSWORD_SECRET_NAME = "spark-submission-server-key-password"
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
