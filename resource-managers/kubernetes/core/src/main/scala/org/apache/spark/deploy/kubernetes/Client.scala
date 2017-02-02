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
import java.util.concurrent.{TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.{SSLContext, TrustManagerFactory, X509TrustManager}

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.commons.codec.binary.Base64
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

import org.apache.spark.{SecurityManager, SparkConf, SparkException, SSLOptions}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.rest.{AppResource, ContainerAppResource, KubernetesCreateSubmissionRequest, RemoteAppResource, TarGzippedData, UploadedAppResource}
import org.apache.spark.deploy.rest.kubernetes._
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class Client(
    sparkConf: SparkConf,
    mainClass: String,
    mainAppResource: String,
    appArgs: Array[String]) extends Logging {
  import Client._

  private val namespace = sparkConf.get(KUBERNETES_NAMESPACE)
  private val master = resolveK8sMaster(sparkConf.get("spark.master"))

  private val launchTime = System.currentTimeMillis
  private val appName = sparkConf.getOption("spark.app.name")
    .getOrElse("spark")
  private val kubernetesAppId = s"$appName-$launchTime".toLowerCase.replaceAll("\\.", "-")
  private val secretName = s"$SUBMISSION_APP_SECRET_PREFIX-$kubernetesAppId"
  private val secretDirectory = s"$DRIVER_CONTAINER_SECRETS_BASE_DIR/$kubernetesAppId"
  private val sslSecretsDirectory = s"$DRIVER_CONTAINER_SECRETS_BASE_DIR/$kubernetesAppId-ssl"
  private val sslSecretsName = s"$SUBMISSION_SSL_SECRETS_PREFIX-$kubernetesAppId"
  private val driverDockerImage = sparkConf.get(DRIVER_DOCKER_IMAGE)
  private val uploadedJars = sparkConf.get(KUBERNETES_DRIVER_UPLOAD_JARS).filter(_.nonEmpty)
  private val uploadedFiles = sparkConf.get(KUBERNETES_DRIVER_UPLOAD_FILES).filter(_.nonEmpty)
  uploadedFiles.foreach(validateNoDuplicateUploadFileNames)
  private val uiPort = sparkConf.getInt("spark.ui.port", DEFAULT_UI_PORT)
  private val driverSubmitTimeoutSecs = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_TIMEOUT)

  private val secretBase64String = {
    val secretBytes = new Array[Byte](128)
    SECURE_RANDOM.nextBytes(secretBytes)
    Base64.encodeBase64String(secretBytes)
  }

  private val serviceAccount = sparkConf.get(KUBERNETES_SERVICE_ACCOUNT_NAME)
  private val customLabels = sparkConf.get(KUBERNETES_DRIVER_LABELS)

  private implicit val retryableExecutionContext = ExecutionContext
    .fromExecutorService(
      ThreadUtils.newDaemonSingleThreadExecutor("kubernetes-client-retryable-futures"))

  def run(): Unit = {
    val (driverSubmitSslOptions, isKeyStoreLocalFile) = parseDriverSubmitSslOptions()
    val parsedCustomLabels = parseCustomLabels(customLabels)
    var k8ConfBuilder = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(master)
      .withNamespace(namespace)
    sparkConf.get(KUBERNETES_CA_CERT_FILE).foreach {
      f => k8ConfBuilder = k8ConfBuilder.withCaCertFile(f)
    }
    sparkConf.get(KUBERNETES_CLIENT_KEY_FILE).foreach {
      f => k8ConfBuilder = k8ConfBuilder.withClientKeyFile(f)
    }
    sparkConf.get(KUBERNETES_CLIENT_CERT_FILE).foreach {
      f => k8ConfBuilder = k8ConfBuilder.withClientCertFile(f)
    }

    val k8ClientConfig = k8ConfBuilder.build
    Utils.tryWithResource(new DefaultKubernetesClient(k8ClientConfig)) { kubernetesClient =>
      val submitServerSecret = kubernetesClient.secrets().createNew()
        .withNewMetadata()
          .withName(secretName)
          .endMetadata()
        .withData(Map((SUBMISSION_APP_SECRET_NAME, secretBase64String)).asJava)
        .withType("Opaque")
        .done()
      val (sslEnvs, sslVolumes, sslVolumeMounts, sslSecrets) = configureSsl(kubernetesClient,
        driverSubmitSslOptions,
        isKeyStoreLocalFile)
      try {
        val driverKubernetesSelectors = (Map(
            SPARK_DRIVER_LABEL -> kubernetesAppId,
            SPARK_APP_ID_LABEL -> kubernetesAppId,
            SPARK_APP_NAME_LABEL -> appName)
          ++ parsedCustomLabels).asJava
        val containerPorts = buildContainerPorts()
        val submitCompletedFuture = SettableFuture.create[Boolean]
        val submitPending = new AtomicBoolean(false)
        val podWatcher = new DriverPodWatcher(
          submitCompletedFuture,
          submitPending,
          kubernetesClient,
          driverSubmitSslOptions,
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
                .withName(SUBMISSION_APP_SECRET_VOLUME_NAME)
                .withNewSecret()
                  .withSecretName(submitServerSecret.getMetadata.getName)
                  .endSecret()
                .endVolume
              .addToVolumes(sslVolumes: _*)
              .withServiceAccount(serviceAccount)
              .addNewContainer()
                .withName(DRIVER_CONTAINER_NAME)
                .withImage(driverDockerImage)
                .withImagePullPolicy("IfNotPresent")
                .addNewVolumeMount()
                  .withName(SUBMISSION_APP_SECRET_VOLUME_NAME)
                  .withMountPath(secretDirectory)
                  .withReadOnly(true)
                  .endVolumeMount()
                .addToVolumeMounts(sslVolumeMounts: _*)
                .addNewEnv()
                  .withName(ENV_SUBMISSION_SECRET_LOCATION)
                  .withValue(s"$secretDirectory/$SUBMISSION_APP_SECRET_NAME")
                  .endEnv()
                .addNewEnv()
                  .withName(ENV_SUBMISSION_SERVER_PORT)
                  .withValue(SUBMISSION_SERVER_PORT.toString)
                  .endEnv()
                .addToEnv(sslEnvs: _*)
                .withPorts(containerPorts.asJava)
                .endContainer()
              .endSpec()
            .done()
          var submitSucceeded = false
          try {
            submitCompletedFuture.get(driverSubmitTimeoutSecs, TimeUnit.SECONDS)
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

  private def parseDriverSubmitSslOptions(): (SSLOptions, Boolean) = {
    val maybeKeyStore = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_KEYSTORE)
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
      resolvedSparkConf.set(KUBERNETES_DRIVER_SUBMIT_KEYSTORE, _)
    }
    sparkConf.get(KUBERNETES_DRIVER_SUBMIT_TRUSTSTORE).foreach { trustStore =>
      val trustStoreURI = Utils.resolveURI(trustStore)
      trustStoreURI.getScheme match {
        case "file" | null =>
          resolvedSparkConf.set(KUBERNETES_DRIVER_SUBMIT_TRUSTSTORE, trustStoreURI.getPath)
        case _ => throw new SparkException(s"Invalid trustStore URI $trustStore; trustStore URI" +
          " for submit server must have no scheme, or scheme file://")
      }
    }
    val securityManager = new SecurityManager(resolvedSparkConf)
    (securityManager.getSSLOptions(KUBERNETES_SUBMIT_SSL_NAMESPACE), isLocalKeyStore)
  }

  private def configureSsl(kubernetesClient: KubernetesClient, driverSubmitSslOptions: SSLOptions,
        isKeyStoreLocalFile: Boolean):
        (Array[EnvVar], Array[Volume], Array[VolumeMount], Array[Secret]) = {
    if (driverSubmitSslOptions.enabled) {
      val sslSecretsMap = mutable.HashMap[String, String]()
      val sslEnvs = mutable.Buffer[EnvVar]()
      val secrets = mutable.Buffer[Secret]()
      driverSubmitSslOptions.keyStore.foreach(store => {
        val resolvedKeyStoreFile = if (isKeyStoreLocalFile) {
          if (!store.isFile) {
            throw new SparkException(s"KeyStore specified at $store is not a file or" +
              s" does not exist.")
          }
          val keyStoreBytes = Files.toByteArray(store)
          val keyStoreBase64 = Base64.encodeBase64String(keyStoreBytes)
          sslSecretsMap += (SUBMISSION_SSL_KEYSTORE_SECRET_NAME -> keyStoreBase64)
          s"$sslSecretsDirectory/$SUBMISSION_SSL_KEYSTORE_SECRET_NAME"
        } else {
          store.getAbsolutePath
        }
        sslEnvs += new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_FILE)
          .withValue(resolvedKeyStoreFile)
          .build()
      })
      driverSubmitSslOptions.keyStorePassword.foreach(password => {
        val passwordBase64 = Base64.encodeBase64String(password.getBytes(Charsets.UTF_8))
        sslSecretsMap += (SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME -> passwordBase64)
        sslEnvs += new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_PASSWORD_FILE)
          .withValue(s"$sslSecretsDirectory/$SUBMISSION_SSL_KEYSTORE_PASSWORD_SECRET_NAME")
          .build()
      })
      driverSubmitSslOptions.keyPassword.foreach(password => {
        val passwordBase64 = Base64.encodeBase64String(password.getBytes(Charsets.UTF_8))
        sslSecretsMap += (SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME -> passwordBase64)
        sslEnvs += new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_KEY_PASSWORD_FILE)
          .withValue(s"$sslSecretsDirectory/$SUBMISSION_SSL_KEY_PASSWORD_SECRET_NAME")
          .build()
      })
      driverSubmitSslOptions.keyStoreType.foreach(storeType => {
        sslEnvs += new EnvVarBuilder()
          .withName(ENV_SUBMISSION_KEYSTORE_TYPE)
          .withValue(storeType)
          .build()
      })
      sslEnvs += new EnvVarBuilder()
        .withName(ENV_SUBMISSION_USE_SSL)
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
        .withName(SUBMISSION_SSL_SECRETS_VOLUME_NAME)
        .withNewSecret()
          .withSecretName(sslSecrets.getMetadata.getName)
          .endSecret()
        .build()
      val sslVolumeMount = new VolumeMountBuilder()
        .withName(SUBMISSION_SSL_SECRETS_VOLUME_NAME)
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
      driverSubmitSslOptions: SSLOptions,
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
              status.getName == DRIVER_CONTAINER_NAME && status.getReady) match {
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

              val driverSubmissionServicePort = new ServicePortBuilder()
                .withName(SUBMISSION_SERVER_PORT_NAME)
                .withPort(SUBMISSION_SERVER_PORT)
                .withNewTargetPort(SUBMISSION_SERVER_PORT)
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
                  .withPorts(driverSubmissionServicePort)
                  .endSpec()
                .done()
              try {
                sparkConf.getOption("spark.app.id").foreach { id =>
                  logWarning(s"Warning: Provided app id in spark.app.id as $id will be" +
                    s" overridden as $kubernetesAppId")
                }
                sparkConf.set(KUBERNETES_DRIVER_POD_NAME, kubernetesAppId)
                sparkConf.set(KUBERNETES_DRIVER_SERVICE_NAME, service.getMetadata.getName)
                sparkConf.set("spark.app.id", kubernetesAppId)
                sparkConf.setIfMissing("spark.app.name", appName)
                sparkConf.setIfMissing("spark.driver.port", DEFAULT_DRIVER_PORT.toString)
                sparkConf.setIfMissing("spark.blockmanager.port",
                  DEFAULT_BLOCKMANAGER_PORT.toString)
                val driverSubmitter = buildDriverSubmissionClient(kubernetesClient, service,
                    driverSubmitSslOptions)
                val ping = Retry.retry(5, 5.seconds,
                    Some("Failed to contact the driver server")) {
                  driverSubmitter.ping()
                }
                ping onFailure {
                  case t: Throwable =>
                    logError("Ping failed to the driver server", t)
                    submitCompletedFuture.setException(t)
                    kubernetesClient.services().delete(service)
                }
                val submitComplete = ping.flatMap { _ =>
                  Future {
                    sparkConf.set("spark.driver.host", pod.getStatus.getPodIP)
                    val submitRequest = buildSubmissionRequest()
                    driverSubmitter.submitApplication(submitRequest)
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
        logError(s"Timed out while waiting $driverSubmitTimeoutSecs seconds for the" +
          " driver pod to start, but an error occurred while fetching the driver" +
          " pod's details.", throwable)
        throw new SparkException(s"Timed out while waiting $driverSubmitTimeoutSecs" +
          " seconds for the driver pod to start. Unfortunately, in attempting to fetch" +
          " the latest state of the pod, another error was thrown. Check the logs for" +
          " the error that was thrown in looking up the driver pod.", e)
    }
    val topLevelMessage = s"The driver pod with name ${driverPod.getMetadata.getName}" +
      s" in namespace ${driverPod.getMetadata.getNamespace} was not ready in" +
      s" $driverSubmitTimeoutSecs seconds."
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
      .find(_.getName == DRIVER_CONTAINER_NAME)
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
      s" container with name $DRIVER_CONTAINER_NAME")
    s"$topLevelMessage\n" +
      s"$podStatusPhase\n" +
      s"$podStatusMessage\n\n$failedDriverContainerStatusString"
  }

  private def buildContainerPorts(): Seq[ContainerPort] = {
    Seq((DRIVER_PORT_NAME, sparkConf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT)),
      (BLOCK_MANAGER_PORT_NAME,
        sparkConf.getInt("spark.blockManager.port", DEFAULT_BLOCKMANAGER_PORT)),
      (SUBMISSION_SERVER_PORT_NAME, SUBMISSION_SERVER_PORT),
      (UI_PORT_NAME, uiPort)).map(port => new ContainerPortBuilder()
        .withName(port._1)
        .withContainerPort(port._2)
        .build())
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
    val uploadJarsBase64Contents = compressFiles(uploadedJars)
    val uploadFilesBase64Contents = compressFiles(uploadedFiles)
    KubernetesCreateSubmissionRequest(
      appResource = resolvedAppResource,
      mainClass = mainClass,
      appArgs = appArgs,
      secret = secretBase64String,
      sparkProperties = sparkConf.getAll.toMap,
      uploadedJarsBase64Contents = uploadJarsBase64Contents,
      uploadedFilesBase64Contents = uploadFilesBase64Contents)
  }

  // Because uploaded files should be added to the working directory of the driver, they
  // need to not have duplicate file names. They are added to the working directory so the
  // user can reliably locate them in their application. This is similar in principle to how
  // YARN handles its `spark.files` setting.
  private def validateNoDuplicateUploadFileNames(uploadedFilesCommaSeparated: String): Unit = {
    val pathsWithDuplicateNames = uploadedFilesCommaSeparated
      .split(",")
      .groupBy(new File(_).getName)
      .filter(_._2.length > 1)
    if (pathsWithDuplicateNames.nonEmpty) {
      val pathsWithDuplicateNamesSorted = pathsWithDuplicateNames
        .values
        .flatten
        .toList
        .sortBy(new File(_).getName)
      throw new SparkException("Cannot upload files with duplicate names via" +
        s" ${KUBERNETES_DRIVER_UPLOAD_FILES.key}. The following paths have a duplicated" +
        s" file name: ${pathsWithDuplicateNamesSorted.mkString(",")}")
    }
  }

  private def compressFiles(maybeFilePaths: Option[String]): Option[TarGzippedData] = {
    maybeFilePaths
      .map(_.split(","))
      .map(CompressionUtils.createTarGzip(_))
  }

  private def buildDriverSubmissionClient(
      kubernetesClient: KubernetesClient,
      service: Service,
      driverSubmitSslOptions: SSLOptions): KubernetesSparkRestApi = {
    val urlScheme = if (driverSubmitSslOptions.enabled) {
      "https"
    } else {
      logWarning("Submitting application details, application secret, and local" +
        " jars to the cluster over an insecure connection. You should configure SSL" +
        " to secure this step.")
      "http"
    }
    val servicePort = service.getSpec.getPorts.asScala
      .filter(_.getName == SUBMISSION_SERVER_PORT_NAME)
      .head.getNodePort
    val nodeUrls = kubernetesClient.nodes.list.getItems.asScala
      .filterNot(_.getSpec.getUnschedulable)
      .flatMap(_.getStatus.getAddresses.asScala.map(address => {
        s"$urlScheme://${address.getAddress}:$servicePort"
      })).toArray
    require(nodeUrls.nonEmpty, "No nodes found to contact the driver!")
    val (trustManager, sslContext): (X509TrustManager, SSLContext) =
      if (driverSubmitSslOptions.enabled) {
        buildSslConnectionConfiguration(driverSubmitSslOptions)
      } else {
        (null, SSLContext.getDefault)
      }
    HttpClientUtil.createClient[KubernetesSparkRestApi](
      uris = nodeUrls,
      sslSocketFactory = sslContext.getSocketFactory,
      trustContext = trustManager)
  }

  private def buildSslConnectionConfiguration(driverSubmitSslOptions: SSLOptions) = {
    driverSubmitSslOptions.trustStore.map(trustStoreFile => {
      val trustManagerFactory = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm)
      val trustStore = KeyStore.getInstance(
        driverSubmitSslOptions.trustStoreType.getOrElse(KeyStore.getDefaultType))
      if (!trustStoreFile.isFile) {
        throw new SparkException(s"TrustStore file at ${trustStoreFile.getAbsolutePath}" +
          s" does not exist or is not a file.")
      }
      Utils.tryWithResource(new FileInputStream(trustStoreFile)) { trustStoreStream =>
        driverSubmitSslOptions.trustStorePassword match {
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

  private def parseCustomLabels(maybeLabels: Option[String]): Map[String, String] = {
    maybeLabels.map(labels => {
      labels.split(",").map(_.trim).filterNot(_.isEmpty).map(label => {
        label.split("=", 2).toSeq match {
          case Seq(k, v) =>
            require(k != SPARK_APP_ID_LABEL, "Label with key" +
              s" $SPARK_APP_ID_LABEL cannot be used in" +
              " spark.kubernetes.driver.labels, as it is reserved for Spark's" +
              " internal configuration.")
            (k, v)
          case _ =>
            throw new SparkException("Custom labels set by spark.kubernetes.driver.labels" +
              " must be a comma-separated list of key-value pairs, with format <key>=<value>." +
              s" Got label: $label. All labels: $labels")
        }
      }).toMap
    }).getOrElse(Map.empty[String, String])
  }
}

private[spark] object Client extends Logging {

  private[spark] val SECURE_RANDOM = new SecureRandom()

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
