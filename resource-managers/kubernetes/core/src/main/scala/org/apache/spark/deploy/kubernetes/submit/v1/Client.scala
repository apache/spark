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
package org.apache.spark.deploy.kubernetes.submit.v1

import java.io.File
import java.security.SecureRandom
import java.util.ServiceLoader
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.google.common.io.Files
import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{ConfigBuilder => K8SConfigBuilder, DefaultKubernetesClient, KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.commons.codec.binary.Base64
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.kubernetes.CompressionUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.rest.kubernetes.v1.{AppResource, ContainerAppResource, HttpClientUtil, KubernetesCreateSubmissionRequest, KubernetesCredentials, KubernetesFileUtils, KubernetesSparkRestApi, RemoteAppResource, UploadedAppResource}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

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
  private val secretDirectory = s"$DRIVER_CONTAINER_SUBMISSION_SECRETS_BASE_DIR/$kubernetesAppId"
  private val driverDockerImage = sparkConf.get(DRIVER_DOCKER_IMAGE)
  private val uiPort = sparkConf.getInt("spark.ui.port", DEFAULT_UI_PORT)
  private val driverSubmitTimeoutSecs = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_TIMEOUT)
  private val driverServiceManagerType = sparkConf.get(DRIVER_SERVICE_MANAGER_TYPE)
  private val sparkFiles = sparkConf.getOption("spark.files")
    .map(_.split(","))
    .getOrElse(Array.empty[String])
  private val sparkJars = sparkConf.getOption("spark.jars")
    .map(_.split(","))
    .getOrElse(Array.empty[String])

  // CPU settings
  private val driverCpuCores = sparkConf.getOption("spark.driver.cores").getOrElse("1")

  // Memory settings
  private val driverMemoryMb = sparkConf.get(org.apache.spark.internal.config.DRIVER_MEMORY)
  private val driverSubmitServerMemoryMb = sparkConf.get(KUBERNETES_DRIVER_SUBMIT_SERVER_MEMORY)
  private val driverSubmitServerMemoryString = sparkConf.get(
    KUBERNETES_DRIVER_SUBMIT_SERVER_MEMORY.key,
    KUBERNETES_DRIVER_SUBMIT_SERVER_MEMORY.defaultValueString)
  private val driverContainerMemoryMb = driverMemoryMb + driverSubmitServerMemoryMb
  private val memoryOverheadMb = sparkConf
    .get(KUBERNETES_DRIVER_MEMORY_OVERHEAD)
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * driverContainerMemoryMb).toInt,
      MEMORY_OVERHEAD_MIN))
  private val driverContainerMemoryWithOverhead = driverContainerMemoryMb + memoryOverheadMb

  private val waitForAppCompletion: Boolean = sparkConf.get(WAIT_FOR_APP_COMPLETION)

  private val secretBase64String = {
    val secretBytes = new Array[Byte](128)
    SECURE_RANDOM.nextBytes(secretBytes)
    Base64.encodeBase64String(secretBytes)
  }

  private val serviceAccount = sparkConf.get(KUBERNETES_SERVICE_ACCOUNT_NAME)
  private val customLabels = sparkConf.get(KUBERNETES_DRIVER_LABELS)
  private val customAnnotations = sparkConf.get(KUBERNETES_DRIVER_ANNOTATIONS)

  private val kubernetesResourceCleaner = new KubernetesResourceCleaner

  def run(): Unit = {
    logInfo(s"Starting application $kubernetesAppId in Kubernetes...")
    val submitterLocalFiles = KubernetesFileUtils.getOnlySubmitterLocalFiles(sparkFiles)
    val submitterLocalJars = KubernetesFileUtils.getOnlySubmitterLocalFiles(sparkJars)
    (submitterLocalFiles ++ submitterLocalJars).foreach { file =>
      if (!new File(Utils.resolveURI(file).getPath).isFile) {
        throw new SparkException(s"File $file does not exist or is a directory.")
      }
    }
    if (KubernetesFileUtils.isUriLocalFile(mainAppResource) &&
        !new File(Utils.resolveURI(mainAppResource).getPath).isFile) {
      throw new SparkException(s"Main app resource file $mainAppResource is not a file or" +
        s" is a directory.")
    }
    val driverServiceManager = getDriverServiceManager
    val parsedCustomLabels = parseKeyValuePairs(customLabels, KUBERNETES_DRIVER_LABELS.key,
      "labels")
    parsedCustomLabels.keys.foreach { key =>
      require(key != SPARK_APP_ID_LABEL, "Label with key" +
        s" $SPARK_APP_ID_LABEL cannot be used in" +
        " spark.kubernetes.driver.labels, as it is reserved for Spark's" +
        " internal configuration.")
    }
    val parsedCustomAnnotations = parseKeyValuePairs(
      customAnnotations,
      KUBERNETES_DRIVER_ANNOTATIONS.key,
      "annotations")
    val driverPodKubernetesCredentials = new DriverPodKubernetesCredentialsProvider(sparkConf).get()
    var k8ConfBuilder = new K8SConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(master)
      .withNamespace(namespace)
    sparkConf.get(KUBERNETES_SUBMIT_CA_CERT_FILE).foreach {
      f => k8ConfBuilder = k8ConfBuilder.withCaCertFile(f)
    }
    sparkConf.get(KUBERNETES_SUBMIT_CLIENT_KEY_FILE).foreach {
      f => k8ConfBuilder = k8ConfBuilder.withClientKeyFile(f)
    }
    sparkConf.get(KUBERNETES_SUBMIT_CLIENT_CERT_FILE).foreach {
      f => k8ConfBuilder = k8ConfBuilder.withClientCertFile(f)
    }
    sparkConf.get(KUBERNETES_SUBMIT_OAUTH_TOKEN).foreach { token =>
      k8ConfBuilder = k8ConfBuilder.withOauthToken(token)
    }

    val k8ClientConfig = k8ConfBuilder.build
    Utils.tryWithResource(new DefaultKubernetesClient(k8ClientConfig)) { kubernetesClient =>
      driverServiceManager.start(kubernetesClient, kubernetesAppId, sparkConf)
      // start outer watch for status logging of driver pod
      // only enable interval logging if in waitForAppCompletion mode
      val loggingInterval = if (waitForAppCompletion) sparkConf.get(REPORT_INTERVAL) else 0
      val driverPodCompletedLatch = new CountDownLatch(1)
      val loggingWatch = new LoggingPodStatusWatcher(driverPodCompletedLatch, kubernetesAppId,
        loggingInterval)
      Utils.tryWithResource(kubernetesClient
          .pods()
          .withName(kubernetesAppId)
          .watch(loggingWatch)) { _ =>
        val resourceCleanShutdownHook = ShutdownHookManager.addShutdownHook(() =>
          kubernetesResourceCleaner.deleteAllRegisteredResourcesFromKubernetes(kubernetesClient))
        val cleanupServiceManagerHook = ShutdownHookManager.addShutdownHook(
          ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY)(
          () => driverServiceManager.stop())
        // Place the error hook at a higher priority in order for the error hook to run before
        // the stop hook.
        val serviceManagerErrorHook = ShutdownHookManager.addShutdownHook(
          ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY + 1)(() =>
          driverServiceManager.handleSubmissionError(
            new SparkException("Submission shutting down early...")))
        try {
          val sslConfigurationProvider = new DriverSubmitSslConfigurationProvider(
            sparkConf, kubernetesAppId, kubernetesClient, kubernetesResourceCleaner)
          val submitServerSecret = kubernetesClient.secrets().createNew()
            .withNewMetadata()
            .withName(secretName)
            .endMetadata()
            .withData(Map((SUBMISSION_APP_SECRET_NAME, secretBase64String)).asJava)
            .withType("Opaque")
            .done()
          kubernetesResourceCleaner.registerOrUpdateResource(submitServerSecret)
          val sslConfiguration = sslConfigurationProvider.getSslConfiguration()
          val (driverPod, driverService) = launchDriverKubernetesComponents(
            kubernetesClient,
            driverServiceManager,
            parsedCustomLabels,
            parsedCustomAnnotations,
            submitServerSecret,
            sslConfiguration)
          configureOwnerReferences(
            kubernetesClient,
            submitServerSecret,
            sslConfiguration.sslSecret,
            driverPod,
            driverService)
          submitApplicationToDriverServer(
            kubernetesClient,
            driverServiceManager,
            sslConfiguration,
            driverService,
            submitterLocalFiles,
            submitterLocalJars,
            driverPodKubernetesCredentials)
          // Now that the application has started, persist the components that were created beyond
          // the shutdown hook. We still want to purge the one-time secrets, so do not unregister
          // those.
          kubernetesResourceCleaner.unregisterResource(driverPod)
          kubernetesResourceCleaner.unregisterResource(driverService)
        } catch {
          case e: Throwable =>
            driverServiceManager.handleSubmissionError(e)
            throw e
        } finally {
          Utils.tryLogNonFatalError {
            kubernetesResourceCleaner.deleteAllRegisteredResourcesFromKubernetes(kubernetesClient)
          }
          Utils.tryLogNonFatalError {
            driverServiceManager.stop()
          }
          // Remove the shutdown hooks that would be redundant
          Utils.tryLogNonFatalError {
            ShutdownHookManager.removeShutdownHook(resourceCleanShutdownHook)
          }
          Utils.tryLogNonFatalError {
            ShutdownHookManager.removeShutdownHook(cleanupServiceManagerHook)
          }
          Utils.tryLogNonFatalError {
            ShutdownHookManager.removeShutdownHook(serviceManagerErrorHook)
          }
        }
        // wait if configured to do so
        if (waitForAppCompletion) {
          logInfo(s"Waiting for application $kubernetesAppId to finish...")
          driverPodCompletedLatch.await()
          logInfo(s"Application $kubernetesAppId finished.")
        } else {
          logInfo(s"Application $kubernetesAppId successfully launched.")
        }
      }
    }
  }

  private def submitApplicationToDriverServer(
      kubernetesClient: KubernetesClient,
      driverServiceManager: DriverServiceManager,
      sslConfiguration: DriverSubmitSslConfiguration,
      driverService: Service,
      submitterLocalFiles: Iterable[String],
      submitterLocalJars: Iterable[String],
      driverPodKubernetesCredentials: KubernetesCredentials): Unit = {
    sparkConf.getOption("spark.app.id").foreach { id =>
      logWarning(s"Warning: Provided app id in spark.app.id as $id will be" +
        s" overridden as $kubernetesAppId")
    }
    sparkConf.set(KUBERNETES_DRIVER_POD_NAME, kubernetesAppId)
    sparkConf.set(KUBERNETES_DRIVER_SERVICE_NAME, driverService.getMetadata.getName)
    sparkConf.set("spark.app.id", kubernetesAppId)
    sparkConf.setIfMissing("spark.app.name", appName)
    sparkConf.setIfMissing("spark.driver.port", DEFAULT_DRIVER_PORT.toString)
    sparkConf.setIfMissing("spark.driver.blockManager.port", DEFAULT_BLOCKMANAGER_PORT.toString)
    sparkConf.setIfMissing("spark.blockManager.port", DEFAULT_BLOCKMANAGER_PORT.toString)
    sparkConf.get(KUBERNETES_SUBMIT_OAUTH_TOKEN).foreach { _ =>
      sparkConf.set(KUBERNETES_SUBMIT_OAUTH_TOKEN, "<present_but_redacted>")
    }
    sparkConf.get(KUBERNETES_DRIVER_OAUTH_TOKEN).foreach { _ =>
      sparkConf.set(KUBERNETES_DRIVER_OAUTH_TOKEN, "<present_but_redacted>")
    }
    val driverSubmitter = buildDriverSubmissionClient(
      kubernetesClient,
      driverServiceManager,
      driverService,
      sslConfiguration)
    // Sanity check to see if the driver submitter is even reachable.
    driverSubmitter.ping()
    logInfo(s"Submitting local resources to driver pod for application " +
      s"$kubernetesAppId ...")
    val submitRequest = buildSubmissionRequest(
      submitterLocalFiles,
      submitterLocalJars,
      driverPodKubernetesCredentials)
    driverSubmitter.submitApplication(submitRequest)
    logInfo("Successfully submitted local resources and driver configuration to" +
      " driver pod.")
    // After submitting, adjust the service to only expose the Spark UI
    val uiServiceType = if (sparkConf.get(EXPOSE_KUBERNETES_DRIVER_SERVICE_UI_PORT)) "NodePort"
      else "ClusterIP"
    val uiServicePort = new ServicePortBuilder()
      .withName(UI_PORT_NAME)
      .withPort(uiPort)
      .withNewTargetPort(uiPort)
      .build()
    val resolvedService = kubernetesClient.services().withName(kubernetesAppId).edit()
      .editSpec()
        .withType(uiServiceType)
        .withPorts(uiServicePort)
        .endSpec()
      .done()
    kubernetesResourceCleaner.registerOrUpdateResource(resolvedService)
    logInfo("Finished submitting application to Kubernetes.")
  }

  private def launchDriverKubernetesComponents(
      kubernetesClient: KubernetesClient,
      driverServiceManager: DriverServiceManager,
      customLabels: Map[String, String],
      customAnnotations: Map[String, String],
      submitServerSecret: Secret,
      sslConfiguration: DriverSubmitSslConfiguration): (Pod, Service) = {
    val driverKubernetesSelectors = (Map(
      SPARK_DRIVER_LABEL -> kubernetesAppId,
      SPARK_APP_ID_LABEL -> kubernetesAppId,
      SPARK_APP_NAME_LABEL -> appName)
      ++ customLabels)
    val endpointsReadyFuture = SettableFuture.create[Endpoints]
    val endpointsReadyWatcher = new DriverEndpointsReadyWatcher(endpointsReadyFuture)
    val serviceReadyFuture = SettableFuture.create[Service]
    val serviceReadyWatcher = new DriverServiceReadyWatcher(serviceReadyFuture)
    val podReadyFuture = SettableFuture.create[Pod]
    val podWatcher = new DriverPodReadyWatcher(podReadyFuture)
    Utils.tryWithResource(kubernetesClient
        .pods()
        .withName(kubernetesAppId)
        .watch(podWatcher)) { _ =>
      Utils.tryWithResource(kubernetesClient
          .services()
          .withName(kubernetesAppId)
          .watch(serviceReadyWatcher)) { _ =>
        Utils.tryWithResource(kubernetesClient
            .endpoints()
            .withName(kubernetesAppId)
            .watch(endpointsReadyWatcher)) { _ =>
          val serviceTemplate = createDriverServiceTemplate(driverKubernetesSelectors)
          val driverService = kubernetesClient.services().create(
            driverServiceManager.customizeDriverService(serviceTemplate).build())
          kubernetesResourceCleaner.registerOrUpdateResource(driverService)
          val driverPod = createDriverPod(
            kubernetesClient,
            driverKubernetesSelectors,
            customAnnotations,
            submitServerSecret,
            sslConfiguration)
          waitForReadyKubernetesComponents(kubernetesClient, endpointsReadyFuture,
            serviceReadyFuture, podReadyFuture)
          (driverPod, driverService)
        }
      }
    }
  }

  /**
   * Sets the owner reference for all the kubernetes components to link to the driver pod.
   *
   * @return The driver service after it has been adjusted to reflect the new owner
   * reference.
   */
  private def configureOwnerReferences(
      kubernetesClient: KubernetesClient,
      submitServerSecret: Secret,
      sslSecret: Option[Secret],
      driverPod: Pod,
      driverService: Service): Service = {
    val driverPodOwnerRef = new OwnerReferenceBuilder()
      .withName(driverPod.getMetadata.getName)
      .withUid(driverPod.getMetadata.getUid)
      .withApiVersion(driverPod.getApiVersion)
      .withKind(driverPod.getKind)
      .withController(true)
      .build()
    sslSecret.foreach(secret => {
      val updatedSecret = kubernetesClient.secrets().withName(secret.getMetadata.getName).edit()
        .editMetadata()
        .addToOwnerReferences(driverPodOwnerRef)
        .endMetadata()
        .done()
      kubernetesResourceCleaner.registerOrUpdateResource(updatedSecret)
    })
    val updatedSubmitServerSecret = kubernetesClient
      .secrets()
      .withName(submitServerSecret.getMetadata.getName)
      .edit()
        .editMetadata()
          .addToOwnerReferences(driverPodOwnerRef)
          .endMetadata()
        .done()
    kubernetesResourceCleaner.registerOrUpdateResource(updatedSubmitServerSecret)
    val updatedService = kubernetesClient
      .services()
      .withName(driverService.getMetadata.getName)
      .edit()
        .editMetadata()
          .addToOwnerReferences(driverPodOwnerRef)
          .endMetadata()
        .done()
    kubernetesResourceCleaner.registerOrUpdateResource(updatedService)
    updatedService
  }

  private def waitForReadyKubernetesComponents(
      kubernetesClient: KubernetesClient,
      endpointsReadyFuture: SettableFuture[Endpoints],
      serviceReadyFuture: SettableFuture[Service],
      podReadyFuture: SettableFuture[Pod]) = {
    try {
      podReadyFuture.get(driverSubmitTimeoutSecs, TimeUnit.SECONDS)
      logInfo("Driver pod successfully created in Kubernetes cluster.")
    } catch {
      case e: Throwable =>
        val finalErrorMessage: String = buildSubmitFailedErrorMessage(kubernetesClient, e)
        logError(finalErrorMessage, e)
        throw new SparkException(finalErrorMessage, e)
    }
    try {
      serviceReadyFuture.get(driverSubmitTimeoutSecs, TimeUnit.SECONDS)
      logInfo("Driver service created successfully in Kubernetes.")
    } catch {
      case e: Throwable =>
        throw new SparkException(s"The driver service was not ready" +
          s" in $driverSubmitTimeoutSecs seconds.", e)
    }
    try {
      endpointsReadyFuture.get(driverSubmitTimeoutSecs, TimeUnit.SECONDS)
      logInfo("Driver endpoints ready to receive application submission")
    } catch {
      case e: Throwable =>
        throw new SparkException(s"The driver service endpoint was not ready" +
          s" in $driverSubmitTimeoutSecs seconds.", e)
    }
  }

  private def createDriverPod(
      kubernetesClient: KubernetesClient,
      driverKubernetesSelectors: Map[String, String],
      customAnnotations: Map[String, String],
      submitServerSecret: Secret,
      sslConfiguration: DriverSubmitSslConfiguration): Pod = {
    val containerPorts = buildContainerPorts()
    val probePingHttpGet = new HTTPGetActionBuilder()
      .withScheme(if (sslConfiguration.enabled) "HTTPS" else "HTTP")
      .withPath("/v1/submissions/ping")
      .withNewPort(SUBMISSION_SERVER_PORT_NAME)
      .build()
    val driverCpuQuantity = new QuantityBuilder(false)
      .withAmount(driverCpuCores)
      .build()
    val driverMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${driverContainerMemoryMb}M")
      .build()
    val driverMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(s"${driverContainerMemoryWithOverhead}M")
      .build()
    val driverPod = kubernetesClient.pods().createNew()
      .withNewMetadata()
        .withName(kubernetesAppId)
        .withLabels(driverKubernetesSelectors.asJava)
        .withAnnotations(customAnnotations.asJava)
        .endMetadata()
      .withNewSpec()
        .withRestartPolicy("Never")
        .addNewVolume()
          .withName(SUBMISSION_APP_SECRET_VOLUME_NAME)
          .withNewSecret()
            .withSecretName(submitServerSecret.getMetadata.getName)
            .endSecret()
          .endVolume()
        .addToVolumes(sslConfiguration.sslPodVolume.toSeq: _*)
        .withServiceAccount(serviceAccount.getOrElse("default"))
        .addNewContainer()
          .withName(DRIVER_CONTAINER_NAME)
          .withImage(driverDockerImage)
          .withImagePullPolicy("IfNotPresent")
          .addNewVolumeMount()
            .withName(SUBMISSION_APP_SECRET_VOLUME_NAME)
            .withMountPath(secretDirectory)
            .withReadOnly(true)
            .endVolumeMount()
          .addToVolumeMounts(sslConfiguration.sslPodVolumeMount.toSeq: _*)
          .addNewEnv()
            .withName(ENV_SUBMISSION_SECRET_LOCATION)
            .withValue(s"$secretDirectory/$SUBMISSION_APP_SECRET_NAME")
            .endEnv()
          .addNewEnv()
            .withName(ENV_SUBMISSION_SERVER_PORT)
            .withValue(SUBMISSION_SERVER_PORT.toString)
            .endEnv()
          // Note that SPARK_DRIVER_MEMORY only affects the REST server via spark-class.
          .addNewEnv()
            .withName(ENV_DRIVER_MEMORY)
            .withValue(driverSubmitServerMemoryString)
            .endEnv()
          .addToEnv(sslConfiguration.sslPodEnvVars: _*)
          .withNewResources()
            .addToRequests("cpu", driverCpuQuantity)
            .addToLimits("cpu", driverCpuQuantity)
            .addToRequests("memory", driverMemoryQuantity)
            .addToLimits("memory", driverMemoryLimitQuantity)
            .endResources()
          .withPorts(containerPorts.asJava)
          .withNewReadinessProbe().withHttpGet(probePingHttpGet).endReadinessProbe()
          .endContainer()
        .endSpec()
      .done()
    kubernetesResourceCleaner.registerOrUpdateResource(driverPod)
    driverPod
  }

   private def createDriverServiceTemplate(driverKubernetesSelectors: Map[String, String])
      : ServiceBuilder = {
    val driverSubmissionServicePort = new ServicePortBuilder()
      .withName(SUBMISSION_SERVER_PORT_NAME)
      .withPort(SUBMISSION_SERVER_PORT)
      .withNewTargetPort(SUBMISSION_SERVER_PORT)
      .build()
    new ServiceBuilder()
      .withNewMetadata()
        .withName(kubernetesAppId)
        .withLabels(driverKubernetesSelectors.asJava)
        .endMetadata()
      .withNewSpec()
        .withSelector(driverKubernetesSelectors.asJava)
        .withPorts(driverSubmissionServicePort)
        .endSpec()
  }

  private class DriverPodReadyWatcher(resolvedDriverPod: SettableFuture[Pod]) extends Watcher[Pod] {
    override def eventReceived(action: Action, pod: Pod): Unit = {
      if ((action == Action.ADDED || action == Action.MODIFIED)
          && pod.getStatus.getPhase == "Running"
          && !resolvedDriverPod.isDone) {
        pod.getStatus
          .getContainerStatuses
          .asScala
          .find(status =>
            status.getName == DRIVER_CONTAINER_NAME && status.getReady)
          .foreach { _ => resolvedDriverPod.set(pod) }
      }
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      logDebug("Driver pod readiness watch closed.", cause)
    }
  }

  private class DriverEndpointsReadyWatcher(resolvedDriverEndpoints: SettableFuture[Endpoints])
      extends Watcher[Endpoints] {
    override def eventReceived(action: Action, endpoints: Endpoints): Unit = {
      if ((action == Action.ADDED || action == Action.MODIFIED)
          && (endpoints != null)
          && (endpoints.getSubsets != null)
          && endpoints.getSubsets.asScala.nonEmpty
          && endpoints.getSubsets.asScala.exists(_.getAddresses.asScala.nonEmpty)
          && !resolvedDriverEndpoints.isDone) {
        resolvedDriverEndpoints.set(endpoints)
      }
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      logDebug("Driver endpoints readiness watch closed.", cause)
    }
  }

  private class DriverServiceReadyWatcher(resolvedDriverService: SettableFuture[Service])
      extends Watcher[Service] {
    override def eventReceived(action: Action, service: Service): Unit = {
      if ((action == Action.ADDED || action == Action.MODIFIED)
          && !resolvedDriverService.isDone) {
        resolvedDriverService.set(service)
      }
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      logDebug("Driver service readiness watch closed.", cause)
    }
  }

  private def buildSubmitFailedErrorMessage(
      kubernetesClient: KubernetesClient,
      e: Throwable): String = {
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

  private def buildSubmissionRequest(
      submitterLocalFiles: Iterable[String],
      submitterLocalJars: Iterable[String],
      driverPodKubernetesCredentials: KubernetesCredentials): KubernetesCreateSubmissionRequest = {
    val mainResourceUri = Utils.resolveURI(mainAppResource)
    val resolvedAppResource: AppResource = Option(mainResourceUri.getScheme)
        .getOrElse("file") match {
      case "file" =>
        val appFile = new File(mainResourceUri.getPath)
        val fileBytes = Files.toByteArray(appFile)
        val fileBase64 = Base64.encodeBase64String(fileBytes)
        UploadedAppResource(resourceBase64Contents = fileBase64, name = appFile.getName)
      case "local" => ContainerAppResource(mainAppResource)
      case other => RemoteAppResource(other)
    }
    val uploadFilesBase64Contents = CompressionUtils.createTarGzip(submitterLocalFiles.map(
      Utils.resolveURI(_).getPath))
    val uploadJarsBase64Contents = CompressionUtils.createTarGzip(submitterLocalJars.map(
      Utils.resolveURI(_).getPath))
    KubernetesCreateSubmissionRequest(
      appResource = resolvedAppResource,
      mainClass = mainClass,
      appArgs = appArgs,
      secret = secretBase64String,
      sparkProperties = sparkConf.getAll.toMap,
      uploadedJarsBase64Contents = uploadJarsBase64Contents,
      uploadedFilesBase64Contents = uploadFilesBase64Contents,
      driverPodKubernetesCredentials = driverPodKubernetesCredentials)
  }

  private def buildDriverSubmissionClient(
      kubernetesClient: KubernetesClient,
      driverServiceManager: DriverServiceManager,
      service: Service,
      sslConfiguration: DriverSubmitSslConfiguration): KubernetesSparkRestApi = {
    val serviceUris = driverServiceManager.getDriverServiceSubmissionServerUris(service)
    require(serviceUris.nonEmpty, "No uris found to contact the driver!")
    HttpClientUtil.createClient[KubernetesSparkRestApi](
      uris = serviceUris,
      maxRetriesPerServer = 10,
      sslSocketFactory = sslConfiguration
        .driverSubmitClientSslContext
        .getSocketFactory,
      trustContext = sslConfiguration
        .driverSubmitClientTrustManager
        .orNull,
      connectTimeoutMillis = 5000)
  }

  private def parseKeyValuePairs(
      maybeKeyValues: Option[String],
      configKey: String,
      keyValueType: String): Map[String, String] = {
    maybeKeyValues.map(keyValues => {
      keyValues.split(",").map(_.trim).filterNot(_.isEmpty).map(keyValue => {
        keyValue.split("=", 2).toSeq match {
          case Seq(k, v) =>
            (k, v)
          case _ =>
            throw new SparkException(s"Custom $keyValueType set by $configKey must be a" +
              s" comma-separated list of key-value pairs, with format <key>=<value>." +
              s" Got value: $keyValue. All values: $keyValues")
        }
      }).toMap
    }).getOrElse(Map.empty[String, String])
  }

  private def getDriverServiceManager: DriverServiceManager = {
    val driverServiceManagerLoader = ServiceLoader.load(classOf[DriverServiceManager])
    val matchingServiceManagers = driverServiceManagerLoader
      .iterator()
      .asScala
      .filter(_.getServiceManagerType == driverServiceManagerType)
      .toList
    require(matchingServiceManagers.nonEmpty,
      s"No driver service manager found matching type $driverServiceManagerType")
    require(matchingServiceManagers.size == 1, "Multiple service managers found" +
      s" matching type $driverServiceManagerType, got: " +
      matchingServiceManagers.map(_.getClass).toList.mkString(","))
    matchingServiceManagers.head
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
}
