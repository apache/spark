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
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit}
import javax.net.ssl.{SSLContext, TrustManagerFactory, X509TrustManager}

import com.google.common.base.Charsets
import com.google.common.io.Files
import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{ConfigBuilder => K8SConfigBuilder, DefaultKubernetesClient, KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.commons.codec.binary.Base64
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SecurityManager, SparkConf, SparkException, SSLOptions}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
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

  private val waitForAppCompletion: Boolean = sparkConf.get(WAIT_FOR_APP_COMPLETION)

  private val secretBase64String = {
    val secretBytes = new Array[Byte](128)
    SECURE_RANDOM.nextBytes(secretBytes)
    Base64.encodeBase64String(secretBytes)
  }

  private val serviceAccount = sparkConf.get(KUBERNETES_SERVICE_ACCOUNT_NAME)
  private val customLabels = sparkConf.get(KUBERNETES_DRIVER_LABELS)

  def run(): Unit = {
    logInfo(s"Starting application $kubernetesAppId in Kubernetes...")

    Seq(uploadedFiles, uploadedJars, Some(mainAppResource)).foreach(checkForFilesExistence)

    val (driverSubmitSslOptions, isKeyStoreLocalFile) = parseDriverSubmitSslOptions()
    val parsedCustomLabels = parseCustomLabels(customLabels)
    var k8ConfBuilder = new K8SConfigBuilder()
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
      try {
        val (sslEnvs, sslVolumes, sslVolumeMounts, sslSecrets) = configureSsl(kubernetesClient,
          driverSubmitSslOptions,
          isKeyStoreLocalFile)
        try {
          // start outer watch for status logging of driver pod
          val driverPodCompletedLatch = new CountDownLatch(1)
          // only enable interval logging if in waitForAppCompletion mode
          val loggingInterval = if (waitForAppCompletion) sparkConf.get(REPORT_INTERVAL) else 0
          val loggingWatch = new LoggingPodStatusWatcher(driverPodCompletedLatch, kubernetesAppId,
            loggingInterval)
          Utils.tryWithResource(kubernetesClient
              .pods()
              .withName(kubernetesAppId)
              .watch(loggingWatch)) { _ =>
            val (driverPod, driverService) = launchDriverKubernetesComponents(
              kubernetesClient,
              parsedCustomLabels,
              submitServerSecret,
              driverSubmitSslOptions,
              sslSecrets,
              sslVolumes,
              sslVolumeMounts,
              sslEnvs,
              isKeyStoreLocalFile)
            val ownerReferenceConfiguredDriverService = try {
              configureOwnerReferences(
                kubernetesClient,
                submitServerSecret,
                sslSecrets,
                driverPod,
                driverService)
            } catch {
              case e: Throwable =>
                cleanupPodAndService(kubernetesClient, driverPod, driverService)
                throw new SparkException("Failed to set owner references to the driver pod.", e)
            }
            try {
              submitApplicationToDriverServer(kubernetesClient, driverSubmitSslOptions,
                ownerReferenceConfiguredDriverService)
              // wait if configured to do so
              if (waitForAppCompletion) {
                logInfo(s"Waiting for application $kubernetesAppId to finish...")
                driverPodCompletedLatch.await()
                logInfo(s"Application $kubernetesAppId finished.")
              } else {
                logInfo(s"Application $kubernetesAppId successfully launched.")
              }
            } catch {
              case e: Throwable =>
                cleanupPodAndService(kubernetesClient, driverPod,
                  ownerReferenceConfiguredDriverService)
                throw new SparkException("Failed to submit the application to the driver pod.", e)
            }
          }
        } finally {
          Utils.tryLogNonFatalError {
            // Secrets may have been mutated so delete by name to avoid problems with not having
            // the latest version.
            sslSecrets.foreach { secret =>
              kubernetesClient.secrets().withName(secret.getMetadata.getName).delete()
            }
          }
        }
      } finally {
        Utils.tryLogNonFatalError {
          kubernetesClient.secrets().withName(submitServerSecret.getMetadata.getName).delete()
        }
      }
    }
  }

  private def cleanupPodAndService(
      kubernetesClient: KubernetesClient,
      driverPod: Pod,
      driverService: Service): Unit = {
    Utils.tryLogNonFatalError {
      kubernetesClient.services().delete(driverService)
    }
    Utils.tryLogNonFatalError {
      kubernetesClient.pods().delete(driverPod)
    }
  }

  private def submitApplicationToDriverServer(
      kubernetesClient: KubernetesClient,
      driverSubmitSslOptions: SSLOptions,
      driverService: Service) = {
    sparkConf.getOption("spark.app.id").foreach { id =>
      logWarning(s"Warning: Provided app id in spark.app.id as $id will be" +
        s" overridden as $kubernetesAppId")
    }
    sparkConf.set(KUBERNETES_DRIVER_POD_NAME, kubernetesAppId)
    sparkConf.set(KUBERNETES_DRIVER_SERVICE_NAME, driverService.getMetadata.getName)
    sparkConf.set("spark.app.id", kubernetesAppId)
    sparkConf.setIfMissing("spark.app.name", appName)
    sparkConf.setIfMissing("spark.driver.port", DEFAULT_DRIVER_PORT.toString)
    sparkConf.setIfMissing("spark.blockmanager.port",
      DEFAULT_BLOCKMANAGER_PORT.toString)
    val driverSubmitter = buildDriverSubmissionClient(kubernetesClient, driverService,
      driverSubmitSslOptions)
    // Sanity check to see if the driver submitter is even reachable.
    driverSubmitter.ping()
    logInfo(s"Submitting local resources to driver pod for application " +
      s"$kubernetesAppId ...")
    val submitRequest = buildSubmissionRequest()
    driverSubmitter.submitApplication(submitRequest)
    logInfo("Successfully submitted local resources and driver configuration to" +
      " driver pod.")
    // After submitting, adjust the service to only expose the Spark UI
    val uiServicePort = new ServicePortBuilder()
      .withName(UI_PORT_NAME)
      .withPort(uiPort)
      .withNewTargetPort(uiPort)
      .build()
    kubernetesClient.services().withName(kubernetesAppId).edit().editSpec()
      .withType("ClusterIP")
      .withPorts(uiServicePort)
      .endSpec()
      .done()
    logInfo("Finished submitting application to Kubernetes.")
  }

  private def launchDriverKubernetesComponents(
      kubernetesClient: KubernetesClient,
      parsedCustomLabels: Map[String, String],
      submitServerSecret: Secret,
      driverSubmitSslOptions: SSLOptions,
      sslSecrets: Array[Secret],
      sslVolumes: Array[Volume],
      sslVolumeMounts: Array[VolumeMount],
      sslEnvs: Array[EnvVar],
      isKeyStoreLocalFile: Boolean): (Pod, Service) = {
    val endpointsReadyFuture = SettableFuture.create[Endpoints]
    val endpointsReadyWatcher = new DriverEndpointsReadyWatcher(endpointsReadyFuture)
    val serviceReadyFuture = SettableFuture.create[Service]
    val driverKubernetesSelectors = (Map(
      SPARK_DRIVER_LABEL -> kubernetesAppId,
      SPARK_APP_ID_LABEL -> kubernetesAppId,
      SPARK_APP_NAME_LABEL -> appName)
      ++ parsedCustomLabels).asJava
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
          val driverService = createDriverService(
            kubernetesClient,
            driverKubernetesSelectors,
            submitServerSecret)
          val driverPod = try {
            createDriverPod(
              kubernetesClient,
              driverKubernetesSelectors,
              submitServerSecret,
              driverSubmitSslOptions,
              sslVolumes,
              sslVolumeMounts,
              sslEnvs)
          } catch {
            case e: Throwable =>
              Utils.tryLogNonFatalError {
                kubernetesClient.services().delete(driverService)
              }
              throw new SparkException("Failed to create the driver pod.", e)
          }
          try {
            waitForReadyKubernetesComponents(kubernetesClient, endpointsReadyFuture,
              serviceReadyFuture, podReadyFuture)
            (driverPod, driverService)
          } catch {
            case e: Throwable =>
              Utils.tryLogNonFatalError {
                kubernetesClient.services().delete(driverService)
              }
              Utils.tryLogNonFatalError {
                kubernetesClient.pods().delete(driverPod)
              }
              throw new SparkException("Timed out while waiting for a Kubernetes component to be" +
                " ready.", e)
          }
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
      sslSecrets: Array[Secret],
      driverPod: Pod,
      driverService: Service): Service = {
    val driverPodOwnerRef = new OwnerReferenceBuilder()
      .withName(driverPod.getMetadata.getName)
      .withUid(driverPod.getMetadata.getUid)
      .withApiVersion(driverPod.getApiVersion)
      .withKind(driverPod.getKind)
      .withController(true)
      .build()
    sslSecrets.foreach(secret => {
      kubernetesClient.secrets().withName(secret.getMetadata.getName).edit()
        .editMetadata()
        .addToOwnerReferences(driverPodOwnerRef)
        .endMetadata()
        .done()
    })
    kubernetesClient.secrets().withName(submitServerSecret.getMetadata.getName).edit()
      .editMetadata()
      .addToOwnerReferences(driverPodOwnerRef)
      .endMetadata()
      .done()
    kubernetesClient.services().withName(driverService.getMetadata.getName).edit()
      .editMetadata()
      .addToOwnerReferences(driverPodOwnerRef)
      .endMetadata()
      .done()
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

  private def createDriverService(
      kubernetesClient: KubernetesClient,
      driverKubernetesSelectors: java.util.Map[String, String],
      submitServerSecret: Secret): Service = {
    val driverSubmissionServicePort = new ServicePortBuilder()
      .withName(SUBMISSION_SERVER_PORT_NAME)
      .withPort(SUBMISSION_SERVER_PORT)
      .withNewTargetPort(SUBMISSION_SERVER_PORT)
      .build()
    kubernetesClient.services().createNew()
      .withNewMetadata()
        .withName(kubernetesAppId)
        .withLabels(driverKubernetesSelectors)
        .endMetadata()
      .withNewSpec()
        .withType("NodePort")
        .withSelector(driverKubernetesSelectors)
        .withPorts(driverSubmissionServicePort)
        .endSpec()
      .done()
  }

  private def createDriverPod(
      kubernetesClient: KubernetesClient,
      driverKubernetesSelectors: util.Map[String, String],
      submitServerSecret: Secret,
      driverSubmitSslOptions: SSLOptions,
      sslVolumes: Array[Volume],
      sslVolumeMounts: Array[VolumeMount],
      sslEnvs: Array[EnvVar]) = {
    val containerPorts = buildContainerPorts()
    val probePingHttpGet = new HTTPGetActionBuilder()
      .withScheme(if (driverSubmitSslOptions.enabled) "HTTPS" else "HTTP")
      .withPath("/v1/submissions/ping")
      .withNewPort(SUBMISSION_SERVER_PORT_NAME)
      .build()
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
          .endVolume()
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
          .withNewReadinessProbe().withHttpGet(probePingHttpGet).endReadinessProbe()
          .endContainer()
        .endSpec()
      .done()
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
      if ((action == Action.ADDED) || (action == Action.MODIFIED)
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
      if ((action == Action.ADDED) || (action == Action.MODIFIED)
          && !resolvedDriverService.isDone) {
        resolvedDriverService.set(service)
      }
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      logDebug("Driver service readiness watch closed.", cause)
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
      val sslVolume = new VolumeBuilder()
        .withName(SUBMISSION_SSL_SECRETS_VOLUME_NAME)
        .withNewSecret()
          .withSecretName(sslSecretsName)
          .endSecret()
        .build()
      val sslVolumeMount = new VolumeMountBuilder()
        .withName(SUBMISSION_SSL_SECRETS_VOLUME_NAME)
        .withReadOnly(true)
        .withMountPath(sslSecretsDirectory)
        .build()
      val sslSecrets = kubernetesClient.secrets().createNew()
        .withNewMetadata()
        .withName(sslSecretsName)
        .endMetadata()
        .withData(sslSecretsMap.asJava)
        .withType("Opaque")
        .done()
      secrets += sslSecrets
      (sslEnvs.toArray, Array(sslVolume), Array(sslVolumeMount), secrets.toArray)
    } else {
      (Array[EnvVar](), Array[Volume](), Array[VolumeMount](), Array[Secret]())
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
      .filterNot(node => node.getSpec.getUnschedulable != null &&
        node.getSpec.getUnschedulable)
      .flatMap(_.getStatus.getAddresses.asScala)
      // The list contains hostnames, internal and external IP addresses.
      // we want only external IP addresses in our list
      // (https://kubernetes.io/docs/admin/node/#addresses)
      .filter(_.getType == "ExternalIP")
      .map(address => {
        s"$urlScheme://${address.getAddress}:$servicePort"
      }).toSet
    require(nodeUrls.nonEmpty, "No nodes found to contact the driver!")
    val (trustManager, sslContext): (X509TrustManager, SSLContext) =
      if (driverSubmitSslOptions.enabled) {
        buildSslConnectionConfiguration(driverSubmitSslOptions)
      } else {
        (null, SSLContext.getDefault)
      }
    HttpClientUtil.createClient[KubernetesSparkRestApi](
      uris = nodeUrls,
      maxRetriesPerServer = 3,
      sslSocketFactory = sslContext.getSocketFactory,
      trustContext = trustManager,
      connectTimeoutMillis = 5000)
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

  private def checkForFilesExistence(maybePaths: Option[String]): Unit = {
    maybePaths.foreach { paths =>
      paths.split(",").foreach { path =>
        val uri = Utils.resolveURI(path)
        uri.getScheme match {
          case "file" | null =>
            val file = new File(uri.getPath)
            if (!file.isFile) {
              throw new SparkException(s"""file "${uri}" does not exist!""")
            }
          case _ =>
        }
      }
    }
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
