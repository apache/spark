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
package org.apache.spark.deploy.kubernetes.submit

import java.io.File
import java.util.{Collections, UUID}

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, OwnerReferenceBuilder, PodBuilder, QuantityBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.{ConfigurationUtils, SparkKubernetesClientFactory}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.rest.kubernetes.ResourceStagingServerSslOptionsProviderImpl
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.Utils

/**
 * Submission client for launching Spark applications on Kubernetes clusters.
 *
 * This class is responsible for instantiating Kubernetes resources that allow a Spark driver to
 * run in a pod on the Kubernetes cluster with the Spark configurations specified by spark-submit.
 * The API of this class makes it such that much of the specific behavior can be stubbed for
 * testing; most of the detailed logic must be dependency-injected when constructing an instance
 * of this client. Therefore the submission process is designed to be as modular as possible,
 * where different steps of submission should be factored out into separate classes.
 */
private[spark] class Client(
    appName: String,
    kubernetesResourceNamePrefix: String,
    kubernetesAppId: String,
    mainClass: String,
    sparkConf: SparkConf,
    appArgs: Array[String],
    sparkJars: Seq[String],
    sparkFiles: Seq[String],
    waitForAppCompletion: Boolean,
    kubernetesClient: KubernetesClient,
    initContainerComponentsProvider: DriverInitContainerComponentsProvider,
    kubernetesCredentialsMounterProvider: DriverPodKubernetesCredentialsMounterProvider,
    loggingPodStatusWatcher: LoggingPodStatusWatcher) extends Logging {
  private val kubernetesDriverPodName = sparkConf.get(KUBERNETES_DRIVER_POD_NAME)
    .getOrElse(s"$kubernetesResourceNamePrefix-driver")
  private val driverDockerImage = sparkConf.get(DRIVER_DOCKER_IMAGE)
  private val dockerImagePullPolicy = sparkConf.get(DOCKER_IMAGE_PULL_POLICY)

  // CPU settings
  private val driverCpuCores = sparkConf.getOption("spark.driver.cores").getOrElse("1")

  // Memory settings
  private val driverMemoryMb = sparkConf.get(org.apache.spark.internal.config.DRIVER_MEMORY)
  private val memoryOverheadMb = sparkConf
    .get(KUBERNETES_DRIVER_MEMORY_OVERHEAD)
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * driverMemoryMb).toInt,
      MEMORY_OVERHEAD_MIN))
  private val driverContainerMemoryWithOverhead = driverMemoryMb + memoryOverheadMb
  private val customLabels = sparkConf.get(KUBERNETES_DRIVER_LABELS)
  private val customAnnotations = sparkConf.get(KUBERNETES_DRIVER_ANNOTATIONS)

  private val driverExtraClasspath = sparkConf.get(
    org.apache.spark.internal.config.DRIVER_CLASS_PATH)
  private val driverJavaOptions = sparkConf.get(
    org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS)

  def run(): Unit = {
    validateNoDuplicateFileNames(sparkJars)
    validateNoDuplicateFileNames(sparkFiles)

    val driverCustomLabels = ConfigurationUtils.combinePrefixedKeyValuePairsWithDeprecatedConf(
      sparkConf,
      KUBERNETES_DRIVER_LABEL_PREFIX,
      KUBERNETES_DRIVER_LABELS,
      "label")
    require(!driverCustomLabels.contains(SPARK_APP_ID_LABEL), s"Label with key " +
        s" $SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping" +
        s" operations.")

    val driverCustomAnnotations = ConfigurationUtils.combinePrefixedKeyValuePairsWithDeprecatedConf(
      sparkConf,
      KUBERNETES_DRIVER_ANNOTATION_PREFIX,
      KUBERNETES_DRIVER_ANNOTATIONS,
      "annotation")
    require(!driverCustomAnnotations.contains(SPARK_APP_NAME_ANNOTATION),
        s"Annotation with key $SPARK_APP_NAME_ANNOTATION is not allowed as it is reserved for" +
        s" Spark bookkeeping operations.")
    val allDriverLabels = driverCustomLabels ++ Map(
        SPARK_APP_ID_LABEL -> kubernetesAppId,
        SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)

    val driverExtraClasspathEnv = driverExtraClasspath.map { classPath =>
      new EnvVarBuilder()
        .withName(ENV_SUBMIT_EXTRA_CLASSPATH)
        .withValue(classPath)
        .build()
    }
    val driverCpuQuantity = new QuantityBuilder(false)
      .withAmount(driverCpuCores)
      .build()
    val driverMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${driverMemoryMb}M")
      .build()
    val driverMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(s"${driverContainerMemoryWithOverhead}M")
      .build()
    val driverContainer = new ContainerBuilder()
      .withName(DRIVER_CONTAINER_NAME)
      .withImage(driverDockerImage)
      .withImagePullPolicy(dockerImagePullPolicy)
      .addToEnv(driverExtraClasspathEnv.toSeq: _*)
      .addNewEnv()
        .withName(ENV_DRIVER_MEMORY)
        .withValue(driverContainerMemoryWithOverhead + "m")
        .endEnv()
      .addNewEnv()
        .withName(ENV_DRIVER_MAIN_CLASS)
        .withValue(mainClass)
        .endEnv()
      .addNewEnv()
        .withName(ENV_DRIVER_ARGS)
        .withValue(appArgs.mkString(" "))
        .endEnv()
      .withNewResources()
        .addToRequests("cpu", driverCpuQuantity)
        .addToLimits("cpu", driverCpuQuantity)
        .addToRequests("memory", driverMemoryQuantity)
        .addToLimits("memory", driverMemoryLimitQuantity)
        .endResources()
      .build()
    val basePod = new PodBuilder()
      .withNewMetadata()
        .withName(kubernetesDriverPodName)
        .addToLabels(allDriverLabels.asJava)
        .addToAnnotations(driverCustomAnnotations.toMap.asJava)
        .addToAnnotations(SPARK_APP_NAME_ANNOTATION, appName)
        .endMetadata()
      .withNewSpec()
        .withRestartPolicy("Never")
        .addToContainers(driverContainer)
        .endSpec()

    val maybeSubmittedDependencyUploader = initContainerComponentsProvider
        .provideInitContainerSubmittedDependencyUploader(allDriverLabels)
    val maybeSubmittedResourceIdentifiers = maybeSubmittedDependencyUploader.map { uploader =>
      SubmittedResources(uploader.uploadJars(), uploader.uploadFiles())
    }
    val maybeSecretBuilder = initContainerComponentsProvider
        .provideSubmittedDependenciesSecretBuilder(
            maybeSubmittedResourceIdentifiers.map(_.secrets()))
    val maybeSubmittedDependenciesSecret = maybeSecretBuilder.map(_.build())
    val initContainerConfigMap = initContainerComponentsProvider
        .provideInitContainerConfigMapBuilder(maybeSubmittedResourceIdentifiers.map(_.ids()))
        .build()
    val podWithInitContainer = initContainerComponentsProvider
        .provideInitContainerBootstrap()
        .bootstrapInitContainerAndVolumes(driverContainer.getName, basePod)

    val containerLocalizedFilesResolver = initContainerComponentsProvider
        .provideContainerLocalizedFilesResolver()
    val resolvedSparkJars = containerLocalizedFilesResolver.resolveSubmittedSparkJars()
    val resolvedSparkFiles = containerLocalizedFilesResolver.resolveSubmittedSparkFiles()

    val executorInitContainerConfiguration = initContainerComponentsProvider
        .provideExecutorInitContainerConfiguration()
    val sparkConfWithExecutorInit = executorInitContainerConfiguration
        .configureSparkConfForExecutorInitContainer(sparkConf)
    val credentialsMounter = kubernetesCredentialsMounterProvider
        .getDriverPodKubernetesCredentialsMounter()
    val credentialsSecret = credentialsMounter.createCredentialsSecret()
    val podWithInitContainerAndMountedCreds = credentialsMounter.mountDriverKubernetesCredentials(
      podWithInitContainer, driverContainer.getName, credentialsSecret)
    val resolvedSparkConf = credentialsMounter.setDriverPodKubernetesCredentialLocations(
        sparkConfWithExecutorInit)
    if (resolvedSparkJars.nonEmpty) {
      resolvedSparkConf.set("spark.jars", resolvedSparkJars.mkString(","))
    }
    if (resolvedSparkFiles.nonEmpty) {
      resolvedSparkConf.set("spark.files", resolvedSparkFiles.mkString(","))
    }
    resolvedSparkConf.setIfMissing(KUBERNETES_DRIVER_POD_NAME, kubernetesDriverPodName)
    resolvedSparkConf.set("spark.app.id", kubernetesAppId)
    resolvedSparkConf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, kubernetesResourceNamePrefix)
    // We don't need this anymore since we just set the JVM options on the environment
    resolvedSparkConf.remove(org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS)
    val resolvedLocalClasspath = containerLocalizedFilesResolver
      .resolveSubmittedAndRemoteSparkJars()
    val resolvedDriverJavaOpts = resolvedSparkConf.getAll.map {
      case (confKey, confValue) => s"-D$confKey=$confValue"
    }.mkString(" ") + driverJavaOptions.map(" " + _).getOrElse("")
    val resolvedDriverPod = podWithInitContainerAndMountedCreds.editSpec()
      .editMatchingContainer(new ContainerNameEqualityPredicate(driverContainer.getName))
        .addNewEnv()
          .withName(ENV_MOUNTED_CLASSPATH)
          .withValue(resolvedLocalClasspath.mkString(File.pathSeparator))
          .endEnv()
        .addNewEnv()
          .withName(ENV_DRIVER_JAVA_OPTS)
          .withValue(resolvedDriverJavaOpts)
          .endEnv()
        .endContainer()
      .endSpec()
      .build()
    Utils.tryWithResource(
        kubernetesClient
            .pods()
            .withName(resolvedDriverPod.getMetadata.getName)
            .watch(loggingPodStatusWatcher)) { _ =>
      val createdDriverPod = kubernetesClient.pods().create(resolvedDriverPod)
      try {
        val driverOwnedResources = Seq(initContainerConfigMap) ++
          maybeSubmittedDependenciesSecret.toSeq ++
          credentialsSecret.toSeq
        val driverPodOwnerReference = new OwnerReferenceBuilder()
          .withName(createdDriverPod.getMetadata.getName)
          .withApiVersion(createdDriverPod.getApiVersion)
          .withUid(createdDriverPod.getMetadata.getUid)
          .withKind(createdDriverPod.getKind)
          .withController(true)
          .build()
        driverOwnedResources.foreach { resource =>
          val originalMetadata = resource.getMetadata
          originalMetadata.setOwnerReferences(Collections.singletonList(driverPodOwnerReference))
        }
        kubernetesClient.resourceList(driverOwnedResources: _*).createOrReplace()
      } catch {
        case e: Throwable =>
          kubernetesClient.pods().delete(createdDriverPod)
          throw e
      }
      if (waitForAppCompletion) {
        logInfo(s"Waiting for application $appName to finish...")
        loggingPodStatusWatcher.awaitCompletion()
        logInfo(s"Application $appName finished.")
      } else {
        logInfo(s"Deployed Spark application $appName into Kubernetes.")
      }
    }
  }

  private def validateNoDuplicateFileNames(allFiles: Seq[String]): Unit = {
    val fileNamesToUris = allFiles.map { file =>
      (new File(Utils.resolveURI(file).getPath).getName, file)
    }
    fileNamesToUris.groupBy(_._1).foreach {
      case (fileName, urisWithFileName) =>
        require(urisWithFileName.size == 1, "Cannot add multiple files with the same name, but" +
          s" file name $fileName is shared by all of these URIs: $urisWithFileName")
    }
  }
}

private[spark] object Client {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf(true)
    val mainAppResource = args(0)
    val mainClass = args(1)
    val appArgs = args.drop(2)
    run(sparkConf, mainAppResource, mainClass, appArgs)
  }

  def run(
      sparkConf: SparkConf,
      mainAppResource: String,
      mainClass: String,
      appArgs: Array[String]): Unit = {
    val sparkJars = sparkConf.getOption("spark.jars")
      .map(_.split(","))
      .getOrElse(Array.empty[String]) ++
      Option(mainAppResource)
        .filterNot(_ == SparkLauncher.NO_RESOURCE)
        .toSeq
    val launchTime = System.currentTimeMillis
    val sparkFiles = sparkConf.getOption("spark.files")
      .map(_.split(","))
      .getOrElse(Array.empty[String])
    val appName = sparkConf.getOption("spark.app.name").getOrElse("spark")
    // The resource name prefix is derived from the application name, making it easy to connect the
    // names of the Kubernetes resources from e.g. Kubectl or the Kubernetes dashboard to the
    // application the user submitted. However, we can't use the application name in the label, as
    // label values are considerably restrictive, e.g. must be no longer than 63 characters in
    // length. So we generate a separate identifier for the app ID itself, and bookkeeping that
    // requires finding "all pods for this application" should use the kubernetesAppId.
    val kubernetesResourceNamePrefix = s"$appName-$launchTime".toLowerCase.replaceAll("\\.", "-")
    val kubernetesAppId = s"spark-${UUID.randomUUID().toString.replaceAll("-", "")}"
    val namespace = sparkConf.get(KUBERNETES_NAMESPACE)
    val master = resolveK8sMaster(sparkConf.get("spark.master"))
    val sslOptionsProvider = new ResourceStagingServerSslOptionsProviderImpl(sparkConf)
    val initContainerComponentsProvider = new DriverInitContainerComponentsProviderImpl(
        sparkConf,
        kubernetesResourceNamePrefix,
        namespace,
        sparkJars,
        sparkFiles,
        sslOptionsProvider.getSslOptions)
    Utils.tryWithResource(SparkKubernetesClientFactory.createKubernetesClient(
        master,
        Some(namespace),
        APISERVER_AUTH_SUBMISSION_CONF_PREFIX,
        sparkConf,
        None,
        None)) { kubernetesClient =>
      val kubernetesCredentialsMounterProvider =
          new DriverPodKubernetesCredentialsMounterProviderImpl(
              sparkConf, kubernetesResourceNamePrefix)
      val waitForAppCompletion = sparkConf.get(WAIT_FOR_APP_COMPLETION)
      val loggingInterval = Option(sparkConf.get(REPORT_INTERVAL))
          .filter( _ => waitForAppCompletion)
      val loggingPodStatusWatcher = new LoggingPodStatusWatcherImpl(
          kubernetesResourceNamePrefix, loggingInterval)
      new Client(
          appName,
          kubernetesResourceNamePrefix,
          kubernetesAppId,
          mainClass,
          sparkConf,
          appArgs,
          sparkJars,
          sparkFiles,
          waitForAppCompletion,
          kubernetesClient,
          initContainerComponentsProvider,
          kubernetesCredentialsMounterProvider,
          loggingPodStatusWatcher).run()
    }
  }
}
