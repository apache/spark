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
package org.apache.spark.deploy.kubernetes.submit.v2

import java.io.File
import java.util.Collections

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, HasMetadata, OwnerReferenceBuilder, PodBuilder}
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SecurityManager => SparkSecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.Utils

/**
 * Submission client for launching Spark applications on Kubernetes clusters.
 *
 * This class is responsible for instantiating Kubernetes resources that allow a Spark driver to
 * run in a pod on the Kubernetes cluster with the Spark configurations specified by spark-submit.
 * Application submitters that desire to provide their application's dependencies from their local
 * disk must provide a resource staging server URI to this client so that the client can push the
 * local resources to the resource staging server and have the driver pod pull the resources in an
 * init-container. Interactions with the resource staging server are offloaded to the
 * {@link MountedDependencyManager} class. If instead the application submitter has their
 * dependencies pre-staged in remote locations like HDFS or their own HTTP servers already, then
 * the mounted dependency manager is bypassed entirely, but the init-container still needs to
 * fetch these remote dependencies (TODO https://github.com/apache-spark-on-k8s/spark/issues/238).
 */
private[spark] class Client(
    mainClass: String,
    sparkConf: SparkConf,
    appArgs: Array[String],
    mainAppResource: String,
    kubernetesClientProvider: SubmissionKubernetesClientProvider,
    mountedDependencyManagerProvider: MountedDependencyManagerProvider) extends Logging {

  private val namespace = sparkConf.get(KUBERNETES_NAMESPACE)
  private val master = resolveK8sMaster(sparkConf.get("spark.master"))
  private val launchTime = System.currentTimeMillis
  private val appName = sparkConf.getOption("spark.app.name")
    .getOrElse("spark")
  private val kubernetesAppId = s"$appName-$launchTime".toLowerCase.replaceAll("\\.", "-")
  private val kubernetesDriverPodName = sparkConf.get(KUBERNETES_DRIVER_POD_NAME)
    .getOrElse(kubernetesAppId)
  private val driverDockerImage = sparkConf.get(DRIVER_DOCKER_IMAGE)
  private val maybeStagingServerUri = sparkConf.get(RESOURCE_STAGING_SERVER_URI)
  private val driverMemoryMb = sparkConf.get(org.apache.spark.internal.config.DRIVER_MEMORY)
  private val memoryOverheadMb = sparkConf
    .get(KUBERNETES_DRIVER_MEMORY_OVERHEAD)
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * driverMemoryMb).toInt,
      MEMORY_OVERHEAD_MIN))
  private val driverContainerMemoryWithOverhead = driverMemoryMb + memoryOverheadMb
  private val customLabels = sparkConf.get(KUBERNETES_DRIVER_LABELS)
  private val customAnnotations = sparkConf.get(KUBERNETES_DRIVER_ANNOTATIONS)
  private val sparkJars = sparkConf.getOption("spark.jars")
    .map(_.split(","))
    .getOrElse(Array.empty[String]) ++
    Option(mainAppResource)
      .filterNot(_ == SparkLauncher.NO_RESOURCE)
      .toSeq

  private val sparkFiles = sparkConf.getOption("spark.files")
    .map(_.split(","))
    .getOrElse(Array.empty[String])
  private val driverExtraClasspath = sparkConf.get(
    org.apache.spark.internal.config.DRIVER_CLASS_PATH)
  private val driverJavaOptions = sparkConf.get(
    org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS)

  def run(): Unit = {
    val parsedCustomLabels = parseKeyValuePairs(customLabels, KUBERNETES_DRIVER_LABELS.key,
      "labels")
    require(!parsedCustomLabels.contains(SPARK_APP_ID_LABEL), s"Label with key " +
      s" $SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping operations.")
    require(!parsedCustomLabels.contains(SPARK_APP_NAME_LABEL), s"Label with key" +
      s" $SPARK_APP_NAME_LABEL is not allowed as it is reserved for Spark bookkeeping operations.")
    val allLabels = parsedCustomLabels ++
      Map(SPARK_APP_ID_LABEL -> kubernetesAppId, SPARK_APP_NAME_LABEL -> appName)
    val parsedCustomAnnotations = parseKeyValuePairs(
      customAnnotations,
      KUBERNETES_DRIVER_ANNOTATIONS.key,
      "annotations")
    Utils.tryWithResource(kubernetesClientProvider.get) { kubernetesClient =>
      val driverExtraClasspathEnv = driverExtraClasspath.map { classPath =>
        new EnvVarBuilder()
          .withName(ENV_SUBMIT_EXTRA_CLASSPATH)
          .withValue(classPath)
          .build()
      }
      val driverContainer = new ContainerBuilder()
        .withName(DRIVER_CONTAINER_NAME)
        .withImage(driverDockerImage)
        .withImagePullPolicy("IfNotPresent")
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
        .build()
      val basePod = new PodBuilder()
        .withNewMetadata()
          .withName(kubernetesDriverPodName)
          .addToLabels(allLabels.asJava)
          .addToAnnotations(parsedCustomAnnotations.asJava)
          .endMetadata()
        .withNewSpec()
          .addToContainers(driverContainer)
          .endSpec()

      val nonDriverPodKubernetesResources = mutable.Buffer[HasMetadata]()
      val resolvedJars = mutable.Buffer[String]()
      val resolvedFiles = mutable.Buffer[String]()
      val driverPodWithMountedDeps = maybeStagingServerUri.map { stagingServerUri =>
        val mountedDependencyManager = mountedDependencyManagerProvider.getMountedDependencyManager(
          kubernetesAppId,
          stagingServerUri,
          allLabels,
          namespace,
          sparkJars,
          sparkFiles)
        val jarsResourceIdentifier = mountedDependencyManager.uploadJars()
        val filesResourceIdentifier = mountedDependencyManager.uploadFiles()
        val initContainerKubernetesSecret = mountedDependencyManager.buildInitContainerSecret(
          jarsResourceIdentifier.resourceSecret, filesResourceIdentifier.resourceSecret)
        val initContainerConfigMap = mountedDependencyManager.buildInitContainerConfigMap(
          jarsResourceIdentifier.resourceId, filesResourceIdentifier.resourceId)
        resolvedJars ++= mountedDependencyManager.resolveSparkJars()
        resolvedFiles ++= mountedDependencyManager.resolveSparkFiles()
        nonDriverPodKubernetesResources += initContainerKubernetesSecret
        nonDriverPodKubernetesResources += initContainerConfigMap
        mountedDependencyManager.configurePodToMountLocalDependencies(
          driverContainer.getName, initContainerKubernetesSecret, initContainerConfigMap, basePod)
      }.getOrElse {
        sparkJars.map(Utils.resolveURI).foreach { jar =>
          require(Option.apply(jar.getScheme).getOrElse("file") != "file",
            "When submitting with local jars, a resource staging server must be provided to" +
              s" deploy your jars into the driver pod. Cannot send jar with URI $jar.")
        }
        sparkFiles.map(Utils.resolveURI).foreach { file =>
          require(Option.apply(file.getScheme).getOrElse("file") != "file",
            "When submitting with local files, a resource staging server must be provided to" +
              s" deploy your files into the driver pod. Cannot send file with URI $file")
        }
        resolvedJars ++= sparkJars
        resolvedFiles ++= sparkFiles
        basePod
      }
      val resolvedSparkConf = sparkConf.clone()
      if (resolvedJars.nonEmpty) {
        resolvedSparkConf.set("spark.jars", resolvedJars.mkString(","))
      }
      if (resolvedFiles.nonEmpty) {
        resolvedSparkConf.set("spark.files", resolvedFiles.mkString(","))
      }
      resolvedSparkConf.setIfMissing(KUBERNETES_DRIVER_POD_NAME, kubernetesDriverPodName)
      resolvedSparkConf.set("spark.app.id", kubernetesAppId)
      // We don't need this anymore since we just set the JVM options on the environment
      resolvedSparkConf.remove(org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS)
      resolvedSparkConf.get(KUBERNETES_SUBMIT_OAUTH_TOKEN).foreach { _ =>
        resolvedSparkConf.set(KUBERNETES_SUBMIT_OAUTH_TOKEN.key, "<present_but_redacted>")
      }
      resolvedSparkConf.get(KUBERNETES_DRIVER_OAUTH_TOKEN).foreach { _ =>
        resolvedSparkConf.set(KUBERNETES_DRIVER_OAUTH_TOKEN.key, "<present_but_redacted>")
      }

      val mountedClassPath = resolvedJars.map(Utils.resolveURI).filter { jarUri =>
        val scheme = Option.apply(jarUri.getScheme).getOrElse("file")
        scheme == "local" || scheme == "file"
      }.map(_.getPath).mkString(File.pathSeparator)
      val resolvedDriverJavaOpts = resolvedSparkConf.getAll.map { case (confKey, confValue) =>
          s"-D$confKey=$confValue"
      }.mkString(" ") + driverJavaOptions.map(" " + _).getOrElse("")
      val resolvedDriverPod = driverPodWithMountedDeps.editSpec()
        .editMatchingContainer(new ContainerNameEqualityPredicate(driverContainer.getName))
          .addNewEnv()
            .withName(ENV_MOUNTED_CLASSPATH)
            .withValue(mountedClassPath)
            .endEnv()
          .addNewEnv()
            .withName(ENV_DRIVER_JAVA_OPTS)
            .withValue(resolvedDriverJavaOpts)
            .endEnv()
          .endContainer()
        .endSpec()
        .build()
      val createdDriverPod = kubernetesClient.pods().create(resolvedDriverPod)
      try {
        val driverPodOwnerReference = new OwnerReferenceBuilder()
          .withName(createdDriverPod.getMetadata.getName)
          .withApiVersion(createdDriverPod.getApiVersion)
          .withUid(createdDriverPod.getMetadata.getUid)
          .withKind(createdDriverPod.getKind)
          .withController(true)
          .build()
        nonDriverPodKubernetesResources.foreach { resource =>
          val originalMetadata = resource.getMetadata
          originalMetadata.setOwnerReferences(Collections.singletonList(driverPodOwnerReference))
        }
        kubernetesClient.resourceList(nonDriverPodKubernetesResources: _*).createOrReplace()
      } catch {
        case e: Throwable =>
          kubernetesClient.pods().delete(createdDriverPod)
          throw e
      }
    }
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
}
