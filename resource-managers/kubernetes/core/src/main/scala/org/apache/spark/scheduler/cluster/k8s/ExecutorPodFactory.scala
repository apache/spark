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
package org.apache.spark.scheduler.cluster.k8s

import io.fabric8.kubernetes.api.model.{ContainerBuilder, ContainerPortBuilder, EnvVar, EnvVarBuilder, EnvVarSourceBuilder, Pod, PodBuilder, QuantityBuilder, VolumeBuilder, VolumeMountBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.{ConfigurationUtils, InitContainerResourceStagingServerSecretPlugin, PodWithDetachedInitContainer, SparkPodInitContainerBootstrap}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.{InitContainerUtil, MountSecretsBootstrap, MountSmallFilesBootstrap}
import org.apache.spark.util.Utils

// Configures executor pods. Construct one of these with a SparkConf to set up properties that are
// common across all executors. Then, pass in dynamic parameters into createExecutorPod.
private[spark] trait ExecutorPodFactory {
  def createExecutorPod(
      executorId: String,
      applicationId: String,
      driverUrl: String,
      executorEnvs: Seq[(String, String)],
      driverPod: Pod,
      nodeToLocalTaskCount: Map[String, Int]): Pod
}

private[spark] class ExecutorPodFactoryImpl(
    sparkConf: SparkConf,
    nodeAffinityExecutorPodModifier: NodeAffinityExecutorPodModifier,
    mountSecretsBootstrap: Option[MountSecretsBootstrap],
    mountSmallFilesBootstrap: Option[MountSmallFilesBootstrap],
    executorInitContainerBootstrap: Option[SparkPodInitContainerBootstrap],
    executorMountInitContainerSecretPlugin: Option[InitContainerResourceStagingServerSecretPlugin],
    executorLocalDirVolumeProvider: ExecutorLocalDirVolumeProvider)
  extends ExecutorPodFactory {

  import ExecutorPodFactoryImpl._

  private val executorExtraClasspath = sparkConf.get(
      org.apache.spark.internal.config.EXECUTOR_CLASS_PATH)
  private val executorJarsDownloadDir = sparkConf.get(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION)

  private val executorLabels = ConfigurationUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_EXECUTOR_LABEL_PREFIX,
      "executor label")
  require(
      !executorLabels.contains(SPARK_APP_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_APP_ID_LABEL as it is reserved for Spark.")
  require(
      !executorLabels.contains(SPARK_EXECUTOR_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_EXECUTOR_ID_LABEL as it is reserved for" +
        s" Spark.")

  private val executorAnnotations =
      ConfigurationUtils.parsePrefixedKeyValuePairs(
          sparkConf,
          KUBERNETES_EXECUTOR_ANNOTATION_PREFIX,
          "executor annotation")
  private val nodeSelector =
      ConfigurationUtils.parsePrefixedKeyValuePairs(
          sparkConf,
          KUBERNETES_NODE_SELECTOR_PREFIX,
          "node selector")

  private val executorDockerImage = sparkConf.get(EXECUTOR_DOCKER_IMAGE)
  private val dockerImagePullPolicy = sparkConf.get(DOCKER_IMAGE_PULL_POLICY)
  private val executorPort = sparkConf.getInt("spark.executor.port", DEFAULT_STATIC_PORT)
  private val blockmanagerPort = sparkConf
      .getInt("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT)
  private val kubernetesDriverPodName = sparkConf
      .get(KUBERNETES_DRIVER_POD_NAME)
      .getOrElse(throw new SparkException("Must specify the driver pod name"))

  private val executorPodNamePrefix = sparkConf.get(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)

  private val executorMemoryMiB = sparkConf.get(org.apache.spark.internal.config.EXECUTOR_MEMORY)
  private val executorMemoryString = sparkConf.get(
      org.apache.spark.internal.config.EXECUTOR_MEMORY.key,
      org.apache.spark.internal.config.EXECUTOR_MEMORY.defaultValueString)

  private val memoryOverheadMiB = sparkConf
      .get(KUBERNETES_EXECUTOR_MEMORY_OVERHEAD)
      .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * executorMemoryMiB).toInt,
        MEMORY_OVERHEAD_MIN_MIB))
  private val executorMemoryWithOverhead = executorMemoryMiB + memoryOverheadMiB

  private val executorCores = sparkConf.getDouble("spark.executor.cores", 1d)
  private val executorLimitCores = sparkConf.getOption(KUBERNETES_EXECUTOR_LIMIT_CORES.key)

  override def createExecutorPod(
      executorId: String,
      applicationId: String,
      driverUrl: String,
      executorEnvs: Seq[(String, String)],
      driverPod: Pod,
      nodeToLocalTaskCount: Map[String, Int]): Pod = {
    val name = s"$executorPodNamePrefix-exec-$executorId"

    // hostname must be no longer than 63 characters, so take the last 63 characters of the pod
    // name as the hostname.  This preserves uniqueness since the end of name contains
    // executorId and applicationId
    val hostname = name.substring(Math.max(0, name.length - 63))
    val resolvedExecutorLabels = Map(
        SPARK_EXECUTOR_ID_LABEL -> executorId,
        SPARK_APP_ID_LABEL -> applicationId,
        SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE) ++
        executorLabels
    val executorMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryMiB}Mi")
      .build()
    val executorMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryWithOverhead}Mi")
      .build()
    val executorCpuQuantity = new QuantityBuilder(false)
      .withAmount(executorCores.toString)
      .build()
    val executorExtraClasspathEnv = executorExtraClasspath.map { cp =>
      new EnvVarBuilder()
        .withName(ENV_EXECUTOR_EXTRA_CLASSPATH)
        .withValue(cp)
        .build()
    }
    val executorExtraJavaOptionsEnv = sparkConf
      .get(org.apache.spark.internal.config.EXECUTOR_JAVA_OPTIONS)
      .map { opts =>
        val delimitedOpts = Utils.splitCommandString(opts)
        delimitedOpts.zipWithIndex.map {
          case (opt, index) =>
            new EnvVarBuilder().withName(s"$ENV_JAVA_OPT_PREFIX$index").withValue(opt).build()
        }
      }.getOrElse(Seq.empty[EnvVar])
    val executorEnv = (Seq(
      (ENV_EXECUTOR_PORT, executorPort.toString),
      (ENV_DRIVER_URL, driverUrl),
      // Executor backend expects integral value for executor cores, so round it up to an int.
      (ENV_EXECUTOR_CORES, math.ceil(executorCores).toInt.toString),
      (ENV_EXECUTOR_MEMORY, executorMemoryString),
      (ENV_APPLICATION_ID, applicationId),
      (ENV_EXECUTOR_ID, executorId),
      (ENV_MOUNTED_CLASSPATH, s"$executorJarsDownloadDir/*")) ++ executorEnvs)
      .map(env => new EnvVarBuilder()
        .withName(env._1)
        .withValue(env._2)
        .build()
      ) ++ Seq(
      new EnvVarBuilder()
        .withName(ENV_EXECUTOR_POD_IP)
        .withValueFrom(new EnvVarSourceBuilder()
          .withNewFieldRef("v1", "status.podIP")
          .build())
        .build()
    ) ++ executorExtraJavaOptionsEnv ++ executorExtraClasspathEnv.toSeq
    val requiredPorts = Seq(
      (EXECUTOR_PORT_NAME, executorPort),
      (BLOCK_MANAGER_PORT_NAME, blockmanagerPort))
      .map(port => {
        new ContainerPortBuilder()
          .withName(port._1)
          .withContainerPort(port._2)
          .build()
      })
    val shuffleVolumesWithMounts = executorLocalDirVolumeProvider
        .getExecutorLocalDirVolumesWithMounts

    val executorContainer = new ContainerBuilder()
      .withName(s"executor")
      .withImage(executorDockerImage)
      .withImagePullPolicy(dockerImagePullPolicy)
      .withNewResources()
        .addToRequests("memory", executorMemoryQuantity)
        .addToLimits("memory", executorMemoryLimitQuantity)
        .addToRequests("cpu", executorCpuQuantity)
        .endResources()
      .addAllToEnv(executorEnv.asJava)
      .withPorts(requiredPorts.asJava)
      .addAllToVolumeMounts(shuffleVolumesWithMounts.map(_._2).asJava)
      .build()

    val executorPod = new PodBuilder()
      .withNewMetadata()
        .withName(name)
        .withLabels(resolvedExecutorLabels.asJava)
        .withAnnotations(executorAnnotations.asJava)
        .withOwnerReferences()
          .addNewOwnerReference()
            .withController(true)
            .withApiVersion(driverPod.getApiVersion)
            .withKind(driverPod.getKind)
            .withName(driverPod.getMetadata.getName)
            .withUid(driverPod.getMetadata.getUid)
            .endOwnerReference()
        .endMetadata()
      .withNewSpec()
        .withHostname(hostname)
        .withRestartPolicy("Never")
        .withNodeSelector(nodeSelector.asJava)
        .addAllToVolumes(shuffleVolumesWithMounts.map(_._1).asJava)
        .endSpec()
      .build()

    val containerWithExecutorLimitCores = executorLimitCores.map {
      limitCores =>
        val executorCpuLimitQuantity = new QuantityBuilder(false)
          .withAmount(limitCores)
          .build()
        new ContainerBuilder(executorContainer)
          .editResources()
            .addToLimits("cpu", executorCpuLimitQuantity)
            .endResources()
          .build()
    }.getOrElse(executorContainer)

    val (withMaybeSecretsMountedPod, withMaybeSecretsMountedContainer) =
      mountSecretsBootstrap.map {bootstrap =>
        bootstrap.mountSecrets(executorPod, containerWithExecutorLimitCores)
      }.getOrElse((executorPod, containerWithExecutorLimitCores))
    val (withMaybeSmallFilesMountedPod, withMaybeSmallFilesMountedContainer) =
      mountSmallFilesBootstrap.map { bootstrap =>
        bootstrap.mountSmallFilesSecret(
          withMaybeSecretsMountedPod, withMaybeSecretsMountedContainer)
      }.getOrElse((withMaybeSecretsMountedPod, withMaybeSecretsMountedContainer))
    val (executorPodWithInitContainer, initBootstrappedExecutorContainer) =
      executorInitContainerBootstrap.map { bootstrap =>
        val podWithDetachedInitContainer = bootstrap.bootstrapInitContainerAndVolumes(
            PodWithDetachedInitContainer(
                withMaybeSmallFilesMountedPod,
                new ContainerBuilder().build(),
                withMaybeSmallFilesMountedContainer))

        val resolvedInitContainer = executorMountInitContainerSecretPlugin.map { plugin =>
          plugin.mountResourceStagingServerSecretIntoInitContainer(
              podWithDetachedInitContainer.initContainer)
        }.getOrElse(podWithDetachedInitContainer.initContainer)

        val podWithAttachedInitContainer = InitContainerUtil.appendInitContainer(
            podWithDetachedInitContainer.pod, resolvedInitContainer)

        val resolvedPodWithMountedSecret = executorMountInitContainerSecretPlugin.map { plugin =>
          plugin.addResourceStagingServerSecretVolumeToPod(podWithAttachedInitContainer)
        }.getOrElse(podWithAttachedInitContainer)

        (resolvedPodWithMountedSecret, podWithDetachedInitContainer.mainContainer)
      }.getOrElse((withMaybeSmallFilesMountedPod, withMaybeSmallFilesMountedContainer))

    val executorPodWithNodeAffinity =
        nodeAffinityExecutorPodModifier.addNodeAffinityAnnotationIfUseful(
            executorPodWithInitContainer, nodeToLocalTaskCount)

    new PodBuilder(executorPodWithNodeAffinity)
      .editSpec()
        .addToContainers(initBootstrappedExecutorContainer)
        .endSpec()
      .build()
  }
}

private object ExecutorPodFactoryImpl {
  private val DEFAULT_STATIC_PORT = 10000
}
