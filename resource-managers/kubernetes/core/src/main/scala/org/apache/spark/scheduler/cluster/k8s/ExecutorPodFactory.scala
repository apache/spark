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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.{ConfigurationUtils, InitContainerBootstrap, MountSecretsBootstrap, PodWithDetachedInitContainer}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.InitContainerUtil
import org.apache.spark.internal.config.{EXECUTOR_CLASS_PATH, EXECUTOR_JAVA_OPTIONS, EXECUTOR_MEMORY, EXECUTOR_MEMORY_OVERHEAD}
import org.apache.spark.util.Utils

/**
 * A factory class for configuring and creating executor pods.
 */
private[spark] class ExecutorPodFactory(
    sparkConf: SparkConf,
    mountSecretsBootstrap: Option[MountSecretsBootstrap],
    initContainerBootstrap: Option[InitContainerBootstrap],
    initContainerMountSecretsBootstrap: Option[MountSecretsBootstrap]) {

  private val executorExtraClasspath = sparkConf.get(EXECUTOR_CLASS_PATH)

  private val executorLabels = ConfigurationUtils.parsePrefixedKeyValuePairs(
    sparkConf,
    KUBERNETES_EXECUTOR_LABEL_PREFIX)
  require(
    !executorLabels.contains(SPARK_APP_ID_LABEL),
    s"Custom executor labels cannot contain $SPARK_APP_ID_LABEL as it is reserved for Spark.")
  require(
    !executorLabels.contains(SPARK_EXECUTOR_ID_LABEL),
    s"Custom executor labels cannot contain $SPARK_EXECUTOR_ID_LABEL as it is reserved for" +
      " Spark.")
  require(
    !executorLabels.contains(SPARK_ROLE_LABEL),
    s"Custom executor labels cannot contain $SPARK_ROLE_LABEL as it is reserved for Spark.")

  private val executorAnnotations =
    ConfigurationUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_EXECUTOR_ANNOTATION_PREFIX)
  private val nodeSelector =
    ConfigurationUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_NODE_SELECTOR_PREFIX)

  private val executorContainerImage = sparkConf
    .get(EXECUTOR_CONTAINER_IMAGE)
    .getOrElse(throw new SparkException("Must specify the executor container image"))
  private val imagePullPolicy = sparkConf.get(CONTAINER_IMAGE_PULL_POLICY)
  private val blockManagerPort = sparkConf
    .getInt("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT)

  private val executorPodNamePrefix = sparkConf.get(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)

  private val executorMemoryMiB = sparkConf.get(EXECUTOR_MEMORY)
  private val executorMemoryString = sparkConf.get(
    EXECUTOR_MEMORY.key, EXECUTOR_MEMORY.defaultValueString)

  private val memoryOverheadMiB = sparkConf
    .get(EXECUTOR_MEMORY_OVERHEAD)
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * executorMemoryMiB).toInt,
      MEMORY_OVERHEAD_MIN_MIB))
  private val executorMemoryWithOverhead = executorMemoryMiB + memoryOverheadMiB

  private val executorCores = sparkConf.getDouble("spark.executor.cores", 1)
  private val executorLimitCores = sparkConf.get(KUBERNETES_EXECUTOR_LIMIT_CORES)

  /**
   * Configure and construct an executor pod with the given parameters.
   */
  def createExecutorPod(
      executorId: String,
      applicationId: String,
      driverUrl: String,
      executorEnvs: Seq[(String, String)],
      driverPod: Pod,
      nodeToLocalTaskCount: Map[String, Int]): Pod = {
    val name = s"$executorPodNamePrefix-exec-$executorId"

    // hostname must be no longer than 63 characters, so take the last 63 characters of the pod
    // name as the hostname.  This preserves uniqueness since the end of name contains
    // executorId
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
      .get(EXECUTOR_JAVA_OPTIONS)
      .map { opts =>
        val delimitedOpts = Utils.splitCommandString(opts)
        delimitedOpts.zipWithIndex.map {
          case (opt, index) =>
            new EnvVarBuilder().withName(s"$ENV_JAVA_OPT_PREFIX$index").withValue(opt).build()
        }
      }.getOrElse(Seq.empty[EnvVar])
    val executorEnv = (Seq(
      (ENV_DRIVER_URL, driverUrl),
      // Executor backend expects integral value for executor cores, so round it up to an int.
      (ENV_EXECUTOR_CORES, math.ceil(executorCores).toInt.toString),
      (ENV_EXECUTOR_MEMORY, executorMemoryString),
      (ENV_APPLICATION_ID, applicationId),
      (ENV_EXECUTOR_ID, executorId)) ++ executorEnvs)
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
      (BLOCK_MANAGER_PORT_NAME, blockManagerPort))
      .map { case (name, port) =>
        new ContainerPortBuilder()
          .withName(name)
          .withContainerPort(port)
          .build()
      }

    val executorContainer = new ContainerBuilder()
      .withName("executor")
      .withImage(executorContainerImage)
      .withImagePullPolicy(imagePullPolicy)
      .withNewResources()
        .addToRequests("memory", executorMemoryQuantity)
        .addToLimits("memory", executorMemoryLimitQuantity)
        .addToRequests("cpu", executorCpuQuantity)
        .endResources()
      .addAllToEnv(executorEnv.asJava)
      .withPorts(requiredPorts.asJava)
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
        .endSpec()
      .build()

    val containerWithLimitCores = executorLimitCores.map { limitCores =>
      val executorCpuLimitQuantity = new QuantityBuilder(false)
        .withAmount(limitCores)
        .build()
      new ContainerBuilder(executorContainer)
        .editResources()
        .addToLimits("cpu", executorCpuLimitQuantity)
        .endResources()
        .build()
    }.getOrElse(executorContainer)

    val (maybeSecretsMountedPod, maybeSecretsMountedContainer) =
      mountSecretsBootstrap.map { bootstrap =>
        bootstrap.mountSecrets(executorPod, containerWithLimitCores)
      }.getOrElse((executorPod, containerWithLimitCores))

    val (bootstrappedPod, bootstrappedContainer) =
      initContainerBootstrap.map { bootstrap =>
        val podWithInitContainer = bootstrap.bootstrapInitContainer(
          PodWithDetachedInitContainer(
            maybeSecretsMountedPod,
            new ContainerBuilder().build(),
            maybeSecretsMountedContainer))

        val (pod, mayBeSecretsMountedInitContainer) =
          initContainerMountSecretsBootstrap.map { bootstrap =>
            bootstrap.mountSecrets(podWithInitContainer.pod, podWithInitContainer.initContainer)
          }.getOrElse((podWithInitContainer.pod, podWithInitContainer.initContainer))

        val bootstrappedPod = InitContainerUtil.appendInitContainer(
          pod, mayBeSecretsMountedInitContainer)

        (bootstrappedPod, podWithInitContainer.mainContainer)
      }.getOrElse((maybeSecretsMountedPod, maybeSecretsMountedContainer))

    new PodBuilder(bootstrappedPod)
      .editSpec()
        .addToContainers(bootstrappedContainer)
        .endSpec()
      .build()
  }
}
