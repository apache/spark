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
package org.apache.spark.deploy.k8s.features

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceProfile}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

private[spark] class BasicExecutorFeatureStep(
    kubernetesConf: KubernetesExecutorConf,
    secMgr: SecurityManager,
    resourceProfile: ResourceProfile)
  extends KubernetesFeatureConfigStep with Logging {

  // Consider moving some of these fields to KubernetesConf or KubernetesExecutorSpecificConf
  private val executorContainerImage = kubernetesConf
    .get(EXECUTOR_CONTAINER_IMAGE)
    .getOrElse(throw new SparkException("Must specify the executor container image"))
  private val blockManagerPort = kubernetesConf
    .sparkConf
    .getInt(BLOCK_MANAGER_PORT.key, DEFAULT_BLOCKMANAGER_PORT)

  require(blockManagerPort == 0 || (1024 <= blockManagerPort && blockManagerPort < 65536),
    "port number must be 0 or in [1024, 65535]")

  private val executorPodNamePrefix = kubernetesConf.resourceNamePrefix

  private val driverUrl = RpcEndpointAddress(
    kubernetesConf.get(DRIVER_HOST_ADDRESS),
    kubernetesConf.sparkConf.getInt(DRIVER_PORT.key, DEFAULT_DRIVER_PORT),
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

  private val isDefaultProfile = resourceProfile.id == ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
  private val isPythonApp = kubernetesConf.get(APP_RESOURCE_TYPE) == Some(APP_RESOURCE_TYPE_PYTHON)
  private val disableConfigMap = kubernetesConf.get(KUBERNETES_EXECUTOR_DISABLE_CONFIGMAP)
  private val minimumMemoryOverhead = kubernetesConf.get(EXECUTOR_MIN_MEMORY_OVERHEAD)
  private val memoryOverheadFactor = if (kubernetesConf.contains(EXECUTOR_MEMORY_OVERHEAD_FACTOR)) {
    kubernetesConf.get(EXECUTOR_MEMORY_OVERHEAD_FACTOR)
  } else {
    kubernetesConf.get(MEMORY_OVERHEAD_FACTOR)
  }

  val execResources = ResourceProfile.getResourcesForClusterManager(
    resourceProfile.id,
    resourceProfile.executorResources,
    minimumMemoryOverhead,
    memoryOverheadFactor,
    kubernetesConf.sparkConf,
    isPythonApp,
    Map.empty)
  assert(execResources.cores.nonEmpty)

  private val executorMemoryString = s"${execResources.executorMemoryMiB}m"
  // we don't include any kubernetes conf specific requests or limits when using custom
  // ResourceProfiles because we don't have a way of overriding them if needed
  private val executorCoresRequest =
    if (isDefaultProfile && kubernetesConf.sparkConf.contains(KUBERNETES_EXECUTOR_REQUEST_CORES)) {
      kubernetesConf.get(KUBERNETES_EXECUTOR_REQUEST_CORES).get
    } else {
      execResources.cores.get.toString
    }
  private val executorLimitCores = kubernetesConf.get(KUBERNETES_EXECUTOR_LIMIT_CORES)

  private def buildExecutorResourcesQuantities(
      customResources: Set[ExecutorResourceRequest]): Map[String, Quantity] = {
    customResources.map { request =>
      val vendorDomain = if (request.vendor.nonEmpty) {
        request.vendor
      } else {
        throw new SparkException(s"Resource: ${request.resourceName} was requested, " +
          "but vendor was not specified.")
      }
      val quantity = new Quantity(request.amount.toString)
      (KubernetesConf.buildKubernetesResourceName(vendorDomain, request.resourceName), quantity)
    }.toMap
  }

  override def configurePod(pod: SparkPod): SparkPod = {
    val name = s"$executorPodNamePrefix-exec-${kubernetesConf.executorId}"
    val configMapName = KubernetesClientUtils.configMapNameExecutor
    val confFilesMap = KubernetesClientUtils
      .buildSparkConfDirFilesMap(configMapName, kubernetesConf.sparkConf, Map.empty)
    val keyToPaths = KubernetesClientUtils.buildKeyToPathObjects(confFilesMap)
    // According to
    // https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names,
    // hostname must be no longer than `KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH`(63) characters,
    // so take the last 63 characters of the pod name as the hostname.
    // This preserves uniqueness since the end of name contains executorId
    val hostname = name.substring(Math.max(0, name.length - KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH))
      // Remove non-word characters from the start of the hostname
      .replaceAll("^[^\\w]+", "")
      // Replace dangerous characters in the remaining string with a safe alternative.
      .replaceAll("[^\\w-]+", "_")

    val executorMemoryQuantity = new Quantity(s"${execResources.totalMemMiB}Mi")
    val executorCpuQuantity = new Quantity(executorCoresRequest)
    val executorResourceQuantities =
      buildExecutorResourcesQuantities(execResources.customResources.values.toSet)

    val executorEnv: Seq[EnvVar] = {
      val sparkAuthSecret = Option(secMgr.getSecretKey()).map {
        case authSecret: String if kubernetesConf.get(AUTH_SECRET_FILE_EXECUTOR).isEmpty =>
          Seq(SecurityManager.ENV_AUTH_SECRET -> authSecret)
        case _ => Nil
      }.getOrElse(Nil)

      val userOpts = kubernetesConf.get(EXECUTOR_JAVA_OPTIONS).toSeq.flatMap { opts =>
        val subsOpts = Utils.substituteAppNExecIds(opts, kubernetesConf.appId,
          kubernetesConf.executorId)
          Utils.splitCommandString(subsOpts)
      }

      val sparkOpts = Utils.sparkJavaOpts(kubernetesConf.sparkConf,
        SparkConf.isExecutorStartupConf)

      val allOpts = (userOpts ++ sparkOpts).zipWithIndex.map { case (opt, index) =>
        (s"$ENV_JAVA_OPT_PREFIX$index", opt)
      }.toMap

      val attributes = if (kubernetesConf.get(UI.CUSTOM_EXECUTOR_LOG_URL).isDefined) {
        Map(
          ENV_EXECUTOR_ATTRIBUTE_APP_ID -> kubernetesConf.appId,
          ENV_EXECUTOR_ATTRIBUTE_EXECUTOR_ID -> kubernetesConf.executorId)
      } else {
        Map.empty[String, String]
      }

      KubernetesUtils.buildEnvVars(
        Seq(
          ENV_DRIVER_URL -> driverUrl,
          ENV_EXECUTOR_CORES -> execResources.cores.get.toString,
          ENV_EXECUTOR_MEMORY -> executorMemoryString,
          ENV_APPLICATION_ID -> kubernetesConf.appId,
          // This is to set the SPARK_CONF_DIR to be /opt/spark/conf
          ENV_SPARK_CONF_DIR -> SPARK_CONF_DIR_INTERNAL,
          ENV_EXECUTOR_ID -> kubernetesConf.executorId,
          ENV_RESOURCE_PROFILE_ID -> resourceProfile.id.toString)
          ++ attributes
          ++ kubernetesConf.environment
          ++ sparkAuthSecret
          ++ Seq(ENV_CLASSPATH -> kubernetesConf.get(EXECUTOR_CLASS_PATH).orNull)
          ++ allOpts) ++
      KubernetesUtils.buildEnvVarsWithFieldRef(
        Seq(
          (ENV_EXECUTOR_POD_IP, "v1", "status.podIP"),
          (ENV_EXECUTOR_POD_NAME, "v1", "metadata.name")
        ))
    }
    executorEnv.find(_.getName == ENV_EXECUTOR_DIRS).foreach { e =>
      e.setValue(e.getValue
        .replaceAll(ENV_APPLICATION_ID, kubernetesConf.appId)
        .replaceAll(ENV_EXECUTOR_ID, kubernetesConf.executorId))
    }

    // 0 is invalid as kubernetes containerPort request, we shall leave it unmounted
    val requiredPorts = if (blockManagerPort != 0) {
      Seq(
        (BLOCK_MANAGER_PORT_NAME, blockManagerPort))
        .map { case (name, port) =>
          new ContainerPortBuilder()
            .withName(name)
            .withContainerPort(port)
            .build()
        }
    } else Nil

    if (!isDefaultProfile) {
      if (pod.container != null && pod.container.getResources() != null) {
        logDebug("NOT using the default profile and removing template resources")
        pod.container.setResources(new ResourceRequirements())
      }
    }

    val executorContainer = new ContainerBuilder(pod.container)
      .withName(Option(pod.container.getName).getOrElse(DEFAULT_EXECUTOR_CONTAINER_NAME))
      .withImage(executorContainerImage)
      .withImagePullPolicy(kubernetesConf.imagePullPolicy)
      .editOrNewResources()
        .addToRequests("memory", executorMemoryQuantity)
        .addToLimits("memory", executorMemoryQuantity)
        .addToRequests("cpu", executorCpuQuantity)
        .addToLimits(executorResourceQuantities.asJava)
        .endResources()
      .addNewEnv()
        .withName(ENV_SPARK_USER)
        .withValue(Utils.getCurrentUserName())
        .endEnv()
      .addAllToEnv(executorEnv.asJava)
      .addAllToPorts(requiredPorts.asJava)
      .addToArgs("executor")
      .build()
    val executorContainerWithConfVolume = if (disableConfigMap) {
      executorContainer
    } else {
      new ContainerBuilder(executorContainer)
        .addNewVolumeMount()
          .withName(SPARK_CONF_VOLUME_EXEC)
          .withMountPath(SPARK_CONF_DIR_INTERNAL)
          .endVolumeMount()
        .build()
    }
    val containerWithLimitCores = if (isDefaultProfile) {
      executorLimitCores.map { limitCores =>
        val executorCpuLimitQuantity = new Quantity(limitCores)
        new ContainerBuilder(executorContainerWithConfVolume)
          .editResources()
          .addToLimits("cpu", executorCpuLimitQuantity)
          .endResources()
          .build()
      }.getOrElse(executorContainerWithConfVolume)
    } else {
      executorContainerWithConfVolume
    }
    val containerWithLifecycle =
      if (!kubernetesConf.workerDecommissioning) {
        logInfo("Decommissioning not enabled, skipping shutdown script")
        containerWithLimitCores
      } else {
        logInfo("Adding decommission script to lifecycle")
        new ContainerBuilder(containerWithLimitCores).withNewLifecycle()
          .withNewPreStop()
            .withNewExec()
              .addToCommand(kubernetesConf.get(DECOMMISSION_SCRIPT))
            .endExec()
          .endPreStop()
          .endLifecycle()
          .build()
      }
    val ownerReference = kubernetesConf.driverPod.map { pod =>
      new OwnerReferenceBuilder()
        .withController(true)
        .withApiVersion(pod.getApiVersion)
        .withKind(pod.getKind)
        .withName(pod.getMetadata.getName)
        .withUid(pod.getMetadata.getUid)
        .build()
    }

    val policy = kubernetesConf.get(KUBERNETES_ALLOCATION_PODS_ALLOCATOR) match {
      case "statefulset" => "Always"
      case _ => "Never"
    }

    val executorPodBuilder = new PodBuilder(pod.pod)
      .editOrNewMetadata()
        .withName(name)
        .addToLabels(kubernetesConf.labels.asJava)
        .addToAnnotations(kubernetesConf.annotations.asJava)
        .addToOwnerReferences(ownerReference.toSeq: _*)
        .endMetadata()
      .editOrNewSpec()
        .withHostname(hostname)
        .withRestartPolicy(policy)
        .addToNodeSelector(kubernetesConf.nodeSelector.asJava)
        .addToNodeSelector(kubernetesConf.executorNodeSelector.asJava)
        .addToImagePullSecrets(kubernetesConf.imagePullSecrets: _*)
    val executorPod = if (disableConfigMap) {
      executorPodBuilder.endSpec().build()
    } else {
      executorPodBuilder
        .addNewVolume()
          .withName(SPARK_CONF_VOLUME_EXEC)
          .withNewConfigMap()
            .withItems(keyToPaths.asJava)
            .withName(configMapName)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()
    }
    kubernetesConf.schedulerName
      .foreach(executorPod.getSpec.setSchedulerName)

    SparkPod(executorPod, containerWithLifecycle)
  }
}
