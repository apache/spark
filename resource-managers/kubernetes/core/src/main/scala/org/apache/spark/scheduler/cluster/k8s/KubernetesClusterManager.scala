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

import java.io.File

import io.fabric8.kubernetes.client.Config

import org.apache.spark.SparkContext
import org.apache.spark.deploy.k8s.{ConfigurationUtils, InitContainerResourceStagingServerSecretPluginImpl, SparkKubernetesClientFactory, SparkPodInitContainerBootstrapImpl}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.{MountSecretsBootstrapImpl, MountSmallFilesBootstrapImpl}
import org.apache.spark.internal.Logging
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.kubernetes.KubernetesExternalShuffleClientImpl
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class KubernetesClusterManager extends ExternalClusterManager with Logging {

  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("k8s")

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    val scheduler = new KubernetesTaskSchedulerImpl(sc)
    sc.taskScheduler = scheduler
    scheduler
  }

  override def createSchedulerBackend(sc: SparkContext, masterURL: String, scheduler: TaskScheduler)
      : SchedulerBackend = {
    val sparkConf = sc.getConf
    val maybeInitContainerConfigMap = sparkConf.get(EXECUTOR_INIT_CONTAINER_CONFIG_MAP)
    val maybeInitContainerConfigMapKey = sparkConf.get(EXECUTOR_INIT_CONTAINER_CONFIG_MAP_KEY)
    val maybeSubmittedFilesSecret = sparkConf.get(EXECUTOR_SUBMITTED_SMALL_FILES_SECRET)
    val maybeSubmittedFilesSecretMountPath = sparkConf.get(
      EXECUTOR_SUBMITTED_SMALL_FILES_SECRET_MOUNT_PATH)

    val maybeExecutorInitContainerSecretName =
      sparkConf.get(EXECUTOR_INIT_CONTAINER_SECRET)
    val maybeExecutorInitContainerSecretMountPath =
      sparkConf.get(EXECUTOR_INIT_CONTAINER_SECRET_MOUNT_DIR)

    val executorInitContainerSecretVolumePlugin = for {
      initContainerSecretName <- maybeExecutorInitContainerSecretName
      initContainerSecretMountPath <- maybeExecutorInitContainerSecretMountPath
    } yield {
      new InitContainerResourceStagingServerSecretPluginImpl(
        initContainerSecretName,
        initContainerSecretMountPath)
    }

    // Only set up the bootstrap if they've provided both the config map key and the config map
    // name. The config map might not be provided if init-containers aren't being used to
    // bootstrap dependencies.
    val executorInitContainerBootstrap = for {
      configMap <- maybeInitContainerConfigMap
      configMapKey <- maybeInitContainerConfigMapKey
    } yield {
      new SparkPodInitContainerBootstrapImpl(
        sparkConf.get(INIT_CONTAINER_DOCKER_IMAGE),
        sparkConf.get(DOCKER_IMAGE_PULL_POLICY),
        sparkConf.get(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION),
        sparkConf.get(INIT_CONTAINER_FILES_DOWNLOAD_LOCATION),
        sparkConf.get(INIT_CONTAINER_MOUNT_TIMEOUT),
        configMap,
        configMapKey)
    }

    val mountSmallFilesBootstrap = for {
      secretName <- maybeSubmittedFilesSecret
      secretMountPath <- maybeSubmittedFilesSecretMountPath
    } yield {
      new MountSmallFilesBootstrapImpl(secretName, secretMountPath)
    }

    val executorSecretNamesToMountPaths = ConfigurationUtils.parsePrefixedKeyValuePairs(sparkConf,
      KUBERNETES_EXECUTOR_SECRETS_PREFIX, "executor secrets")
    val mountSecretBootstrap = if (executorSecretNamesToMountPaths.nonEmpty) {
      Some(new MountSecretsBootstrapImpl(executorSecretNamesToMountPaths))
    } else {
      None
    }

    if (maybeInitContainerConfigMap.isEmpty) {
      logWarning("The executor's init-container config map was not specified. Executors will" +
        " therefore not attempt to fetch remote or submitted dependencies.")
    }
    if (maybeInitContainerConfigMapKey.isEmpty) {
      logWarning("The executor's init-container config map key was not specified. Executors will" +
        " therefore not attempt to fetch remote or submitted dependencies.")
    }

    val kubernetesClient = SparkKubernetesClientFactory.createKubernetesClient(
        KUBERNETES_MASTER_INTERNAL_URL,
        Some(sparkConf.get(KUBERNETES_NAMESPACE)),
        APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        sparkConf,
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)),
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)))

    val kubernetesShuffleManager = if (sparkConf.get(
        org.apache.spark.internal.config.SHUFFLE_SERVICE_ENABLED)) {
      val kubernetesExternalShuffleClient = new KubernetesExternalShuffleClientImpl(
          SparkTransportConf.fromSparkConf(sparkConf, "shuffle"),
          sc.env.securityManager,
          sc.env.securityManager.isAuthenticationEnabled(),
          sparkConf.get(org.apache.spark.internal.config.SHUFFLE_REGISTRATION_TIMEOUT))
      Some(new KubernetesExternalShuffleManagerImpl(
          sparkConf,
          kubernetesClient,
          kubernetesExternalShuffleClient))
    } else None

    val executorLocalDirVolumeProvider = new ExecutorLocalDirVolumeProviderImpl(
        sparkConf, kubernetesShuffleManager)
    val executorPodFactory = new ExecutorPodFactoryImpl(
        sparkConf,
        NodeAffinityExecutorPodModifierImpl,
        mountSecretBootstrap,
        mountSmallFilesBootstrap,
        executorInitContainerBootstrap,
        executorInitContainerSecretVolumePlugin,
        executorLocalDirVolumeProvider)
    val allocatorExecutor = ThreadUtils
        .newDaemonSingleThreadScheduledExecutor("kubernetes-pod-allocator")
    val requestExecutorsService = ThreadUtils.newDaemonCachedThreadPool(
        "kubernetes-executor-requests")
    new KubernetesClusterSchedulerBackend(
        scheduler.asInstanceOf[TaskSchedulerImpl],
        sc.env.rpcEnv,
        executorPodFactory,
        kubernetesShuffleManager,
        kubernetesClient,
        allocatorExecutor,
        requestExecutorsService)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
