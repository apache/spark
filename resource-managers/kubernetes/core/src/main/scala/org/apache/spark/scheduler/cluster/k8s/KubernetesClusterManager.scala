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

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.deploy.k8s.{InitContainerBootstrap, KubernetesUtils, MountSecretsBootstrap, SparkKubernetesClientFactory}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.util.ThreadUtils

private[spark] class KubernetesClusterManager extends ExternalClusterManager with Logging {

  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("k8s")

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    if (masterURL.startsWith("k8s") && sc.deployMode == "client") {
      throw new SparkException("Client mode is currently not supported for Kubernetes.")
    }

    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    val sparkConf = sc.getConf
    val initContainerConfigMap = sparkConf.get(INIT_CONTAINER_CONFIG_MAP_NAME)
    val initContainerConfigMapKey = sparkConf.get(INIT_CONTAINER_CONFIG_MAP_KEY_CONF)

    if (initContainerConfigMap.isEmpty) {
      logWarning("The executor's init-container config map is not specified. Executors will " +
        "therefore not attempt to fetch remote or submitted dependencies.")
    }

    if (initContainerConfigMapKey.isEmpty) {
      logWarning("The executor's init-container config map key is not specified. Executors will " +
        "therefore not attempt to fetch remote or submitted dependencies.")
    }

    // Only set up the bootstrap if they've provided both the config map key and the config map
    // name. The config map might not be provided if init-containers aren't being used to
    // bootstrap dependencies.
    val initContainerBootstrap = for {
      configMap <- initContainerConfigMap
      configMapKey <- initContainerConfigMapKey
    } yield {
      val initContainerImage = sparkConf
        .get(INIT_CONTAINER_IMAGE)
        .getOrElse(throw new SparkException(
          "Must specify the init-container image when there are remote dependencies"))
      new InitContainerBootstrap(
        initContainerImage,
        sparkConf.get(CONTAINER_IMAGE_PULL_POLICY),
        sparkConf.get(JARS_DOWNLOAD_LOCATION),
        sparkConf.get(FILES_DOWNLOAD_LOCATION),
        configMap,
        configMapKey,
        SPARK_POD_EXECUTOR_ROLE,
        sparkConf)
    }

    val executorSecretNamesToMountPaths = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_EXECUTOR_SECRETS_PREFIX)
    val mountSecretBootstrap = if (executorSecretNamesToMountPaths.nonEmpty) {
      Some(new MountSecretsBootstrap(executorSecretNamesToMountPaths))
    } else {
      None
    }
    // Mount user-specified executor secrets also into the executor's init-container. The
    // init-container may need credentials in the secrets to be able to download remote
    // dependencies. The executor's main container and its init-container share the secrets
    // because the init-container is sort of an implementation details and this sharing
    // avoids introducing a dedicated configuration property just for the init-container.
    val initContainerMountSecretsBootstrap = if (initContainerBootstrap.nonEmpty &&
      executorSecretNamesToMountPaths.nonEmpty) {
      Some(new MountSecretsBootstrap(executorSecretNamesToMountPaths))
    } else {
      None
    }

    val kubernetesClient = SparkKubernetesClientFactory.createKubernetesClient(
      KUBERNETES_MASTER_INTERNAL_URL,
      Some(sparkConf.get(KUBERNETES_NAMESPACE)),
      KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
      sparkConf,
      Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)),
      Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)))

    val executorPodFactory = new ExecutorPodFactory(
      sparkConf,
      mountSecretBootstrap,
      initContainerBootstrap,
      initContainerMountSecretsBootstrap)

    val allocatorExecutor = ThreadUtils
      .newDaemonSingleThreadScheduledExecutor("kubernetes-pod-allocator")
    val requestExecutorsService = ThreadUtils.newDaemonCachedThreadPool(
      "kubernetes-executor-requests")
    new KubernetesClusterSchedulerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl],
      sc.env.rpcEnv,
      executorPodFactory,
      kubernetesClient,
      allocatorExecutor,
      requestExecutorsService)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
