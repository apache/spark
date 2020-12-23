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
import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import io.fabric8.kubernetes.client.Config

import org.apache.spark.SparkContext
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkKubernetesClientFactory}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants.DEFAULT_EXECUTOR_CONTAINER_NAME
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.util.{SystemClock, ThreadUtils}

private[spark] class KubernetesClusterManager extends ExternalClusterManager with Logging {

  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("k8s")

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    val wasSparkSubmittedInClusterMode = sc.conf.get(KUBERNETES_DRIVER_SUBMIT_CHECK)
    val (authConfPrefix,
      apiServerUri,
      defaultServiceAccountToken,
      defaultServiceAccountCaCrt) = if (wasSparkSubmittedInClusterMode) {
      require(sc.conf.get(KUBERNETES_DRIVER_POD_NAME).isDefined,
        "If the application is deployed using spark-submit in cluster mode, the driver pod name " +
          "must be provided.")
      val serviceAccountToken =
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)).filter(_.exists)
      val serviceAccountCaCrt =
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)).filter(_.exists)
      (KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        sc.conf.get(KUBERNETES_DRIVER_MASTER_URL),
        serviceAccountToken,
        serviceAccountCaCrt)
    } else {
      (KUBERNETES_AUTH_CLIENT_MODE_PREFIX,
        KubernetesUtils.parseMasterUrl(masterURL),
        None,
        None)
    }

    // If KUBERNETES_EXECUTOR_POD_NAME_PREFIX is not set, initialize it so that all executors have
    // the same prefix. This is needed for client mode, where the feature steps code that sets this
    // configuration is not used.
    //
    // If/when feature steps are executed in client mode, they should instead take care of this,
    // and this code should be removed.
    if (!sc.conf.contains(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)) {
      sc.conf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX,
        KubernetesConf.getResourceNamePrefix(sc.conf.get("spark.app.name")))
    }

    val kubernetesClient = SparkKubernetesClientFactory.createKubernetesClient(
      apiServerUri,
      Some(sc.conf.get(KUBERNETES_NAMESPACE)),
      authConfPrefix,
      SparkKubernetesClientFactory.ClientType.Driver,
      sc.conf,
      defaultServiceAccountToken,
      defaultServiceAccountCaCrt)

    if (sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).isDefined) {
      KubernetesUtils.loadPodFromTemplate(
        kubernetesClient,
        new File(sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).get),
        sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME))
    }

    val schedulerExecutorService = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kubernetes-executor-maintenance")

    ExecutorPodsSnapshot.setShouldCheckAllContainers(
      sc.conf.get(KUBERNETES_EXECUTOR_CHECK_ALL_CONTAINERS))
    val sparkContainerName = sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME)
      .getOrElse(DEFAULT_EXECUTOR_CONTAINER_NAME)
    ExecutorPodsSnapshot.setSparkContainerName(sparkContainerName)
    val subscribersExecutor = ThreadUtils
      .newDaemonThreadPoolScheduledExecutor(
        "kubernetes-executor-snapshots-subscribers", 2)
    val snapshotsStore = new ExecutorPodsSnapshotsStoreImpl(subscribersExecutor)

    val removedExecutorsCache = CacheBuilder.newBuilder()
      .expireAfterWrite(3, TimeUnit.MINUTES)
      .build[java.lang.Long, java.lang.Long]()
    val executorPodsLifecycleEventHandler = new ExecutorPodsLifecycleManager(
      sc.conf,
      kubernetesClient,
      snapshotsStore,
      removedExecutorsCache)

    val executorPodsAllocator = new ExecutorPodsAllocator(
      sc.conf,
      sc.env.securityManager,
      new KubernetesExecutorBuilder(),
      kubernetesClient,
      snapshotsStore,
      new SystemClock())

    val podsWatchEventSource = new ExecutorPodsWatchSnapshotSource(
      snapshotsStore,
      kubernetesClient)

    val eventsPollingExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kubernetes-executor-pod-polling-sync")
    val podsPollingEventSource = new ExecutorPodsPollingSnapshotSource(
      sc.conf, kubernetesClient, snapshotsStore, eventsPollingExecutor)

    new KubernetesClusterSchedulerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl],
      sc,
      kubernetesClient,
      schedulerExecutorService,
      snapshotsStore,
      executorPodsAllocator,
      executorPodsLifecycleEventHandler,
      podsWatchEventSource,
      podsPollingEventSource)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
