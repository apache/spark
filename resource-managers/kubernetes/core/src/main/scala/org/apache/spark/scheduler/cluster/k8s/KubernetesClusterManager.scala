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
import org.apache.spark.deploy.k8s.{KubernetesUtils, SparkKubernetesClientFactory}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
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
      (KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        KUBERNETES_MASTER_INTERNAL_URL,
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)),
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)))
    } else {
      (KUBERNETES_AUTH_CLIENT_MODE_PREFIX,
        KubernetesUtils.parseMasterUrl(masterURL),
        None,
        None)
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

    val requestExecutorsService = ThreadUtils.newDaemonCachedThreadPool(
      "kubernetes-executor-requests")

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
      requestExecutorsService,
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
