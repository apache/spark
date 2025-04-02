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
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SparkConf, SparkContext, SparkMasterRegex}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesUtils, SparkKubernetesClientFactory}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants.DEFAULT_EXECUTOR_CONTAINER_NAME
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.MASTER_URL
import org.apache.spark.internal.config.TASK_MAX_FAILURES
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

private[spark] class KubernetesClusterManager extends ExternalClusterManager with Logging {
  import SparkMasterRegex._

  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("k8s")

  private def isLocal(conf: SparkConf): Boolean =
    conf.get(KUBERNETES_DRIVER_MASTER_URL).startsWith("local")

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    val maxTaskFailures = sc.conf.get(KUBERNETES_DRIVER_MASTER_URL) match {
      case "local" | LOCAL_N_REGEX(_) => 1
      case LOCAL_N_FAILURES_REGEX(_, maxFailures) => maxFailures.toInt
      case _ => sc.conf.get(TASK_MAX_FAILURES)
    }
    new TaskSchedulerImpl(sc, maxTaskFailures, isLocal(sc.conf))
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    if (isLocal(sc.conf)) {
      def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
      val threadCount = sc.conf.get(KUBERNETES_DRIVER_MASTER_URL) match {
        case LOCAL_N_REGEX(threads) =>
          if (threads == "*") localCpuCount else threads.toInt
        case LOCAL_N_FAILURES_REGEX(threads, _) =>
          if (threads == "*") localCpuCount else threads.toInt
        case _ => 1
      }
      logInfo(log"Running Spark with ${MDC(MASTER_URL, sc.conf.get(KUBERNETES_DRIVER_MASTER_URL))}")
      val schedulerImpl = scheduler.asInstanceOf[TaskSchedulerImpl]
      // KubernetesClusterSchedulerBackend respects `spark.app.id` while LocalSchedulerBackend
      // does not. Propagate `spark.app.id` via `spark.test.appId` to match the behavior.
      val conf = sc.conf.getOption("spark.app.id").map(sc.conf.set("spark.test.appId", _))
      val backend = new LocalSchedulerBackend(conf.getOrElse(sc.conf), schedulerImpl, threadCount)
      schedulerImpl.initialize(backend)
      return backend
    }
    val wasSparkSubmittedInClusterMode = sc.conf.get(KUBERNETES_DRIVER_SUBMIT_CHECK)
    val (authConfPrefix,
      apiServerUri,
      defaultServiceAccountCaCrt) = if (wasSparkSubmittedInClusterMode) {
      require(sc.conf.get(KUBERNETES_DRIVER_POD_NAME).isDefined,
        "If the application is deployed using spark-submit in cluster mode, the driver pod name " +
          "must be provided.")
      val serviceAccountCaCrt =
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)).filter(_.exists)
      (KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        sc.conf.get(KUBERNETES_DRIVER_MASTER_URL),
        serviceAccountCaCrt)
    } else {
      (KUBERNETES_AUTH_CLIENT_MODE_PREFIX,
        KubernetesUtils.parseMasterUrl(masterURL),
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
      defaultServiceAccountCaCrt)

    if (sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).isDefined) {
      KubernetesUtils.loadPodFromTemplate(
        kubernetesClient,
        sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).get,
        sc.conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME),
        sc.conf)
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
    val snapshotsStore = new ExecutorPodsSnapshotsStoreImpl(subscribersExecutor, conf = sc.conf)

    val executorPodsLifecycleEventHandler = new ExecutorPodsLifecycleManager(
      sc.conf,
      kubernetesClient,
      snapshotsStore)

    val executorPodsAllocator = makeExecutorPodsAllocator(sc, kubernetesClient, snapshotsStore)

    val podsWatchEventSource = new ExecutorPodsWatchSnapshotSource(
      snapshotsStore,
      kubernetesClient,
      sc.conf)

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

  private[k8s] def makeExecutorPodsAllocator(sc: SparkContext, kubernetesClient: KubernetesClient,
      snapshotsStore: ExecutorPodsSnapshotsStore) = {
    val executorPodsAllocatorName = sc.conf.get(KUBERNETES_ALLOCATION_PODS_ALLOCATOR) match {
      case "statefulset" =>
        classOf[StatefulSetPodsAllocator].getName
      case "direct" =>
        classOf[ExecutorPodsAllocator].getName
      case fullClass =>
        fullClass
    }

    val cls = Utils.classForName[AbstractPodsAllocator](executorPodsAllocatorName)
    val cstr = cls.getConstructor(
      classOf[SparkConf], classOf[org.apache.spark.SecurityManager],
      classOf[KubernetesExecutorBuilder], classOf[KubernetesClient],
      classOf[ExecutorPodsSnapshotsStore], classOf[Clock])
    cstr.newInstance(
      sc.conf,
      sc.env.securityManager,
      new KubernetesExecutorBuilder(),
      kubernetesClient,
      snapshotsStore,
      new SystemClock())
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
