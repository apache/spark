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

import java.util.concurrent.ExecutorService

import scala.concurrent.{ExecutionContext, Future}

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.scheduler.{ExecutorLossReason, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    rpcEnv: RpcEnv,
    kubernetesClient: KubernetesClient,
    requestExecutorsService: ExecutorService,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    podAllocator: ExecutorPodsAllocator,
    lifecycleEventHandler: ExecutorPodsLifecycleManager,
    watchEvents: ExecutorPodsWatchSnapshotSource,
    pollEvents: ExecutorPodsPollingSnapshotSource)
  extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  private implicit val requestExecutorContext =
    ExecutionContext.fromExecutorService(requestExecutorsService)

  protected override val minRegisteredRatio =
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  // Allow removeExecutor to be accessible by ExecutorPodsLifecycleEventHandler
  private[k8s] def doRemoveExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    removeExecutor(executorId, reason)
  }

  /**
   * Get an application ID associated with the job.
   * This returns the string value of spark.app.id if set, otherwise
   * the locally-generated ID from the superclass.
   *
   * @return The application ID
   */
  override def applicationId(): String = {
    conf.getOption("spark.app.id").map(_.toString).getOrElse(super.applicationId)
  }

  override def start(): Unit = {
    super.start()
    if (!Utils.isDynamicAllocationEnabled(conf)) {
      podAllocator.setTotalExpectedExecutors(initialExecutors)
    }
    lifecycleEventHandler.start(this)
    podAllocator.start(applicationId())
    watchEvents.start(applicationId())
    pollEvents.start(applicationId())
  }

  override def stop(): Unit = {
    // When `CoarseGrainedSchedulerBackend.stop` throws `SparkException`,
    // K8s cluster scheduler should log and proceed in order to delete the K8s cluster resources.
    Utils.tryLogNonFatalError {
      super.stop()
    }

    Utils.tryLogNonFatalError {
      snapshotsStore.stop()
    }

    Utils.tryLogNonFatalError {
      watchEvents.stop()
    }

    Utils.tryLogNonFatalError {
      pollEvents.stop()
    }

    Utils.tryLogNonFatalError {
      kubernetesClient
        .pods()
        .withLabel(SPARK_APP_ID_LABEL, applicationId())
        .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        .delete()
    }

    Utils.tryLogNonFatalError {
      ThreadUtils.shutdown(requestExecutorsService)
    }

    Utils.tryLogNonFatalError {
      kubernetesClient.close()
    }
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    // TODO when we support dynamic allocation, the pod allocator should be told to process the
    // current snapshot in order to decrease/increase the number of executors accordingly.
    podAllocator.setTotalExpectedExecutors(requestedTotal)
    true
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def getExecutorIds(): Seq[String] = synchronized {
    super.getExecutorIds()
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future[Boolean] {
    kubernetesClient
      .pods()
      .withLabel(SPARK_APP_ID_LABEL, applicationId())
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, executorIds: _*)
      .delete()
    // Don't do anything else - let event handling from the Kubernetes API do the Spark changes
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new KubernetesDriverEndpoint(rpcEnv, properties)
  }

  private class KubernetesDriverEndpoint(rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
      extends DriverEndpoint(rpcEnv, sparkProperties) {

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      // Don't do anything besides disabling the executor - allow the Kubernetes API events to
      // drive the rest of the lifecycle decisions
      // TODO what if we disconnect from a networking issue? Probably want to mark the executor
      // to be deleted eventually.
      addressToExecutorId.get(rpcAddress).foreach(disableExecutor)
    }
  }

}
