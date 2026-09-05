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

import java.util.concurrent.{Future, ScheduledExecutorService, TimeUnit}

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.cache.Lister

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_LISTER_POLLING_INTERVAL
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Periodically snapshots the local cache of the shared [[InformerManager]] and replaces the
 * contents of the [[ExecutorPodsSnapshotsStore]] with the result. Companion to
 * [[ExecutorPodsInformerSnapshotSource]], which pushes updates as informer events arrive.
 */
class ExecutorPodsListerSnapshotSource(
    conf: SparkConf,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    informerManager: InformerManager,
    pollingExecutor: ScheduledExecutorService)
  extends ExecutorPodsSnapshotSource with Logging {

  private val pollingInterval = conf.get(KUBERNETES_EXECUTOR_LISTER_POLLING_INTERVAL)

  private var pollingFuture: Future[_] = _

  override def start(applicationId: String): Unit = {
    informerManager.initInformer(applicationId)
    informerManager.startInformer()
    val lister = new Lister[Pod](
      informerManager.getInformer().getIndexer, kubernetesClient.getNamespace)
    pollingFuture = pollingExecutor.scheduleWithFixedDelay(
      new PollRunnable(lister), pollingInterval, pollingInterval, TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    if (pollingFuture != null) {
      pollingFuture.cancel(true)
      pollingFuture = null
    }
    Utils.tryLogNonFatalError {
      informerManager.stopInformer()
    }
    ThreadUtils.shutdown(pollingExecutor)
  }

  private class PollRunnable(lister: Lister[Pod]) extends Runnable {
    override def run(): Unit = Utils.tryLogNonFatalError {
      // The informer is already scoped server-side to app-id + role=executor + non-inactive
      // pods, so we can hand its snapshot to the store as-is.
      snapshotsStore.replaceSnapshot(lister.list().asScala.toSeq)
    }
  }
}
