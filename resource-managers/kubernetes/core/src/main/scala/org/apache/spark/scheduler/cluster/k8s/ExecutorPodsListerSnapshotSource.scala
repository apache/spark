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
import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.util.{ThreadUtils, Utils}

class ExecutorPodsListerSnapshotSource extends ExecutorPodsInformerCustomSnapshotSource {

  private var conf: SparkConf = _
  private var store: ExecutorPodsSnapshotsStore = _
  private var appId: String = _
  private var client: KubernetesClient = _
  private var pollingExecutor: ScheduledExecutorService = _
  private var pollingFuture: Future[_] = _
  private var im: InformerManager = _

  override def init(sparkConf: SparkConf, kubernetesClient: KubernetesClient,
    snapshotStore: ExecutorPodsSnapshotsStore,
    informerManager: InformerManager): Unit = {
    logDebug(s"Starting lister for pods with labels $SPARK_APP_ID_LABEL=$appId," +
      s" $SPARK_ROLE_LABEL=$SPARK_POD_EXECUTOR_ROLE.")
    im = informerManager
    store = snapshotStore
    client = kubernetesClient
    pollingExecutor = createExecutorService()
    im = informerManager
    conf = sparkConf
  }

  def init(sparkConf: SparkConf, kubernetesClient: KubernetesClient,
           snapshotStore: ExecutorPodsSnapshotsStore,
           informerManager: InformerManager,
           pollingExecutor: ScheduledExecutorService): Unit = {
    this.pollingExecutor = pollingExecutor
    this.init(sparkConf, kubernetesClient, snapshotStore, informerManager)
  }

  private def createExecutorService(): ScheduledExecutorService = {
    if (pollingExecutor == null) {
      pollingExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "kubernetes-executor-pod-lister-sync")
    }
    pollingExecutor
  }

  override def start(applicationId: String): Unit = {
    appId = applicationId
    im.initInformer()
    im.startInformer()
    val pollingInterval = conf.get(KUBERNETES_EXECUTOR_LISTER_POLLING_INTERVAL)
    pollingFuture = pollingExecutor.scheduleWithFixedDelay(
      new PollRunnable(
        new Lister(im.getInformer.getIndexer, client.getNamespace)),
      pollingInterval,
      pollingInterval,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    if (pollingFuture != null) {
      pollingFuture.cancel(true)
      pollingFuture = null
    }
    ThreadUtils.shutdown(pollingExecutor)
    im.stopInformer()
  }

  private class PollRunnable(lister: Lister[Pod]) extends Runnable {
    override def run(): Unit = Utils.tryLogNonFatalError {
      store.replaceSnapshot(lister.list().asScala.toSeq)
    }
  }
}
