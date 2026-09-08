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

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.informers.ResourceEventHandler

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Publishes executor pod updates to [[ExecutorPodsSnapshotsStore]] using the shared informer
 * owned by [[InformerManager]]. Event-driven counterpart of [[ExecutorPodsListerSnapshotSource]],
 * which periodically snapshots the same informer's local cache.
 */
private[spark] class ExecutorPodsInformerSnapshotSource(
    snapshotsStore: ExecutorPodsSnapshotsStore,
    informerManager: InformerManager)
  extends ExecutorPodsSnapshotSource with Logging {

  private var started = false

  override def start(applicationId: String): Unit = {
    require(!started, "Cannot start the informer source twice.")
    started = true
    informerManager.initInformer(applicationId)
    informerManager.getInformer().addEventHandler(new ExecutorPodsInformer())
    informerManager.startInformer()
  }

  override def stop(): Unit = {
    Utils.tryLogNonFatalError {
      informerManager.stopInformer()
    }
  }

  private class ExecutorPodsInformer extends ResourceEventHandler[Pod] {
    override def onAdd(pod: Pod): Unit = {
      logDebug(s"Received add executor pod event for pod named ${pod.getMetadata.getName}")
      snapshotsStore.updatePod(pod)
    }

    override def onUpdate(oldPod: Pod, newPod: Pod): Unit = {
      // When the informer runs with a non-zero resync period, every cached pod is replayed as
      // an onUpdate with an unchanged resourceVersion. Skip those no-op replays to avoid
      // driving snapshot subscribers with N churn snapshots per resync round.
      if (oldPod.getMetadata.getResourceVersion != newPod.getMetadata.getResourceVersion) {
        logDebug(s"Received update executor pod event for pod named " +
          s"${newPod.getMetadata.getName}")
        snapshotsStore.updatePod(newPod)
      }
    }

    override def onDelete(pod: Pod, deletedFinalStateUnknown: Boolean): Unit = {
      logDebug(s"Received delete executor pod event for pod named ${pod.getMetadata.getName}")
      snapshotsStore.updatePod(pod)
    }
  }
}
