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
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.ResourceEventHandler

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

class ExecutorPodsInformerSnapshotSource extends ExecutorPodsInformerCustomSnapshotSource {

    private var client: KubernetesClient = _
    private var store: ExecutorPodsSnapshotsStore = _
    private var im: InformerManager = _

    def init(conf: SparkConf, kubernetesClient: KubernetesClient,
             snapshotsStore: ExecutorPodsSnapshotsStore,
             informerManager: InformerManager): Unit = {
      client = kubernetesClient
      store = snapshotsStore
      im = informerManager
      informerManager.getInformer.addEventHandler(new ExecutorPodsInformer)
  }

  override def start(applicationId: String): Unit = {
    im.startInformer()
  }

  override def stop(): Unit = {
    Utils.tryLogNonFatalError {
      im.stopInformer()
    }
  }

  private class ExecutorPodsInformer extends ResourceEventHandler[Pod] {
    override def onAdd(pod: Pod): Unit = {
      logDebug(s"Received add executor pod event for pod named ${pod.getMetadata.getName}")
      store.updatePod(pod)
      }

    override def onUpdate(oldPod: Pod, newPod: Pod): Unit = {
      logDebug(s"Received update executor pod event for pod named ${newPod.getMetadata.getName}")
      if (!(oldPod.getMetadata.getResourceVersion == newPod.getMetadata.getResourceVersion)) {
        store.updatePod(newPod)
      }
    }

    override def onDelete(pod: Pod, deletedFinalStateUnknown: Boolean): Unit = {
      logDebug(s"Received delete executor pod update for pod named ${pod.getMetadata.getName}")
      store.updatePod(pod)
    }
  }
}
