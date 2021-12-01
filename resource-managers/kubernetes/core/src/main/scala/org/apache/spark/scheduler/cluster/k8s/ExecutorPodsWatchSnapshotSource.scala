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

import java.io.Closeable

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.annotation.{DeveloperApi, Since, Stable}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 *
 * A class used for watching K8s executor pods by ExternalClusterManagers.
 *
 * @since 3.1.3
 */
@Stable
@DeveloperApi
class ExecutorPodsWatchSnapshotSource(
    snapshotsStore: ExecutorPodsSnapshotsStore,
    kubernetesClient: KubernetesClient) extends Logging {

  private var watchConnection: Closeable = _

  @Since("3.1.3")
  def start(applicationId: String): Unit = {
    require(watchConnection == null, "Cannot start the watcher twice.")
    logDebug(s"Starting watch for pods with labels $SPARK_APP_ID_LABEL=$applicationId," +
      s" $SPARK_ROLE_LABEL=$SPARK_POD_EXECUTOR_ROLE.")
    watchConnection = kubernetesClient.pods()
      .withLabel(SPARK_APP_ID_LABEL, applicationId)
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      .watch(new ExecutorPodsWatcher())
  }

  @Since("3.1.3")
  def stop(): Unit = {
    if (watchConnection != null) {
      Utils.tryLogNonFatalError {
        watchConnection.close()
      }
      watchConnection = null
    }
  }

  private class ExecutorPodsWatcher extends Watcher[Pod] {
    override def eventReceived(action: Action, pod: Pod): Unit = {
      val podName = pod.getMetadata.getName
      logDebug(s"Received executor pod update for pod named $podName, action $action")
      snapshotsStore.updatePod(pod)
    }

    override def onClose(e: KubernetesClientException): Unit = {
      logWarning("Kubernetes client has been closed (this is expected if the application is" +
        " shutting down.)", e)
    }
  }

}
