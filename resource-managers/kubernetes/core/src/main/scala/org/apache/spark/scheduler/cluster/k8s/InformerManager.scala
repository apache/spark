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
import io.fabric8.kubernetes.client.informers.SharedIndexInformer

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL
import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_EXECUTOR_INACTIVE_LABEL, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Owns the shared [[SharedIndexInformer]] used by executor pod snapshot sources when the
 * informer-based mode is enabled. The informer is scoped server-side to the current
 * application's executor pods that are not marked inactive, matching the filter set used by
 * [[ExecutorPodsWatchSnapshotSource]] and [[ExecutorPodsPollingSnapshotSource]].
 */
class InformerManager(kubernetesClient: KubernetesClient, conf: SparkConf)
  extends Logging {

  private val resyncInterval = conf.get(KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL)
  // VisibleForTesting
  private[k8s] var informer: SharedIndexInformer[Pod] = _
  private var stopped = false

  def initInformer(applicationId: String): Unit = {
    if (informer == null) {
      logInfo(s"Initializing executor pods informer for application $applicationId")
      informer = kubernetesClient.pods()
        .withLabel(SPARK_APP_ID_LABEL, applicationId)
        .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        .withoutLabel(SPARK_EXECUTOR_INACTIVE_LABEL, "true")
        .runnableInformer(resyncInterval)
    }
  }

  def getInformer(): SharedIndexInformer[Pod] = {
    if (informer == null) {
      throw new IllegalStateException(
        "Informer has not been initialized. Call initInformer() first.")
    }
    informer
  }

  def startInformer(): Unit = {
    if (informer == null) {
      throw new IllegalStateException(
        "Informer has not been initialized. Call initInformer() first.")
    }
    if (stopped) {
      throw new IllegalStateException("Cannot run informer after stopInformer() has been called.")
    }
    if (!informer.isRunning) {
      informer.run()
    } else {
      logInfo("Informer is already running.")
    }
  }

  def stopInformer(): Unit = {
    if (informer != null) {
      Utils.tryLogNonFatalError {
        informer.close()
      }
      informer = null
      stopped = true
    }
  }
}
