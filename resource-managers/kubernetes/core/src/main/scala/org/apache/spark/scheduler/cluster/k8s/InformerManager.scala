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
import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

class InformerManager(kubernetesClient: KubernetesClient, applicationId: String, conf: SparkConf)
  extends Logging {

  private val resyncInterval = conf.get(KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL)
  // VisibleForTesting
  private[k8s] var informer: SharedIndexInformer[Pod] = _
  private var stopped = false

  def initInformer(): Unit = {
    informer = kubernetesClient.pods()
      .withLabel(SPARK_APP_ID_LABEL, applicationId)
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      .runnableInformer(resyncInterval)
  }

  def getInformer(): SharedIndexInformer[Pod] = {
    // Don't initialize the informer if stopInformer() has been called
    if (informer == null && !stopped) {
      initInformer()
    }
    informer
  }

  def startInformer(): Unit = {
    if (informer == null) {
      throw new IllegalStateException("Informer has not been initialized. " +
        "Call initInformer() first.")
    }
    if (stopped) {
      throw new IllegalStateException("Cannot run informer after stopInformer() has been called.")
    }
    if (!informer.isRunning) {
      logDebug(s"Running informer for pods with labels $SPARK_APP_ID_LABEL=$applicationId," +
        s" $SPARK_ROLE_LABEL=$SPARK_POD_EXECUTOR_ROLE.")
      informer.run()
    } else {
      logDebug("Informer is already running.")
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
