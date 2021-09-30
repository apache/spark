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

import com.google.common.primitives.UnsignedLong
import io.fabric8.kubernetes.api.model.ListOptionsBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class ExecutorPodsPollingSnapshotSource(
    conf: SparkConf,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    pollingExecutor: ScheduledExecutorService) extends Logging {

  private val pollingInterval = conf.get(KUBERNETES_EXECUTOR_API_POLLING_INTERVAL)

  private var pollingFuture: Future[_] = _

  def start(applicationId: String): Unit = {
    require(pollingFuture == null, "Cannot start polling more than once.")
    logDebug(s"Starting to check for executor pod state every $pollingInterval ms.")
    pollingFuture = pollingExecutor.scheduleWithFixedDelay(
      new PollRunnable(applicationId), pollingInterval, pollingInterval, TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    if (pollingFuture != null) {
      pollingFuture.cancel(true)
      pollingFuture = null
    }
    ThreadUtils.shutdown(pollingExecutor)
  }

  private class PollRunnable(applicationId: String) extends Runnable {
    private var resourceVersion: UnsignedLong = _

    override def run(): Unit = Utils.tryLogNonFatalError {
      logDebug(s"Resynchronizing full executor pod state from Kubernetes.")
      val pods = kubernetesClient
        .pods()
        .withLabel(SPARK_APP_ID_LABEL, applicationId)
        .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        .withoutLabel(SPARK_EXECUTOR_INACTIVE_LABEL, "true")
      if (conf.get(KUBERNETES_EXECUTOR_API_POLLING_WITH_RESOURCE_VERSION)) {
        val list = pods.list(new ListOptionsBuilder().withResourceVersion("0").build())
        val newResourceVersion = UnsignedLong.valueOf(list.getMetadata.getResourceVersion())
        // Replace only when we receive a monotonically increased or equal resourceVersion
        // because some K8s API servers may return old(smaller) cached versions in case of HA setup.
        if (resourceVersion == null || newResourceVersion.compareTo(resourceVersion) >= 0) {
          resourceVersion = newResourceVersion
          snapshotsStore.replaceSnapshot(list.getItems.asScala.toSeq)
        }
      } else {
        snapshotsStore.replaceSnapshot(pods.list().getItems.asScala.toSeq)
      }
    }
  }

}
