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

import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.Constants._

private[spark] class ExecutorPodsPollingEventSource(
    kubernetesClient: KubernetesClient,
    eventQueue: ExecutorPodsEventQueue,
    pollingExecutor: ScheduledExecutorService) {
  private var pollingFuture: Future[_] = _

  def start(applicationId: String): Unit = {
    require(pollingFuture == null, "Cannot start polling more than once.")
    pollingFuture = pollingExecutor.scheduleWithFixedDelay(
      new PollRunnable(applicationId), 0L, 30L, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
    if (pollingFuture != null) {
      pollingFuture.cancel(true)
      pollingFuture = null
    }
  }

  private class PollRunnable(applicationId: String) extends Runnable {
    override def run(): Unit = {
      kubernetesClient
        .pods()
        .withLabel(SPARK_APP_ID_LABEL, applicationId)
        .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        .list()
        .getItems
        .asScala
        .foreach(eventQueue.pushPodUpdate)
    }
  }

}
