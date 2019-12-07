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


/**
 * Responsible for the creation and deletion of Pods as per the
 * request of the ExecutorPodAllocator, ExecutorPodLifecycleManager, and the
 * KubernetesClusterSchedulerBackend. The default implementation:
 * ExecutorPodControllerImpl communicates directly
 * with the KubernetesClient to create Pods. This class can be extended
 * to have your communication be done with a unique CRD that satisfies
 * your specific SLA and security concerns.
 */
private[spark] trait ExecutorPodController {

  def initialize(kubernetesClient: KubernetesClient, appId: String): Unit

  def addPod(pod: Pod): Unit

  def commitAndGetTotalAllocated(): Int

  def removePodById(execId: String): Unit

  def removePod(pod: Pod): Unit

  def commitAndGetTotalDeleted(): Int

  def removePods(execIds: Seq[Long], state: Option[String] = None): Iterable[Long]

  def removeAllPods(): Boolean

  def stop(): Boolean
}
