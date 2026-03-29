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

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.resource.ResourceProfile


/**
 * :: DeveloperApi ::
 * A abstract interface for allowing different types of pods allocation.
 *
 * The internal Spark implementations are [[StatefulSetPodsAllocator]],
 * [[DeploymentPodsAllocator]], and [[ExecutorPodsAllocator]].
 * This may be useful for folks integrating with custom schedulers.
 *
 * @since 3.3.0
 */
@DeveloperApi
@Since("3.3.0")
abstract class AbstractPodsAllocator {
  /**
   * Optional lifecycle manager for tracking executor pod lifecycle events.
   * Set via setExecutorPodsLifecycleManager for backward compatibility.
   *
   * @since 4.2.0
   */
  @Since("4.2.0")
  protected var executorPodsLifecycleManager: Option[ExecutorPodsLifecycleManager] = None

  /**
   * Set the lifecycle manager for tracking executor pod lifecycle events.
   * This method is optional and may not exist in custom implementations based on older versions.
   *
   * @since 4.2.0
   */
  @Since("4.2.0")
  def setExecutorPodsLifecycleManager(manager: ExecutorPodsLifecycleManager): Unit = {
    executorPodsLifecycleManager = Some(manager)
  }

  /**
   * Set the total expected executors for an application
   *
   * @since 3.3.0
   */
  @Since("3.3.0")
  def setTotalExpectedExecutors(resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit

  /**
   * Reference to driver pod.
   *
   * @since 3.3.0
   */
  @Since("3.3.0")
  def driverPod: Option[Pod]

  /**
   * If the pod for a given exec id is deleted.
   *
   * @since 3.3.0
   */
  @Since("3.3.0")
  def isDeleted(executorId: String): Boolean

  /**
   * Start hook.
   *
   * @since 3.3.0
   */
  @Since("3.3.0")
  def start(applicationId: String, schedulerBackend: KubernetesClusterSchedulerBackend): Unit

  /**
   * Stop hook
   *
   * @since 3.3.0
   */
  @Since("3.3.0")
  def stop(applicationId: String): Unit
}
