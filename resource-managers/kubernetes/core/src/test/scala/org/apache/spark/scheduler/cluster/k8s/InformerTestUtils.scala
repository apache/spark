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
import io.fabric8.kubernetes.client.informers.{ExceptionHandler, SharedIndexInformer}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when

import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_EXECUTOR_INACTIVE_LABEL, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.deploy.k8s.Fabric8Aliases.{LABELED_PODS, PODS}

/**
 * Shared Mockito stubs for the `kubernetesClient.pods().withLabel(...).withoutLabel(...)
 * .runnableInformer(resyncInterval)` chain that [[InformerManager]] uses. Extracted so the
 * three informer-related suites don't have to copy the same 5-line label-filter setup.
 */
object InformerTestUtils {

  /**
   * Stubs the label-filter chain that [[InformerManager.initInformer]] walks, plus the two
   * calls on the returned informer that [[InformerManager]] makes at build time
   * (`exceptionHandler`, chained back to the informer) and at start time
   * (`isRunning`, defaulted to `false` so `startInformer()` will actually call `start()`).
   * Individual tests can override any of these with a further `when(...)` as needed.
   */
  def stubInformerBuilder(
      kubernetesClient: KubernetesClient,
      podOperations: PODS,
      scopedPods: LABELED_PODS,
      informer: SharedIndexInformer[Pod],
      applicationId: String,
      resyncInterval: Long): Unit = {
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withLabel(SPARK_APP_ID_LABEL, applicationId)).thenReturn(scopedPods)
    when(scopedPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(scopedPods)
    when(scopedPods.withoutLabel(SPARK_EXECUTOR_INACTIVE_LABEL, "true")).thenReturn(scopedPods)
    when(scopedPods.runnableInformer(resyncInterval)).thenReturn(informer)
    when(informer.exceptionHandler(any(classOf[ExceptionHandler]))).thenReturn(informer)
    when(informer.isRunning).thenReturn(false)
  }
}
