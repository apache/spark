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

import java.util.concurrent.TimeUnit

import io.fabric8.kubernetes.api.model.{ListOptionsBuilder, PodListBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import org.jmock.lib.concurrent.DeterministicScheduler
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class ExecutorPodsPollingSnapshotSourceSuite extends SparkFunSuite with BeforeAndAfter {

  private val defaultConf = new SparkConf()

  private val pollingInterval = defaultConf.get(KUBERNETES_EXECUTOR_API_POLLING_INTERVAL)

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var appIdLabeledPods: LABELED_PODS = _

  @Mock
  private var executorRoleLabeledPods: LABELED_PODS = _

  @Mock
  private var activeExecutorPods: LABELED_PODS = _

  @Mock
  private var eventQueue: ExecutorPodsSnapshotsStore = _

  private var pollingExecutor: DeterministicScheduler = _
  private var pollingSourceUnderTest: ExecutorPodsPollingSnapshotSource = _

  before {
    MockitoAnnotations.openMocks(this).close()
    pollingExecutor = new DeterministicScheduler()
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(appIdLabeledPods)
    when(appIdLabeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(executorRoleLabeledPods)
    when(executorRoleLabeledPods.withoutLabel(SPARK_EXECUTOR_INACTIVE_LABEL, "true"))
      .thenReturn(activeExecutorPods)
  }

  test("Items returned by the API should be pushed to the event queue") {
    val sparkConf = new SparkConf()
    pollingSourceUnderTest = new ExecutorPodsPollingSnapshotSource(
      sparkConf,
      kubernetesClient,
      eventQueue,
      pollingExecutor)
    pollingSourceUnderTest.start(TEST_SPARK_APP_ID)
    val exec1 = runningExecutor(1)
    val exec2 = runningExecutor(2)
    when(activeExecutorPods.list())
      .thenReturn(new PodListBuilder()
        .addToItems(
          exec1,
          exec2)
        .build())
    pollingExecutor.tick(pollingInterval, TimeUnit.MILLISECONDS)
    verify(eventQueue).replaceSnapshot(Seq(exec1, exec2))
  }

  test("SPARK-36462: If polling is disabled we don't call pods() on the client") {
    val sparkConf = new SparkConf()
    val source = new ExecutorPodsPollingSnapshotSource(
      sparkConf.set(KUBERNETES_EXECUTOR_ENABLE_API_POLLING, false),
      kubernetesClient,
      eventQueue,
      pollingExecutor)
    source.start(TEST_SPARK_APP_ID)
    pollingExecutor.tick(pollingInterval, TimeUnit.MILLISECONDS)
    verify(kubernetesClient, never()).pods()
  }

  test("SPARK-36334: Support pod listing with resource version") {
    Seq(true, false).foreach { value =>
      val sparkConf = new SparkConf()
      val source = new ExecutorPodsPollingSnapshotSource(
        sparkConf.set(KUBERNETES_EXECUTOR_API_POLLING_WITH_RESOURCE_VERSION, value),
        kubernetesClient,
        eventQueue,
        pollingExecutor)
      source.start(TEST_SPARK_APP_ID)
      pollingExecutor.tick(pollingInterval, TimeUnit.MILLISECONDS)
      if (value) {
        verify(activeExecutorPods).list(new ListOptionsBuilder().withResourceVersion("0").build())
      } else {
        verify(activeExecutorPods).list()
      }
    }
  }
}
