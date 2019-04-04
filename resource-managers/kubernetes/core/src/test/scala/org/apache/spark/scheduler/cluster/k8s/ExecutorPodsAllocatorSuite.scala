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

import io.fabric8.kubernetes.api.model.{DoneablePod, Pod, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{never, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._
import org.apache.spark.util.ManualClock

class ExecutorPodsAllocatorSuite extends SparkFunSuite with BeforeAndAfter {

  private val driverPodName = "driver"

  private val driverPod = new PodBuilder()
    .withNewMetadata()
      .withName(driverPodName)
      .addToLabels(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)
      .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_DRIVER_ROLE)
      .withUid("driver-pod-uid")
      .endMetadata()
    .build()

  private val conf = new SparkConf().set(KUBERNETES_DRIVER_POD_NAME, driverPodName)

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)
  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)
  private val podCreationTimeout = math.max(podAllocationDelay * 5, 60000L)
  private val secMgr = new SecurityManager(conf)

  private var waitForExecutorPodsClock: ManualClock = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var labeledPods: LABELED_PODS = _

  @Mock
  private var driverPodOperations: PodResource[Pod, DoneablePod] = _

  @Mock
  private var executorBuilder: KubernetesExecutorBuilder = _

  private var snapshotsStore: DeterministicExecutorPodsSnapshotsStore = _

  private var podsAllocatorUnderTest: ExecutorPodsAllocator = _

  before {
    MockitoAnnotations.initMocks(this)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(driverPodName)).thenReturn(driverPodOperations)
    when(driverPodOperations.get).thenReturn(driverPod)
    when(executorBuilder.buildFromFeatures(any(classOf[KubernetesExecutorConf]), meq(secMgr),
      meq(kubernetesClient))).thenAnswer(executorPodAnswer())
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()
    waitForExecutorPodsClock = new ManualClock(0L)
    podsAllocatorUnderTest = new ExecutorPodsAllocator(
      conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID)
  }

  test("Initially request executors in batches. Do not request another batch if the" +
    " first has not finished.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    snapshotsStore.notifySubscribers()
    for (nextId <- 1 to podAllocationSize) {
      verify(podOperations).create(podWithAttachedContainerForId(nextId))
    }
    verify(podOperations, never()).create(podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("Request executors in batches. Allow another batch to be requested if" +
    " all pending executors start running.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    snapshotsStore.notifySubscribers()
    for (execId <- 1 until podAllocationSize) {
      snapshotsStore.updatePod(runningExecutor(execId))
    }
    snapshotsStore.notifySubscribers()
    verify(podOperations, never()).create(podWithAttachedContainerForId(podAllocationSize + 1))
    snapshotsStore.updatePod(runningExecutor(podAllocationSize))
    snapshotsStore.notifySubscribers()
    verify(podOperations).create(podWithAttachedContainerForId(podAllocationSize + 1))
    snapshotsStore.updatePod(runningExecutor(podAllocationSize))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(podAllocationSize + 1)).create(any(classOf[Pod]))
  }

  test("When a current batch reaches error states immediately, re-request" +
    " them on the next batch.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(podAllocationSize)
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    snapshotsStore.notifySubscribers()
    for (execId <- 1 until podAllocationSize) {
      snapshotsStore.updatePod(runningExecutor(execId))
    }
    val failedPod = failedExecutorWithoutDeletion(podAllocationSize)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    verify(podOperations).create(podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("When an executor is requested but the API does not report it in a reasonable time, retry" +
    " requesting that executor.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(1)
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    snapshotsStore.notifySubscribers()
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    waitForExecutorPodsClock.setTime(podCreationTimeout + 1)
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_EXECUTOR_ID_LABEL, "1"))
      .thenReturn(labeledPods)
    snapshotsStore.notifySubscribers()
    verify(labeledPods).delete()
    verify(podOperations).create(podWithAttachedContainerForId(2))
  }

  private def executorPodAnswer(): Answer[SparkPod] =
    (invocation: InvocationOnMock) => {
      val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
      executorPodWithId(k8sConf.executorId.toInt)
  }
}
