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

import java.util

import io.fabric8.kubernetes.api.model.{DoneablePod, Pod, PodBuilder, PodList}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{doNothing, never, times, verify, when}
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
  private var podList: PodList = _

  @Mock
  private var pods: util.List[Pod] = _

  @Mock
  private var driverPodOperations: PodResource[Pod, DoneablePod] = _

  @Mock
  private var executorBuilder: KubernetesExecutorBuilder = _

  @Mock
  private var executorPodController: ExecutorPodController = _

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
    doNothing().when(executorPodController).initialize(kubernetesClient, TEST_SPARK_APP_ID)
    podsAllocatorUnderTest = new ExecutorPodsAllocator(
      conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID, executorPodController)
  }

  test("Initially request executors in batches. Do not request another batch if the" +
    " first has not finished.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    for (nextId <- 1 to podAllocationSize) {
      verify(executorPodController).addPod(podWithAttachedContainerForId(nextId))
    }
    verify(executorPodController,
      never()).addPod(podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("Request executors in batches. Allow another batch to be requested if" +
    " all pending executors start running.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    for (execId <- 1 until podAllocationSize) {
      snapshotsStore.updatePod(runningExecutor(execId))
    }
    snapshotsStore.notifySubscribers()
    verify(executorPodController,
      never()).addPod(podWithAttachedContainerForId(podAllocationSize + 1))
    snapshotsStore.updatePod(runningExecutor(podAllocationSize))
    snapshotsStore.notifySubscribers()
    verify(executorPodController).addPod(podWithAttachedContainerForId(podAllocationSize + 1))
    snapshotsStore.updatePod(runningExecutor(podAllocationSize))
    snapshotsStore.notifySubscribers()
    verify(executorPodController, times(podAllocationSize + 1)).addPod(any(classOf[Pod]))
  }

  test("When a current batch reaches error states immediately, re-request" +
    " them on the next batch.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(podAllocationSize)
    for (execId <- 1 until podAllocationSize) {
      snapshotsStore.updatePod(runningExecutor(execId))
    }
    val failedPod = failedExecutorWithoutDeletion(podAllocationSize)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    verify(executorPodController).addPod(podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("When an executor is requested but the API does not report it in a reasonable time, retry" +
    " requesting that executor.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(1)
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    snapshotsStore.notifySubscribers()
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    waitForExecutorPodsClock.setTime(podCreationTimeout + 1)
    when(podOperations.withLabel(SPARK_EXECUTOR_ID_LABEL, "1")).thenReturn(labeledPods)
    when(labeledPods.list()).thenReturn(podList)
    when(podList.getItems).thenReturn(pods)
    snapshotsStore.notifySubscribers()
    verify(executorPodController).removePodById("1")
    verify(executorPodController).commitAndGetTotalDeleted()
    verify(executorPodController).addPod(podWithAttachedContainerForId(2))
  }

  test("SPARK-28487: scale up and down on target executor count changes") {
    // Target 1 executor, make sure it's requested, even with an empty initial snapshot.
    podsAllocatorUnderTest.setTotalExpectedExecutors(1)
    verify(executorPodController, times(1)).addPod(podWithAttachedContainerForId(1))

    // Mark executor as running, verify that subsequent allocation cycle is a no-op.
    snapshotsStore.updatePod(runningExecutor(1))
    snapshotsStore.notifySubscribers()
    verify(executorPodController, never()).removePodById(any())

    // Request 3 more executors, make sure all are requested.
    podsAllocatorUnderTest.setTotalExpectedExecutors(4)
    snapshotsStore.notifySubscribers()
    verify(executorPodController, times(1)).addPod(podWithAttachedContainerForId(2))
    verify(executorPodController, times(1)).addPod(podWithAttachedContainerForId(3))
    verify(executorPodController, times(1)).addPod(podWithAttachedContainerForId(4))

    // Mark 2 as running, 3 as pending. Allocation cycle should do nothing.
    snapshotsStore.updatePod(runningExecutor(2))
    snapshotsStore.updatePod(pendingExecutor(3))
    snapshotsStore.notifySubscribers()
    verify(executorPodController, never()).removePodById(any())

    // Scale down to 1. Pending executors (both acknowledged and not) should be deleted.
    podsAllocatorUnderTest.setTotalExpectedExecutors(1)
    snapshotsStore.notifySubscribers()
    verify(executorPodController, times(1)).removePods(Seq(4L, 3L), Some("Pending"))
  }

  private def executorPodAnswer(): Answer[SparkPod] =
    (invocation: InvocationOnMock) => {
      val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
      executorPodWithId(k8sConf.executorId.toInt)
  }
}
