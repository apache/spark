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

import java.time.Instant

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
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesExecutorSpec}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.internal.config.DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT
import org.apache.spark.resource._
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

  private val conf = new SparkConf()
    .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)
    .set(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, "10s")

  private val defaultProfile: ResourceProfile = ResourceProfile.getOrCreateDefaultProfile(conf)
  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)
  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)
  private val executorIdleTimeout = conf.get(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT) * 1000
  private val podCreationTimeout = math.max(podAllocationDelay * 5,
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_TIMEOUT))

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
    MockitoAnnotations.openMocks(this).close()
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(driverPodName)).thenReturn(driverPodOperations)
    when(driverPodOperations.get).thenReturn(driverPod)
    when(executorBuilder.buildFromFeatures(any(classOf[KubernetesExecutorConf]), meq(secMgr),
      meq(kubernetesClient), any(classOf[ResourceProfile]))).thenAnswer(executorPodAnswer())
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()
    waitForExecutorPodsClock = new ManualClock(0L)
    podsAllocatorUnderTest = new ExecutorPodsAllocator(
      conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID)
  }

  test("Initially request executors in batches. Do not request another batch if the" +
    " first has not finished.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> (podAllocationSize + 1)))
    for (nextId <- 1 to podAllocationSize) {
      verify(podOperations).create(podWithAttachedContainerForId(nextId))
    }
    verify(podOperations, never()).create(podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("Request executors in batches. Allow another batch to be requested if" +
    " all pending executors start running.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> (podAllocationSize + 1)))
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
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> podAllocationSize))
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
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1"))
      .thenReturn(labeledPods)
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    verify(podOperations).create(podWithAttachedContainerForId(1))
    waitForExecutorPodsClock.setTime(podCreationTimeout + 1)
    snapshotsStore.notifySubscribers()
    verify(labeledPods).delete()
    verify(podOperations).create(podWithAttachedContainerForId(2))
  }

  test("SPARK-28487: scale up and down on target executor count changes") {
    when(podOperations
      .withField("status.phase", "Pending"))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any()))
      .thenReturn(podOperations)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    // Target 1 executor, make sure it's requested, even with an empty initial snapshot.
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    verify(podOperations).create(podWithAttachedContainerForId(1))

    // Mark executor as running, verify that subsequent allocation cycle is a no-op.
    snapshotsStore.updatePod(runningExecutor(1))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(1)).create(any())
    verify(podOperations, never()).delete()

    // Request 3 more executors, make sure all are requested.
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 4))
    snapshotsStore.notifySubscribers()
    verify(podOperations).create(podWithAttachedContainerForId(2))
    verify(podOperations).create(podWithAttachedContainerForId(3))
    verify(podOperations).create(podWithAttachedContainerForId(4))

    // Mark 2 as running, 3 as pending. Allocation cycle should do nothing.
    snapshotsStore.updatePod(runningExecutor(2))
    snapshotsStore.updatePod(pendingExecutor(3))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(4)).create(any())
    verify(podOperations, never()).delete()

    // Scale down to 1. Pending executors (both acknowledged and not) should be deleted.
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(4)).create(any())
    verify(podOperations).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "3", "4")
    verify(podOperations).delete()
    assert(podsAllocatorUnderTest.isDeleted("3"))
    assert(podsAllocatorUnderTest.isDeleted("4"))

    // Update the snapshot to not contain the deleted executors, make sure the
    // allocator cleans up internal state.
    snapshotsStore.updatePod(deletedExecutor(3))
    snapshotsStore.updatePod(deletedExecutor(4))
    snapshotsStore.removeDeletedExecutors()
    snapshotsStore.notifySubscribers()
    assert(!podsAllocatorUnderTest.isDeleted("3"))
    assert(!podsAllocatorUnderTest.isDeleted("4"))
  }

  test("SPARK-34334: correctly identify timed out pending pod requests as excess") {
    when(podOperations
      .withField("status.phase", "Pending"))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any()))
      .thenReturn(podOperations)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    verify(podOperations).create(podWithAttachedContainerForId(1))
    verify(podOperations).create(any())

    snapshotsStore.updatePod(pendingExecutor(1))
    snapshotsStore.notifySubscribers()

    waitForExecutorPodsClock.advance(executorIdleTimeout)

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 2))
    snapshotsStore.notifySubscribers()
    verify(podOperations).create(podWithAttachedContainerForId(2))

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    snapshotsStore.notifySubscribers()

    verify(podOperations, never()).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1")
    verify(podOperations, never()).delete()

    waitForExecutorPodsClock.advance(executorIdleTimeout)
    snapshotsStore.notifySubscribers()

    // before SPARK-34334 this verify() call failed as the non-timed out newly created request
    // decreased the number of requests taken from timed out pending pod requests
    verify(podOperations).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1")
    verify(podOperations).delete()
  }

  test("SPARK-33099: Respect executor idle timeout configuration") {
    when(podOperations
      .withField("status.phase", "Pending"))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any()))
      .thenReturn(podOperations)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 5))
    verify(podOperations).create(podWithAttachedContainerForId(1))
    verify(podOperations).create(podWithAttachedContainerForId(2))
    verify(podOperations).create(podWithAttachedContainerForId(3))
    verify(podOperations).create(podWithAttachedContainerForId(4))
    verify(podOperations).create(podWithAttachedContainerForId(5))
    verify(podOperations, times(5)).create(any())

    snapshotsStore.updatePod(pendingExecutor(1))
    snapshotsStore.updatePod(pendingExecutor(2))

    // Newly created executors (both acknowledged and not) are protected by executorIdleTimeout
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 0))
    snapshotsStore.notifySubscribers()
    verify(podOperations, never()).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1", "2", "3", "4", "5")
    verify(podOperations, never()).delete()

    // Newly created executors (both acknowledged and not) are cleaned up.
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    snapshotsStore.notifySubscribers()
    verify(podOperations).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1", "2", "3", "4", "5")
    verify(podOperations).delete()
  }

  test("SPARK-33288: multiple resource profiles") {
    when(podOperations
      .withField("status.phase", "Pending"))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any()))
      .thenReturn(podOperations)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    val rpb = new ResourceProfileBuilder()
    val ereq = new ExecutorResourceRequests()
    val treq = new TaskResourceRequests()
    ereq.cores(4).memory("2g")
    treq.cpus(2)
    rpb.require(ereq).require(treq)
    val rp = rpb.build

    // Target 1 executor for default profile, 2 for other profile,
    // make sure it's requested, even with an empty initial snapshot.
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1, rp -> 2))
    verify(podOperations).create(podWithAttachedContainerForId(1, defaultProfile.id))
    verify(podOperations).create(podWithAttachedContainerForId(2, rp.id))
    verify(podOperations).create(podWithAttachedContainerForId(3, rp.id))

    // Mark executor as running, verify that subsequent allocation cycle is a no-op.
    snapshotsStore.updatePod(runningExecutor(1, defaultProfile.id))
    snapshotsStore.updatePod(runningExecutor(2, rp.id))
    snapshotsStore.updatePod(runningExecutor(3, rp.id))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(3)).create(any())
    verify(podOperations, never()).delete()

    // Request 3 more executors for default profile and 1 more for other profile,
    // make sure all are requested.
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 4, rp -> 3))
    snapshotsStore.notifySubscribers()
    verify(podOperations).create(podWithAttachedContainerForId(4, defaultProfile.id))
    verify(podOperations).create(podWithAttachedContainerForId(5, defaultProfile.id))
    verify(podOperations).create(podWithAttachedContainerForId(6, defaultProfile.id))
    verify(podOperations).create(podWithAttachedContainerForId(7, rp.id))

    // Mark 4 as running, 5 and 7 as pending. Allocation cycle should do nothing.
    snapshotsStore.updatePod(runningExecutor(4, defaultProfile.id))
    snapshotsStore.updatePod(pendingExecutor(5, defaultProfile.id))
    snapshotsStore.updatePod(pendingExecutor(7, rp.id))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(7)).create(any())
    verify(podOperations, never()).delete()

    // Scale down to 1 for both resource profiles. Pending executors
    // (both acknowledged and not) should be deleted.
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1, rp -> 1))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(7)).create(any())
    verify(podOperations).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "5", "6")
    verify(podOperations).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "7")
    verify(podOperations, times(2)).delete()
    assert(podsAllocatorUnderTest.isDeleted("5"))
    assert(podsAllocatorUnderTest.isDeleted("6"))
    assert(podsAllocatorUnderTest.isDeleted("7"))

    // Update the snapshot to not contain the deleted executors, make sure the
    // allocator cleans up internal state.
    snapshotsStore.updatePod(deletedExecutor(5))
    snapshotsStore.updatePod(deletedExecutor(6))
    snapshotsStore.updatePod(deletedExecutor(7))
    snapshotsStore.removeDeletedExecutors()
    snapshotsStore.notifySubscribers()
    assert(!podsAllocatorUnderTest.isDeleted("5"))
    assert(!podsAllocatorUnderTest.isDeleted("6"))
    assert(!podsAllocatorUnderTest.isDeleted("7"))
  }

  test("SPARK-33262: pod allocator does not stall with pending pods") {
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1"))
      .thenReturn(labeledPods)
    when(podOperations
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, "2", "3", "4", "5", "6"))
      .thenReturn(podOperations)

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 6))
    // Initial request of pods
    verify(podOperations).create(podWithAttachedContainerForId(1))
    verify(podOperations).create(podWithAttachedContainerForId(2))
    verify(podOperations).create(podWithAttachedContainerForId(3))
    verify(podOperations).create(podWithAttachedContainerForId(4))
    verify(podOperations).create(podWithAttachedContainerForId(5))
    // 4 come up, 1 pending
    snapshotsStore.updatePod(pendingExecutor(1))
    snapshotsStore.updatePod(runningExecutor(2))
    snapshotsStore.updatePod(runningExecutor(3))
    snapshotsStore.updatePod(runningExecutor(4))
    snapshotsStore.updatePod(runningExecutor(5))
    // We move forward one allocation cycle
    waitForExecutorPodsClock.setTime(podAllocationDelay + 1)
    snapshotsStore.notifySubscribers()
    // We request pod 6
    verify(podOperations).create(podWithAttachedContainerForId(6))
  }

  private def executorPodAnswer(): Answer[KubernetesExecutorSpec] =
    (invocation: InvocationOnMock) => {
      val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
      KubernetesExecutorSpec(executorPodWithId(k8sConf.executorId.toInt,
        k8sConf.resourceProfileId.toInt), Seq.empty)
  }
}
