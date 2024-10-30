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
import java.time.temporal.ChronoUnit.MILLIS
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException}
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.{any, anyString, eq => meq}
import org.mockito.Mockito.{never, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester._

import org.apache.spark.{SecurityManager, SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesExecutorSpec}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.internal.config._
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
    .set(KUBERNETES_ALLOCATION_BATCH_SIZE.key, "5")

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
  private var podsWithNamespace: PODS_WITH_NAMESPACE = _

  @Mock
  private var podResource: PodResource = _

  @Mock
  private var persistentVolumeClaims: PERSISTENT_VOLUME_CLAIMS = _

  @Mock
  private var pvcWithNamespace: PVC_WITH_NAMESPACE = _

  @Mock
  private var pvcResource: io.fabric8.kubernetes.client.dsl.Resource[PersistentVolumeClaim] = _

  @Mock
  private var labeledPersistentVolumeClaims: LABELED_PERSISTENT_VOLUME_CLAIMS = _

  @Mock
  private var persistentVolumeClaimList: PersistentVolumeClaimList = _

  @Mock
  private var labeledPods: LABELED_PODS = _

  @Mock
  private var driverPodOperations: PodResource = _

  @Mock
  private var executorBuilder: KubernetesExecutorBuilder = _

  @Mock
  private var schedulerBackend: KubernetesClusterSchedulerBackend = _

  private var snapshotsStore: DeterministicExecutorPodsSnapshotsStore = _

  private var podsAllocatorUnderTest: ExecutorPodsAllocator = _

  val appId = "testapp"

  before {
    MockitoAnnotations.openMocks(this).close()
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace("default")).thenReturn(podsWithNamespace)
    when(podsWithNamespace.withName(driverPodName)).thenReturn(driverPodOperations)
    when(podsWithNamespace.resource(any())).thenReturn(podResource)
    when(podsWithNamespace.withLabel(anyString(), anyString())).thenReturn(labeledPods)
    when(podsWithNamespace.withLabelIn(
      anyString(), any(classOf[Array[String]]): _*)).thenReturn(labeledPods)
    when(podsWithNamespace.withField(anyString(), anyString())).thenReturn(labeledPods)
    when(labeledPods.withLabel(anyString(), anyString())).thenReturn(labeledPods)
    when(labeledPods.withLabelIn(
      anyString(), any(classOf[Array[String]]): _*)).thenReturn(labeledPods)
    when(labeledPods.withField(anyString(), anyString())).thenReturn(labeledPods)
    when(driverPodOperations.get).thenReturn(driverPod)
    when(driverPodOperations.waitUntilReady(any(), any())).thenReturn(driverPod)
    when(executorBuilder.buildFromFeatures(any(classOf[KubernetesExecutorConf]), meq(secMgr),
      meq(kubernetesClient), any(classOf[ResourceProfile]))).thenAnswer(executorPodAnswer())
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()
    waitForExecutorPodsClock = new ManualClock(0L)
    podsAllocatorUnderTest = new ExecutorPodsAllocator(
      conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq.empty)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID, schedulerBackend)
    when(kubernetesClient.persistentVolumeClaims()).thenReturn(persistentVolumeClaims)
    when(persistentVolumeClaims.inNamespace("default")).thenReturn(pvcWithNamespace)
    when(pvcWithNamespace.withLabel(any(), any())).thenReturn(labeledPersistentVolumeClaims)
    when(pvcWithNamespace.resource(any())).thenReturn(pvcResource)
    when(labeledPersistentVolumeClaims.list()).thenReturn(persistentVolumeClaimList)
    when(persistentVolumeClaimList.getItems).thenReturn(Seq.empty[PersistentVolumeClaim].asJava)
  }

  test("SPARK-49447: Prevent small values less than 100 for batch delay") {
    val m = intercept[IllegalArgumentException] {
      val conf = new SparkConf().set(KUBERNETES_ALLOCATION_BATCH_DELAY.key, "1")
      conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)
    }.getMessage
    assert(m.contains("Allocation batch delay must be greater than 0.1s."))
  }

  test("SPARK-36052: test splitSlots") {
    val seq1 = Seq("a")
    assert(ExecutorPodsAllocator.splitSlots(seq1, 0) === Seq(("a", 0)))
    assert(ExecutorPodsAllocator.splitSlots(seq1, 1) === Seq(("a", 1)))
    assert(ExecutorPodsAllocator.splitSlots(seq1, 2) === Seq(("a", 2)))

    val seq2 = Seq("a", "b", "c")
    assert(ExecutorPodsAllocator.splitSlots(seq2, 0) === Seq(("a", 0), ("b", 0), ("c", 0)))
    assert(ExecutorPodsAllocator.splitSlots(seq2, 1) === Seq(("a", 1), ("b", 0), ("c", 0)))
    assert(ExecutorPodsAllocator.splitSlots(seq2, 2) === Seq(("a", 1), ("b", 1), ("c", 0)))
    assert(ExecutorPodsAllocator.splitSlots(seq2, 3) === Seq(("a", 1), ("b", 1), ("c", 1)))
    assert(ExecutorPodsAllocator.splitSlots(seq2, 4) === Seq(("a", 2), ("b", 1), ("c", 1)))
  }

  test("SPARK-36052: pending pod limit with multiple resource profiles") {
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
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any(classOf[Array[String]]): _*))
      .thenReturn(podOperations)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    val rpb = new ResourceProfileBuilder()
    val ereq = new ExecutorResourceRequests()
    val treq = new TaskResourceRequests()
    ereq.cores(4).memory("2g")
    treq.cpus(2)
    rpb.require(ereq).require(treq)
    val rp = rpb.build()

    val confWithLowMaxPendingPods = conf.clone.set(KUBERNETES_MAX_PENDING_PODS.key, "3")
    podsAllocatorUnderTest = new ExecutorPodsAllocator(confWithLowMaxPendingPods, secMgr,
      executorBuilder, kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID, schedulerBackend)

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 2, rp -> 3))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 3)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(1, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(2, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(3, rp.id))
    verify(podResource, times(3)).create()

    // Mark executor 2 and 3 as pending, leave 1 as newly created but this does not free up
    // any pending pod slot so no new pod is requested
    snapshotsStore.updatePod(pendingExecutor(2, defaultProfile.id))
    snapshotsStore.updatePod(pendingExecutor(3, rp.id))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 3)
    verify(podResource, times(3)).create()
    verify(labeledPods, never()).delete()

    // Downscaling for defaultProfile resource ID with 1 executor to make one free slot
    // for pendings pods
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1, rp -> 3))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 3)
    verify(labeledPods, times(1)).delete()
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(4, rp.id))

    // Make one pod running this way we have one more free slot for pending pods
    snapshotsStore.updatePod(runningExecutor(3, rp.id))
    snapshotsStore.updatePod(pendingExecutor(4, rp.id))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 3)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(5, rp.id))
    verify(labeledPods, times(1)).delete()
  }

  test("Initially request executors in batches. Do not request another batch if the" +
    " first has not finished.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> (podAllocationSize + 1)))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 5)
    for (nextId <- 1 to podAllocationSize) {
      verify(podsWithNamespace).resource(podWithAttachedContainerForId(nextId))
    }
    verify(podsWithNamespace, never())
      .resource(podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("Request executors in batches. Allow another batch to be requested if" +
    " all pending executors start running.") {
    val counter = PrivateMethod[AtomicInteger](Symbol("EXECUTOR_ID_COUNTER"))()
    assert(podsAllocatorUnderTest.invokePrivate(counter).get() === 0)

    podsAllocatorUnderTest.setTotalExpectedExecutors(
      Map(defaultProfile -> (podAllocationSize + 1)))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 5)
    for (execId <- 1 until podAllocationSize) {
      snapshotsStore.updatePod(runningExecutor(execId))
    }
    assert(podsAllocatorUnderTest.invokePrivate(counter).get() === 5)
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(podsWithNamespace, never())
      .resource(podWithAttachedContainerForId(podAllocationSize + 1))
    verify(podResource, times(podAllocationSize)).create()
    snapshotsStore.updatePod(runningExecutor(podAllocationSize))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(podAllocationSize + 1))
    snapshotsStore.updatePod(runningExecutor(podAllocationSize))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(podResource, times(podAllocationSize + 1)).create()
  }

  test("When a current batch reaches error states immediately, re-request" +
    " them on the next batch.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(
      Map(defaultProfile -> podAllocationSize))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 5)
    for (execId <- 1 until podAllocationSize) {
      snapshotsStore.updatePod(runningExecutor(execId))
    }
    val failedPod = failedExecutorWithoutDeletion(podAllocationSize)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("Verify stopping deletes the labeled pods") {
    when(podsWithNamespace
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    podsAllocatorUnderTest.stop(TEST_SPARK_APP_ID)
    verify(labeledPods).delete()
  }

  test("When an executor is requested but the API does not report it in a reasonable time, retry" +
    " requesting that executor.") {
    when(podsWithNamespace
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1"))
      .thenReturn(labeledPods)
    podsAllocatorUnderTest.setTotalExpectedExecutors(
      Map(defaultProfile -> 1))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(1))
    waitForExecutorPodsClock.setTime(podCreationTimeout + 1)
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(labeledPods).delete()
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(2))
  }

  test("SPARK-28487: scale up and down on target executor count changes") {
    when(podsWithNamespace
      .withField("status.phase", "Pending"))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any(classOf[Array[String]]): _*))
      .thenReturn(labeledPods)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    // Target 1 executor, make sure it's requested, even with an empty initial snapshot.
    podsAllocatorUnderTest.setTotalExpectedExecutors(
      Map(defaultProfile -> 1))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(1))

    // Mark executor as running, verify that subsequent allocation cycle is a no-op.
    snapshotsStore.updatePod(runningExecutor(1))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(podResource, times(1)).create()
    verify(labeledPods, never()).delete()

    // Request 3 more executors, make sure all are requested.
    podsAllocatorUnderTest.setTotalExpectedExecutors(
      Map(defaultProfile -> 4))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 3)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(2))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(3))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(4))

    // Mark 2 as running, 3 as pending. Allocation cycle should do nothing.
    snapshotsStore.updatePod(runningExecutor(2))
    snapshotsStore.updatePod(pendingExecutor(3))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 2)
    verify(podResource, times(4)).create()
    verify(labeledPods, never()).delete()

    // Scale down to 1. Pending executors (both acknowledged and not) should be deleted.
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    podsAllocatorUnderTest.setTotalExpectedExecutors(
      Map(defaultProfile -> 1))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(podResource, times(4)).create()
    verify(labeledPods).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "3", "4")
    verify(labeledPods).delete()
    assert(podsAllocatorUnderTest.isDeleted("3"))
    assert(podsAllocatorUnderTest.isDeleted("4"))

    // Update the snapshot to not contain the deleted executors, make sure the
    // allocator cleans up internal state.
    snapshotsStore.updatePod(deletedExecutor(3))
    snapshotsStore.updatePod(deletedExecutor(4))
    snapshotsStore.removeDeletedExecutors()
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    assert(!podsAllocatorUnderTest.isDeleted("3"))
    assert(!podsAllocatorUnderTest.isDeleted("4"))
  }

  test("SPARK-34334: correctly identify timed out pending pod requests as excess") {
    when(podsWithNamespace
      .withField("status.phase", "Pending"))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any(classOf[Array[String]]): _*))
      .thenReturn(labeledPods)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(1))
    verify(podResource).create()

    snapshotsStore.updatePod(pendingExecutor(1))
    snapshotsStore.notifySubscribers()

    waitForExecutorPodsClock.advance(executorIdleTimeout)

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 2))
    snapshotsStore.notifySubscribers()
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(2))

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    snapshotsStore.notifySubscribers()

    verify(labeledPods, never()).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1")
    verify(labeledPods, never()).delete()

    waitForExecutorPodsClock.advance(executorIdleTimeout)
    snapshotsStore.notifySubscribers()

    // before SPARK-34334 this verify() call failed as the non-timed out newly created request
    // decreased the number of requests taken from timed out pending pod requests
    verify(labeledPods).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1")
    verify(labeledPods).delete()
  }

  test("SPARK-33099: Respect executor idle timeout configuration") {
    when(podsWithNamespace
      .withField("status.phase", "Pending"))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any(classOf[Array[String]]): _*))
      .thenReturn(labeledPods)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 5))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 5)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(1))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(2))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(3))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(4))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(5))
    verify(podResource, times(5)).create()

    snapshotsStore.updatePod(pendingExecutor(1))
    snapshotsStore.updatePod(pendingExecutor(2))

    // Newly created executors (both acknowledged and not) are protected by executorIdleTimeout
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 0))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 5)
    verify(podOperations, never()).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1", "2", "3", "4", "5")
    verify(podResource, never()).delete()

    // Newly created executors (both acknowledged and not) are cleaned up.
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq("1", "3", "4"))
    snapshotsStore.notifySubscribers()
    // SPARK-34361: even as 1, 3 and 4 are not timed out as they are considered as known PODs so
    // this is why they are not counted into the outstanding PODs and /they are not removed even
    // though executor 1 is still in pending state and executor 3 and 4 are new request without
    // any state reported by kubernetes and all the three are already timed out
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(labeledPods).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "2", "5")
    verify(labeledPods).delete()
  }

  /**
   * This test covers some downscaling and upscaling of dynamic allocation on kubernetes
   * along with multiple resource profiles (default and rp) when some executors
   * already know by the scheduler backend.
   *
   * Legend:
   *
   * N-: newly created not known by the scheduler backend
   * N+: newly created known by the scheduler backend
   * P- / P+ : pending (not know / known) by the scheduler backend
   * D: deleted
   *                                       |   default    ||         rp        | expected
   *                                       |              ||                   | outstanding
   *                                       | 1  | 2  | 3  || 4  | 5  | 6  | 7  | PODs
   * ==========================================================================================
   *  0) setTotalExpectedExecs with        | N- | N- | N- || N- | N- | N- | N- |
   *       default->3, ro->4               |    |    |    ||    |    |    |    |      7
   * ------------------------------------------------------------------------------------------
   *  1) make 1 from each rp               | N+ | N- | N- || N+ | N- | N- | N- |
   *     known by backend                  |    |    |    ||    |    |    |    |      5
   * -------------------------------------------------------------------------------------------
   *  2) some more backend known + pending | N+ | P+ | P- || N+ | P+ | P- | N- |      3
   * -------------------------------------------------------------------------------------------
   *  3) advance time with idle timeout    |    |    |    ||    |    |    |    |
   *     setTotalExpectedExecs with        | N+ | P+ | D  || N+ | P+ | D  | D  |      0
   *       default->1, rp->1               |    |    |    ||    |    |    |    |
   * -------------------------------------------------------------------------------------------
   *  4) setTotalExpectedExecs with        | N+ | P+ | D  || N+ | P+ | D  | D  |      0 and
   *       default->2, rp->2               |    |    |    ||    |    |    |    | no new POD req.
   * ===========================================================================================
   *
   *  5) setTotalExpectedExecs with default -> 3, rp -> 3 which will lead to creation of the new
   *     PODs: 8 and 9
   */
  test("SPARK-34361: scheduler backend known pods with multiple resource profiles at downscaling") {
    when(podsWithNamespace
      .withField("status.phase", "Pending"))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any(classOf[Array[String]]): _*))
      .thenReturn(labeledPods)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    val rpb = new ResourceProfileBuilder()
    val ereq = new ExecutorResourceRequests()
    val treq = new TaskResourceRequests()
    ereq.cores(4).memory("2g")
    treq.cpus(2)
    rpb.require(ereq).require(treq)
    val rp = rpb.build()

    // 0) request 3 PODs for the default and 4 PODs for the other resource profile
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 3, rp -> 4))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 7)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(1, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(2, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(3, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(4, rp.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(5, rp.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(6, rp.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(7, rp.id))

    // 1) make 1 POD known by the scheduler backend for each resource profile
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq("1", "4"))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 5,
      "scheduler backend known PODs are not outstanding")
    verify(podResource, times(7)).create()

    // 2) make 1 extra POD known by the scheduler backend for each resource profile
    // and make some to pending
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq("1", "2", "4", "5"))
    snapshotsStore.updatePod(pendingExecutor(2, defaultProfile.id))
    snapshotsStore.updatePod(pendingExecutor(3, defaultProfile.id))
    snapshotsStore.updatePod(pendingExecutor(5, rp.id))
    snapshotsStore.updatePod(pendingExecutor(6, rp.id))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 3)
    verify(podResource, times(7)).create()

    // 3) downscale to 1 POD for default and 1 POD for the other resource profile
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1, rp -> 1))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(podResource, times(7)).create()
    verify(labeledPods, times(2)).delete()
    assert(podsAllocatorUnderTest.isDeleted("3"))
    assert(podsAllocatorUnderTest.isDeleted("6"))
    assert(podsAllocatorUnderTest.isDeleted("7"))

    // 4) upscale to 2 PODs for default and 2 for the other resource profile but as there is still
    // 2 PODs known by the scheduler backend there must be no new POD requested to be created
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 2, rp -> 2))
    snapshotsStore.notifySubscribers()
    verify(podResource, times(7)).create()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(podResource, times(7)).create()

    // 5) requesting 1 more executor for each resource
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 3, rp -> 3))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 2)
    verify(podResource, times(9)).create()
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(8, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(9, rp.id))
  }

  test("SPARK-33288: multiple resource profiles") {
    when(podsWithNamespace
      .withField("status.phase", "Pending"))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any(classOf[Array[String]]): _*))
      .thenReturn(labeledPods)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    val rpb = new ResourceProfileBuilder()
    val ereq = new ExecutorResourceRequests()
    val treq = new TaskResourceRequests()
    ereq.cores(4).memory("2g")
    treq.cpus(2)
    rpb.require(ereq).require(treq)
    val rp = rpb.build()

    // Target 1 executor for default profile, 2 for other profile,
    // make sure it's requested, even with an empty initial snapshot.
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1, rp -> 2))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 3)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(1, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(2, rp.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(3, rp.id))

    // Mark executor as running, verify that subsequent allocation cycle is a no-op.
    snapshotsStore.updatePod(runningExecutor(1, defaultProfile.id))
    snapshotsStore.updatePod(runningExecutor(2, rp.id))
    snapshotsStore.updatePod(runningExecutor(3, rp.id))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(podResource, times(3)).create()
    verify(podResource, never()).delete()

    // Request 3 more executors for default profile and 1 more for other profile,
    // make sure all are requested.
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 4, rp -> 3))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 4)
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(4, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(5, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(6, defaultProfile.id))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(7, rp.id))

    // Mark 4 as running, 5 and 7 as pending. Allocation cycle should do nothing.
    snapshotsStore.updatePod(runningExecutor(4, defaultProfile.id))
    snapshotsStore.updatePod(pendingExecutor(5, defaultProfile.id))
    snapshotsStore.updatePod(pendingExecutor(7, rp.id))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 3)
    verify(podResource, times(7)).create()
    verify(podResource, never()).delete()

    // Scale down to 1 for both resource profiles. Pending executors
    // (both acknowledged and not) should be deleted.
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1, rp -> 1))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(podResource, times(7)).create()
    verify(labeledPods).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "5", "6")
    verify(labeledPods).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "7")
    verify(labeledPods, times(2)).delete()
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
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    assert(!podsAllocatorUnderTest.isDeleted("5"))
    assert(!podsAllocatorUnderTest.isDeleted("6"))
    assert(!podsAllocatorUnderTest.isDeleted("7"))
  }

  test("SPARK-33262: pod allocator does not stall with pending pods") {
    when(podsWithNamespace
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1"))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, "2", "3", "4", "5", "6"))
      .thenReturn(labeledPods)

    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 6))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 5)
    // Initial request of pods
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(1))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(2))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(3))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(4))
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(5))
    // 4 come up, 1 pending
    snapshotsStore.updatePod(pendingExecutor(1))
    snapshotsStore.updatePod(runningExecutor(2))
    snapshotsStore.updatePod(runningExecutor(3))
    snapshotsStore.updatePod(runningExecutor(4))
    snapshotsStore.updatePod(runningExecutor(5))
    // We move forward one allocation cycle
    waitForExecutorPodsClock.setTime(podAllocationDelay + 1)
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 2)
    // We request pod 6
    verify(podsWithNamespace).resource(podWithAttachedContainerForId(6))
  }

  test("SPARK-35416: Support PersistentVolumeClaim Reuse") {
    val prefix = "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1"
    val confWithPVC = conf.clone
      .set(KUBERNETES_DRIVER_OWN_PVC.key, "true")
      .set(KUBERNETES_DRIVER_REUSE_PVC.key, "true")
      .set(s"$prefix.mount.path", "/spark-local-dir")
      .set(s"$prefix.mount.readOnly", "false")
      .set(s"$prefix.option.claimName", "OnDemand")
      .set(s"$prefix.option.sizeLimit", "200Gi")
      .set(s"$prefix.option.storageClass", "gp2")

    val pvc = persistentVolumeClaim("pvc-0", "gp2", "200Gi")
    pvc.getMetadata
      .setCreationTimestamp(Instant.now().minus(podCreationTimeout + 1, MILLIS).toString)
    when(persistentVolumeClaimList.getItems).thenReturn(Seq(pvc).asJava)
    when(executorBuilder.buildFromFeatures(any(classOf[KubernetesExecutorConf]), meq(secMgr),
        meq(kubernetesClient), any(classOf[ResourceProfile])))
      .thenAnswer((invocation: InvocationOnMock) => {
      val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
      KubernetesExecutorSpec(
        executorPodWithIdAndVolume(k8sConf.executorId.toInt, k8sConf.resourceProfileId),
        Seq(persistentVolumeClaim("pvc-0", "gp2", "200Gi")))
    })

    podsAllocatorUnderTest = new ExecutorPodsAllocator(
      confWithPVC, secMgr, executorBuilder,
      kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID, schedulerBackend)

    when(podsWithNamespace
      .withField("status.phase", "Pending"))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any(classOf[Array[String]]): _*))
      .thenReturn(labeledPods)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    // Target 1 executor, make sure it's requested, even with an empty initial snapshot.
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(podsWithNamespace).resource(podWithAttachedContainerForIdAndVolume(1))

    // Mark executor as running, verify that subsequent allocation cycle is a no-op.
    snapshotsStore.updatePod(runningExecutor(1))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(podResource, times(1)).create()
    verify(podResource, never()).delete()

    // Request a new executor, make sure it's using reused PVC
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 2))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(podsWithNamespace).resource(podWithAttachedContainerForIdAndVolume(2))
    verify(pvcWithNamespace, never()).resource(any())
  }

  test("print the pod name instead of Some(name) if pod is absent") {
    val nonexistentPod = "i-do-not-exist"
    val conf = new SparkConf().set(KUBERNETES_DRIVER_POD_NAME, nonexistentPod)
    when(podsWithNamespace.withName(nonexistentPod)).thenReturn(driverPodOperations)
    when(driverPodOperations.get()).thenReturn(null)
    val e = intercept[SparkException](new ExecutorPodsAllocator(
      conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, waitForExecutorPodsClock))
    assert(e.getMessage.contains("No pod was found named i-do-not-exist in the cluster in the" +
      " namespace default"))
  }

  test("SPARK-39688: getReusablePVCs should handle accounts with no PVC permission") {
    val getReusablePVCs =
      PrivateMethod[mutable.Buffer[PersistentVolumeClaim]](Symbol("getReusablePVCs"))
    when(persistentVolumeClaimList.getItems).thenThrow(new KubernetesClientException("Error"))
    podsAllocatorUnderTest invokePrivate getReusablePVCs("appId", Seq.empty[String])
  }

  test("SPARK-41388: getReusablePVCs should ignore recently created PVCs in the previous batch") {
    val getReusablePVCs =
      PrivateMethod[mutable.Buffer[PersistentVolumeClaim]](Symbol("getReusablePVCs"))

    val pvc1 = persistentVolumeClaim("pvc-1", "gp2", "200Gi")
    val pvc2 = persistentVolumeClaim("pvc-2", "gp2", "200Gi")

    val now = Instant.now()
    pvc1.getMetadata.setCreationTimestamp(now.minus(podCreationTimeout + 1, MILLIS).toString)
    pvc2.getMetadata.setCreationTimestamp(now.toString)

    when(persistentVolumeClaimList.getItems).thenReturn(Seq(pvc1, pvc2).asJava)
    val reusablePVCs = podsAllocatorUnderTest invokePrivate getReusablePVCs("appId", Seq.empty)
    assert(reusablePVCs.size == 1)
    assert(reusablePVCs.head.getMetadata.getName == "pvc-1")
  }

  test("SPARK-41410: Support waitToReusePersistentVolumeClaims") {
    val prefix = "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1"
    val confWithPVC = conf.clone
      .set(KUBERNETES_DRIVER_OWN_PVC.key, "true")
      .set(KUBERNETES_DRIVER_REUSE_PVC.key, "true")
      .set(KUBERNETES_DRIVER_WAIT_TO_REUSE_PVC.key, "true")
      .set(EXECUTOR_INSTANCES.key, "1")
      .set(s"$prefix.mount.path", "/spark-local-dir")
      .set(s"$prefix.mount.readOnly", "false")
      .set(s"$prefix.option.claimName", "OnDemand")
      .set(s"$prefix.option.sizeLimit", "200Gi")
      .set(s"$prefix.option.storageClass", "gp3")

    when(executorBuilder.buildFromFeatures(any(classOf[KubernetesExecutorConf]), meq(secMgr),
      meq(kubernetesClient), any(classOf[ResourceProfile])))
      .thenAnswer((invocation: InvocationOnMock) => {
        val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
        KubernetesExecutorSpec(
          executorPodWithIdAndVolume(k8sConf.executorId.toInt, k8sConf.resourceProfileId),
          Seq(persistentVolumeClaim("pvc-0", "gp3", "200Gi")))
      })

    podsAllocatorUnderTest = new ExecutorPodsAllocator(
      confWithPVC, secMgr, executorBuilder,
      kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID, schedulerBackend)

    when(podsWithNamespace
      .withField("status.phase", "Pending"))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledPods)
    when(labeledPods
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any(classOf[Array[String]]): _*))
      .thenReturn(labeledPods)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    val counter = PrivateMethod[AtomicInteger](Symbol("PVC_COUNTER"))()
    assert(podsAllocatorUnderTest.invokePrivate(counter).get() === 0)

    // Target 1 executor, make sure it's requested, even with an empty initial snapshot.
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 1)
    verify(podsWithNamespace).resource(podWithAttachedContainerForIdAndVolume(1))
    assert(podsAllocatorUnderTest.invokePrivate(counter).get() === 1)

    // Mark executor as running, verify that subsequent allocation cycle is a no-op.
    snapshotsStore.updatePod(runningExecutor(1))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(podResource, times(1)).create()
    verify(podResource, never()).delete()
    verify(pvcWithNamespace, times(1)).resource(any())
    assert(podsAllocatorUnderTest.invokePrivate(counter).get() === 1)

    // Request a new executor, make sure that no new pod and pvc are created
    podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 2))
    snapshotsStore.notifySubscribers()
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
    verify(podResource, times(1)).create()
    assert(podsAllocatorUnderTest.invokePrivate(counter).get() === 1)
  }

  test("SPARK-41410: An exception during PVC creation should not increase PVC counter") {
    val prefix = "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1"
    val confWithPVC = conf.clone
      .set(KUBERNETES_DRIVER_OWN_PVC.key, "true")
      .set(KUBERNETES_DRIVER_REUSE_PVC.key, "true")
      .set(KUBERNETES_DRIVER_WAIT_TO_REUSE_PVC.key, "true")
      .set(EXECUTOR_INSTANCES.key, "1")
      .set(s"$prefix.mount.path", "/spark-local-dir")
      .set(s"$prefix.mount.readOnly", "false")
      .set(s"$prefix.option.claimName", "OnDemand")
      .set(s"$prefix.option.sizeLimit", "200Gi")
      .set(s"$prefix.option.storageClass", "gp3")

    when(executorBuilder.buildFromFeatures(any(classOf[KubernetesExecutorConf]), meq(secMgr),
      meq(kubernetesClient), any(classOf[ResourceProfile])))
      .thenAnswer((invocation: InvocationOnMock) => {
        val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
        KubernetesExecutorSpec(
          executorPodWithIdAndVolume(k8sConf.executorId.toInt, k8sConf.resourceProfileId),
          Seq(persistentVolumeClaim("pvc-0", "gp3", "200Gi")))
      })

    podsAllocatorUnderTest = new ExecutorPodsAllocator(
      confWithPVC, secMgr, executorBuilder,
      kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID, schedulerBackend)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    val counter = PrivateMethod[AtomicInteger](Symbol("PVC_COUNTER"))()
    assert(podsAllocatorUnderTest.invokePrivate(counter).get() === 0)

    when(pvcResource.create()).thenThrow(new KubernetesClientException("PVC fails to create"))
    intercept[KubernetesClientException] {
      podsAllocatorUnderTest.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    }
    assert(podsAllocatorUnderTest.invokePrivate(counter).get() === 0)
    assert(podsAllocatorUnderTest.numOutstandingPods.get() == 0)
  }

  private def executorPodAnswer(): Answer[KubernetesExecutorSpec] =
    (invocation: InvocationOnMock) => {
      val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
      KubernetesExecutorSpec(executorPodWithId(k8sConf.executorId.toInt,
        k8sConf.resourceProfileId.toInt), Seq.empty)
  }
}
