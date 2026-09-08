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

import java.util.Arrays
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{ConfigMap, Pod, PodBuilder, PodList}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import io.fabric8.kubernetes.client.dsl.base.PatchContext
import org.jmock.lib.concurrent.DeterministicScheduler
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.{any, anyBoolean, eq => mockitoEq}
import org.mockito.Mockito.{atLeastOnce, inOrder, mock, never, spy, times, verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.internal.config.SCHEDULER_MAX_RETAINED_UNKNOWN_EXECUTORS
import org.apache.spark.resource.{ResourceProfile, ResourceProfileManager}
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{ExecutorDecommissionInfo, ExecutorKilled, ExecutorLossReason, LiveListenerBus, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{DecommissionExecutor, RegisterExecutor, RemoveExecutor, StopDriver, StopExecutors}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils.TEST_SPARK_APP_ID
import org.apache.spark.storage.{BlockManager, BlockManagerMaster}

class KubernetesClusterSchedulerBackendSuite extends SparkFunSuite with BeforeAndAfter {

  private var schedulerExecutorService: DeterministicScheduler = _
  private val sparkConf = new SparkConf(false)
    .set("spark.executor.instances", "3")
    .set("spark.app.id", TEST_SPARK_APP_ID)
    .set(KUBERNETES_EXECUTOR_DECOMMISSION_LABEL.key, "soLong")
    .set(KUBERNETES_EXECUTOR_DECOMMISSION_LABEL_VALUE.key, "cruelWorld")

  @Mock
  private var sc: SparkContext = _

  @Mock
  private var env: SparkEnv = _

  @Mock
  private var blockManager: BlockManager = _

  @Mock
  private var blockManagerMaster: BlockManagerMaster = _

  @Mock
  private var rpcEnv: RpcEnv = _

  @Mock
  private var driverEndpointRef: RpcEndpointRef = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var podsWithNamespace: PODS_WITH_NAMESPACE = _

  @Mock
  private var labeledPods: LABELED_PODS = _

  @Mock
  private var configMapsOperations: CONFIG_MAPS = _

  @Mock
  private var configMapsWithNamespace: CONFIG_MAPS_WITH_NAMESPACE = _

  @Mock
  private var configMapResource: CONFIG_MAPS_RESOURCE = _

  @Mock
  private var labeledConfigMaps: LABELED_CONFIG_MAPS = _

  @Mock
  private var taskScheduler: TaskSchedulerImpl = _

  @Mock
  private var eventQueue: ExecutorPodsSnapshotsStore = _

  @Mock
  private var podAllocator: ExecutorPodsAllocator = _

  @Mock
  private var lifecycleManager: ExecutorPodsLifecycleManager = _

  @Mock
  private var watchEvents: ExecutorPodsWatchSnapshotSource = _

  @Mock
  private var pollEvents: ExecutorPodsPollingSnapshotSource = _

  @Mock
  private var context: RpcCallContext = _

  private var driverEndpoint: ArgumentCaptor[RpcEndpoint] = _
  private var schedulerBackendUnderTest: KubernetesClusterSchedulerBackend = _

  private val listenerBus = new LiveListenerBus(new SparkConf())
  private val resourceProfileManager = new ResourceProfileManager(sparkConf, listenerBus)
  private val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)

  before {
    schedulerExecutorService = new DeterministicScheduler()
    MockitoAnnotations.openMocks(this).close()
    when(taskScheduler.sc).thenReturn(sc)
    when(taskScheduler.excludedNodes()).thenReturn(Set.empty[String])
    when(sc.conf).thenReturn(sparkConf)
    when(sc.listenerBus).thenReturn(listenerBus)
    when(sc.resourceProfileManager).thenReturn(resourceProfileManager)
    when(sc.env).thenReturn(env)
    when(env.rpcEnv).thenReturn(rpcEnv)
    when(env.blockManager).thenReturn(blockManager)
    when(blockManager.master).thenReturn(blockManagerMaster)
    driverEndpoint = ArgumentCaptor.forClass(classOf[RpcEndpoint])
    when(
      rpcEnv.setupEndpoint(
        mockitoEq(CoarseGrainedSchedulerBackend.ENDPOINT_NAME),
        driverEndpoint.capture()))
      .thenReturn(driverEndpointRef)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace("default")).thenReturn(podsWithNamespace)
    when(kubernetesClient.configMaps()).thenReturn(configMapsOperations)
    when(configMapsOperations.inNamespace("default")).thenReturn(configMapsWithNamespace)
    when(configMapsWithNamespace.resource(any[ConfigMap]())).thenReturn(configMapResource)
    when(podAllocator.driverPod).thenReturn(None)
    schedulerBackendUnderTest = createSchedulerBackend()
  }

  after {
    ResourceProfile.clearDefaultProfile()
  }

  private def createSchedulerBackend(): KubernetesClusterSchedulerBackend = {
    new KubernetesClusterSchedulerBackend(
      taskScheduler,
      sc,
      kubernetesClient,
      schedulerExecutorService,
      eventQueue,
      podAllocator,
      lifecycleManager,
      watchEvents,
      pollEvents)
  }

  private def registerExecutor(
      backend: KubernetesClusterSchedulerBackend,
      executorId: String): RpcEndpointRef = {
    val executorEndpoint = mock(classOf[RpcEndpointRef])
    when(executorEndpoint.address).thenReturn(RpcAddress("localhost", 10000 + executorId.toInt))
    backend.createDriverEndpoint().receiveAndReply(mock(classOf[RpcCallContext])).apply(
      RegisterExecutor(executorId, executorEndpoint, s"host-$executorId", 1,
        Map.empty, Map.empty, Map.empty, defaultProfile.id))
    assert(backend.isExecutorActive(executorId))
    executorEndpoint
  }

  private def withDecommissionMetadata(
      f: KubernetesClusterSchedulerBackend => Unit): Unit = {
    val keys = Seq(KUBERNETES_ALLOCATION_PODS_ALLOCATOR.key,
      KUBERNETES_EXECUTOR_POD_DELETION_COST.key, SCHEDULER_MAX_RETAINED_UNKNOWN_EXECUTORS.key)
    val originalValues = keys.map(key => key -> sparkConf.getOption(key))
    sparkConf.set(KUBERNETES_ALLOCATION_PODS_ALLOCATOR, "deployment")
    sparkConf.set(KUBERNETES_EXECUTOR_POD_DELETION_COST, 7)
    sparkConf.set(SCHEDULER_MAX_RETAINED_UNKNOWN_EXECUTORS, 10)
    try {
      // The backend captures the unknown-executor cache size when it is constructed.
      f(createSchedulerBackend())
    } finally {
      originalValues.foreach {
        case (key, Some(value)) => sparkConf.set(key, value)
        case (key, None) => sparkConf.remove(key)
      }
    }
  }

  test("Start all components") {
    schedulerBackendUnderTest.start()
    verify(podAllocator).setTotalExpectedExecutors(Map(defaultProfile -> 3))
    verify(podAllocator).start(TEST_SPARK_APP_ID, schedulerBackendUnderTest)
    verify(lifecycleManager).start(schedulerBackendUnderTest)
    verify(watchEvents).start(TEST_SPARK_APP_ID)
    verify(pollEvents).start(TEST_SPARK_APP_ID)
    verify(configMapResource).create()
  }

  test("SPARK-38794: executor ConfigMap is created before executors are requested") {
    schedulerBackendUnderTest.start()
    val ordered = inOrder(configMapResource, podAllocator)
    ordered.verify(configMapResource).create()
    ordered.verify(podAllocator).setTotalExpectedExecutors(Map(defaultProfile -> 3))
  }

  test("SPARK-56684: kubernetesClient is exposed within the k8s package") {
    assert(schedulerBackendUnderTest.kubernetesClient eq kubernetesClient)
  }

  test("Stop all components") {
    when(podsWithNamespace.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(labeledPods)
    when(configMapsWithNamespace.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labeledConfigMaps)
    when(labeledConfigMaps.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labeledConfigMaps)
    schedulerBackendUnderTest.stop()
    verify(eventQueue).stop()
    verify(watchEvents).stop()
    verify(pollEvents).stop()
    verify(podAllocator).stop(TEST_SPARK_APP_ID)
    verify(labeledConfigMaps).delete()
    verify(kubernetesClient).close()
  }

  test("Remove executor") {
    val backend = spy[KubernetesClusterSchedulerBackend](schedulerBackendUnderTest)
    when(backend.isExecutorActive(any())).thenReturn(false)
    when(backend.isExecutorActive(mockitoEq("2"))).thenReturn(true)

    backend.start()
    backend.doRemoveExecutor("1", ExecutorKilled)
    verify(driverEndpointRef).send(RemoveExecutor("1", ExecutorKilled))

    backend.doRemoveExecutor("2", ExecutorKilled)
    verify(driverEndpointRef).send(RemoveExecutor("2", ExecutorKilled))
  }

  test("SPARK-55639: doRemoveExecutor triggers setRecoveryMode on OOM") {
    val backend = spy[KubernetesClusterSchedulerBackend](schedulerBackendUnderTest)
    when(backend.isExecutorActive(any())).thenReturn(false)
    backend.start()

    val reason = mock(classOf[ExecutorLossReason])
    when(reason.message).thenReturn("Executor lost due to OOM")

    backend.doRemoveExecutor("1", reason)
    verify(driverEndpointRef).send(RemoveExecutor("1", reason))
    verify(podAllocator).setRecoveryMode()
  }

  test("Kill executors") {
    schedulerBackendUnderTest.start()

    when(podsWithNamespace.withField(any(), any())).thenReturn(labeledPods)
    when(podsWithNamespace.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(labeledPods)
    when(labeledPods.withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1", "2")).thenReturn(labeledPods)
    val pod1op = mock(classOf[PodResource])
    val pod2op = mock(classOf[PodResource])
    when(labeledPods.resources()).thenReturn(Arrays.asList[PodResource]().stream)
    schedulerExecutorService.tick(sparkConf.get(KUBERNETES_DYN_ALLOC_KILL_GRACE_PERIOD) * 2,
      TimeUnit.MILLISECONDS)
    verify(labeledPods, never()).delete()

    schedulerBackendUnderTest.doKillExecutors(Seq("1", "2"))
    verify(driverEndpointRef).send(RemoveExecutor("1", ExecutorKilled))
    verify(driverEndpointRef).send(RemoveExecutor("2", ExecutorKilled))
    verify(labeledPods, never()).delete()
    verify(pod1op, never()).patch(any(classOf[PatchContext]), any(classOf[Pod]))
    verify(pod2op, never()).patch(any(classOf[PatchContext]), any(classOf[Pod]))
    schedulerExecutorService.tick(sparkConf.get(KUBERNETES_DYN_ALLOC_KILL_GRACE_PERIOD) * 2,
      TimeUnit.MILLISECONDS)
    verify(labeledPods, never()).delete()
    verify(pod1op, never()).patch(any(classOf[PatchContext]), any(classOf[Pod]))
    verify(pod2op, never()).patch(any(classOf[PatchContext]), any(classOf[Pod]))

    when(labeledPods.resources()).thenReturn(Arrays.asList(pod1op).stream)
    val podList = mock(classOf[PodList])
    when(labeledPods.list()).thenReturn(podList)
    val pod1 = mock(classOf[Pod])
    val pod2 = mock(classOf[Pod])
    when(podList.getItems).thenReturn(Arrays.asList(pod1, pod2))

    schedulerBackendUnderTest.doKillExecutors(Seq("1", "2"))
    verify(labeledPods, never()).delete()
    schedulerExecutorService.runUntilIdle()
    verify(pod1op).patch(any(classOf[PatchContext]), any(classOf[Pod]))
    verify(pod2op, never()).patch(any(classOf[PatchContext]), any(classOf[Pod]))
    verify(labeledPods, never()).delete()
    schedulerExecutorService.tick(sparkConf.get(KUBERNETES_DYN_ALLOC_KILL_GRACE_PERIOD) * 2,
      TimeUnit.MILLISECONDS)
    verify(labeledPods).delete()
  }

  test("Annotates executor pods with deletion cost when configured") {
    sparkConf.set(KUBERNETES_EXECUTOR_POD_DELETION_COST, 7)
    schedulerBackendUnderTest.start()

    when(podsWithNamespace.withField(any(), any())).thenReturn(labeledPods)
    when(podsWithNamespace.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(labeledPods)
    when(labeledPods.withLabelIn(SPARK_EXECUTOR_ID_LABEL, "3")).thenReturn(labeledPods)

    val podResource = mock(classOf[PodResource])
    val basePod = new PodBuilder()
      .withNewMetadata()
        .withName("exec-3")
        .withNamespace("default")
        .endMetadata()
      .build()

    val patchCaptor = ArgumentCaptor.forClass(classOf[Pod])
    when(podResource.patch(any(), any(classOf[Pod]))).thenReturn(basePod)

    when(labeledPods.resources())
      .thenAnswer(_ => java.util.stream.Stream.of[PodResource](podResource))

    val method = classOf[KubernetesClusterSchedulerBackend]
      .getDeclaredMethods
      .find(_.getName == "annotateExecutorDeletionCost")
      .get
    method.setAccessible(true)
    method.invoke(schedulerBackendUnderTest, Seq("3"))
    schedulerExecutorService.runUntilIdle()

    verify(podResource, atLeastOnce()).patch(any(), patchCaptor.capture())
    val appliedPods = patchCaptor.getAllValues.asScala
    val annotated = appliedPods
      .find(_.getMetadata.getAnnotations.asScala
        .contains("controller.kubernetes.io/pod-deletion-cost"))
    assert(annotated.isDefined,
      s"expected controller.kubernetes.io/pod-deletion-cost annotation, got annotations " +
        s"${appliedPods.map(_.getMetadata.getAnnotations).asJava}")
    val annotations = annotated.get.getMetadata.getAnnotations.asScala
    assert(annotations("controller.kubernetes.io/pod-deletion-cost") === "7")
    sparkConf.remove(KUBERNETES_EXECUTOR_POD_DELETION_COST.key)
  }

  test("SPARK-58879: idle decommission passes only selected executors to Kubernetes") {
    withDecommissionMetadata { schedulerBackend =>
      val backend = spy[KubernetesClusterSchedulerBackend](schedulerBackend)
      val idleExecutor = registerExecutor(backend, "1")
      val busyExecutor = registerExecutor(backend, "2")
      when(taskScheduler.isExecutorBusy("2")).thenReturn(true)
      when(podsWithNamespace.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
        .thenReturn(labeledPods)
      when(labeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
        .thenReturn(labeledPods)
      when(labeledPods.withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1")).thenReturn(labeledPods)
      val podResource = mock(classOf[PodResource])
      when(labeledPods.resources())
        .thenAnswer(_ => java.util.stream.Stream.of[PodResource](podResource))
      val decomInfo = ExecutorDecommissionInfo("test")

      val accepted = backend.decommissionExecutorsIfIdle(
        Array("1" -> decomInfo, "2" -> decomInfo, "3" -> decomInfo, "1" -> decomInfo),
        adjustTargetNumExecutors = false)

      assert(accepted === Seq("1"))
      val requests = ArgumentCaptor.forClass(
        classOf[Array[(String, ExecutorDecommissionInfo)]])
      verify(backend).decommissionExecutors(
        requests.capture(), mockitoEq(false), mockitoEq(false))
      assert(requests.getValue.toSeq === Seq("1" -> decomInfo))
      assert(!backend.isExecutorActive("1"))
      assert(backend.isExecutorActive("2"))
      verify(blockManagerMaster).decommissionBlockManagers(Seq("1"))
      verify(idleExecutor).send(DecommissionExecutor)
      verify(busyExecutor, never()).send(DecommissionExecutor)
      verify(kubernetesClient, never()).pods()

      schedulerExecutorService.runUntilIdle()
      verify(labeledPods, times(2)).withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      val executorIds = ArgumentCaptor.forClass(classOf[Array[String]])
      verify(labeledPods, atLeastOnce()).withLabelIn(
        mockitoEq(SPARK_EXECUTOR_ID_LABEL), executorIds.capture(): _*)
      assert(executorIds.getAllValues.asScala.forall(_.toSeq == Seq("1")))
      verify(labeledPods, times(2)).resources()
      val patches = ArgumentCaptor.forClass(classOf[Pod])
      verify(podResource, times(2)).patch(any(classOf[PatchContext]), patches.capture())
      val appliedPods = patches.getAllValues.asScala
      assert(appliedPods.exists { pod =>
        Option(pod.getMetadata.getLabels).exists(_.get("soLong") == "cruelWorld")
      })
      assert(appliedPods.exists { pod =>
        Option(pod.getMetadata.getAnnotations).exists(_.get(POD_DELETION_COST) == "7")
      })
    }
  }

  test("SPARK-58879: rejected idle decommission has no pod updates or replay") {
    withDecommissionMetadata { schedulerBackend =>
      val backend = spy[KubernetesClusterSchedulerBackend](schedulerBackend)
      val busyExecutor = registerExecutor(backend, "1")
      when(taskScheduler.isExecutorBusy("1")).thenReturn(true)
      val decomInfo = ExecutorDecommissionInfo("test")

      assert(backend.decommissionExecutorsIfIdle(
        Array.empty[(String, ExecutorDecommissionInfo)],
        adjustTargetNumExecutors = false).isEmpty)
      assert(backend.decommissionExecutorsIfIdle(
        Array("1" -> decomInfo, "2" -> decomInfo, "1" -> decomInfo),
        adjustTargetNumExecutors = false).isEmpty)

      val laterExecutor = registerExecutor(backend, "2")
      schedulerExecutorService.runUntilIdle()
      verify(backend, never()).decommissionExecutors(
        any[Array[(String, ExecutorDecommissionInfo)]](), anyBoolean(), anyBoolean())
      verify(blockManagerMaster, never()).decommissionBlockManagers(any[Seq[String]]())
      verify(busyExecutor, never()).send(DecommissionExecutor)
      verify(laterExecutor, never()).send(DecommissionExecutor)
      verify(kubernetesClient, never()).pods()
    }
  }

  test("SPARK-34407: CoarseGrainedSchedulerBackend.stop may throw SparkException") {
    schedulerBackendUnderTest.start()

    when(driverEndpointRef.askSync[Boolean](StopDriver)).thenThrow(new RuntimeException)
    schedulerBackendUnderTest.stop()

    // Verify the last operation of `schedulerBackendUnderTest.stop`.
    verify(kubernetesClient).close()
  }

  test("stopExecutors() reports a failed StopExecutors RPC as SCHEDULER_BACKEND_SHUTDOWN_FAILED") {
    val rpcFailure = new RuntimeException("StopExecutors timed out")
    when(driverEndpointRef.askSync[Boolean](StopExecutors)).thenThrow(rpcFailure)
    val e = intercept[SparkException] {
      schedulerBackendUnderTest.stopExecutors()
    }
    checkError(
      exception = e,
      condition = "SCHEDULER_BACKEND_SHUTDOWN_FAILED.EXECUTORS",
      sqlState = Some("58030"),
      parameters = Map.empty[String, String])
    assert(e.getCause === rpcFailure)
  }

  test("SPARK-34469: Ignore RegisterExecutor when SparkContext is stopped") {
    when(sc.isStopped).thenReturn(true)
    val endpoint = schedulerBackendUnderTest.createDriverEndpoint()
    endpoint.receiveAndReply(null).apply(
      RegisterExecutor("1", null, "host1", 1, Map.empty, Map.empty, Map.empty, 0))
  }

  test("Dynamically fetch an executor ID") {
    val endpoint = schedulerBackendUnderTest.createDriverEndpoint()
    endpoint.receiveAndReply(context).apply(GenerateExecID("cheeseBurger"))
    verify(context).reply("1")
  }

  test("SPARK-56238: applicationId() is stable across calls when spark.app.id is not set") {
    // Use isolated mocks so we don't mutate the shared sc/rpcEnv state.
    val confWithoutAppId = new SparkConf(false)
      .set("spark.executor.instances", "3")
      .set(KUBERNETES_EXECUTOR_DECOMMISSION_LABEL.key, "soLong")
      .set(KUBERNETES_EXECUTOR_DECOMMISSION_LABEL_VALUE.key, "cruelWorld")
    val localSc = mock(classOf[SparkContext])
    val localEnv = mock(classOf[SparkEnv])
    val localRpcEnv = mock(classOf[RpcEnv])
    when(localSc.conf).thenReturn(confWithoutAppId)
    when(localSc.env).thenReturn(localEnv)
    when(localSc.resourceProfileManager).thenReturn(resourceProfileManager)
    when(localEnv.rpcEnv).thenReturn(localRpcEnv)
    when(localRpcEnv.setupEndpoint(any(), any())).thenReturn(driverEndpointRef)
    val localTaskScheduler = mock(classOf[TaskSchedulerImpl])
    when(localTaskScheduler.sc).thenReturn(localSc)
    val backendWithoutAppId = new KubernetesClusterSchedulerBackend(
      localTaskScheduler,
      localSc,
      kubernetesClient,
      schedulerExecutorService,
      eventQueue,
      podAllocator,
      lifecycleManager,
      watchEvents,
      pollEvents)
    val id1 = backendWithoutAppId.applicationId()
    val id2 = backendWithoutAppId.applicationId()
    assert(id1 === id2, "applicationId() must return the same value on repeated calls")
    assert(id1.startsWith("spark-"), "generated app ID should have the spark- prefix")
  }

  test("SPARK-58915: the executors can be held only with the direct pods allocator") {
    assert(schedulerBackendUnderTest.supportsExecutorHold)
    Seq("statefulset", "deployment", "com.example.CustomAllocator").foreach { allocator =>
      sparkConf.set(KUBERNETES_ALLOCATION_PODS_ALLOCATOR, allocator)
      try {
        assert(!schedulerBackendUnderTest.supportsExecutorHold,
          s"holding must not be supported with the $allocator allocator")
      } finally {
        sparkConf.remove(KUBERNETES_ALLOCATION_PODS_ALLOCATOR.key)
      }
    }
    assert(schedulerBackendUnderTest.supportsExecutorHold)
  }
}
