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

import io.fabric8.kubernetes.api.model.{Pod, PodList}
import io.fabric8.kubernetes.client.KubernetesClient
import org.jmock.lib.concurrent.DeterministicScheduler
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{mock, never, spy, verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.resource.{ResourceProfile, ResourceProfileManager}
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{ExecutorKilled, LiveListenerBus, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RemoveExecutor, StopDriver}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils.TEST_SPARK_APP_ID

class KubernetesClusterSchedulerBackendSuite extends SparkFunSuite with BeforeAndAfter {

  private val schedulerExecutorService = new DeterministicScheduler()
  private val sparkConf = new SparkConf(false)
    .set("spark.executor.instances", "3")
    .set("spark.app.id", TEST_SPARK_APP_ID)

  @Mock
  private var sc: SparkContext = _

  @Mock
  private var env: SparkEnv = _

  @Mock
  private var rpcEnv: RpcEnv = _

  @Mock
  private var driverEndpointRef: RpcEndpointRef = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var labeledPods: LABELED_PODS = _

  @Mock
  private var configMapsOperations: CONFIG_MAPS = _

  @Mock
  private var labledConfigMaps: LABELED_CONFIG_MAPS = _

  @Mock
  private var taskScheduler: TaskSchedulerImpl = _

  @Mock
  private var eventQueue: ExecutorPodsSnapshotsStore = _

  @Mock
  private var podAllocator: ExecutorPodsAllocator = _

  @Mock
  private var lifecycleEventHandler: ExecutorPodsLifecycleManager = _

  @Mock
  private var watchEvents: ExecutorPodsWatchSnapshotSource = _

  @Mock
  private var pollEvents: ExecutorPodsPollingSnapshotSource = _

  private var driverEndpoint: ArgumentCaptor[RpcEndpoint] = _
  private var schedulerBackendUnderTest: KubernetesClusterSchedulerBackend = _

  private val listenerBus = new LiveListenerBus(new SparkConf())
  private val resourceProfileManager = new ResourceProfileManager(sparkConf, listenerBus)
  private val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)

  before {
    MockitoAnnotations.initMocks(this)
    when(taskScheduler.sc).thenReturn(sc)
    when(sc.conf).thenReturn(sparkConf)
    when(sc.resourceProfileManager).thenReturn(resourceProfileManager)
    when(sc.env).thenReturn(env)
    when(env.rpcEnv).thenReturn(rpcEnv)
    driverEndpoint = ArgumentCaptor.forClass(classOf[RpcEndpoint])
    when(
      rpcEnv.setupEndpoint(
        mockitoEq(CoarseGrainedSchedulerBackend.ENDPOINT_NAME),
        driverEndpoint.capture()))
      .thenReturn(driverEndpointRef)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(kubernetesClient.configMaps()).thenReturn(configMapsOperations)
    when(podAllocator.driverPod).thenReturn(None)
    schedulerBackendUnderTest = new KubernetesClusterSchedulerBackend(
      taskScheduler,
      sc,
      kubernetesClient,
      schedulerExecutorService,
      eventQueue,
      podAllocator,
      lifecycleEventHandler,
      watchEvents,
      pollEvents)
  }

  test("Start all components") {
    schedulerBackendUnderTest.start()
    verify(podAllocator).setTotalExpectedExecutors(Map(defaultProfile -> 3))
    verify(podAllocator).start(TEST_SPARK_APP_ID, schedulerBackendUnderTest)
    verify(lifecycleEventHandler).start(schedulerBackendUnderTest)
    verify(watchEvents).start(TEST_SPARK_APP_ID)
    verify(pollEvents).start(TEST_SPARK_APP_ID)
    verify(configMapsOperations).create(any())
  }

  test("Stop all components") {
    when(podOperations.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(labeledPods)
    when(configMapsOperations.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(labledConfigMaps)
    when(labledConfigMaps.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(labledConfigMaps)
    schedulerBackendUnderTest.stop()
    verify(eventQueue).stop()
    verify(watchEvents).stop()
    verify(pollEvents).stop()
    verify(labeledPods).delete()
    verify(labledConfigMaps).delete()
    verify(kubernetesClient).close()
  }

  test("Remove executor") {
    val backend = spy(schedulerBackendUnderTest)
    when(backend.isExecutorActive(any())).thenReturn(false)
    when(backend.isExecutorActive(mockitoEq("2"))).thenReturn(true)

    backend.start()
    backend.doRemoveExecutor("1", ExecutorKilled)
    verify(driverEndpointRef, never()).send(RemoveExecutor("1", ExecutorKilled))

    backend.doRemoveExecutor("2", ExecutorKilled)
    verify(driverEndpointRef, never()).send(RemoveExecutor("1", ExecutorKilled))
  }

  test("Kill executors") {
    schedulerBackendUnderTest.start()
    when(podOperations.withField(any(), any())).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(labeledPods)
    when(labeledPods.withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1", "2")).thenReturn(labeledPods)

    val podList = mock(classOf[PodList])
    when(labeledPods.list()).thenReturn(podList)
    when(podList.getItems()).thenReturn(Arrays.asList[Pod]())

    schedulerBackendUnderTest.doKillExecutors(Seq("1", "2"))
    verify(driverEndpointRef).send(RemoveExecutor("1", ExecutorKilled))
    verify(driverEndpointRef).send(RemoveExecutor("2", ExecutorKilled))
    verify(labeledPods, never()).delete()
    schedulerExecutorService.tick(sparkConf.get(KUBERNETES_DYN_ALLOC_KILL_GRACE_PERIOD) * 2,
      TimeUnit.MILLISECONDS)
    verify(labeledPods, never()).delete()

    when(podList.getItems()).thenReturn(Arrays.asList(mock(classOf[Pod])))
    schedulerBackendUnderTest.doKillExecutors(Seq("1", "2"))
    verify(labeledPods, never()).delete()
    schedulerExecutorService.tick(sparkConf.get(KUBERNETES_DYN_ALLOC_KILL_GRACE_PERIOD) * 2,
      TimeUnit.MILLISECONDS)
    verify(labeledPods).delete()
  }

  test("SPARK-34407: CoarseGrainedSchedulerBackend.stop may throw SparkException") {
    schedulerBackendUnderTest.start()

    when(driverEndpointRef.askSync[Boolean](StopDriver)).thenThrow(new RuntimeException)
    schedulerBackendUnderTest.stop()

    // Verify the last operation of `schedulerBackendUnderTest.stop`.
    verify(kubernetesClient).close()
  }

  test("SPARK-34469: Ignore RegisterExecutor when SparkContext is stopped") {
    when(sc.isStopped).thenReturn(true)
    val endpoint = schedulerBackendUnderTest.createDriverEndpoint()
    endpoint.receiveAndReply(null).apply(
      RegisterExecutor("1", null, "host1", 1, Map.empty, Map.empty, Map.empty, 0))
  }
}
