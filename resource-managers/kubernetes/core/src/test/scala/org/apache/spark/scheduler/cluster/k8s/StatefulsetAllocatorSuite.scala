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

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apps.StatefulSet
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl._
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
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

class StatefulSetAllocatorSuite extends SparkFunSuite with BeforeAndAfter {

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
  private val secondProfile: ResourceProfile = ResourceProfile.getOrCreateDefaultProfile(conf)

  private val secMgr = new SecurityManager(conf)


  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var appOperations: AppsAPIGroupDSL = _

  @Mock
  private var statefulSetOperations: MixedOperation[
    apps.StatefulSet, apps.StatefulSetList, RollableScalableResource[apps.StatefulSet]] = _

  @Mock
  private var editableSet: RollableScalableResource[apps.StatefulSet] = _

  @Mock
  private var podOperations: PODS = _


  @Mock
  private var driverPodOperations: PodResource[Pod] = _

  private var podsAllocatorUnderTest: StatefulsetPodsAllocator = _

  private var snapshotsStore: DeterministicExecutorPodsSnapshotsStore = _

  @Mock
  private var executorBuilder: KubernetesExecutorBuilder = _

  @Mock
  private var schedulerBackend: KubernetesClusterSchedulerBackend = _

  val appId = "testapp"

  private def executorPodAnswer(): Answer[KubernetesExecutorSpec] =
    (invocation: InvocationOnMock) => {
      val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
      KubernetesExecutorSpec(executorPodWithId(0,
        k8sConf.resourceProfileId.toInt), Seq.empty)
  }

  before {
    MockitoAnnotations.openMocks(this).close()
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(kubernetesClient.apps()).thenReturn(appOperations)
    when(appOperations.statefulSets()).thenReturn(statefulSetOperations)
    when(statefulSetOperations.withName(any())).thenReturn(editableSet)
    when(podOperations.withName(driverPodName)).thenReturn(driverPodOperations)
    when(driverPodOperations.get).thenReturn(driverPod)
    when(driverPodOperations.waitUntilReady(any(), any())).thenReturn(driverPod)
    when(executorBuilder.buildFromFeatures(any(classOf[KubernetesExecutorConf]), meq(secMgr),
      meq(kubernetesClient), any(classOf[ResourceProfile]))).thenAnswer(executorPodAnswer())
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()
    podsAllocatorUnderTest = new StatefulsetPodsAllocator(
      conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, null)
    when(schedulerBackend.getExecutorIds).thenReturn(Seq.empty)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID, schedulerBackend)
  }

  test("Validate initial statefulSet creation & cleanup with two resource profiles") {
    val rprof = new ResourceProfileBuilder()
    val taskReq = new TaskResourceRequests().resource("gpu", 1)
    val execReq =
      new ExecutorResourceRequests().resource("gpu", 2, "myscript", "nvidia")
    rprof.require(taskReq).require(execReq)
    val immrprof = new ResourceProfile(rprof.executorResources, rprof.taskResources)
    podsAllocatorUnderTest.setTotalExpectedExecutors(
      Map(defaultProfile -> (10),
          immrprof -> (420)))
    val captor = ArgumentCaptor.forClass(classOf[StatefulSet])
    verify(statefulSetOperations, times(2)).create(any())
    podsAllocatorUnderTest.stop(appId)
    verify(editableSet, times(2)).delete()
  }

  test("Validate statefulSet scale up") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(
      Map(defaultProfile -> (10)))
    val captor = ArgumentCaptor.forClass(classOf[StatefulSet])
    verify(statefulSetOperations, times(1)).create(captor.capture())
    val set = captor.getValue()
    val setName = set.getMetadata().getName()
    val namespace = set.getMetadata().getNamespace()
    assert(namespace === "default")
    val spec = set.getSpec()
    assert(spec.getReplicas() === 10)
    assert(spec.getPodManagementPolicy() === "Parallel")
    verify(podOperations, never()).create(any())
    podsAllocatorUnderTest.setTotalExpectedExecutors(
      Map(defaultProfile -> (20)))
    verify(editableSet, times(1)).scale(any(), any())
  }
}
