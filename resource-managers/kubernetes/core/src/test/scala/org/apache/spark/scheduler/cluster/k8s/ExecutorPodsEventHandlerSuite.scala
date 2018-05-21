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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, DoneablePod, Pod, PodBuilder, PodList}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{MixedOperation, PodResource}
import org.jmock.lib.concurrent.DeterministicScheduler
import org.mockito.{ArgumentMatcher, Matchers, MockitoAnnotations}
import org.mockito.Mockito.{never, verify, when}
import org.mockito.MockitoAnnotations.Mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesExecutorSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

class ExecutorPodsEventHandlerSuite extends SparkFunSuite with BeforeAndAfter {

  private type Pods = MixedOperation[Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]

  private val driverPodName = "driver"

  private val appId = "spark"

  private val driverPod = new PodBuilder()
    .withNewMetadata()
      .withName(driverPodName)
      .addToLabels(SPARK_APP_ID_LABEL, appId)
      .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_DRIVER_ROLE)
      .withUid("driver-pod-uid")
      .endMetadata()
    .build()

  private val conf = new SparkConf().set(KUBERNETES_DRIVER_POD_NAME, driverPodName)

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)

  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)

  private val eventProcessorExecutor = new DeterministicScheduler

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: Pods = _

  @Mock
  private var namedPods: PodResource[Pod, DoneablePod] = _

  @Mock
  private var executorBuilder: KubernetesExecutorBuilder = _

  @Mock
  private var schedulerBackend: KubernetesClusterSchedulerBackend = _

  private var eventHandlerUnderTest: ExecutorPodsEventHandler = _

  before {
    MockitoAnnotations.initMocks(this)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(driverPodName)).thenReturn(namedPods)
    when(namedPods.get).thenReturn(driverPod)
    when(executorBuilder.buildFromFeatures(kubernetesConfWithCorrectFields()))
      .thenAnswer(executorPodAnswer())
    eventHandlerUnderTest = new ExecutorPodsEventHandler(
      conf, executorBuilder, kubernetesClient, eventProcessorExecutor)
    eventHandlerUnderTest.start(appId, schedulerBackend)
  }

  test("Initially request executors in batches. Do not request another batch if the" +
    " first has not finished.") {
    eventHandlerUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    eventProcessorExecutor.tick(podAllocationDelay, TimeUnit.MILLISECONDS)
    for (nextId <- 1 to podAllocationSize) {
      verify(podOperations).create(podWithAttachedContainerForId(nextId))
    }
    verify(podOperations, never()).create(
      podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("Request executors in batches. Allow another batch to be requested if" +
    " all pending executors start running.") {
    eventHandlerUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    eventProcessorExecutor.tick(podAllocationDelay, TimeUnit.MILLISECONDS)
    for (execId <- 1 until podAllocationSize) {
      eventHandlerUnderTest.sendUpdatedPodMetadata(runExecutor(execId))
    }
    eventProcessorExecutor.tick(podAllocationDelay, TimeUnit.MILLISECONDS)
    verify(podOperations, never()).create(
      podWithAttachedContainerForId(podAllocationSize + 1))
    eventHandlerUnderTest.sendUpdatedPodMetadata(runExecutor(podAllocationSize))
    eventProcessorExecutor.tick(podAllocationDelay, TimeUnit.MILLISECONDS)
    verify(podOperations).create(podWithAttachedContainerForId(podAllocationSize + 1))
    verify(podOperations, never()).create(podWithAttachedContainerForId(podAllocationSize + 2))
  }

  private def runExecutor(executorId: Int): Pod = {
    new PodBuilder(podWithAttachedContainerForId(executorId))
      .editOrNewStatus()
        .withPhase("running")
        .endStatus()
      .build()
  }

  private def kubernetesConfWithCorrectFields(): KubernetesConf[KubernetesExecutorSpecificConf] =
    Matchers.argThat(new ArgumentMatcher[KubernetesConf[KubernetesExecutorSpecificConf]] {
      override def matches(argument: scala.Any): Boolean = {
        if (!argument.isInstanceOf[KubernetesConf[KubernetesExecutorSpecificConf]]) {
          false
        } else {
          val k8sConf = argument.asInstanceOf[KubernetesConf[KubernetesExecutorSpecificConf]]
          val executorSpecificConf = k8sConf.roleSpecificConf
          val expectedK8sConf = KubernetesConf.createExecutorConf(
            conf,
            executorSpecificConf.executorId,
            appId,
            driverPod)
          k8sConf.sparkConf.getAll.toMap == conf.getAll.toMap &&
              // Since KubernetesConf.createExecutorConf clones the SparkConf object, force
              // deep equality comparison for the SparkConf object and use object equality
              // comparison on all other fields.
              k8sConf.copy(sparkConf = conf) == expectedK8sConf.copy(sparkConf = conf)
        }
      }
    })

  private def executorPodAnswer(): Answer[SparkPod] = {
    new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        val k8sConf = invocation.getArgumentAt(
          0, classOf[KubernetesConf[KubernetesExecutorSpecificConf]])
        executorPodWithId(k8sConf.roleSpecificConf.executorId)
      }
    }
  }

  private def podWithAttachedContainerForId(executorId: Int): Pod = {
    val sparkPod = executorPodWithId(executorId.toString)
    val podWithAttachedContainer = new PodBuilder(sparkPod.pod)
      .editOrNewSpec()
      .addToContainers(sparkPod.container)
      .endSpec()
      .build()
    podWithAttachedContainer
  }

  private def executorPodWithId(executorId: String): SparkPod = {
    val pod = new PodBuilder()
      .withNewMetadata()
      .withName(s"spark-executor-$executorId")
      .addToLabels(SPARK_APP_ID_LABEL, appId)
      .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      .addToLabels(SPARK_EXECUTOR_ID_LABEL, executorId)
      .endMetadata()
      .build()
    val container = new ContainerBuilder()
      .withName("spark-executor")
      .withImage("k8s-spark")
      .build()
    SparkPod(pod, container)
  }
}
