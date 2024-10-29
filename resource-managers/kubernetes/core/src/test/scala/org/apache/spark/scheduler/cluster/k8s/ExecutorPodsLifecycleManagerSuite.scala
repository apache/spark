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

import java.util.function.UnaryOperator

import scala.collection.mutable

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{mock, never, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.deploy.k8s.KubernetesUtils._
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.ExecutorExited
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._
import org.apache.spark.util.{ManualClock, SparkExitCode}

class ExecutorPodsLifecycleManagerSuite extends SparkFunSuite with BeforeAndAfter {

  private var namedExecutorPods: mutable.Map[String, PodResource] = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var podsWithNamespace: PODS_WITH_NAMESPACE = _

  @Mock
  private var schedulerBackend: KubernetesClusterSchedulerBackend = _

  private var snapshotsStore: DeterministicExecutorPodsSnapshotsStore = _
  private var eventHandlerUnderTest: ExecutorPodsLifecycleManager = _

  before {
    MockitoAnnotations.openMocks(this).close()
    val sparkConf = new SparkConf()
      .set(KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME, TEST_SPARK_EXECUTOR_CONTAINER_NAME)
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()
    namedExecutorPods = mutable.Map.empty[String, PodResource]
    when(schedulerBackend.getExecutorsWithRegistrationTs()).thenReturn(Map.empty[String, Long])
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq.empty)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace(anyString())).thenReturn(podsWithNamespace)
    when(podsWithNamespace.withName(any(classOf[String]))).thenAnswer(namedPodsAnswer())
    eventHandlerUnderTest = new ExecutorPodsLifecycleManager(
      sparkConf,
      kubernetesClient,
      snapshotsStore)
    eventHandlerUnderTest.start(schedulerBackend)
  }

  test("SPARK-41210: Window based executor failure tracking mechanism") {
    var _exitCode = -1
    var waitForExecutorPodsClock = new ManualClock(0L)
    val _conf = eventHandlerUnderTest.conf.clone
      .set(MAX_EXECUTOR_FAILURES.key, "2")
      .set(EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS.key, "2s")
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()
    eventHandlerUnderTest = new ExecutorPodsLifecycleManager(_conf,
      kubernetesClient, snapshotsStore, waitForExecutorPodsClock) {
      override private[k8s] def stopApplication(exitCode: Int): Unit = {
        logError("!!!")
        _exitCode = exitCode
      }
    }
    eventHandlerUnderTest.start(schedulerBackend)
    assert(eventHandlerUnderTest.getNumExecutorsFailed === 0)

    waitForExecutorPodsClock.advance(1000)
    snapshotsStore.updatePod(failedExecutorWithoutDeletion(1))
    snapshotsStore.updatePod(failedExecutorWithoutDeletion(2))
    snapshotsStore.notifySubscribers()
    assert(eventHandlerUnderTest.getNumExecutorsFailed === 2)
    assert(_exitCode === -1)

    waitForExecutorPodsClock.advance(1000)
    snapshotsStore.notifySubscribers()
    assert(eventHandlerUnderTest.getNumExecutorsFailed === 2)
    assert(_exitCode === -1)

    waitForExecutorPodsClock.advance(2000)
    assert(eventHandlerUnderTest.getNumExecutorsFailed === 0)
    assert(_exitCode === -1)

    waitForExecutorPodsClock.advance(1000)
    snapshotsStore.updatePod(failedExecutorWithoutDeletion(3))
    snapshotsStore.updatePod(failedExecutorWithoutDeletion(4))
    snapshotsStore.updatePod(failedExecutorWithoutDeletion(5))
    snapshotsStore.notifySubscribers()
    assert(eventHandlerUnderTest.getNumExecutorsFailed === 3)
    assert(_exitCode === SparkExitCode.EXCEED_MAX_EXECUTOR_FAILURES)
  }

  test("When an executor reaches error states immediately, remove from the scheduler backend.") {
    val failedPod = failedExecutorWithoutDeletion(1)
    val mockPodResource = mock(classOf[PodResource])
    namedExecutorPods.put("spark-executor-1", mockPodResource)
    when(mockPodResource.get()).thenReturn(failedPod)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod, 1)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend).doRemoveExecutor("1", expectedLossReason)
    verify(namedExecutorPods(failedPod.getMetadata.getName)).delete()
  }

  test("Don't remove executors twice from Spark but remove from K8s repeatedly.") {
    val failedPod = failedExecutorWithoutDeletion(1)
    val mockPodResource = mock(classOf[PodResource])
    namedExecutorPods.put("spark-executor-1", mockPodResource)
    when(mockPodResource.get()).thenReturn(failedPod)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod, 1)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend, times(1)).doRemoveExecutor("1", expectedLossReason)
    verify(namedExecutorPods(failedPod.getMetadata.getName), times(2)).delete()
  }

  test("Don't remove executors twice from Spark and K8s.") {
    val failedPod = failedExecutorWithoutDeletion(1)
    val mockPodResource = mock(classOf[PodResource])
    namedExecutorPods.put("spark-executor-1", mockPodResource)
    when(mockPodResource.get()).thenReturn(failedPod)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod, 1)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend, times(1)).doRemoveExecutor("1", expectedLossReason)
    verify(namedExecutorPods(failedPod.getMetadata.getName), times(1)).delete()

    // Now remove the pod from K8s
    when(mockPodResource.get()).thenReturn(null)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    verify(schedulerBackend, times(1)).doRemoveExecutor("1", expectedLossReason)
    verify(namedExecutorPods(failedPod.getMetadata.getName), times(1)).delete()
  }

  test("When the scheduler backend lists executor ids that aren't present in the cluster," +
    " remove those executors from Spark.") {
      when(schedulerBackend.getExecutorsWithRegistrationTs()).thenReturn(Map("1" -> 7L))
    val missingPodDelta =
      eventHandlerUnderTest.conf.get(Config.KUBERNETES_EXECUTOR_MISSING_POD_DETECT_DELTA)
    snapshotsStore.clock.advance(missingPodDelta + 7)
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    snapshotsStore.notifySubscribers()
    verify(schedulerBackend, never()).doRemoveExecutor(any(), any())

    // 1 more millisecond and the accepted delta is over so the missing POD will be detected
    snapshotsStore.clock.advance(1)
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    snapshotsStore.notifySubscribers()
    val msg = "The executor with ID 1 (registered at 7 ms) was not found in the cluster at " +
      "the polling time (30008 ms) which is after the accepted detect delta time (30000 ms) " +
      "configured by `spark.kubernetes.executor.missingPodDetectDelta`. The executor may have " +
      "been deleted but the driver missed the deletion event. Marking this executor as failed."
    val expectedLossReason = ExecutorExited(-1, exitCausedByApp = false, msg)
    verify(schedulerBackend).doRemoveExecutor("1", expectedLossReason)
  }

  test("SPARK-40458: test executor inactivation function") {
    val failedPod = failedExecutorWithoutDeletion(1)
    val inactivated = ExecutorPodsLifecycleManager.executorInactivationFn(failedPod)
    assert(inactivated.getMetadata().getLabels().get(SPARK_EXECUTOR_INACTIVE_LABEL) === "true")
  }

  test("Keep executor pods in k8s if configured.") {
    val failedPod = failedExecutorWithoutDeletion(1)
    eventHandlerUnderTest.conf.set(Config.KUBERNETES_DELETE_EXECUTORS, false)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod, 1)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend).doRemoveExecutor("1", expectedLossReason)
    verify(namedExecutorPods(failedPod.getMetadata.getName), never()).delete()
    verify(namedExecutorPods(failedPod.getMetadata.getName))
      .edit(any[UnaryOperator[Pod]]())
  }

  test("SPARK-49804: Use the exit code of executor container always") {
    val failedPod = failedExecutorWithSidecarStatusListedFirst(1)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod, 1)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend).doRemoveExecutor("1", expectedLossReason)
  }

  private def exitReasonMessage(execId: Int, failedPod: Pod, exitCode: Int): String = {
    val reason = Option(failedPod.getStatus.getReason)
    val message = Option(failedPod.getStatus.getMessage)
    val explained = ExecutorPodsLifecycleManager.describeExitCode(exitCode)
    val exitMsg = s"The executor with id $execId exited with exit code $explained."
    val reasonStr = reason.map(r => s"The API gave the following brief reason: ${r}")
    val msgStr = message.map(m => s"The API gave the following message: ${m}")


    s"""
       |${exitMsg}
       |${reasonStr.getOrElse("")}
       |${msgStr.getOrElse("")}
       |
       |The API gave the following container statuses:
       |
       |${containersDescription(failedPod)}
      """.stripMargin
  }

  private def namedPodsAnswer(): Answer[PodResource] =
    (invocation: InvocationOnMock) => {
      val podName: String = invocation.getArgument(0)
      namedExecutorPods.getOrElseUpdate(
        podName, mock(classOf[PodResource]))
    }
}
