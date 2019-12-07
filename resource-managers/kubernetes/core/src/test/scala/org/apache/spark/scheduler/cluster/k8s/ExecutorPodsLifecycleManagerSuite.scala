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

import com.google.common.cache.CacheBuilder
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, never, times, verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.deploy.k8s.KubernetesUtils._
import org.apache.spark.scheduler.ExecutorExited
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class ExecutorPodsLifecycleManagerSuite extends SparkFunSuite with BeforeAndAfter {

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var schedulerBackend: KubernetesClusterSchedulerBackend = _

  @Mock
  private var executorPodController: ExecutorPodController = _

  private var snapshotsStore: DeterministicExecutorPodsSnapshotsStore = _
  private var eventHandlerUnderTest: ExecutorPodsLifecycleManager = _

  before {
    MockitoAnnotations.initMocks(this)
    val removedExecutorsCache = CacheBuilder.newBuilder().build[java.lang.Long, java.lang.Long]
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq.empty[String])
    when(kubernetesClient.pods()).thenReturn(podOperations)
    doNothing().when(executorPodController).initialize(kubernetesClient, TEST_SPARK_APP_ID)
    eventHandlerUnderTest = new ExecutorPodsLifecycleManager(
      new SparkConf(),
      kubernetesClient,
      snapshotsStore,
      removedExecutorsCache)
    eventHandlerUnderTest.start(schedulerBackend, executorPodController)
  }

  test("When an executor reaches error states immediately, remove from the scheduler backend.") {
    val failedPod = failedExecutorWithoutDeletion(1)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend).doRemoveExecutor("1", expectedLossReason)
    verify(executorPodController).removePodById("1")
    verify(executorPodController, times(1)).commitAndGetTotalDeleted()
  }

  test("Don't remove executors twice from Spark but remove from K8s repeatedly.") {
    val failedPod = failedExecutorWithoutDeletion(1)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend, times(1)).doRemoveExecutor("1", expectedLossReason)
    verify(executorPodController, times(2)).removePodById("1")
    verify(executorPodController, times(1)).commitAndGetTotalDeleted()
  }

  test("When the scheduler backend lists executor ids that aren't present in the cluster," +
    " remove those executors from Spark.") {
    when(schedulerBackend.getExecutorIds()).thenReturn(Seq("1"))
    val msg = s"The executor with ID 1 was not found in the cluster but we didn't" +
      s" get a reason why. Marking the executor as failed. The executor may have been" +
      s" deleted but the driver missed the deletion event."
    val expectedLossReason = ExecutorExited(-1, exitCausedByApp = false, msg)
    snapshotsStore.replaceSnapshot(Seq.empty[Pod])
    snapshotsStore.notifySubscribers()
    verify(schedulerBackend).doRemoveExecutor("1", expectedLossReason)
    verify(executorPodController, never()).removePodById(any())
  }

  test("Keep executor pods in k8s if configured.") {
    val failedPod = failedExecutorWithoutDeletion(1)
    eventHandlerUnderTest.conf.set(Config.KUBERNETES_DELETE_EXECUTORS, false)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    val msg = exitReasonMessage(1, failedPod)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend).doRemoveExecutor("1", expectedLossReason)
    verify(executorPodController, never()).removePodById(any())
  }

  private def exitReasonMessage(failedExecutorId: Int, failedPod: Pod): String = {
    val reason = Option(failedPod.getStatus.getReason)
    val message = Option(failedPod.getStatus.getMessage)
    s"""
       |The executor with id $failedExecutorId exited with exit code 1.
       |The API gave the following brief reason: ${reason.getOrElse("N/A")}
       |The API gave the following message: ${message.getOrElse("N/A")}
       |The API gave the following container statuses:
       |
       |${containersDescription(failedPod)}
      """.stripMargin
  }
}
