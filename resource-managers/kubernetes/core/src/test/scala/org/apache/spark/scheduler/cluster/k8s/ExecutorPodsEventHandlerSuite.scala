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

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite

class ExecutorPodsEventHandlerSuite extends SparkFunSuite with BeforeAndAfter {

  // TODO
  /*
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

  private var namedExecutorPods: mutable.Map[String, PodResource[Pod, DoneablePod]] = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: Pods = _

  @Mock
  private var driverPodOperations: PodResource[Pod, DoneablePod] = _

  @Mock
  private var executorBuilder: KubernetesExecutorBuilder = _

  @Mock
  private var schedulerBackend: KubernetesClusterSchedulerBackend = _

  private var eventHandlerUnderTest: ExecutorPodsLifecycleEventHandler = _

  before {
    MockitoAnnotations.initMocks(this)
    namedExecutorPods = mutable.Map.empty[String, PodResource[Pod, DoneablePod]]
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(any(classOf[String]))).thenAnswer(namedPodsAnswer())
    when(podOperations.withName(driverPodName)).thenReturn(driverPodOperations)
    when(driverPodOperations.get).thenReturn(driverPod)
    when(executorBuilder.buildFromFeatures(kubernetesConfWithCorrectFields()))
      .thenAnswer(executorPodAnswer())
    eventHandlerUnderTest = new ExecutorPodsLifecycleEventHandler(
      conf, executorBuilder, kubernetesClient, eventProcessorExecutor)
    eventHandlerUnderTest.start(appId, schedulerBackend)
  }

  test("Initially request executors in batches. Do not request another batch if the" +
    " first has not finished.") {
    eventHandlerUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    runProcessor()
    for (nextId <- 1 to podAllocationSize) {
      verify(podOperations).create(podWithAttachedContainerForId(nextId))
    }
    verify(podOperations, never()).create(
      podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("Request executors in batches. Allow another batch to be requested if" +
    " all pending executors start running.") {
    eventHandlerUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    runProcessor()
    for (execId <- 1 until podAllocationSize) {
      eventHandlerUnderTest.sendUpdatedPodMetadata(runningExecutor(execId))
    }
    runProcessor()
    verify(podOperations, never()).create(
      podWithAttachedContainerForId(podAllocationSize + 1))
    eventHandlerUnderTest.sendUpdatedPodMetadata(runningExecutor(podAllocationSize))
    runProcessor()
    verify(podOperations).create(podWithAttachedContainerForId(podAllocationSize + 1))
    runProcessor()
    verify(podOperations, times(podAllocationSize + 1)).create(any(classOf[Pod]))
  }

  test("When a current batch reaches error states immediately, re-request" +
    " them on the next batch.") {
    eventHandlerUnderTest.setTotalExpectedExecutors(podAllocationSize)
    runProcessor()
    for (execId <- 1 until podAllocationSize) {
      eventHandlerUnderTest.sendUpdatedPodMetadata(runningExecutor(execId))
    }
    val failedPod = failedExecutorWithoutDeletion(podAllocationSize)
    eventHandlerUnderTest.sendUpdatedPodMetadata(failedPod)
    runProcessor()
    val msg = exitReasonMessage(podAllocationSize, failedPod)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend).doRemoveExecutor(podAllocationSize.toString, expectedLossReason)
    verify(podOperations).create(podWithAttachedContainerForId(podAllocationSize + 1))
    verify(namedExecutorPods(failedPod.getMetadata.getName)).delete()
  }

  test("When a current batch reaches a running state and then one executor reaches an error" +
    " state, re-request it on the next batch.") {
    eventHandlerUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    runProcessor()
    for (execId <- 1 to podAllocationSize) {
      eventHandlerUnderTest.sendUpdatedPodMetadata(runningExecutor(execId))
    }
    runProcessor()
    eventHandlerUnderTest.sendUpdatedPodMetadata(runningExecutor(podAllocationSize + 1))
    val failedExecutorId = podAllocationSize - 1
    val failedPod = failedExecutorWithoutDeletion(failedExecutorId)
    eventHandlerUnderTest.sendUpdatedPodMetadata(failedPod)
    runProcessor()
    val msg = exitReasonMessage(failedExecutorId, failedPod)
    val expectedLossReason = ExecutorExited(1, exitCausedByApp = true, msg)
    verify(schedulerBackend).doRemoveExecutor(failedExecutorId.toString, expectedLossReason)
    verify(podOperations).create(podWithAttachedContainerForId(podAllocationSize + 2))
    verify(namedExecutorPods(failedPod.getMetadata.getName)).delete()
  }

  private def exitReasonMessage(failedExecutorId: Int, failedPod: Pod): String = {
    s"""
       |The executor with id $failedExecutorId exited with exit code 1.
       |The API gave the following brief reason: ${failedPod.getStatus.getReason}
       |The API gave the following message: ${failedPod.getStatus.getMessage}
       |The API gave the following container statuses:
       |
       |${failedPod.getStatus.getContainerStatuses.asScala.map(_.toString).mkString("\n===\n")}
      """.stripMargin
  }

  private def runProcessor(): Unit = {
    eventProcessorExecutor.tick(podAllocationDelay, TimeUnit.MILLISECONDS)
  }

  private def runningExecutor(executorId: Int): Pod = {
    new PodBuilder(podWithAttachedContainerForId(executorId))
      .editOrNewStatus()
        .withPhase("running")
        .endStatus()
      .build()
  }

  private def failedExecutorWithoutDeletion(executorId: Int): Pod = {
    new PodBuilder(podWithAttachedContainerForId(executorId))
      .editOrNewStatus()
        .withPhase("error")
        .addNewContainerStatus()
          .withName("spark-executor")
          .withImage("k8s-spark")
          .withNewState()
            .withNewTerminated()
              .withMessage("Failed")
              .withExitCode(1)
              .endTerminated()
            .endState()
          .endContainerStatus()
        .addNewContainerStatus()
          .withName("spark-executor-sidecar")
          .withImage("k8s-spark-sidecar")
          .withNewState()
            .withNewTerminated()
              .withMessage("Failed")
              .withExitCode(1)
              .endTerminated()
            .endState()
          .endContainerStatus()
        .withMessage("Executor failed.")
        .withReason("Executor failed because of a thrown error.")
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

  private def namedPodsAnswer(): Answer[PodResource[Pod, DoneablePod]] = {
    new Answer[PodResource[Pod, DoneablePod]] {
      override def answer(invocation: InvocationOnMock): PodResource[Pod, DoneablePod] = {
        val podName = invocation.getArgumentAt(0, classOf[String])
        namedExecutorPods.getOrElseUpdate(
          podName, mock(classOf[PodResource[Pod, DoneablePod]]))
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
  */
}
