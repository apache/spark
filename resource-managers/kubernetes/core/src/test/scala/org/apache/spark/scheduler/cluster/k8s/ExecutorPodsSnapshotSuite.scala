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

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.Constants.SPARK_EXECUTOR_INACTIVE_LABEL
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class ExecutorPodsSnapshotSuite extends SparkFunSuite {

  def testCase(pod: Pod, state: Pod => ExecutorPodState): (Pod, ExecutorPodState) =
    (pod, state(pod))

  def doTest(testCases: Seq[(Pod, ExecutorPodState)]): Unit = {
    val snapshot = ExecutorPodsSnapshot(testCases.map(_._1), 0)
    for (((_, state), i) <- testCases.zipWithIndex) {
      assertResult(state.getClass.getName, s"executor ID $i") {
        snapshot.executorPods(i).getClass.getName
      }
    }
  }

  test("States are interpreted correctly from pod metadata.") {
    ExecutorPodsSnapshot.setShouldCheckAllContainers(false)
    val testCases = Seq(
      testCase(pendingExecutor(0), PodPending),
      testCase(runningExecutor(1), PodRunning),
      testCase(succeededExecutor(2), PodSucceeded),
      testCase(failedExecutorWithoutDeletion(3), PodFailed),
      testCase(deletedExecutor(4), PodDeleted),
      testCase(unknownExecutor(5), PodUnknown),
      testCase(finishedExecutorWithRunningSidecar(6, 0), PodSucceeded),
      testCase(finishedExecutorWithRunningSidecar(7, 1), PodFailed)
    )
    doTest(testCases)
  }

  test("SPARK-30821: States are interpreted correctly from pod metadata"
    + " when configured to check all containers.") {
    ExecutorPodsSnapshot.setShouldCheckAllContainers(true)
    val testCases = Seq(
      testCase(pendingExecutor(0), PodPending),
      testCase(runningExecutor(1), PodRunning),
      testCase(runningExecutorWithFailedContainer(2), PodFailed),
      testCase(succeededExecutor(3), PodSucceeded),
      testCase(failedExecutorWithoutDeletion(4), PodFailed),
      testCase(deletedExecutor(5), PodDeleted),
      testCase(unknownExecutor(6), PodUnknown)
    )
    doTest(testCases)
  }

  test("Updates add new pods for non-matching ids and edit existing pods for matching ids") {
    ExecutorPodsSnapshot.setShouldCheckAllContainers(false)
    val originalPods = Seq(
      pendingExecutor(0),
      runningExecutor(1))
    val originalSnapshot = ExecutorPodsSnapshot(originalPods, 0)
    val snapshotWithUpdatedPod = originalSnapshot.withUpdate(succeededExecutor(1))
    assert(snapshotWithUpdatedPod.executorPods ===
      Map(
        0L -> PodPending(originalPods(0)),
        1L -> PodSucceeded(succeededExecutor(1))))
    val pendingExec = pendingExecutor(2)
    val snapshotWithNewPod = snapshotWithUpdatedPod.withUpdate(pendingExec)
    assert(snapshotWithNewPod.executorPods ===
      Map(
        0L -> PodPending(originalPods(0)),
        1L -> PodSucceeded(succeededExecutor(1)),
        2L -> PodPending(pendingExec)))
  }

  test("Lifecycle index retains terminal pods and follows inactive label changes") {
    ExecutorPodsSnapshot.setShouldCheckAllContainers(false)
    val inactive = new PodBuilder(runningExecutor(2)).editMetadata()
      .addToLabels(SPARK_EXECUTOR_INACTIVE_LABEL, "true").endMetadata().build()
    val original = ExecutorPodsSnapshot(Seq(
      runningExecutor(1), inactive, failedExecutorWithoutDeletion(3), succeededExecutor(4)), 123)
    assert(original.lifecyclePods.keySet == Set(2L, 3L, 4L))

    val changed = original.withUpdate(runningExecutor(2)).withUpdate(succeededExecutor(1))
    assert(changed.lifecyclePods.keySet == Set(1L, 3L, 4L))
    assert(changed.lifecyclePods(1).isInstanceOf[PodSucceeded])
    assert(changed.fullSnapshotTs == 123)
    assert(original.lifecyclePods.keySet == Set(2L, 3L, 4L))

    val inactiveAgain = changed.withUpdate(inactive)
    assert(inactiveAgain.lifecyclePods.keySet == Set(1L, 2L, 3L, 4L))
    val recovered = inactiveAgain.withUpdate(runningExecutor(3))
    assert(!recovered.lifecyclePods.contains(3L))

    val replaced = ExecutorPodsSnapshot(Seq(runningExecutor(1)), 456)
    assert(replaced.lifecyclePods.isEmpty)
  }
}
