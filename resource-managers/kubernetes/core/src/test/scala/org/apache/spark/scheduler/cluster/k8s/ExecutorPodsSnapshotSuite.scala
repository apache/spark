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

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class ExecutorPodsSnapshotSuite extends SparkFunSuite {

  test("States are interpreted correctly from pod metadata.") {

    def testCase(pod: Pod, state: Pod => ExecutorPodState): (Pod, ExecutorPodState) =
      (pod, state(pod))

    val testCases = Seq(
      testCase(pendingExecutor(0), PodPending),
      testCase(runningExecutor(1), PodRunning),
      testCase(succeededExecutor(2), PodSucceeded),
      testCase(failedExecutorWithoutDeletion(3), PodFailed),
      testCase(deletedExecutor(4), PodDeleted),
      testCase(unknownExecutor(5), PodUnknown),
      testCase(runningExecutorWithFailedContainer(6), PodFailed))

    val snapshot = ExecutorPodsSnapshot(testCases.map(_._1))

    for (((_, state), i) <- testCases.zipWithIndex) {
      assertResult(state.getClass.getName, s"executor ID ${i}") {
        snapshot.executorPods.get(i).get.getClass.getName
      }
    }
  }


  test("Updates add new pods for non-matching ids and edit existing pods for matching ids") {
    val originalPods = Seq(
      pendingExecutor(0),
      runningExecutor(1))
    val originalSnapshot = ExecutorPodsSnapshot(originalPods)
    val snapshotWithUpdatedPod = originalSnapshot.withUpdate(succeededExecutor(1))
    assert(snapshotWithUpdatedPod.executorPods ===
      Map(
        0L -> PodPending(originalPods(0)),
        1L -> PodSucceeded(succeededExecutor(1))))
    val snapshotWithNewPod = snapshotWithUpdatedPod.withUpdate(pendingExecutor(2))
    assert(snapshotWithNewPod.executorPods ===
      Map(
        0L -> PodPending(originalPods(0)),
        1L -> PodSucceeded(succeededExecutor(1)),
        2L -> PodPending(pendingExecutor(2))))
  }
}
