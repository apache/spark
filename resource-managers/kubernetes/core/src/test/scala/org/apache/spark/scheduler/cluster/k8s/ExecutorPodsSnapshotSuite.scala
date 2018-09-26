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

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class ExecutorPodsSnapshotSuite extends SparkFunSuite {

  test("States are interpreted correctly from pod metadata.") {
    val pods = Seq(
      pendingExecutor(0),
      runningExecutor(1),
      succeededExecutor(2),
      failedExecutorWithoutDeletion(3),
      deletedExecutor(4),
      unknownExecutor(5))
    val snapshot = ExecutorPodsSnapshot(pods)
    assert(snapshot.executorPods ===
      Map(
        0L -> PodPending(pods(0)),
        1L -> PodRunning(pods(1)),
        2L -> PodSucceeded(pods(2)),
        3L -> PodFailed(pods(3)),
        4L -> PodDeleted(pods(4)),
        5L -> PodUnknown(pods(5))))
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
