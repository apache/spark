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

import java.util.concurrent.atomic.AtomicInteger

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.SparkFunSuite

private[spark] class ExecutorPodBatchSubscriberSuite extends SparkFunSuite {

  test("Check the correctness of states.") {
    val statesDetectionFunction = new StatesDetectionFunction()
    val subscriber = new ExecutorPodBatchSubscriber(statesDetectionFunction, () => {})
    val running = ExecutorLifecycleTestUtils.runningExecutor(1)
    val failed = ExecutorLifecycleTestUtils.failedExecutorWithoutDeletion(2)
    val deleted = ExecutorLifecycleTestUtils.deletedExecutor(3)
    val succeeded = ExecutorLifecycleTestUtils.succeededExecutor(4)
    val unknown = ExecutorLifecycleTestUtils.unknownExecutor(5)
    subscriber.onNextBatch(Seq(running, failed, deleted, succeeded, unknown))
    assert(statesDetectionFunction.podRunning === PodRunning(running))
    assert(statesDetectionFunction.podFailed === PodFailed(failed))
    assert(statesDetectionFunction.podDeleted === PodDeleted(deleted))
    assert(statesDetectionFunction.podSucceeded === PodSucceeded(succeeded))
    assert(statesDetectionFunction.podUnknown === PodUnknown(unknown))
  }

  test("Invoke post batch processor after every batch.") {
    val batchProcessedCounter = new AtomicInteger(0)
    val subscriber = new ExecutorPodBatchSubscriber(
      new StatesDetectionFunction(), () => batchProcessedCounter.getAndIncrement())
    subscriber.onNextBatch(Seq.empty[Pod])
    assert(batchProcessedCounter.get === 1)
    subscriber.onNextBatch(Seq.empty[Pod])
    assert(batchProcessedCounter.get === 2)
  }

  private class StatesDetectionFunction extends PartialFunction[ExecutorPodState, Unit] {
    var podRunning: PodRunning = _
    var podFailed: PodFailed = _
    var podDeleted: PodDeleted = _
    var podSucceeded: PodSucceeded = _
    var podUnknown: PodUnknown = _

    override def isDefinedAt(state: ExecutorPodState): Boolean = {
      state match {
        case PodRunning(_) | PodFailed(_) | PodDeleted(_) | PodSucceeded(_) | PodUnknown(_) => true
        case _ => false
      }
    }

    override def apply(state: ExecutorPodState): Unit = {
      state match {
        case running @ PodRunning(_) => podRunning = running
        case failed @ PodFailed(_) => podFailed = failed
        case deleted @ PodDeleted(_) => podDeleted = deleted
        case succeeded @ PodSucceeded(_) => podSucceeded = succeeded
        case unknown @ PodUnknown(_) => podUnknown = unknown
        case other @ _ => throw new IllegalArgumentException(s"Unknown state $other")
      }
    }
  }

}
