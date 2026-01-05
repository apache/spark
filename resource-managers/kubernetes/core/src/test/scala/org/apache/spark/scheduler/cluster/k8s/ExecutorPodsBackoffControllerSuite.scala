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

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.scheduler.cluster.k8s.ExecutorPodsBackoffController._
import org.apache.spark.util.ManualClock

class ExecutorPodsBackoffControllerSuite extends SparkFunSuite {

  private val FAILURE_THRESHOLD = 2
  private val FAILURE_INTERVAL_MS = 10 * 60 * 1000L // 10 minutes
  private val INITIAL_DELAY_MS = 10 * 1000L // 10 seconds
  private val MAX_DELAY_MS = 2 * 60 * 1000L // 2 minutes

  test("Normal state") {
    val (controller, _, metrics) = createController()

    assert(controller.isNormalState())
    assert(!controller.isBackoffState())
    assert(controller.canRequestNow())
    assert(controller.startupFailureCountInWindow() == 0)
    assert(controller.currentStateDescription() == "Normal (recent startup failures: 0)")
    assert(metrics.startupFailureCounter.getCount == 0)
    assert(metrics.backoffEntryCounter.getCount == 0)
    assert(metrics.backoffExitCounter.getCount == 0)

    for (i <- 1 to 10) {
      assert(controller.canRequestNow())
      controller.recordPodRequest(i)
      assert(controller.isNormalState())
    }

    Random.shuffle(List.range(1, 11)).foreach { i =>
      controller.recordExecutorStarted(i)
      assert(controller.isNormalState())
      assert(controller.canRequestNow())
    }

    assert(metrics.startupFailureCounter.getCount == 0)
    assert(metrics.backoffEntryCounter.getCount == 0)
  }

  test("failure for executor that was seen started is not counted") {
    val (controller, _, metrics) = createController()

    controller.recordPodRequest(1L)
    controller.recordPodRequest(2L)
    controller.recordExecutorStarted(2L)
    controller.recordExecutorStarted(1L)
    controller.recordFailure(1L)
    controller.recordFailure(2L)

    assert(controller.startupFailureCountInWindow() == 0)
    assert(controller.isNormalState())
    assert(metrics.startupFailureCounter.getCount == 0)
  }

  test("duplicate failure for same executor is ignored") {
    val (controller, _, metrics) = createController()

    controller.recordPodRequest(1L)
    controller.recordFailure(1L)
    controller.recordFailure(1L) // duplicate

    assert(controller.isNormalState())
    assert(controller.startupFailureCountInWindow() == 1)
    assert(controller.currentStateDescription() == "Normal (recent startup failures: 1)")
    assert(metrics.startupFailureCounter.getCount == 1)
  }

  test("failures within sliding window accumulate to trigger Backoff") {
    val (controller, clock, metrics) = createController()

    for (i <- 1 to FAILURE_THRESHOLD) {
      controller.recordPodRequest(i)
    }
    for (i <- 1 to FAILURE_THRESHOLD) {
      clock.advance(5000) // increments within window
      controller.recordFailure(i.toLong)
    }
    assert(controller.isBackoffState())
    assert(controller.currentStateDescription() ==
      "Backoff (attempts in backoff: 0, current delay: 10s)")
    assert(metrics.startupFailureCounter.getCount == FAILURE_THRESHOLD)
    assert(metrics.backoffEntryCounter.getCount == 1)
  }

  test("failures outside the sliding window are not counted") {
    val clock = new ManualClock(0L)
    val conf = new SparkConf()
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_THRESHOLD, 3)
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_INTERVAL, 60 * 1000L) // 1 minute
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_INITIAL_DELAY, INITIAL_DELAY_MS)
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_MAX_DELAY, MAX_DELAY_MS)
    val controller = new ExecutorPodsBackoffController(conf, clock)

    // record first failure at t=0
    controller.recordPodRequest(1L)
    controller.recordFailure(1L)
    assert(controller.startupFailureCountInWindow() == 1)
    assert(controller.isNormalState())

    // record second failure at t=20s (within window)
    clock.advance(20 * 1000L)
    controller.recordPodRequest(2L)
    controller.recordFailure(2L)
    assert(controller.startupFailureCountInWindow() == 2)
    assert(controller.isNormalState())

    // advance to t=61s - first failure (at t=0) slides out of window
    clock.advance(41 * 1000L)
    assert(controller.startupFailureCountInWindow() == 1)
    assert(controller.isNormalState())

    // advance to t=81s - second failure (at t=20s) also slides out
    clock.advance(20 * 1000L)
    assert(controller.startupFailureCountInWindow() == 0)
    assert(controller.isNormalState())
  }

  test("exponential backoff delay capped by MAX_DELAY") {
    val (controller, clock, _) = createController()

    transitionToBackoff(controller)

    // delays: 10s, 20s, 40s, 80s, 120s (capped), 120s, 120s...
    // iterate enough times to reach and verify the cap
    for (i <- 0 until 10) {
      val expectedDelay = Math.min(INITIAL_DELAY_MS * (1L << i), MAX_DELAY_MS)

      assert(controller.isBackoffState())

      // cannot request before delay is over
      clock.advance(expectedDelay - 1)
      assert(!controller.canRequestNow())

      // can request exactly when delay is over
      clock.advance(1)
      assert(controller.canRequestNow())
      controller.recordPodRequest(100 + i)

      // cannot request immediately after
      assert(!controller.canRequestNow())
    }
  }

  test("recordExecutorStarted for executor requested before Backoff does not exit Backoff") {
    val (controller, _, _) = createController()

    controller.recordPodRequest(1L)
    for (i <- 2 to 3) {
      controller.recordPodRequest(i)
      controller.recordFailure(i)
    }

    assert(controller.isBackoffState())
    controller.recordExecutorStarted(1L)
    assert(controller.isBackoffState())
  }

  test("recordExecutorStarted for executor requested during Backoff transitions to Normal") {
    val (controller, clock, metrics) = createController()

    transitionToBackoff(controller)
    assert(metrics.backoffEntryCounter.getCount == 1)
    assert(metrics.backoffExitCounter.getCount == 0)

    clock.advance(INITIAL_DELAY_MS + 1)
    controller.recordPodRequest(100L)
    controller.recordExecutorStarted(100L)

    assert(controller.isNormalState())
    assert(controller.startupFailureCountInWindow() == 0)
    assert(controller.canRequestNow())
    assert(metrics.backoffEntryCounter.getCount == 1)
    assert(metrics.backoffExitCounter.getCount == 1)
  }

  test("multiple Backoff -> Normal transitions") {
    val (controller, clock, metrics) = createController()

    for (cycle <- 1 to 3) {
      // enter Backoff
      for (i <- 1 to FAILURE_THRESHOLD) {
        controller.recordPodRequest(cycle * 100 + i)
        controller.recordFailure(cycle * 100 + i)
      }
      assert(controller.isBackoffState())
      assert(metrics.backoffEntryCounter.getCount == cycle)

      // exit Backoff
      clock.advance(INITIAL_DELAY_MS)
      val successExecId = cycle * 100 + 99
      controller.recordPodRequest(successExecId)
      controller.recordExecutorStarted(successExecId)
      assert(controller.isNormalState())
      assert(metrics.backoffExitCounter.getCount == cycle)
    }
  }

  test("duplicate recordExecutorStarted calls are handled gracefully") {
    val (controller, _, _) = createController()
    controller.recordPodRequest(1)
    controller.recordExecutorStarted(1)
    controller.recordExecutorStarted(1) // duplicate
    assert(controller.isNormalState())
  }

  test("multiple executors can be tracked simultaneously") {
    val (controller, _, _) = createController()

    // request multiple executors
    for (i <- 1 to 10) {
      controller.recordPodRequest(i.toLong)
    }

    // fail some, start others
    for (i <- 1 to 5) {
      controller.recordFailure(i.toLong)
    }
    for (i <- 6 to 10) {
      controller.recordExecutorStarted(i.toLong)
    }

    assert(controller.isBackoffState())
  }

  test("backoff exit then re-entry with mixed success and failures") {
    val (controller, clock, metrics) = createController()

    for (i <- 1 to FAILURE_THRESHOLD) {
      controller.recordPodRequest(i.toLong)
      controller.recordFailure(i.toLong)
    }
    assert(controller.isBackoffState())
    clock.advance(INITIAL_DELAY_MS)
    // request 3 executors while in backoff
    controller.recordPodRequest(10L)
    controller.recordPodRequest(11L)
    controller.recordPodRequest(12L)
    assert(controller.isBackoffState())

    // first executor succeeds - should exit backoff
    controller.recordExecutorStarted(10L)
    assert(controller.isNormalState())
    assert(controller.startupFailureCountInWindow() == 0)

    // two other executors fail - should accumulate failures and re-enter backoff
    controller.recordFailure(11L)
    assert(controller.isNormalState())
    assert(controller.startupFailureCountInWindow() == 1)

    controller.recordFailure(12L)
    assert(controller.isBackoffState())
    assert(metrics.backoffEntryCounter.getCount == 2)
    assert(metrics.backoffExitCounter.getCount == 1)
  }

  test("backoff delay calculation doesn't overflow") {
    val (controller, _, _) = createController()

    // delays: 10s, 20s, 40s, 80s, 120s (capped), 120s, 120s...
    for (i <- 0 until 2048) {
      val backoff = BackoffState(
        requestedExecutors = mutable.HashSet((0 until i).map(_.toLong): _*),
        delayReferenceTime = 0L)
      val delay = controller.calculateBackoffDelay(backoff)
      if (i >= 4) {
        assert(delay == MAX_DELAY_MS, s"delay should be capped at MAX_DELAY_MS for $i requests")
      } else {
        assert(delay > 0 && delay < MAX_DELAY_MS)
      }
    }
  }

  private def createController():
  (ExecutorPodsBackoffController, ManualClock, ExecutorPodsBackoffControllerSource) = {
    val clock = new ManualClock(0L)
    val conf = new SparkConf()
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_THRESHOLD, FAILURE_THRESHOLD)
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_INTERVAL, FAILURE_INTERVAL_MS)
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_INITIAL_DELAY, INITIAL_DELAY_MS)
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_MAX_DELAY, MAX_DELAY_MS)
    val controller = new ExecutorPodsBackoffController(conf, clock)
    (controller, clock, controller.metricsSource)
  }

  private def transitionToBackoff(controller: ExecutorPodsBackoffController): Unit = {
    for (i <- 1 to FAILURE_THRESHOLD) {
      controller.recordPodRequest(i.toLong)
      controller.recordFailure(i.toLong)
    }
    assert(controller.isBackoffState())
  }
}
