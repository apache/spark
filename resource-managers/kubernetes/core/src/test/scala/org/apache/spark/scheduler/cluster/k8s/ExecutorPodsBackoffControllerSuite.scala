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
    val (controller, _) = createController()

    assert(controller.isInNormalState())
    assert(!controller.isInBackoffState())
    assert(controller.getCurrentState() == Normal)
    assert(controller.canRequestNow())
    assert(controller.startupFailureCountInWindow() == 0)
    assert(controller.currentStateDescription() == "Normal (recent startup failures: 0)")

    for (i <- 1 to 10) {
      assert(controller.canRequestNow())
      controller.recordPodRequest(i)
      assert(controller.isInNormalState())
    }

    Random.shuffle(List.range(1, 11)).foreach { i =>
      controller.recordExecutorStarted(i)
      assert(controller.isInNormalState())
      assert(controller.canRequestNow())
    }
  }

  test("failure for executor that was seen started is not counted") {
    val (controller, _) = createController()

    controller.recordPodRequest(1L)
    controller.recordPodRequest(2L)
    controller.recordExecutorStarted(2L)
    controller.recordExecutorStarted(1L)
    controller.recordFailure(1L)
    controller.recordFailure(2L)

    assert(controller.startupFailureCountInWindow() == 0)
    assert(controller.isInNormalState())
  }

  test("duplicate failure for same executor is ignored") {
    val (controller, _) = createController()

    controller.recordPodRequest(1L)
    controller.recordFailure(1L)
    controller.recordFailure(1L) // duplicate

    assert(controller.isInNormalState())
    assert(controller.startupFailureCountInWindow() == 1)
    assert(controller.currentStateDescription() == "Normal (recent startup failures: 1)")
  }

  test("failures within sliding window accumulate to trigger Backoff") {
    val (controller, clock) = createController()

    for (i <- 1 to FAILURE_THRESHOLD) {
      controller.recordPodRequest(i)
    }
    for (i <- 1 to FAILURE_THRESHOLD) {
      clock.advance(5000) // increments within window
      controller.recordFailure(i.toLong)
    }
    assert(controller.isInBackoffState())
    assert(controller.currentStateDescription() ==
      "Backoff (attempts in backoff: 0, current delay: 10s)")
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
    assert(controller.isInNormalState())

    // record second failure at t=20s (within window)
    clock.advance(20 * 1000L)
    controller.recordPodRequest(2L)
    controller.recordFailure(2L)
    assert(controller.startupFailureCountInWindow() == 2)
    assert(controller.isInNormalState())

    // advance to t=61s - first failure (at t=0) slides out of window
    clock.advance(41 * 1000L)
    assert(controller.startupFailureCountInWindow() == 1)
    assert(controller.isInNormalState())

    // advance to t=81s - second failure (at t=20s) also slides out
    clock.advance(20 * 1000L)
    assert(controller.startupFailureCountInWindow() == 0)
    assert(controller.isInNormalState())
  }

  test("exponential backoff delay capped by MAX_DELAY") {
    val (controller, clock) = createController()

    transitionToBackoff(controller)

    // delays: 10s, 20s, 40s, 80s, 120s (capped), 120s, 120s...
    // iterate enough times to reach and verify the cap
    for (i <- 0 until 10) {
      val expectedDelay = Math.min(INITIAL_DELAY_MS * (1L << i), MAX_DELAY_MS)

      assert(controller.isInBackoffState(), s"should remain in Backoff")
      assert(controller.calculateBackoffDelay() == expectedDelay)

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
    assert(controller.calculateBackoffDelay() == MAX_DELAY_MS)
  }

  test("recordExecutorStarted for executor requested before Backoff does not exit Backoff") {
    val (controller, _) = createController()

    controller.recordPodRequest(1L)
    for (i <- 2 to 3) {
      controller.recordPodRequest(i)
      controller.recordFailure(i)
    }

    assert(controller.isInBackoffState())
    controller.recordExecutorStarted(1L)
    assert(controller.isInBackoffState())
  }

  test("recordExecutorStarted for executor requested during Backoff transitions to Normal") {
    val (controller, clock) = createController()

    transitionToBackoff(controller)

    clock.advance(INITIAL_DELAY_MS + 1)
    controller.recordPodRequest(100L)
    controller.recordExecutorStarted(100L)

    assert(controller.isInNormalState())
    assert(controller.startupFailureCountInWindow() == 0)
    assert(controller.canRequestNow())
  }

  test("multiple Backoff -> Normal transitions") {
    val (controller, clock) = createController()

    for (cycle <- 1 to 3) {
      // enter Backoff
      for (i <- 1 to FAILURE_THRESHOLD) {
        controller.recordPodRequest(cycle * 100 + i)
        controller.recordFailure(cycle * 100 + i)
      }
      assert(controller.isInBackoffState())

      // exit Backoff
      clock.advance(INITIAL_DELAY_MS)
      val successExecId = cycle * 100 + 99
      controller.recordPodRequest(successExecId)
      controller.recordExecutorStarted(successExecId)
      assert(controller.isInNormalState())
    }
  }

  test("duplicate recordExecutorStarted calls are handled gracefully") {
    val (controller, _) = createController()
    controller.recordPodRequest(1)
    controller.recordExecutorStarted(1)
    controller.recordExecutorStarted(1) // duplicate
    assert(controller.isInNormalState())
  }

  test("multiple executors can be tracked simultaneously") {
    val (controller, _) = createController()

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

    assert(controller.isInBackoffState())
  }

  test("backoff delay calculation doesn't overflow") {
    val (controller, clock) = createController()
    transitionToBackoff(controller)

    // make many requests to potentially overflow
    for (i <- 0 until 2048) {
      clock.advance(MAX_DELAY_MS + 1)
      controller.recordPodRequest(100 + i)
      assert(controller.calculateBackoffDelay() > 0)
      assert(controller.calculateBackoffDelay() <= MAX_DELAY_MS)
    }

    assert(controller.calculateBackoffDelay() == MAX_DELAY_MS)
  }

  private def createController(): (ExecutorPodsBackoffController, ManualClock) = {
    val clock = new ManualClock(0L)
    val conf = new SparkConf()
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_THRESHOLD, FAILURE_THRESHOLD)
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_FAILURE_INTERVAL, FAILURE_INTERVAL_MS)
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_INITIAL_DELAY, INITIAL_DELAY_MS)
      .set(KUBERNETES_ALLOCATION_EXECUTOR_BACKOFF_MAX_DELAY, MAX_DELAY_MS)
    val controller = new ExecutorPodsBackoffController(conf, clock)
    (controller, clock)
  }

  private def transitionToBackoff(controller: ExecutorPodsBackoffController): Unit = {
    for (i <- 1 to FAILURE_THRESHOLD) {
      controller.recordPodRequest(i.toLong)
      controller.recordFailure(i.toLong)
    }
    assert(controller.isInBackoffState())
  }
}
