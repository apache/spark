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

package org.apache.spark

import scala.collection.mutable

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.PrivateMethodTester

import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests.TEST_SCHEDULE_INTERVAL
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.util.{Clock, ManualClock, SystemClock}

/**
 * Test add and remove behavior of ExecutorAllocationManager.
 */
class ExecutorAllocationManagerSuite extends SparkFunSuite {

  import ExecutorAllocationManager._
  import ExecutorAllocationManagerSuite._

  private val managers = new mutable.ListBuffer[ExecutorAllocationManager]()
  private var listenerBus: LiveListenerBus = _
  private var client: ExecutorAllocationClient = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    managers.clear()
    listenerBus = new LiveListenerBus(new SparkConf())
    listenerBus.start(null, mock(classOf[MetricsSystem]))
    client = mock(classOf[ExecutorAllocationClient])
    when(client.isExecutorActive(any())).thenReturn(true)
  }

  override def afterEach(): Unit = {
    try {
      listenerBus.stop()
      managers.foreach(_.stop())
    } finally {
      listenerBus = null
      super.afterEach()
    }
  }

  private def post(event: SparkListenerEvent): Unit = {
    listenerBus.post(event)
    listenerBus.waitUntilEmpty(1000)
  }

  test("initialize dynamic allocation in SparkContext") {
    val conf = createConf(0, 1, 0)
      .setMaster("local-cluster[1,1,1024]")
      .setAppName(getClass().getName())

    val sc0 = new SparkContext(conf)
    try {
      assert(sc0.executorAllocationManager.isDefined)
    } finally {
      sc0.stop()
    }
  }

  test("verify min/max executors") {
    // Min < 0
    intercept[SparkException] {
      createManager(createConf().set(config.DYN_ALLOCATION_MIN_EXECUTORS, -1))
    }

    // Max < 0
    intercept[SparkException] {
      createManager(createConf().set(config.DYN_ALLOCATION_MAX_EXECUTORS, -1))
    }

    // Both min and max, but min > max
    intercept[SparkException] {
      createManager(createConf(2, 1))
    }

    // Both min and max, and min == max
    createManager(createConf(1, 1))

    // Both min and max, and min < max
    createManager(createConf(1, 2))
  }

  test("starting state") {
    val manager = createManager(createConf())
    assert(numExecutorsTarget(manager) === 1)
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(addTime(manager) === ExecutorAllocationManager.NOT_SET)
  }

  test("add executors") {
    val manager = createManager(createConf(1, 10, 1))
    post(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Keep adding until the limit is reached
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 2)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 4)
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 4)
    assert(numExecutorsTarget(manager) === 8)
    assert(numExecutorsToAdd(manager) === 8)
    assert(addExecutors(manager) === 2) // reached the limit of 10
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)

    // Register previously requested executors
    onExecutorAdded(manager, "first")
    assert(numExecutorsTarget(manager) === 10)
    onExecutorAdded(manager, "second")
    onExecutorAdded(manager, "third")
    onExecutorAdded(manager, "fourth")
    assert(numExecutorsTarget(manager) === 10)
    onExecutorAdded(manager, "first") // duplicates should not count
    onExecutorAdded(manager, "second")
    assert(numExecutorsTarget(manager) === 10)

    // Try adding again
    // This should still fail because the number pending + running is still at the limit
    assert(addExecutors(manager) === 0)
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)
  }

  def testAllocationRatio(cores: Int, divisor: Double, expected: Int): Unit = {
    val conf = createConf(3, 15)
      .set(config.DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO, divisor)
      .set(config.EXECUTOR_CORES, cores)
    val manager = createManager(conf)
    post(SparkListenerStageSubmitted(createStageInfo(0, 20)))
    for (i <- 0 to 5) {
      addExecutors(manager)
    }
    assert(numExecutorsTarget(manager) === expected)
  }

  test("executionAllocationRatio is correctly handled") {
    testAllocationRatio(1, 0.5, 10)
    testAllocationRatio(1, 1.0/3.0, 7)
    testAllocationRatio(2, 1.0/3.0, 4)
    testAllocationRatio(1, 0.385, 8)

    // max/min executors capping
    testAllocationRatio(1, 1.0, 15) // should be 20 but capped by max
    testAllocationRatio(4, 1.0/3.0, 3)  // should be 2 but elevated by min
  }


  test("add executors capped by num pending tasks") {
    val manager = createManager(createConf(0, 10, 0))
    post(SparkListenerStageSubmitted(createStageInfo(0, 5)))

    // Verify that we're capped at number of tasks in the stage
    assert(numExecutorsTarget(manager) === 0)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 3)
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 5)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that running a task doesn't affect the target
    post(SparkListenerStageSubmitted(createStageInfo(1, 3)))
    post(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty, Map.empty)))
    post(SparkListenerTaskStart(1, 0, createTaskInfo(0, 0, "executor-1")))
    assert(numExecutorsTarget(manager) === 5)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 6)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 8)
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsTarget(manager) === 8)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that re-running a task doesn't blow things up
    post(SparkListenerStageSubmitted(createStageInfo(2, 3)))
    post(SparkListenerTaskStart(2, 0, createTaskInfo(0, 0, "executor-1")))
    post(SparkListenerTaskStart(2, 0, createTaskInfo(1, 0, "executor-1")))
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 9)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that running a task once we're at our limit doesn't blow things up
    post(SparkListenerTaskStart(2, 0, createTaskInfo(0, 1, "executor-1")))
    assert(addExecutors(manager) === 0)
    assert(numExecutorsTarget(manager) === 10)
  }

  test("add executors when speculative tasks added") {
    val manager = createManager(createConf(0, 10, 0))

    // Verify that we're capped at number of tasks including the speculative ones in the stage
    post(SparkListenerSpeculativeTaskSubmitted(1))
    assert(numExecutorsTarget(manager) === 0)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 1)
    post(SparkListenerSpeculativeTaskSubmitted(1))
    post(SparkListenerSpeculativeTaskSubmitted(1))
    post(SparkListenerStageSubmitted(createStageInfo(1, 2)))
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 3)
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 5)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that running a task doesn't affect the target
    post(SparkListenerTaskStart(1, 0, createTaskInfo(0, 0, "executor-1")))
    assert(numExecutorsTarget(manager) === 5)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that running a speculative task doesn't affect the target
    post(SparkListenerTaskStart(1, 0, createTaskInfo(1, 0, "executor-2", true)))
    assert(numExecutorsTarget(manager) === 5)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsToAdd(manager) === 1)
  }

  test("properly handle task end events from completed stages") {
    val manager = createManager(createConf(0, 10, 0))

    // We simulate having a stage fail, but with tasks still running.  Then another attempt for
    // that stage is started, and we get task completions from the first stage attempt.  Make sure
    // the value of `totalTasksRunning` is consistent as tasks finish from both attempts (we count
    // all running tasks, from the zombie & non-zombie attempts)
    val stage = createStageInfo(0, 5)
    post(SparkListenerStageSubmitted(stage))
    val taskInfo1 = createTaskInfo(0, 0, "executor-1")
    val taskInfo2 = createTaskInfo(1, 1, "executor-1")
    post(SparkListenerTaskStart(0, 0, taskInfo1))
    post(SparkListenerTaskStart(0, 0, taskInfo2))

    // The tasks in the zombie attempt haven't completed yet, so we still count them
    post(SparkListenerStageCompleted(stage))

    // There are still two tasks that belong to the zombie stage running.
    assert(totalRunningTasks(manager) === 2)

    // submit another attempt for the stage.  We count completions from the first zombie attempt
    val stageAttempt1 = createStageInfo(stage.stageId, 5, attemptId = 1)
    post(SparkListenerStageSubmitted(stageAttempt1))
    post(SparkListenerTaskEnd(0, 0, null, Success, taskInfo1, null))
    assert(totalRunningTasks(manager) === 1)
    val attemptTaskInfo1 = createTaskInfo(3, 0, "executor-1")
    val attemptTaskInfo2 = createTaskInfo(4, 1, "executor-1")
    post(SparkListenerTaskStart(0, 1, attemptTaskInfo1))
    post(SparkListenerTaskStart(0, 1, attemptTaskInfo2))
    assert(totalRunningTasks(manager) === 3)
    post(SparkListenerTaskEnd(0, 1, null, Success, attemptTaskInfo1, null))
    assert(totalRunningTasks(manager) === 2)
    post(SparkListenerTaskEnd(0, 0, null, Success, taskInfo2, null))
    assert(totalRunningTasks(manager) === 1)
    post(SparkListenerTaskEnd(0, 1, null, Success, attemptTaskInfo2, null))
    assert(totalRunningTasks(manager) === 0)
  }

  testRetry("cancel pending executors when no longer needed") {
    val manager = createManager(createConf(0, 10, 0))
    post(SparkListenerStageSubmitted(createStageInfo(2, 5)))

    assert(numExecutorsTarget(manager) === 0)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 3)

    val task1Info = createTaskInfo(0, 0, "executor-1")
    post(SparkListenerTaskStart(2, 0, task1Info))

    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 2)

    val task2Info = createTaskInfo(1, 0, "executor-1")
    post(SparkListenerTaskStart(2, 0, task2Info))

    task1Info.markFinished(TaskState.FINISHED, System.currentTimeMillis())
    post(SparkListenerTaskEnd(2, 0, null, Success, task1Info, null))

    task2Info.markFinished(TaskState.FINISHED, System.currentTimeMillis())
    post(SparkListenerTaskEnd(2, 0, null, Success, task2Info, null))

    assert(adjustRequestedExecutors(manager) === -1)
  }

  test("remove executors") {
    val manager = createManager(createConf(5, 10, 5))
    (1 to 10).map(_.toString).foreach { id => onExecutorAdded(manager, id) }

    // Keep removing until the limit is reached
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutor(manager, "1"))
    assert(executorsPendingToRemove(manager).size === 1)
    assert(executorsPendingToRemove(manager).contains("1"))
    assert(removeExecutor(manager, "2"))
    assert(removeExecutor(manager, "3"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(executorsPendingToRemove(manager).contains("2"))
    assert(executorsPendingToRemove(manager).contains("3"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(removeExecutor(manager, "4"))
    assert(removeExecutor(manager, "5"))
    assert(!removeExecutor(manager, "6")) // reached the limit of 5
    assert(executorsPendingToRemove(manager).size === 5)
    assert(executorsPendingToRemove(manager).contains("4"))
    assert(executorsPendingToRemove(manager).contains("5"))
    assert(!executorsPendingToRemove(manager).contains("6"))

    // Kill executors previously requested to remove
    onExecutorRemoved(manager, "1")
    assert(executorsPendingToRemove(manager).size === 4)
    assert(!executorsPendingToRemove(manager).contains("1"))
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")
    assert(executorsPendingToRemove(manager).size === 2)
    assert(!executorsPendingToRemove(manager).contains("2"))
    assert(!executorsPendingToRemove(manager).contains("3"))
    onExecutorRemoved(manager, "2") // duplicates should not count
    onExecutorRemoved(manager, "3")
    assert(executorsPendingToRemove(manager).size === 2)
    onExecutorRemoved(manager, "4")
    onExecutorRemoved(manager, "5")
    assert(executorsPendingToRemove(manager).isEmpty)

    // Try removing again
    // This should still fail because the number pending + running is still at the limit
    assert(!removeExecutor(manager, "7"))
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(!removeExecutor(manager, "8"))
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test("remove multiple executors") {
    val manager = createManager(createConf(5, 10, 5))
    (1 to 10).map(_.toString).foreach { id => onExecutorAdded(manager, id) }

    // Keep removing until the limit is reached
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutors(manager, Seq("1")) === Seq("1"))
    assert(executorsPendingToRemove(manager).size === 1)
    assert(executorsPendingToRemove(manager).contains("1"))
    assert(removeExecutors(manager, Seq("2", "3")) === Seq("2", "3"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(executorsPendingToRemove(manager).contains("2"))
    assert(executorsPendingToRemove(manager).contains("3"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(removeExecutor(manager, "4"))
    assert(removeExecutors(manager, Seq("5")) === Seq("5"))
    assert(!removeExecutor(manager, "6")) // reached the limit of 5
    assert(executorsPendingToRemove(manager).size === 5)
    assert(executorsPendingToRemove(manager).contains("4"))
    assert(executorsPendingToRemove(manager).contains("5"))
    assert(!executorsPendingToRemove(manager).contains("6"))

    // Kill executors previously requested to remove
    onExecutorRemoved(manager, "1")
    assert(executorsPendingToRemove(manager).size === 4)
    assert(!executorsPendingToRemove(manager).contains("1"))
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")
    assert(executorsPendingToRemove(manager).size === 2)
    assert(!executorsPendingToRemove(manager).contains("2"))
    assert(!executorsPendingToRemove(manager).contains("3"))
    onExecutorRemoved(manager, "2") // duplicates should not count
    onExecutorRemoved(manager, "3")
    assert(executorsPendingToRemove(manager).size === 2)
    onExecutorRemoved(manager, "4")
    onExecutorRemoved(manager, "5")
    assert(executorsPendingToRemove(manager).isEmpty)

    // Try removing again
    // This should still fail because the number pending + running is still at the limit
    assert(!removeExecutor(manager, "7"))
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutors(manager, Seq("8")) !== Seq("8"))
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test ("Removing with various numExecutorsTarget condition") {
    val manager = createManager(createConf(5, 12, 5))

    post(SparkListenerStageSubmitted(createStageInfo(0, 8)))

    // Remove when numExecutorsTarget is the same as the current number of executors
    assert(addExecutors(manager) === 1)
    assert(addExecutors(manager) === 2)
    (1 to 8).foreach(execId => onExecutorAdded(manager, execId.toString))
    (1 to 8).map { i => createTaskInfo(i, i, s"$i") }.foreach {
      info => post(SparkListenerTaskStart(0, 0, info)) }
    assert(manager.executorMonitor.executorCount === 8)
    assert(numExecutorsTarget(manager) === 8)
    assert(maxNumExecutorsNeeded(manager) == 8)
    assert(!removeExecutor(manager, "1")) // won't work since numExecutorsTarget == numExecutors

    // Remove executors when numExecutorsTarget is lower than current number of executors
    (1 to 3).map { i => createTaskInfo(i, i, s"$i") }.foreach { info =>
      post(SparkListenerTaskEnd(0, 0, null, Success, info, null))
    }
    adjustRequestedExecutors(manager)
    assert(manager.executorMonitor.executorCount === 8)
    assert(numExecutorsTarget(manager) === 5)
    assert(maxNumExecutorsNeeded(manager) == 5)
    assert(removeExecutor(manager, "1"))
    assert(removeExecutors(manager, Seq("2", "3"))=== Seq("2", "3"))
    onExecutorRemoved(manager, "1")
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")

    // numExecutorsTarget is lower than minNumExecutors
    post(SparkListenerTaskEnd(0, 0, null, Success, createTaskInfo(4, 4, "4"), null))
    assert(manager.executorMonitor.executorCount === 5)
    assert(numExecutorsTarget(manager) === 5)
    assert(maxNumExecutorsNeeded(manager) == 4)
    assert(!removeExecutor(manager, "4")) // lower limit
    assert(addExecutors(manager) === 0) // upper limit
  }

  test ("interleaving add and remove") {
    val manager = createManager(createConf(5, 12, 5))
    post(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Add a few executors
    assert(addExecutors(manager) === 1)
    assert(addExecutors(manager) === 2)
    onExecutorAdded(manager, "1")
    onExecutorAdded(manager, "2")
    onExecutorAdded(manager, "3")
    onExecutorAdded(manager, "4")
    onExecutorAdded(manager, "5")
    onExecutorAdded(manager, "6")
    onExecutorAdded(manager, "7")
    onExecutorAdded(manager, "8")
    assert(manager.executorMonitor.executorCount === 8)
    assert(numExecutorsTarget(manager) === 8)


    // Remove when numTargetExecutors is equal to the current number of executors
    assert(!removeExecutor(manager, "1"))
    assert(removeExecutors(manager, Seq("2", "3")) !== Seq("2", "3"))

    // Remove until limit
    onExecutorAdded(manager, "9")
    onExecutorAdded(manager, "10")
    onExecutorAdded(manager, "11")
    onExecutorAdded(manager, "12")
    assert(manager.executorMonitor.executorCount === 12)
    assert(numExecutorsTarget(manager) === 8)

    assert(removeExecutor(manager, "1"))
    assert(removeExecutors(manager, Seq("2", "3", "4")) === Seq("2", "3", "4"))
    assert(!removeExecutor(manager, "5")) // lower limit reached
    assert(!removeExecutor(manager, "6"))
    onExecutorRemoved(manager, "1")
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")
    onExecutorRemoved(manager, "4")
    assert(manager.executorMonitor.executorCount === 8)

    // Add until limit
    assert(!removeExecutor(manager, "7")) // still at lower limit
    assert((manager, Seq("8")) !== Seq("8"))
    onExecutorAdded(manager, "13")
    onExecutorAdded(manager, "14")
    onExecutorAdded(manager, "15")
    onExecutorAdded(manager, "16")
    assert(manager.executorMonitor.executorCount === 12)

    // Remove succeeds again, now that we are no longer at the lower limit
    assert(removeExecutors(manager, Seq("5", "6", "7")) === Seq("5", "6", "7"))
    assert(removeExecutor(manager, "8"))
    assert(manager.executorMonitor.executorCount === 12)
    onExecutorRemoved(manager, "5")
    onExecutorRemoved(manager, "6")
    assert(manager.executorMonitor.executorCount === 10)
    assert(numExecutorsToAdd(manager) === 4)
    onExecutorRemoved(manager, "9")
    onExecutorRemoved(manager, "10")
    assert(addExecutors(manager) === 4) // at upper limit
    onExecutorAdded(manager, "17")
    onExecutorAdded(manager, "18")
    assert(manager.executorMonitor.executorCount === 10)
    assert(addExecutors(manager) === 0) // still at upper limit
    onExecutorAdded(manager, "19")
    onExecutorAdded(manager, "20")
    assert(manager.executorMonitor.executorCount === 12)
    assert(numExecutorsTarget(manager) === 12)
  }

  test("starting/canceling add timer") {
    val clock = new ManualClock(8888L)
    val manager = createManager(createConf(2, 10, 2), clock = clock)

    // Starting add timer is idempotent
    assert(addTime(manager) === NOT_SET)
    onSchedulerBacklogged(manager)
    val firstAddTime = addTime(manager)
    assert(firstAddTime === clock.getTimeMillis + schedulerBacklogTimeout * 1000)
    clock.advance(100L)
    onSchedulerBacklogged(manager)
    assert(addTime(manager) === firstAddTime) // timer is already started
    clock.advance(200L)
    onSchedulerBacklogged(manager)
    assert(addTime(manager) === firstAddTime)
    onSchedulerQueueEmpty(manager)

    // Restart add timer
    clock.advance(1000L)
    assert(addTime(manager) === NOT_SET)
    onSchedulerBacklogged(manager)
    val secondAddTime = addTime(manager)
    assert(secondAddTime === clock.getTimeMillis + schedulerBacklogTimeout * 1000)
    clock.advance(100L)
    onSchedulerBacklogged(manager)
    assert(addTime(manager) === secondAddTime) // timer is already started
    assert(addTime(manager) !== firstAddTime)
    assert(firstAddTime !== secondAddTime)
  }

  test("mock polling loop with no events") {
    val clock = new ManualClock(2020L)
    val manager = createManager(createConf(0, 20, 0), clock = clock)

    // No events - we should not be adding or removing
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(100L)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(1000L)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(10000L)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test("mock polling loop add behavior") {
    val clock = new ManualClock(2020L)
    val manager = createManager(createConf(0, 20, 0), clock = clock)
    post(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Scheduler queue backlogged
    onSchedulerBacklogged(manager)
    clock.advance(schedulerBacklogTimeout * 1000 / 2)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 0) // timer not exceeded yet
    clock.advance(schedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 1) // first timer exceeded
    clock.advance(sustainedSchedulerBacklogTimeout * 1000 / 2)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 1) // second timer not exceeded yet
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 1 + 2) // second timer exceeded
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 1 + 2 + 4) // third timer exceeded

    // Scheduler queue drained
    onSchedulerQueueEmpty(manager)
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7) // timer is canceled
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7)

    // Scheduler queue backlogged again
    onSchedulerBacklogged(manager)
    clock.advance(schedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7 + 1) // timer restarted
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7 + 1 + 2)
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 7 + 1 + 2 + 4)
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTarget(manager) === 20) // limit reached
  }

  test("mock polling loop remove behavior") {
    val clock = new ManualClock(2020L)
    val manager = createManager(createConf(1, 20, 1), clock = clock)

    // Remove idle executors on timeout
    onExecutorAdded(manager, "executor-1")
    onExecutorAdded(manager, "executor-2")
    onExecutorAdded(manager, "executor-3")
    assert(executorsPendingToRemove(manager).isEmpty)

    // idle threshold not reached yet
    clock.advance(executorIdleTimeout * 1000 / 2)
    schedule(manager)
    assert(manager.executorMonitor.timedOutExecutors().isEmpty)
    assert(executorsPendingToRemove(manager).isEmpty)

    // idle threshold exceeded
    clock.advance(executorIdleTimeout * 1000)
    assert(manager.executorMonitor.timedOutExecutors().size === 3)
    schedule(manager)
    assert(executorsPendingToRemove(manager).size === 2) // limit reached (1 executor remaining)

    // Mark a subset as busy - only idle executors should be removed
    onExecutorAdded(manager, "executor-4")
    onExecutorAdded(manager, "executor-5")
    onExecutorAdded(manager, "executor-6")
    onExecutorAdded(manager, "executor-7")
    assert(manager.executorMonitor.executorCount === 7)
    assert(executorsPendingToRemove(manager).size === 2) // 2 pending to be removed
    onExecutorBusy(manager, "executor-4")
    onExecutorBusy(manager, "executor-5")
    onExecutorBusy(manager, "executor-6") // 3 busy and 2 idle (of the 5 active ones)

    // after scheduling, the previously timed out executor should be removed, since
    // there are new active ones.
    schedule(manager)
    assert(executorsPendingToRemove(manager).size === 3)

    // advance the clock so that idle executors should time out and move to the pending list
    clock.advance(executorIdleTimeout * 1000)
    schedule(manager)
    assert(executorsPendingToRemove(manager).size === 4)
    assert(!executorsPendingToRemove(manager).contains("executor-4"))
    assert(!executorsPendingToRemove(manager).contains("executor-5"))
    assert(!executorsPendingToRemove(manager).contains("executor-6"))

    // Busy executors are now idle and should be removed
    onExecutorIdle(manager, "executor-4")
    onExecutorIdle(manager, "executor-5")
    onExecutorIdle(manager, "executor-6")
    schedule(manager)
    assert(executorsPendingToRemove(manager).size === 4)
    clock.advance(executorIdleTimeout * 1000)
    schedule(manager)
    assert(executorsPendingToRemove(manager).size === 6) // limit reached (1 executor remaining)
  }

  test("listeners trigger add executors correctly") {
    val manager = createManager(createConf(1, 20, 1))
    assert(addTime(manager) === NOT_SET)

    // Starting a stage should start the add timer
    val numTasks = 10
    post(SparkListenerStageSubmitted(createStageInfo(0, numTasks)))
    assert(addTime(manager) !== NOT_SET)

    // Starting a subset of the tasks should not cancel the add timer
    val taskInfos = (0 to numTasks - 1).map { i => createTaskInfo(i, i, "executor-1") }
    taskInfos.tail.foreach { info => post(SparkListenerTaskStart(0, 0, info)) }
    assert(addTime(manager) !== NOT_SET)

    // Starting all remaining tasks should cancel the add timer
    post(SparkListenerTaskStart(0, 0, taskInfos.head))
    assert(addTime(manager) === NOT_SET)

    // Start two different stages
    // The add timer should be canceled only if all tasks in both stages start running
    post(SparkListenerStageSubmitted(createStageInfo(1, numTasks)))
    post(SparkListenerStageSubmitted(createStageInfo(2, numTasks)))
    assert(addTime(manager) !== NOT_SET)
    taskInfos.foreach { info => post(SparkListenerTaskStart(1, 0, info)) }
    assert(addTime(manager) !== NOT_SET)
    taskInfos.foreach { info => post(SparkListenerTaskStart(2, 0, info)) }
    assert(addTime(manager) === NOT_SET)
  }

  test("avoid ramp up when target < running executors") {
    val manager = createManager(createConf(0, 100000, 0))
    val stage1 = createStageInfo(0, 1000)
    post(SparkListenerStageSubmitted(stage1))

    assert(addExecutors(manager) === 1)
    assert(addExecutors(manager) === 2)
    assert(addExecutors(manager) === 4)
    assert(addExecutors(manager) === 8)
    assert(numExecutorsTarget(manager) === 15)
    (0 until 15).foreach { i =>
      onExecutorAdded(manager, s"executor-$i")
    }
    assert(manager.executorMonitor.executorCount === 15)
    post(SparkListenerStageCompleted(stage1))

    adjustRequestedExecutors(manager)
    assert(numExecutorsTarget(manager) === 0)

    post(SparkListenerStageSubmitted(createStageInfo(1, 1000)))
    addExecutors(manager)
    assert(numExecutorsTarget(manager) === 16)
  }

  test("avoid ramp down initial executors until first job is submitted") {
    val clock = new ManualClock(10000L)
    val manager = createManager(createConf(2, 5, 3), clock = clock)

    // Verify the initial number of executors
    assert(numExecutorsTarget(manager) === 3)
    schedule(manager)
    // Verify whether the initial number of executors is kept with no pending tasks
    assert(numExecutorsTarget(manager) === 3)

    post(SparkListenerStageSubmitted(createStageInfo(1, 2)))
    clock.advance(100L)

    assert(maxNumExecutorsNeeded(manager) === 2)
    schedule(manager)

    // Verify that current number of executors should be ramp down when first job is submitted
    assert(numExecutorsTarget(manager) === 2)
  }

  test("avoid ramp down initial executors until idle executor is timeout") {
    val clock = new ManualClock(10000L)
    val manager = createManager(createConf(2, 5, 3), clock = clock)

    // Verify the initial number of executors
    assert(numExecutorsTarget(manager) === 3)
    schedule(manager)
    // Verify the initial number of executors is kept when no pending tasks
    assert(numExecutorsTarget(manager) === 3)
    (0 until 3).foreach { i =>
      onExecutorAdded(manager, s"executor-$i")
    }

    clock.advance(executorIdleTimeout * 1000)

    assert(maxNumExecutorsNeeded(manager) === 0)
    schedule(manager)
    // Verify executor is timeout,numExecutorsTarget is recalculated
    assert(numExecutorsTarget(manager) === 2)
  }

  test("get pending task number and related locality preference") {
    val manager = createManager(createConf(2, 5, 3))

    val localityPreferences1 = Seq(
      Seq(TaskLocation("host1"), TaskLocation("host2"), TaskLocation("host3")),
      Seq(TaskLocation("host1"), TaskLocation("host2"), TaskLocation("host4")),
      Seq(TaskLocation("host2"), TaskLocation("host3"), TaskLocation("host4")),
      Seq.empty,
      Seq.empty
    )
    val stageInfo1 = createStageInfo(1, 5, localityPreferences1)
    post(SparkListenerStageSubmitted(stageInfo1))

    assert(localityAwareTasks(manager) === 3)
    assert(hostToLocalTaskCount(manager) ===
      Map("host1" -> 2, "host2" -> 3, "host3" -> 2, "host4" -> 2))

    val localityPreferences2 = Seq(
      Seq(TaskLocation("host2"), TaskLocation("host3"), TaskLocation("host5")),
      Seq(TaskLocation("host3"), TaskLocation("host4"), TaskLocation("host5")),
      Seq.empty
    )
    val stageInfo2 = createStageInfo(2, 3, localityPreferences2)
    post(SparkListenerStageSubmitted(stageInfo2))

    assert(localityAwareTasks(manager) === 5)
    assert(hostToLocalTaskCount(manager) ===
      Map("host1" -> 2, "host2" -> 4, "host3" -> 4, "host4" -> 3, "host5" -> 2))

    post(SparkListenerStageCompleted(stageInfo1))
    assert(localityAwareTasks(manager) === 2)
    assert(hostToLocalTaskCount(manager) ===
      Map("host2" -> 1, "host3" -> 2, "host4" -> 1, "host5" -> 2))
  }

  test("SPARK-8366: maxNumExecutorsNeeded should properly handle failed tasks") {
    val manager = createManager(createConf())
    assert(maxNumExecutorsNeeded(manager) === 0)

    post(SparkListenerStageSubmitted(createStageInfo(0, 1)))
    assert(maxNumExecutorsNeeded(manager) === 1)

    val taskInfo = createTaskInfo(1, 1, "executor-1")
    post(SparkListenerTaskStart(0, 0, taskInfo))
    assert(maxNumExecutorsNeeded(manager) === 1)

    // If the task is failed, we expect it to be resubmitted later.
    val taskEndReason = ExceptionFailure(null, null, null, null, None)
    post(SparkListenerTaskEnd(0, 0, null, taskEndReason, taskInfo, null))
    assert(maxNumExecutorsNeeded(manager) === 1)
  }

  test("reset the state of allocation manager") {
    val manager = createManager(createConf())
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 1)

    // Allocation manager is reset when adding executor requests are sent without reporting back
    // executor added.
    post(SparkListenerStageSubmitted(createStageInfo(0, 10)))

    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsTarget(manager) === 4)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsTarget(manager) === 5)

    manager.reset()
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 1)
    assert(manager.executorMonitor.executorCount === 0)

    // Allocation manager is reset when executors are added.
    post(SparkListenerStageSubmitted(createStageInfo(0, 10)))

    addExecutors(manager)
    addExecutors(manager)
    addExecutors(manager)
    assert(numExecutorsTarget(manager) === 5)

    onExecutorAdded(manager, "first")
    onExecutorAdded(manager, "second")
    onExecutorAdded(manager, "third")
    onExecutorAdded(manager, "fourth")
    onExecutorAdded(manager, "fifth")
    assert(manager.executorMonitor.executorCount === 5)

    // Cluster manager lost will make all the live executors lost, so here simulate this behavior
    onExecutorRemoved(manager, "first")
    onExecutorRemoved(manager, "second")
    onExecutorRemoved(manager, "third")
    onExecutorRemoved(manager, "fourth")
    onExecutorRemoved(manager, "fifth")

    manager.reset()
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 1)
    assert(manager.executorMonitor.executorCount === 0)

    // Allocation manager is reset when executors are pending to remove
    addExecutors(manager)
    addExecutors(manager)
    addExecutors(manager)
    assert(numExecutorsTarget(manager) === 5)

    onExecutorAdded(manager, "first")
    onExecutorAdded(manager, "second")
    onExecutorAdded(manager, "third")
    onExecutorAdded(manager, "fourth")
    onExecutorAdded(manager, "fifth")
    onExecutorAdded(manager, "sixth")
    onExecutorAdded(manager, "seventh")
    onExecutorAdded(manager, "eighth")
    assert(manager.executorMonitor.executorCount === 8)

    removeExecutor(manager, "first")
    removeExecutors(manager, Seq("second", "third"))
    assert(executorsPendingToRemove(manager) === Set("first", "second", "third"))
    assert(manager.executorMonitor.executorCount === 8)


    // Cluster manager lost will make all the live executors lost, so here simulate this behavior
    onExecutorRemoved(manager, "first")
    onExecutorRemoved(manager, "second")
    onExecutorRemoved(manager, "third")
    onExecutorRemoved(manager, "fourth")
    onExecutorRemoved(manager, "fifth")

    manager.reset()

    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 1)
    assert(executorsPendingToRemove(manager) === Set.empty)
    assert(manager.executorMonitor.executorCount === 0)
  }

  test("SPARK-23365 Don't update target num executors when killing idle executors") {
    val clock = new ManualClock()
    val manager = createManager(
      createConf(1, 2, 1).set(config.DYN_ALLOCATION_TESTING, false),
      clock = clock)

    when(client.requestTotalExecutors(meq(2), any(), any())).thenReturn(true)
    // test setup -- job with 2 tasks, scale up to two executors
    assert(numExecutorsTarget(manager) === 1)
    post(SparkListenerExecutorAdded(
      clock.getTimeMillis(), "executor-1", new ExecutorInfo("host1", 1, Map.empty, Map.empty)))
    post(SparkListenerStageSubmitted(createStageInfo(0, 2)))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.getTimeMillis())
    assert(numExecutorsTarget(manager) === 2)
    val taskInfo0 = createTaskInfo(0, 0, "executor-1")
    post(SparkListenerTaskStart(0, 0, taskInfo0))
    post(SparkListenerExecutorAdded(
      clock.getTimeMillis(), "executor-2", new ExecutorInfo("host1", 1, Map.empty, Map.empty)))
    val taskInfo1 = createTaskInfo(1, 1, "executor-2")
    post(SparkListenerTaskStart(0, 0, taskInfo1))
    assert(numExecutorsTarget(manager) === 2)

    // have one task finish -- we should adjust the target number of executors down
    // but we should *not* kill any executors yet
    post(SparkListenerTaskEnd(0, 0, null, Success, taskInfo0, null))
    assert(maxNumExecutorsNeeded(manager) === 1)
    assert(numExecutorsTarget(manager) === 2)
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.getTimeMillis())
    assert(numExecutorsTarget(manager) === 1)
    verify(client, never).killExecutors(any(), any(), any(), any())

    // now we cross the idle timeout for executor-1, so we kill it.  the really important
    // thing here is that we do *not* ask the executor allocation client to adjust the target
    // number of executors down
    when(client.killExecutors(Seq("executor-1"), false, false, false))
      .thenReturn(Seq("executor-1"))
    clock.advance(3000)
    schedule(manager)
    assert(maxNumExecutorsNeeded(manager) === 1)
    assert(numExecutorsTarget(manager) === 1)
    // here's the important verify -- we did kill the executors, but did not adjust the target count
    verify(client).killExecutors(Seq("executor-1"), false, false, false)
  }

  test("SPARK-26758 check executor target number after idle time out ") {
    val clock = new ManualClock(10000L)
    val manager = createManager(createConf(1, 5, 3), clock = clock)
    assert(numExecutorsTarget(manager) === 3)
    post(SparkListenerExecutorAdded(
      clock.getTimeMillis(), "executor-1", new ExecutorInfo("host1", 1, Map.empty)))
    post(SparkListenerExecutorAdded(
      clock.getTimeMillis(), "executor-2", new ExecutorInfo("host1", 2, Map.empty)))
    post(SparkListenerExecutorAdded(
      clock.getTimeMillis(), "executor-3", new ExecutorInfo("host1", 3, Map.empty)))
    // make all the executors as idle, so that it will be killed
    clock.advance(executorIdleTimeout * 1000)
    schedule(manager)
    // once the schedule is run target executor number should be 1
    assert(numExecutorsTarget(manager) === 1)
  }

  private def createConf(
      minExecutors: Int = 1,
      maxExecutors: Int = 5,
      initialExecutors: Int = 1): SparkConf = {
    new SparkConf()
      .set(config.DYN_ALLOCATION_ENABLED, true)
      .set(config.DYN_ALLOCATION_MIN_EXECUTORS, minExecutors)
      .set(config.DYN_ALLOCATION_MAX_EXECUTORS, maxExecutors)
      .set(config.DYN_ALLOCATION_INITIAL_EXECUTORS, initialExecutors)
      .set(config.DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT.key,
        s"${schedulerBacklogTimeout.toString}s")
      .set(config.DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT.key,
        s"${sustainedSchedulerBacklogTimeout.toString}s")
      .set(config.DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, s"${executorIdleTimeout.toString}s")
      .set(config.SHUFFLE_SERVICE_ENABLED, true)
      .set(config.DYN_ALLOCATION_TESTING, true)
      // SPARK-22864: effectively disable the allocation schedule by setting the period to a
      // really long value.
      .set(TEST_SCHEDULE_INTERVAL, 10000L)
  }

  private def createManager(
      conf: SparkConf,
      clock: Clock = new SystemClock()): ExecutorAllocationManager = {
    val manager = new ExecutorAllocationManager(client, listenerBus, conf, clock)
    managers += manager
    manager.start()
    manager
  }

  private def onExecutorAdded(manager: ExecutorAllocationManager, id: String): Unit = {
    post(SparkListenerExecutorAdded(0L, id, null))
  }

  private def onExecutorRemoved(manager: ExecutorAllocationManager, id: String): Unit = {
    post(SparkListenerExecutorRemoved(0L, id, null))
  }

  private def onExecutorBusy(manager: ExecutorAllocationManager, id: String): Unit = {
    val info = new TaskInfo(1, 1, 1, 0, id, "foo.example.com", TaskLocality.PROCESS_LOCAL, false)
    post(SparkListenerTaskStart(1, 1, info))
  }

  private def onExecutorIdle(manager: ExecutorAllocationManager, id: String): Unit = {
    val info = new TaskInfo(1, 1, 1, 0, id, "foo.example.com", TaskLocality.PROCESS_LOCAL, false)
    info.markFinished(TaskState.FINISHED, 1)
    post(SparkListenerTaskEnd(1, 1, "foo", Success, info, null))
  }

  private def removeExecutor(manager: ExecutorAllocationManager, executorId: String): Boolean = {
    val executorsRemoved = removeExecutors(manager, Seq(executorId))
    executorsRemoved.nonEmpty && executorsRemoved(0) == executorId
  }

  private def executorsPendingToRemove(manager: ExecutorAllocationManager): Set[String] = {
    manager.executorMonitor.executorsPendingToRemove()
  }
}

/**
 * Helper methods for testing ExecutorAllocationManager.
 * This includes methods to access private methods and fields in ExecutorAllocationManager.
 */
private object ExecutorAllocationManagerSuite extends PrivateMethodTester {
  private val schedulerBacklogTimeout = 1L
  private val sustainedSchedulerBacklogTimeout = 2L
  private val executorIdleTimeout = 3L

  private def createStageInfo(
      stageId: Int,
      numTasks: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
      attemptId: Int = 0
    ): StageInfo = {
    new StageInfo(stageId, attemptId, "name", numTasks, Seq.empty, Seq.empty, "no details",
      taskLocalityPreferences = taskLocalityPreferences)
  }

  private def createTaskInfo(
      taskId: Int,
      taskIndex: Int,
      executorId: String,
      speculative: Boolean = false): TaskInfo = {
    new TaskInfo(taskId, taskIndex, 0, 0, executorId, "", TaskLocality.ANY, speculative)
  }

  /* ------------------------------------------------------- *
   | Helper methods for accessing private methods and fields |
   * ------------------------------------------------------- */

  private val _numExecutorsToAdd = PrivateMethod[Int]('numExecutorsToAdd)
  private val _numExecutorsTarget = PrivateMethod[Int]('numExecutorsTarget)
  private val _maxNumExecutorsNeeded = PrivateMethod[Int]('maxNumExecutorsNeeded)
  private val _addTime = PrivateMethod[Long]('addTime)
  private val _schedule = PrivateMethod[Unit]('schedule)
  private val _addExecutors = PrivateMethod[Int]('addExecutors)
  private val _updateAndSyncNumExecutorsTarget =
    PrivateMethod[Int]('updateAndSyncNumExecutorsTarget)
  private val _removeExecutors = PrivateMethod[Seq[String]]('removeExecutors)
  private val _onSchedulerBacklogged = PrivateMethod[Unit]('onSchedulerBacklogged)
  private val _onSchedulerQueueEmpty = PrivateMethod[Unit]('onSchedulerQueueEmpty)
  private val _localityAwareTasks = PrivateMethod[Int]('localityAwareTasks)
  private val _hostToLocalTaskCount = PrivateMethod[Map[String, Int]]('hostToLocalTaskCount)
  private val _onSpeculativeTaskSubmitted = PrivateMethod[Unit]('onSpeculativeTaskSubmitted)
  private val _totalRunningTasks = PrivateMethod[Int]('totalRunningTasks)

  private def numExecutorsToAdd(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _numExecutorsToAdd()
  }

  private def numExecutorsTarget(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _numExecutorsTarget()
  }

  private def addTime(manager: ExecutorAllocationManager): Long = {
    manager invokePrivate _addTime()
  }

  private def schedule(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _schedule()
  }

  private def maxNumExecutorsNeeded(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _maxNumExecutorsNeeded()
  }

  private def addExecutors(manager: ExecutorAllocationManager): Int = {
    val maxNumExecutorsNeeded = manager invokePrivate _maxNumExecutorsNeeded()
    manager invokePrivate _addExecutors(maxNumExecutorsNeeded)
  }

  private def adjustRequestedExecutors(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _updateAndSyncNumExecutorsTarget(0L)
  }

  private def removeExecutors(manager: ExecutorAllocationManager, ids: Seq[String]): Seq[String] = {
    manager invokePrivate _removeExecutors(ids)
  }

  private def onSchedulerBacklogged(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _onSchedulerBacklogged()
  }

  private def onSchedulerQueueEmpty(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _onSchedulerQueueEmpty()
  }

  private def onSpeculativeTaskSubmitted(manager: ExecutorAllocationManager, id: String) : Unit = {
    manager invokePrivate _onSpeculativeTaskSubmitted(id)
  }

  private def localityAwareTasks(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _localityAwareTasks()
  }

  private def totalRunningTasks(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _totalRunningTasks()
  }

  private def hostToLocalTaskCount(manager: ExecutorAllocationManager): Map[String, Int] = {
    manager invokePrivate _hostToLocalTaskCount()
  }
}
