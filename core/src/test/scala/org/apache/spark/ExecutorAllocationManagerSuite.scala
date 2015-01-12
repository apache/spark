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

import org.scalatest.{FunSuite, PrivateMethodTester}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId

/**
 * Test add and remove behavior of ExecutorAllocationManager.
 */
class ExecutorAllocationManagerSuite extends FunSuite with LocalSparkContext {
  import ExecutorAllocationManager._
  import ExecutorAllocationManagerSuite._

  test("verify min/max executors") {
    // No min or max
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
    intercept[SparkException] { new SparkContext(conf) }
    SparkEnv.get.stop() // cleanup the created environment
    SparkContext.clearActiveContext()

    // Only min
    val conf1 = conf.clone().set("spark.dynamicAllocation.minExecutors", "1")
    intercept[SparkException] { new SparkContext(conf1) }
    SparkEnv.get.stop()
    SparkContext.clearActiveContext()

    // Only max
    val conf2 = conf.clone().set("spark.dynamicAllocation.maxExecutors", "2")
    intercept[SparkException] { new SparkContext(conf2) }
    SparkEnv.get.stop()
    SparkContext.clearActiveContext()

    // Both min and max, but min > max
    intercept[SparkException] { createSparkContext(2, 1) }
    SparkEnv.get.stop()
    SparkContext.clearActiveContext()

    // Both min and max, and min == max
    val sc1 = createSparkContext(1, 1)
    assert(sc1.executorAllocationManager.isDefined)
    sc1.stop()

    // Both min and max, and min < max
    val sc2 = createSparkContext(1, 2)
    assert(sc2.executorAllocationManager.isDefined)
    sc2.stop()
  }

  test("starting state") {
    sc = createSparkContext()
    val manager = sc.executorAllocationManager.get
    assert(numExecutorsPending(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(executorIds(manager).isEmpty)
    assert(addTime(manager) === ExecutorAllocationManager.NOT_SET)
    assert(removeTimes(manager).isEmpty)
  }

  test("add executors") {
    sc = createSparkContext(1, 10)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Keep adding until the limit is reached
    assert(numExecutorsPending(manager) === 0)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsPending(manager) === 1)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsPending(manager) === 3)
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 4)
    assert(numExecutorsPending(manager) === 7)
    assert(numExecutorsToAdd(manager) === 8)
    assert(addExecutors(manager) === 3) // reached the limit of 10
    assert(numExecutorsPending(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsPending(manager) === 10)
    assert(numExecutorsToAdd(manager) === 1)

    // Register previously requested executors
    onExecutorAdded(manager, "first")
    assert(numExecutorsPending(manager) === 9)
    onExecutorAdded(manager, "second")
    onExecutorAdded(manager, "third")
    onExecutorAdded(manager, "fourth")
    assert(numExecutorsPending(manager) === 6)
    onExecutorAdded(manager, "first") // duplicates should not count
    onExecutorAdded(manager, "second")
    assert(numExecutorsPending(manager) === 6)

    // Try adding again
    // This should still fail because the number pending + running is still at the limit
    assert(addExecutors(manager) === 0)
    assert(numExecutorsPending(manager) === 6)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsPending(manager) === 6)
    assert(numExecutorsToAdd(manager) === 1)
  }

  test("add executors capped by num pending tasks") {
    sc = createSparkContext(1, 10)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 5)))

    // Verify that we're capped at number of tasks in the stage
    assert(numExecutorsPending(manager) === 0)
    assert(numExecutorsToAdd(manager) === 1)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsPending(manager) === 1)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsPending(manager) === 3)
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsPending(manager) === 5)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that running a task reduces the cap
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, 3)))
    sc.listenerBus.postToAll(SparkListenerBlockManagerAdded(
      0L, BlockManagerId("executor-1", "host1", 1), 100L))
    sc.listenerBus.postToAll(SparkListenerTaskStart(1, 0, createTaskInfo(0, 0, "executor-1")))
    assert(numExecutorsPending(manager) === 4)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsPending(manager) === 5)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 2)
    assert(numExecutorsPending(manager) === 7)
    assert(numExecutorsToAdd(manager) === 4)
    assert(addExecutors(manager) === 0)
    assert(numExecutorsPending(manager) === 7)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that re-running a task doesn't reduce the cap further
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, 3)))
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, createTaskInfo(1, 0, "executor-1")))
    assert(addExecutors(manager) === 1)
    assert(numExecutorsPending(manager) === 8)
    assert(numExecutorsToAdd(manager) === 2)
    assert(addExecutors(manager) === 1)
    assert(numExecutorsPending(manager) === 9)
    assert(numExecutorsToAdd(manager) === 1)

    // Verify that running a task once we're at our limit doesn't blow things up
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, createTaskInfo(0, 1, "executor-1")))
    assert(addExecutors(manager) === 0)
    assert(numExecutorsPending(manager) === 9)
  }

  test("remove executors") {
    sc = createSparkContext(5, 10)
    val manager = sc.executorAllocationManager.get
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
    assert(!removeExecutor(manager, "100")) // remove non-existent executors
    assert(!removeExecutor(manager, "101"))
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

  test ("interleaving add and remove") {
    sc = createSparkContext(5, 10)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Add a few executors
    assert(addExecutors(manager) === 1)
    assert(addExecutors(manager) === 2)
    assert(addExecutors(manager) === 4)
    onExecutorAdded(manager, "1")
    onExecutorAdded(manager, "2")
    onExecutorAdded(manager, "3")
    onExecutorAdded(manager, "4")
    onExecutorAdded(manager, "5")
    onExecutorAdded(manager, "6")
    onExecutorAdded(manager, "7")
    assert(executorIds(manager).size === 7)

    // Remove until limit
    assert(removeExecutor(manager, "1"))
    assert(removeExecutor(manager, "2"))
    assert(!removeExecutor(manager, "3")) // lower limit reached
    assert(!removeExecutor(manager, "4"))
    onExecutorRemoved(manager, "1")
    onExecutorRemoved(manager, "2")
    assert(executorIds(manager).size === 5)

    // Add until limit
    assert(addExecutors(manager) === 5) // upper limit reached
    assert(addExecutors(manager) === 0)
    assert(!removeExecutor(manager, "3")) // still at lower limit
    assert(!removeExecutor(manager, "4"))
    onExecutorAdded(manager, "8")
    onExecutorAdded(manager, "9")
    onExecutorAdded(manager, "10")
    onExecutorAdded(manager, "11")
    onExecutorAdded(manager, "12")
    assert(executorIds(manager).size === 10)

    // Remove succeeds again, now that we are no longer at the lower limit
    assert(removeExecutor(manager, "3"))
    assert(removeExecutor(manager, "4"))
    assert(removeExecutor(manager, "5"))
    assert(removeExecutor(manager, "6"))
    assert(executorIds(manager).size === 10)
    assert(addExecutors(manager) === 0) // still at upper limit
    onExecutorRemoved(manager, "3")
    onExecutorRemoved(manager, "4")
    assert(executorIds(manager).size === 8)

    // Add succeeds again, now that we are no longer at the upper limit
    // Number of executors added restarts at 1
    assert(addExecutors(manager) === 1)
    assert(addExecutors(manager) === 1) // upper limit reached again
    assert(addExecutors(manager) === 0)
    assert(executorIds(manager).size === 8)
    onExecutorRemoved(manager, "5")
    onExecutorRemoved(manager, "6")
    onExecutorAdded(manager, "13")
    onExecutorAdded(manager, "14")
    assert(executorIds(manager).size === 8)
    assert(addExecutors(manager) === 1)
    assert(addExecutors(manager) === 1) // upper limit reached again
    assert(addExecutors(manager) === 0)
    onExecutorAdded(manager, "15")
    onExecutorAdded(manager, "16")
    assert(executorIds(manager).size === 10)
  }

  test("starting/canceling add timer") {
    sc = createSparkContext(2, 10)
    val clock = new TestClock(8888L)
    val manager = sc.executorAllocationManager.get
    manager.setClock(clock)

    // Starting add timer is idempotent
    assert(addTime(manager) === NOT_SET)
    onSchedulerBacklogged(manager)
    val firstAddTime = addTime(manager)
    assert(firstAddTime === clock.getTimeMillis + schedulerBacklogTimeout * 1000)
    clock.tick(100L)
    onSchedulerBacklogged(manager)
    assert(addTime(manager) === firstAddTime) // timer is already started
    clock.tick(200L)
    onSchedulerBacklogged(manager)
    assert(addTime(manager) === firstAddTime)
    onSchedulerQueueEmpty(manager)

    // Restart add timer
    clock.tick(1000L)
    assert(addTime(manager) === NOT_SET)
    onSchedulerBacklogged(manager)
    val secondAddTime = addTime(manager)
    assert(secondAddTime === clock.getTimeMillis + schedulerBacklogTimeout * 1000)
    clock.tick(100L)
    onSchedulerBacklogged(manager)
    assert(addTime(manager) === secondAddTime) // timer is already started
    assert(addTime(manager) !== firstAddTime)
    assert(firstAddTime !== secondAddTime)
  }

  test("starting/canceling remove timers") {
    sc = createSparkContext(2, 10)
    val clock = new TestClock(14444L)
    val manager = sc.executorAllocationManager.get
    manager.setClock(clock)

    executorIds(manager).asInstanceOf[mutable.Set[String]] ++= List("1", "2", "3")

    // Starting remove timer is idempotent for each executor
    assert(removeTimes(manager).isEmpty)
    onExecutorIdle(manager, "1")
    assert(removeTimes(manager).size === 1)
    assert(removeTimes(manager).contains("1"))
    val firstRemoveTime = removeTimes(manager)("1")
    assert(firstRemoveTime === clock.getTimeMillis + executorIdleTimeout * 1000)
    clock.tick(100L)
    onExecutorIdle(manager, "1")
    assert(removeTimes(manager)("1") === firstRemoveTime) // timer is already started
    clock.tick(200L)
    onExecutorIdle(manager, "1")
    assert(removeTimes(manager)("1") === firstRemoveTime)
    clock.tick(300L)
    onExecutorIdle(manager, "2")
    assert(removeTimes(manager)("2") !== firstRemoveTime) // different executor
    assert(removeTimes(manager)("2") === clock.getTimeMillis + executorIdleTimeout * 1000)
    clock.tick(400L)
    onExecutorIdle(manager, "3")
    assert(removeTimes(manager)("3") !== firstRemoveTime)
    assert(removeTimes(manager)("3") === clock.getTimeMillis + executorIdleTimeout * 1000)
    assert(removeTimes(manager).size === 3)
    assert(removeTimes(manager).contains("2"))
    assert(removeTimes(manager).contains("3"))

    // Restart remove timer
    clock.tick(1000L)
    onExecutorBusy(manager, "1")
    assert(removeTimes(manager).size === 2)
    onExecutorIdle(manager, "1")
    assert(removeTimes(manager).size === 3)
    assert(removeTimes(manager).contains("1"))
    val secondRemoveTime = removeTimes(manager)("1")
    assert(secondRemoveTime === clock.getTimeMillis + executorIdleTimeout * 1000)
    assert(removeTimes(manager)("1") === secondRemoveTime) // timer is already started
    assert(removeTimes(manager)("1") !== firstRemoveTime)
    assert(firstRemoveTime !== secondRemoveTime)
  }

  test("mock polling loop with no events") {
    sc = createSparkContext(1, 20)
    val manager = sc.executorAllocationManager.get
    val clock = new TestClock(2020L)
    manager.setClock(clock)

    // No events - we should not be adding or removing
    assert(numExecutorsPending(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    schedule(manager)
    assert(numExecutorsPending(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.tick(100L)
    schedule(manager)
    assert(numExecutorsPending(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.tick(1000L)
    schedule(manager)
    assert(numExecutorsPending(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.tick(10000L)
    schedule(manager)
    assert(numExecutorsPending(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test("mock polling loop add behavior") {
    sc = createSparkContext(1, 20)
    val clock = new TestClock(2020L)
    val manager = sc.executorAllocationManager.get
    manager.setClock(clock)
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Scheduler queue backlogged
    onSchedulerBacklogged(manager)
    clock.tick(schedulerBacklogTimeout * 1000 / 2)
    schedule(manager)
    assert(numExecutorsPending(manager) === 0) // timer not exceeded yet
    clock.tick(schedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsPending(manager) === 1) // first timer exceeded
    clock.tick(sustainedSchedulerBacklogTimeout * 1000 / 2)
    schedule(manager)
    assert(numExecutorsPending(manager) === 1) // second timer not exceeded yet
    clock.tick(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsPending(manager) === 1 + 2) // second timer exceeded
    clock.tick(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsPending(manager) === 1 + 2 + 4) // third timer exceeded

    // Scheduler queue drained
    onSchedulerQueueEmpty(manager)
    clock.tick(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsPending(manager) === 7) // timer is canceled
    clock.tick(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsPending(manager) === 7)

    // Scheduler queue backlogged again
    onSchedulerBacklogged(manager)
    clock.tick(schedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsPending(manager) === 7 + 1) // timer restarted
    clock.tick(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsPending(manager) === 7 + 1 + 2)
    clock.tick(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsPending(manager) === 7 + 1 + 2 + 4)
    clock.tick(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsPending(manager) === 20) // limit reached
  }

  test("mock polling loop remove behavior") {
    sc = createSparkContext(1, 20)
    val clock = new TestClock(2020L)
    val manager = sc.executorAllocationManager.get
    manager.setClock(clock)

    // Remove idle executors on timeout
    onExecutorAdded(manager, "executor-1")
    onExecutorAdded(manager, "executor-2")
    onExecutorAdded(manager, "executor-3")
    assert(removeTimes(manager).size === 3)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.tick(executorIdleTimeout * 1000 / 2)
    schedule(manager)
    assert(removeTimes(manager).size === 3) // idle threshold not reached yet
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.tick(executorIdleTimeout * 1000)
    schedule(manager)
    assert(removeTimes(manager).isEmpty) // idle threshold exceeded
    assert(executorsPendingToRemove(manager).size === 2) // limit reached (1 executor remaining)

    // Mark a subset as busy - only idle executors should be removed
    onExecutorAdded(manager, "executor-4")
    onExecutorAdded(manager, "executor-5")
    onExecutorAdded(manager, "executor-6")
    onExecutorAdded(manager, "executor-7")
    assert(removeTimes(manager).size === 5)              // 5 active executors
    assert(executorsPendingToRemove(manager).size === 2) // 2 pending to be removed
    onExecutorBusy(manager, "executor-4")
    onExecutorBusy(manager, "executor-5")
    onExecutorBusy(manager, "executor-6") // 3 busy and 2 idle (of the 5 active ones)
    schedule(manager)
    assert(removeTimes(manager).size === 2) // remove only idle executors
    assert(!removeTimes(manager).contains("executor-4"))
    assert(!removeTimes(manager).contains("executor-5"))
    assert(!removeTimes(manager).contains("executor-6"))
    assert(executorsPendingToRemove(manager).size === 2)
    clock.tick(executorIdleTimeout * 1000)
    schedule(manager)
    assert(removeTimes(manager).isEmpty) // idle executors are removed
    assert(executorsPendingToRemove(manager).size === 4)
    assert(!executorsPendingToRemove(manager).contains("executor-4"))
    assert(!executorsPendingToRemove(manager).contains("executor-5"))
    assert(!executorsPendingToRemove(manager).contains("executor-6"))

    // Busy executors are now idle and should be removed
    onExecutorIdle(manager, "executor-4")
    onExecutorIdle(manager, "executor-5")
    onExecutorIdle(manager, "executor-6")
    schedule(manager)
    assert(removeTimes(manager).size === 3) // 0 busy and 3 idle
    assert(removeTimes(manager).contains("executor-4"))
    assert(removeTimes(manager).contains("executor-5"))
    assert(removeTimes(manager).contains("executor-6"))
    assert(executorsPendingToRemove(manager).size === 4)
    clock.tick(executorIdleTimeout * 1000)
    schedule(manager)
    assert(removeTimes(manager).isEmpty)
    assert(executorsPendingToRemove(manager).size === 6) // limit reached (1 executor remaining)
  }

  test("listeners trigger add executors correctly") {
    sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(addTime(manager) === NOT_SET)

    // Starting a stage should start the add timer
    val numTasks = 10
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, numTasks)))
    assert(addTime(manager) !== NOT_SET)

    // Starting a subset of the tasks should not cancel the add timer
    val taskInfos = (0 to numTasks - 1).map { i => createTaskInfo(i, i, "executor-1") }
    taskInfos.tail.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, info)) }
    assert(addTime(manager) !== NOT_SET)

    // Starting all remaining tasks should cancel the add timer
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, taskInfos.head))
    assert(addTime(manager) === NOT_SET)

    // Start two different stages
    // The add timer should be canceled only if all tasks in both stages start running
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, numTasks)))
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, numTasks)))
    assert(addTime(manager) !== NOT_SET)
    taskInfos.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(1, 0, info)) }
    assert(addTime(manager) !== NOT_SET)
    taskInfos.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, info)) }
    assert(addTime(manager) === NOT_SET)
  }

  test("listeners trigger remove executors correctly") {
    sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(removeTimes(manager).isEmpty)

    // Added executors should start the remove timers for each executor
    (1 to 5).map("executor-" + _).foreach { id => onExecutorAdded(manager, id) }
    assert(removeTimes(manager).size === 5)

    // Starting a task cancel the remove timer for that executor
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(1, 1, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(2, 2, "executor-2")))
    assert(removeTimes(manager).size === 3)
    assert(!removeTimes(manager).contains("executor-1"))
    assert(!removeTimes(manager).contains("executor-2"))

    // Finishing all tasks running on an executor should start the remove timer for that executor
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(0, 0, "executor-1"), new TaskMetrics))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(2, 2, "executor-2"), new TaskMetrics))
    assert(removeTimes(manager).size === 4)
    assert(!removeTimes(manager).contains("executor-1")) // executor-1 has not finished yet
    assert(removeTimes(manager).contains("executor-2"))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(1, 1, "executor-1"), new TaskMetrics))
    assert(removeTimes(manager).size === 5)
    assert(removeTimes(manager).contains("executor-1")) // executor-1 has now finished
  }

  test("listeners trigger add and remove executor callbacks correctly") {
    sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(executorIds(manager).isEmpty)
    assert(removeTimes(manager).isEmpty)

    // New executors have registered
    sc.listenerBus.postToAll(SparkListenerBlockManagerAdded(
      0L, BlockManagerId("executor-1", "host1", 1), 100L))
    assert(executorIds(manager).size === 1)
    assert(executorIds(manager).contains("executor-1"))
    assert(removeTimes(manager).size === 1)
    assert(removeTimes(manager).contains("executor-1"))
    sc.listenerBus.postToAll(SparkListenerBlockManagerAdded(
      0L, BlockManagerId("executor-2", "host2", 1), 100L))
    assert(executorIds(manager).size === 2)
    assert(executorIds(manager).contains("executor-2"))
    assert(removeTimes(manager).size === 2)
    assert(removeTimes(manager).contains("executor-2"))

    // Existing executors have disconnected
    sc.listenerBus.postToAll(SparkListenerBlockManagerRemoved(
      0L, BlockManagerId("executor-1", "host1", 1)))
    assert(executorIds(manager).size === 1)
    assert(!executorIds(manager).contains("executor-1"))
    assert(removeTimes(manager).size === 1)
    assert(!removeTimes(manager).contains("executor-1"))

    // Unknown executor has disconnected
    sc.listenerBus.postToAll(SparkListenerBlockManagerRemoved(
      0L, BlockManagerId("executor-3", "host3", 1)))
    assert(executorIds(manager).size === 1)
    assert(removeTimes(manager).size === 1)
  }

  test("SPARK-4951: call onTaskStart before onBlockManagerAdded") {
    sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(executorIds(manager).isEmpty)
    assert(removeTimes(manager).isEmpty)

    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerBlockManagerAdded(
      0L, BlockManagerId("executor-1", "host1", 1), 100L))
    assert(executorIds(manager).size === 1)
    assert(executorIds(manager).contains("executor-1"))
    assert(removeTimes(manager).size === 0)
  }

  test("SPARK-4951: onExecutorAdded should not add a busy executor to removeTimes") {
    sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(executorIds(manager).isEmpty)
    assert(removeTimes(manager).isEmpty)
    sc.listenerBus.postToAll(SparkListenerBlockManagerAdded(
      0L, BlockManagerId("executor-1", "host1", 1), 100L))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))

    assert(executorIds(manager).size === 1)
    assert(executorIds(manager).contains("executor-1"))
    assert(removeTimes(manager).size === 0)

    sc.listenerBus.postToAll(SparkListenerBlockManagerAdded(
      0L, BlockManagerId("executor-2", "host1", 1), 100L))
    assert(executorIds(manager).size === 2)
    assert(executorIds(manager).contains("executor-2"))
    assert(removeTimes(manager).size === 1)
    assert(removeTimes(manager).contains("executor-2"))
    assert(!removeTimes(manager).contains("executor-1"))
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

  private def createSparkContext(minExecutors: Int = 1, maxExecutors: Int = 5): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", minExecutors.toString)
      .set("spark.dynamicAllocation.maxExecutors", maxExecutors.toString)
      .set("spark.dynamicAllocation.schedulerBacklogTimeout", schedulerBacklogTimeout.toString)
      .set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout",
        sustainedSchedulerBacklogTimeout.toString)
      .set("spark.dynamicAllocation.executorIdleTimeout", executorIdleTimeout.toString)
      .set("spark.dynamicAllocation.testing", "true")
    new SparkContext(conf)
  }

  private def createStageInfo(stageId: Int, numTasks: Int): StageInfo = {
    new StageInfo(stageId, 0, "name", numTasks, Seq.empty, "no details")
  }

  private def createTaskInfo(taskId: Int, taskIndex: Int, executorId: String): TaskInfo = {
    new TaskInfo(taskId, taskIndex, 0, 0, executorId, "", TaskLocality.ANY, speculative = false)
  }

  /* ------------------------------------------------------- *
   | Helper methods for accessing private methods and fields |
   * ------------------------------------------------------- */

  private val _numExecutorsToAdd = PrivateMethod[Int]('numExecutorsToAdd)
  private val _numExecutorsPending = PrivateMethod[Int]('numExecutorsPending)
  private val _executorsPendingToRemove =
    PrivateMethod[collection.Set[String]]('executorsPendingToRemove)
  private val _executorIds = PrivateMethod[collection.Set[String]]('executorIds)
  private val _addTime = PrivateMethod[Long]('addTime)
  private val _removeTimes = PrivateMethod[collection.Map[String, Long]]('removeTimes)
  private val _schedule = PrivateMethod[Unit]('schedule)
  private val _addExecutors = PrivateMethod[Int]('addExecutors)
  private val _removeExecutor = PrivateMethod[Boolean]('removeExecutor)
  private val _onExecutorAdded = PrivateMethod[Unit]('onExecutorAdded)
  private val _onExecutorRemoved = PrivateMethod[Unit]('onExecutorRemoved)
  private val _onSchedulerBacklogged = PrivateMethod[Unit]('onSchedulerBacklogged)
  private val _onSchedulerQueueEmpty = PrivateMethod[Unit]('onSchedulerQueueEmpty)
  private val _onExecutorIdle = PrivateMethod[Unit]('onExecutorIdle)
  private val _onExecutorBusy = PrivateMethod[Unit]('onExecutorBusy)

  private def numExecutorsToAdd(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _numExecutorsToAdd()
  }

  private def numExecutorsPending(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _numExecutorsPending()
  }

  private def executorsPendingToRemove(
      manager: ExecutorAllocationManager): collection.Set[String] = {
    manager invokePrivate _executorsPendingToRemove()
  }

  private def executorIds(manager: ExecutorAllocationManager): collection.Set[String] = {
    manager invokePrivate _executorIds()
  }

  private def addTime(manager: ExecutorAllocationManager): Long = {
    manager invokePrivate _addTime()
  }

  private def removeTimes(manager: ExecutorAllocationManager): collection.Map[String, Long] = {
    manager invokePrivate _removeTimes()
  }

  private def schedule(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _schedule()
  }

  private def addExecutors(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _addExecutors()
  }

  private def removeExecutor(manager: ExecutorAllocationManager, id: String): Boolean = {
    manager invokePrivate _removeExecutor(id)
  }

  private def onExecutorAdded(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorAdded(id)
  }

  private def onExecutorRemoved(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorRemoved(id)
  }

  private def onSchedulerBacklogged(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _onSchedulerBacklogged()
  }

  private def onSchedulerQueueEmpty(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _onSchedulerQueueEmpty()
  }

  private def onExecutorIdle(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorIdle(id)
  }

  private def onExecutorBusy(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorBusy(id)
  }
}
