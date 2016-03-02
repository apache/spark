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

import java.util.concurrent.{
  BlockingQueue,
  ScheduledExecutorService,
  ScheduledThreadPoolExecutor,
  RunnableScheduledFuture
}
import scala.collection.mutable

import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo

/**
 * Test add and remove behavior of ExecutorAllocationManager.
 */
class ExecutorAllocationManagerSuite
  extends SparkFunSuite
  with LocalSparkContext
  with BeforeAndAfter {

  import ExecutorAllocationManager._
  import ExecutorAllocationManagerSuite._

  private val contexts = new mutable.ListBuffer[SparkContext]()

  before {
    contexts.clear()
  }

  after {
    contexts.foreach(_.stop())
  }

  test("verify min/max executors") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.testing", "true")
    val sc0 = new SparkContext(conf)
    contexts += sc0
    assert(sc0.executorAllocationManager.isDefined)
    sc0.stop()

    // Min < 0
    val conf1 = conf.clone().set("spark.dynamicAllocation.minExecutors", "-1")
    intercept[SparkException] { contexts += new SparkContext(conf1) }

    // Max < 0
    val conf2 = conf.clone().set("spark.dynamicAllocation.maxExecutors", "-1")
    intercept[SparkException] { contexts += new SparkContext(conf2) }

    // Both min and max, but min > max
    intercept[SparkException] { createSparkContext(2, 1) }

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

    assert(state(manager).isInstanceOf[manager.GrowableState])
    assert(numExecutorsTarget(manager) === 1)
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(executorIds(manager).isEmpty)
  }

  test("add executors") {
    sc = createSparkContext(1, 10, 1)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    assert(numExecutorsTarget(manager) === 1)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 2)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 4)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 8)

    inbox(manager).put(manager.Timeout) // reached the limit of 10
    tick(manager)
    assert(numExecutorsTarget(manager) === 10)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 10)

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
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 10)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 10)
  }

  test("add executors capped by num pending tasks") {
    sc = createSparkContext(0, 10, 0)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 5)))

    tick(manager)
    assert(numExecutorsTarget(manager) === 0)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 1)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 3)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 5)

    // Verify that running a task doesn't affect the target
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, 3)))
    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty)))
    sc.listenerBus.postToAll(SparkListenerTaskStart(1, 0, createTaskInfo(0, 0, "executor-1")))

    tick(manager)
    assert(numExecutorsTarget(manager) === 5)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 6)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 8)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 8)

    // Verify that re-running a task doesn't blow things up
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, 3)))
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, createTaskInfo(1, 0, "executor-1")))

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 9)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 10)

    // Verify that running a task once we're at our limit doesn't blow things up
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, createTaskInfo(0, 1, "executor-1")))
    tick(manager)
    assert(numExecutorsTarget(manager) === 10)
  }

  test("cancel pending executors when no longer needed") {
    sc = createSparkContext(0, 10, 0)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, 5)))

    inbox(manager).put(manager.WorkAvailable)
    tick(manager)
    assert(numExecutorsTarget(manager) === 0)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 1)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 3)

    val task1Info = createTaskInfo(0, 0, "executor-1")
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, task1Info))
    val task2Info = createTaskInfo(1, 0, "executor-1")
    sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, task2Info))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(2, 0, null, Success, task1Info, null))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(2, 0, null, Success, task2Info, null))

    //TODO: make this check more meaningful.
  }

  test("remove executors") {
    sc = createSparkContext(5, 10, 5)
    val manager = sc.executorAllocationManager.get
    (1 to 10).map(_.toString).foreach { id => onExecutorAdded(manager, id) }

    // Keep removing until the limit is reached
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeIdleExecutor(manager, "1"))
    assert(executorsPendingToRemove(manager).size === 1)
    assert(executorsPendingToRemove(manager).contains("1"))
    assert(removeIdleExecutor(manager, "2"))
    assert(removeIdleExecutor(manager, "3"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(executorsPendingToRemove(manager).contains("2"))
    assert(executorsPendingToRemove(manager).contains("3"))
    assert(!removeIdleExecutor(manager, "100")) // remove non-existent executors
    assert(!removeIdleExecutor(manager, "101"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(removeIdleExecutor(manager, "4"))
    assert(removeIdleExecutor(manager, "5"))
    assert(!removeIdleExecutor(manager, "6")) // reached the limit of 5
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
    assert(!removeIdleExecutor(manager, "7"))
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(!removeIdleExecutor(manager, "8"))
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test ("interleaving add and remove") {
    sc = createSparkContext(5, 10, 5)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    // Add a few executors
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) == 8)
    onExecutorAdded(manager, "1")
    onExecutorAdded(manager, "2")
    onExecutorAdded(manager, "3")
    onExecutorAdded(manager, "4")
    onExecutorAdded(manager, "5")
    onExecutorAdded(manager, "6")
    onExecutorAdded(manager, "7")
    onExecutorAdded(manager, "8")
    assert(executorIds(manager).size === 8)

    // Remove until limit
    assert(removeIdleExecutor(manager, "1"))
    assert(removeIdleExecutor(manager, "2"))
    assert(removeIdleExecutor(manager, "3"))
    assert(!removeIdleExecutor(manager, "4")) // lower limit reached
    assert(!removeIdleExecutor(manager, "5"))
    tick(manager)
    assert(numExecutorsTarget(manager) == 5)
    onExecutorRemoved(manager, "1")
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")
    assert(executorIds(manager).size === 5)

    // Add until limit
    assert(!removeIdleExecutor(manager, "4")) // still at lower limit
    assert(!removeIdleExecutor(manager, "5"))
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) == 10)
    onExecutorAdded(manager, "9")
    onExecutorAdded(manager, "10")
    onExecutorAdded(manager, "11")
    onExecutorAdded(manager, "12")
    onExecutorAdded(manager, "13")
    assert(executorIds(manager).size === 10)

    // Remove succeeds again, now that we are no longer at the lower limit
    assert(removeIdleExecutor(manager, "4"))
    assert(removeIdleExecutor(manager, "5"))
    assert(removeIdleExecutor(manager, "6"))
    assert(removeIdleExecutor(manager, "7"))
    
    // Make more work after removing executors.
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, 1000)))
    assert(executorIds(manager).size === 10)
    onExecutorRemoved(manager, "4")
    onExecutorRemoved(manager, "5")
    assert(executorIds(manager).size === 8)

    // Number of executors pending restarts at 1
    assert(numExecutorsToAdd(manager) === 1)
    assert(executorIds(manager).size === 8)
    onExecutorRemoved(manager, "6")
    onExecutorRemoved(manager, "7")
    onExecutorAdded(manager, "14")
    onExecutorAdded(manager, "15")
    assert(executorIds(manager).size === 8)
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    onExecutorAdded(manager, "16")
    onExecutorAdded(manager, "17")
    assert(executorIds(manager).size === 10)
    assert(numExecutorsTarget(manager) == 10)
  }


  test("mock polling loop with no events") {
    sc = createSparkContext(0, 20, 0)
    val manager = sc.executorAllocationManager.get

    // No events - we should not be adding or removing
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test("mock polling loop add behavior") {
    sc = createSparkContext(0, 20, 0)
    val manager = sc.executorAllocationManager.get
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    tick(manager)
    assert(numExecutorsTarget(manager) === 0)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 1)

    tick(manager)
    assert(numExecutorsTarget(manager) === 1)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 1 + 2)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 1 + 2 + 4)

    // Scheduler queue drained
    inbox(manager).put(manager.WorkFinished)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 7)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 7)

    // Scheduler queue backlogged again
    inbox(manager).put(manager.WorkAvailable)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 7 + 1)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 7 + 1 + 2)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 7 + 1 + 2 + 4)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 20)
  }

  test("mock polling loop remove behavior") {
    sc = createSparkContext(1, 20, 1)
    val manager = sc.executorAllocationManager.get

    // Remove idle executors on timeout
    onExecutorAdded(manager, "executor-1")
    onExecutorAdded(manager, "executor-2")
    onExecutorAdded(manager, "executor-3")
    assert(executorsPendingToRemove(manager).isEmpty)

    assert(idleExecutors(manager).size === 3) // idle threshold not reached yet
    assert(executorsPendingToRemove(manager).isEmpty)

    runScheduledTasks(manager)

    assert(idleExecutors(manager).isEmpty) // idle threshold exceeded
    assert(executorsPendingToRemove(manager).size === 2) // limit reached (1 executor remaining)

    // Mark a subset as busy - only idle executors should be removed
    onExecutorAdded(manager, "executor-4")
    onExecutorAdded(manager, "executor-5")
    onExecutorAdded(manager, "executor-6")
    onExecutorAdded(manager, "executor-7")
    assert(idleExecutors(manager).size === 5)
    assert(executorsPendingToRemove(manager).size === 2)

    onExecutorBusy(manager, "executor-4")
    onExecutorBusy(manager, "executor-5")
    onExecutorBusy(manager, "executor-6") // 3 busy and 2 idle (of the 5 active ones)

    assert(idleExecutors(manager).size === 2) // remove only idle executors
    assert(!idleExecutors(manager).contains("executor-4"))
    assert(!idleExecutors(manager).contains("executor-5"))
    assert(!idleExecutors(manager).contains("executor-6"))
    assert(executorsPendingToRemove(manager).size === 2)

    runScheduledTasks(manager)

    assert(idleExecutors(manager).isEmpty) // idle executors are removed
    assert(executorsPendingToRemove(manager).size === 4)
    assert(!executorsPendingToRemove(manager).contains("executor-4"))
    assert(!executorsPendingToRemove(manager).contains("executor-5"))
    assert(!executorsPendingToRemove(manager).contains("executor-6"))

    // Busy executors are now idle and should be removed
    onExecutorIdle(manager, "executor-4")
    onExecutorIdle(manager, "executor-5")
    onExecutorIdle(manager, "executor-6")

    assert(idleExecutors(manager).size === 3) // 0 busy and 3 idle
    assert(idleExecutors(manager).contains("executor-4"))
    assert(idleExecutors(manager).contains("executor-5"))
    assert(idleExecutors(manager).contains("executor-6"))
    assert(executorsPendingToRemove(manager).size === 4)

    runScheduledTasks(manager)

    assert(idleExecutors(manager).isEmpty)
    assert(executorsPendingToRemove(manager).size === 6) // limit reached (1 executor remaining)
  }

  test("listeners trigger add executors correctly") {
    sc = createSparkContext(2, 10, 2)
    val manager = sc.executorAllocationManager.get

    assert(state(manager).isInstanceOf[manager.GrowableState])

    // Starting a stage should start the add timer
    val numTasks = 10
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, numTasks)))
    tick(manager)
    assert(state(manager).isInstanceOf[manager.ThrottleGrowthState])

    // Starting a subset of the tasks should not cancel the add timer
    val taskInfos = (0 to numTasks - 1).map { i => createTaskInfo(i, i, "executor-1") }
    taskInfos.tail.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, info)) }
    tick(manager)
    assert(state(manager).isInstanceOf[manager.ThrottleGrowthState])

    // Starting all remaining tasks should cancel the add timer
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, taskInfos.head))
    tick(manager)
    assert(state(manager).isInstanceOf[manager.GrowableState])

    // Start two different stages
    // The add timer should be canceled only if all tasks in both stages start running
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, numTasks)))
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, numTasks)))
    tick(manager)
    assert(state(manager).isInstanceOf[manager.ThrottleGrowthState])

    taskInfos.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(1, 0, info)) }
    tick(manager)
    assert(state(manager).isInstanceOf[manager.ThrottleGrowthState])

    taskInfos.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, info)) }
    tick(manager)
    assert(state(manager).isInstanceOf[manager.GrowableState])
  }

  test("listeners trigger remove executors correctly") {
    sc = createSparkContext(2, 10, 2)
    val manager = sc.executorAllocationManager.get
    assert(idleExecutors(manager).isEmpty)

    // Added executors should start the remove timers for each executor
    (1 to 5).map("executor-" + _).foreach { id => onExecutorAdded(manager, id) }
    assert(idleExecutors(manager).size === 5)

    // Starting a task cancel the remove timer for that executor
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(1, 1, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(2, 2, "executor-2")))
    assert(idleExecutors(manager).size === 3)
    assert(!idleExecutors(manager).contains("executor-1"))
    assert(!idleExecutors(manager).contains("executor-2"))

    // Finishing all tasks running on an executor should start the remove timer for that executor
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(0, 0, "executor-1"), new TaskMetrics))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(2, 2, "executor-2"), new TaskMetrics))
    assert(idleExecutors(manager).size === 4)
    assert(!idleExecutors(manager).contains("executor-1")) // executor-1 has not finished yet
    assert(idleExecutors(manager).contains("executor-2"))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(1, 1, "executor-1"), new TaskMetrics))
    assert(idleExecutors(manager).size === 5)
    assert(idleExecutors(manager).contains("executor-1")) // executor-1 has now finished
  }

  test("listeners trigger add and remove executor callbacks correctly") {
    sc = createSparkContext(2, 10, 2)
    val manager = sc.executorAllocationManager.get
    assert(executorIds(manager).isEmpty)
    assert(idleExecutors(manager).isEmpty)

    // New executors have registered
    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty)))
    assert(executorIds(manager).size === 1)
    assert(executorIds(manager).contains("executor-1"))
    assert(idleExecutors(manager).size === 1)
    assert(idleExecutors(manager).contains("executor-1"))

    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-2", new ExecutorInfo("host2", 1, Map.empty)))
    assert(executorIds(manager).size === 2)
    assert(executorIds(manager).contains("executor-2"))
    assert(idleExecutors(manager).size === 2)
    assert(idleExecutors(manager).contains("executor-2"))

    // Existing executors have disconnected
    sc.listenerBus.postToAll(SparkListenerExecutorRemoved(0L, "executor-1", ""))
    assert(executorIds(manager).size === 1)
    assert(!executorIds(manager).contains("executor-1"))
    assert(idleExecutors(manager).size === 1)
    assert(!idleExecutors(manager).contains("executor-1"))

    // Unknown executor has disconnected
    sc.listenerBus.postToAll(SparkListenerExecutorRemoved(0L, "executor-3", ""))
    assert(executorIds(manager).size === 1)
    assert(idleExecutors(manager).size === 1)
  }

  test("SPARK-4951: call onTaskStart before onBlockManagerAdded") {
    sc = createSparkContext(2, 10, 2)
    val manager = sc.executorAllocationManager.get
    assert(executorIds(manager).isEmpty)
    assert(idleExecutors(manager).isEmpty)

    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty)))
    assert(executorIds(manager).size === 1)
    assert(executorIds(manager).contains("executor-1"))
    assert(idleExecutors(manager).size === 0)
  }

  test("SPARK-4951: onExecutorAdded should not add a busy executor to idleExecutors") {
    sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(executorIds(manager).isEmpty)
    assert(idleExecutors(manager).isEmpty)

    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty)))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))
    assert(executorIds(manager).size === 1)
    assert(executorIds(manager).contains("executor-1"))
    assert(idleExecutors(manager).size === 0)

    sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, "executor-2", new ExecutorInfo("host1", 1, Map.empty)))
    assert(executorIds(manager).size === 2)
    assert(executorIds(manager).contains("executor-2"))
    assert(idleExecutors(manager).size === 1)
    assert(idleExecutors(manager).contains("executor-2"))
    assert(!idleExecutors(manager).contains("executor-1"))
  }

  test("avoid ramp up when target < running executors") {
    sc = createSparkContext(0, 100000, 0)
    val manager = sc.executorAllocationManager.get

    val stage1 = createStageInfo(0, 1000)
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(stage1))

    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 15)
    (0 until 15).foreach { i =>
      onExecutorAdded(manager, s"executor-$i")
    }
    assert(executorIds(manager).size === 15)

    sc.listenerBus.postToAll(SparkListenerStageCompleted(stage1))

    runScheduledTasks(manager)
    tick(manager)

    assert(numExecutorsTarget(manager) === 0)

    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, 1000)))

    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 15)
  }

  test("avoid ramp down initial executors until first job is submitted") {
    sc = createSparkContext(2, 5, 3)
    val manager = sc.executorAllocationManager.get

    // Verify the initial number of executors
    assert(numExecutorsTarget(manager) === 3)

    inbox(manager).put(manager.Timeout)
    tick(manager)
    // Verify whether the initial number of executors is kept with no pending tasks
    assert(numExecutorsTarget(manager) === 3)

    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, 2)))

    inbox(manager).put(manager.Timeout)
    tick(manager)

    // Verify that current number of executors should be ramp down when first job is submitted
    assert(numExecutorsTarget(manager) === 2)
  }

  test("avoid ramp down initial executors until idle executor is timeout") {
    // sc = createSparkContext(2, 5, 3)
    // val manager = sc.executorAllocationManager.get
    //
    // // Verify the initial number of executors
    // assert(numExecutorsTarget(manager) === 3)
    //
    // tick(manager)
    // // Verify the initial number of executors is kept when no pending tasks
    // assert(numExecutorsTarget(manager) === 3)
    // (0 until 3).foreach { i =>
    //   onExecutorAdded(manager, s"executor-$i")
    // }
    //
    // runScheduledTasks(manager)
    //
    // inbox(manager).put(manager.Timeout)
    // tick(manager)
    // // Verify executor is timeout but numExecutorsTarget is not recalculated
    // assert(numExecutorsTarget(manager) === 3)
    //
    // // Schedule again to recalculate the numExecutorsTarget after executor is timeout
    // inbox(manager).put(manager.Timeout)
    // tick(manager)
    // // Verify that current number of executors should be ramp down when executor is timeout
    // assert(numExecutorsTarget(manager) === 2)
  }

  test("get pending task number and related locality preference") {
    sc = createSparkContext(2, 5, 3)
    val manager = sc.executorAllocationManager.get

    val localityPreferences1 = Seq(
      Seq(TaskLocation("host1"), TaskLocation("host2"), TaskLocation("host3")),
      Seq(TaskLocation("host1"), TaskLocation("host2"), TaskLocation("host4")),
      Seq(TaskLocation("host2"), TaskLocation("host3"), TaskLocation("host4")),
      Seq.empty,
      Seq.empty
    )
    val stageInfo1 = createStageInfo(1, 5, localityPreferences1)
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(stageInfo1))

    assert(localityAwareTasks(manager) === 3)
    assert(hostToLocalTaskCount(manager) ===
      Map("host1" -> 2, "host2" -> 3, "host3" -> 2, "host4" -> 2))

    val localityPreferences2 = Seq(
      Seq(TaskLocation("host2"), TaskLocation("host3"), TaskLocation("host5")),
      Seq(TaskLocation("host3"), TaskLocation("host4"), TaskLocation("host5")),
      Seq.empty
    )
    val stageInfo2 = createStageInfo(2, 3, localityPreferences2)
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(stageInfo2))

    assert(localityAwareTasks(manager) === 5)
    assert(hostToLocalTaskCount(manager) ===
      Map("host1" -> 2, "host2" -> 4, "host3" -> 4, "host4" -> 3, "host5" -> 2))

    sc.listenerBus.postToAll(SparkListenerStageCompleted(stageInfo1))
    assert(localityAwareTasks(manager) === 2)
    assert(hostToLocalTaskCount(manager) ===
      Map("host2" -> 1, "host3" -> 2, "host4" -> 1, "host5" -> 2))
  }

  test("SPARK-8366: maxNumExecutorsNeeded should properly handle failed tasks") {
    sc = createSparkContext()
    val manager = sc.executorAllocationManager.get
    assert(maxNumExecutorsNeeded(manager) === 0)

    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1)))
    assert(maxNumExecutorsNeeded(manager) === 1)

    val taskInfo = createTaskInfo(1, 1, "executor-1")
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, taskInfo))
    assert(maxNumExecutorsNeeded(manager) === 1)

    // If the task is failed, we expect it to be resubmitted later.
    val taskEndReason = ExceptionFailure(null, null, null, null, None)
    sc.listenerBus.postToAll(SparkListenerTaskEnd(0, 0, null, taskEndReason, taskInfo, null))
    assert(maxNumExecutorsNeeded(manager) === 1)
  }

  test("reset the state of allocation manager") {
    sc = createSparkContext()
    val manager = sc.executorAllocationManager.get
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 1)

    // Allocation manager is reset when adding executor requests are sent without reporting back
    // executor added.
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 10)))

    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 2)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 4)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 5)

    manager.reset()
    tick(manager)
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 1)
    assert(executorIds(manager) === Set.empty)

    // Allocation manager is reset when executors are added.
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 10)))

    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 5)

    onExecutorAdded(manager, "first")
    onExecutorAdded(manager, "second")
    onExecutorAdded(manager, "third")
    onExecutorAdded(manager, "fourth")
    onExecutorAdded(manager, "fifth")
    assert(executorIds(manager) === Set("first", "second", "third", "fourth", "fifth"))

    // Cluster manager lost will make all the live executors lost, so here simulate this behavior
    onExecutorRemoved(manager, "first")
    onExecutorRemoved(manager, "second")
    onExecutorRemoved(manager, "third")
    onExecutorRemoved(manager, "fourth")
    onExecutorRemoved(manager, "fifth")

    manager.reset()
    tick(manager)
    assert(numExecutorsTarget(manager) === 1)
    assert(numExecutorsToAdd(manager) === 1)
    assert(executorIds(manager) === Set.empty)
    assert(idleExecutors(manager) === Map.empty)

    // Allocation manager is reset when executors are added.
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 10)))

    // Allocation manager is reset when executors are pending to remove
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    inbox(manager).put(manager.Timeout)
    tick(manager)
    assert(numExecutorsTarget(manager) === 5)

    onExecutorAdded(manager, "first")
    onExecutorAdded(manager, "second")
    onExecutorAdded(manager, "third")
    onExecutorAdded(manager, "fourth")
    onExecutorAdded(manager, "fifth")
    assert(executorIds(manager) === Set("first", "second", "third", "fourth", "fifth"))

    removeIdleExecutor(manager, "first")
    removeIdleExecutor(manager, "second")
    assert(executorsPendingToRemove(manager) === Set("first", "second"))
    assert(executorIds(manager) === Set("first", "second", "third", "fourth", "fifth"))


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
    assert(idleExecutors(manager) === Map.empty)
  }

  private def createSparkContext(
      minExecutors: Int = 1,
      maxExecutors: Int = 5,
      initialExecutors: Int = 1): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", minExecutors.toString)
      .set("spark.dynamicAllocation.maxExecutors", maxExecutors.toString)
      .set("spark.dynamicAllocation.initialExecutors", initialExecutors.toString)
      .set("spark.dynamicAllocation.schedulerBacklogTimeout",
          s"${schedulerBacklogTimeout.toString}s")
      .set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout",
        s"${sustainedSchedulerBacklogTimeout.toString}s")
      .set("spark.dynamicAllocation.executorIdleTimeout", s"${executorIdleTimeout.toString}s")
      .set("spark.dynamicAllocation.testing", "true")
    val sc = new SparkContext(conf)
    contexts += sc
    sc
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
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty
    ): StageInfo = {
    new StageInfo(
      stageId, 0, "name", numTasks, Seq.empty, Seq.empty, "no details", taskLocalityPreferences)
  }

  private def createTaskInfo(taskId: Int, taskIndex: Int, executorId: String): TaskInfo = {
    new TaskInfo(taskId, taskIndex, 0, 0, executorId, "", TaskLocality.ANY, speculative = false)
  }

  /* ------------------------------------------------------- *
   | Helper methods for accessing private methods and fields |
   * ------------------------------------------------------- */

  private val _numExecutorsToAdd = PrivateMethod[Int]('numExecutorsToAdd)
  private val _numExecutorsTarget = PrivateMethod[Int]('numExecutorsTarget)
  private val _maxNumExecutorsNeeded = PrivateMethod[Int]('maxNumExecutorsNeeded)
  private val _executorsPendingToRemove =
    PrivateMethod[collection.Set[String]]('executorsPendingToRemove)
  private val _executorIds = PrivateMethod[collection.Set[String]]('executorIds)
  private val _idleExecutors = PrivateMethod[collection.Map[String, Long]]('idleExecutors)
  private val _removeIdleExecutor = PrivateMethod[Boolean]('removeIdleExecutor)
  private val _onExecutorAdded = PrivateMethod[Unit]('onExecutorAdded)
  private val _onExecutorRemoved = PrivateMethod[Unit]('onExecutorRemoved)
  private val _onExecutorIdle = PrivateMethod[Unit]('onExecutorIdle)
  private val _onExecutorBusy = PrivateMethod[Unit]('onExecutorBusy)
  private val _localityAwareTasks = PrivateMethod[Int]('localityAwareTasks)
  private val _hostToLocalTaskCount = PrivateMethod[Map[String, Int]]('hostToLocalTaskCount)
  private val _state = PrivateMethod[ExecutorAllocationManager#State]('state)
  private val _inbox = PrivateMethod[BlockingQueue[ExecutorAllocationManager#Message]]('inbox)
  private val _executor = PrivateMethod[ScheduledExecutorService]('executor)

  private def numExecutorsToAdd(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _numExecutorsToAdd()
  }

  private def numExecutorsTarget(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _numExecutorsTarget()
  }

  private def executorsPendingToRemove(
      manager: ExecutorAllocationManager): collection.Set[String] = {
    manager invokePrivate _executorsPendingToRemove()
  }

  private def executorIds(manager: ExecutorAllocationManager): collection.Set[String] = {
    manager invokePrivate _executorIds()
  }

  private def idleExecutors(manager: ExecutorAllocationManager): collection.Map[String, Long] = {
    manager invokePrivate _idleExecutors()
  }

  private def maxNumExecutorsNeeded(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _maxNumExecutorsNeeded()
  }

  private def removeIdleExecutor(manager: ExecutorAllocationManager, id: String): Boolean = {
    manager invokePrivate _removeIdleExecutor(id)
  }

  private def onExecutorAdded(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorAdded(id)
  }

  private def onExecutorRemoved(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorRemoved(id)
  }

  private def onExecutorIdle(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorIdle(id)
  }

  private def onExecutorBusy(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorBusy(id)
  }

  private def localityAwareTasks(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _localityAwareTasks()
  }

  private def hostToLocalTaskCount(manager: ExecutorAllocationManager): Map[String, Int] = {
    manager invokePrivate _hostToLocalTaskCount()
  }

  private def state(manager: ExecutorAllocationManager): ExecutorAllocationManager#State = {
    manager invokePrivate _state()
  }

  private def inbox(manager: ExecutorAllocationManager): BlockingQueue[ExecutorAllocationManager#Message] = {
    manager invokePrivate _inbox()
  }

  private def executor(manager: ExecutorAllocationManager): ScheduledThreadPoolExecutor = {
    (manager invokePrivate _executor()).asInstanceOf[ScheduledThreadPoolExecutor]
  }

  private def tick(manager: ExecutorAllocationManager): Unit = {
    inbox(manager).put(manager.Ping)
  }

  private def runScheduledTasks(manager: ExecutorAllocationManager): Unit = {
    val e = executor(manager)
    val p = e.getQueue.toArray()
    p.foreach(_.asInstanceOf[RunnableScheduledFuture[_]].run())
  }
}
