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

import org.scalatest.FunSuite
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId

class ExecutorAllocationManagerSuite extends FunSuite {
  import ExecutorAllocationManager._

  private def createSparkContext(
      minExecutors: Int = 1,
      maxExecutors: Int = 5,
      addThresholdSeconds: Long = 1,
      addIntervalSeconds: Long = 1,
      removeThresholdSeconds: Long = 3): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", minExecutors + "")
      .set("spark.dynamicAllocation.maxExecutors", maxExecutors + "")
      .set("spark.dynamicAllocation.addExecutorThresholdSeconds", addThresholdSeconds + "")
      .set("spark.dynamicAllocation.addExecutorIntervalSeconds", addIntervalSeconds + "")
      .set("spark.dynamicAllocation.removeExecutorThresholdSeconds", removeThresholdSeconds + "")
    new SparkContext(conf)
  }

  private def createStageInfo(stageId: Int, numTasks: Int): StageInfo = {
    new StageInfo(stageId, 0, "name", numTasks, Seq.empty, "no details")
  }

  private def createTaskInfo(taskId: Int, taskIndex: Int, executorId: String): TaskInfo = {
    new TaskInfo(taskId, taskIndex, 0, 0, executorId, "", TaskLocality.ANY, speculative = false)
  }

  test("verify min/max executors") {
    // No min or max
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
    intercept[SparkException] { new SparkContext(conf) }

    // Only min
    val conf1 = conf.clone().set("spark.dynamicAllocation.minExecutors", "1")
    intercept[SparkException] { new SparkContext(conf1) }

    // Only max
    val conf2 = conf.clone().set("spark.dynamicAllocation.maxExecutors", "2")
    intercept[SparkException] { new SparkContext(conf2) }

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
    val sc = createSparkContext()
    val manager = sc.executorAllocationManager.get
    assert(manager.getNumExecutorsPending === 0)
    assert(manager.getExecutorsPendingToRemove.isEmpty)
    assert(manager.getExecutorIds.isEmpty)
    assert(manager.getAddTime === ExecutorAllocationManager.NOT_SET)
    assert(manager.getRemoveTimes.isEmpty)
    sc.stop()
  }

  test("add executors") {
    val sc = createSparkContext(1, 10)
    val manager = sc.executorAllocationManager.get

    // Keep adding until the limit is reached
    assert(manager.getNumExecutorsPending === 0)
    assert(manager.getNumExecutorsToAdd === 1)
    assert(manager.addExecutors() === 1)
    assert(manager.getNumExecutorsPending === 1)
    assert(manager.getNumExecutorsToAdd === 2)
    assert(manager.addExecutors() === 2)
    assert(manager.getNumExecutorsPending === 3)
    assert(manager.getNumExecutorsToAdd === 4)
    assert(manager.addExecutors() === 4)
    assert(manager.getNumExecutorsPending === 7)
    assert(manager.getNumExecutorsToAdd === 8)
    assert(manager.addExecutors() === 3) // reached the limit of 10
    assert(manager.getNumExecutorsPending === 10)
    assert(manager.getNumExecutorsToAdd === 1)
    assert(manager.addExecutors() === 0)
    assert(manager.getNumExecutorsPending === 10)
    assert(manager.getNumExecutorsToAdd === 1)

    // Register previously requested executors
    manager.onExecutorAdded("first")
    assert(manager.getNumExecutorsPending === 9)
    manager.onExecutorAdded("second")
    manager.onExecutorAdded("third")
    manager.onExecutorAdded("fourth")
    assert(manager.getNumExecutorsPending === 6)
    manager.onExecutorAdded("first") // duplicates should not count
    manager.onExecutorAdded("second")
    assert(manager.getNumExecutorsPending === 6)

    // Try adding again
    // This should still fail because the number pending + running is still at the limit
    assert(manager.addExecutors() === 0)
    assert(manager.getNumExecutorsPending === 6)
    assert(manager.getNumExecutorsToAdd === 1)
    assert(manager.addExecutors() === 0)
    assert(manager.getNumExecutorsPending === 6)
    assert(manager.getNumExecutorsToAdd === 1)
    sc.stop()
  }

  test("remove executors") {
    val sc = createSparkContext(5, 10)
    val manager = sc.executorAllocationManager.get
    (1 to 10).map(_.toString).foreach(manager.onExecutorAdded)

    // Keep removing until the limit is reached
    assert(manager.getExecutorsPendingToRemove.isEmpty)
    assert(manager.removeExecutor("1"))
    assert(manager.getExecutorsPendingToRemove.size === 1)
    assert(manager.getExecutorsPendingToRemove.contains("1"))
    assert(manager.removeExecutor("2"))
    assert(manager.removeExecutor("3"))
    assert(manager.getExecutorsPendingToRemove.size === 3)
    assert(manager.getExecutorsPendingToRemove.contains("2"))
    assert(manager.getExecutorsPendingToRemove.contains("3"))
    assert(!manager.removeExecutor("100")) // remove non-existent executors
    assert(!manager.removeExecutor("101"))
    assert(manager.getExecutorsPendingToRemove.size === 3)
    assert(manager.removeExecutor("4"))
    assert(manager.removeExecutor("5"))
    assert(!manager.removeExecutor("6")) // reached the limit of 5
    assert(manager.getExecutorsPendingToRemove.size === 5)
    assert(manager.getExecutorsPendingToRemove.contains("4"))
    assert(manager.getExecutorsPendingToRemove.contains("5"))
    assert(!manager.getExecutorsPendingToRemove.contains("6"))

    // Kill executors previously requested to remove
    manager.onExecutorRemoved("1")
    assert(manager.getExecutorsPendingToRemove.size === 4)
    assert(!manager.getExecutorsPendingToRemove.contains("1"))
    manager.onExecutorRemoved("2")
    manager.onExecutorRemoved("3")
    assert(manager.getExecutorsPendingToRemove.size === 2)
    assert(!manager.getExecutorsPendingToRemove.contains("2"))
    assert(!manager.getExecutorsPendingToRemove.contains("3"))
    manager.onExecutorRemoved("2") // duplicates should not count
    manager.onExecutorRemoved("3")
    assert(manager.getExecutorsPendingToRemove.size === 2)
    manager.onExecutorRemoved("4")
    manager.onExecutorRemoved("5")
    assert(manager.getExecutorsPendingToRemove.isEmpty)

    // Try removing again
    // This should still fail because the number pending + running is still at the limit
    assert(!manager.removeExecutor("7"))
    assert(manager.getExecutorsPendingToRemove.isEmpty)
    assert(!manager.removeExecutor("8"))
    assert(manager.getExecutorsPendingToRemove.isEmpty)
    sc.stop()
  }

  test ("interleaving add and remove") {
    val sc = createSparkContext(5, 10)
    val manager = sc.executorAllocationManager.get

    // Add a few executors
    assert(manager.addExecutors() === 1)
    assert(manager.addExecutors() === 2)
    assert(manager.addExecutors() === 4)
    manager.onExecutorAdded("1")
    manager.onExecutorAdded("2")
    manager.onExecutorAdded("3")
    manager.onExecutorAdded("4")
    manager.onExecutorAdded("5")
    manager.onExecutorAdded("6")
    manager.onExecutorAdded("7")
    assert(manager.getExecutorIds.size === 7)

    // Remove until limit
    assert(manager.removeExecutor("1"))
    assert(manager.removeExecutor("2"))
    assert(!manager.removeExecutor("3")) // lower limit reached
    assert(!manager.removeExecutor("4"))
    manager.onExecutorRemoved("1")
    manager.onExecutorRemoved("2")
    assert(manager.getExecutorIds.size === 5)

    // Add until limit
    assert(manager.addExecutors() === 5) // upper limit reached
    assert(manager.addExecutors() === 0)
    assert(!manager.removeExecutor("3")) // still at lower limit
    assert(!manager.removeExecutor("4"))
    manager.onExecutorAdded("8")
    manager.onExecutorAdded("9")
    manager.onExecutorAdded("10")
    manager.onExecutorAdded("11")
    manager.onExecutorAdded("12")
    assert(manager.getExecutorIds.size === 10)

    // Remove succeeds again, now that we are no longer at the lower limit
    assert(manager.removeExecutor("3"))
    assert(manager.removeExecutor("4"))
    assert(manager.removeExecutor("5"))
    assert(manager.removeExecutor("6"))
    assert(manager.getExecutorIds.size === 10)
    assert(manager.addExecutors() === 0) // still at upper limit
    manager.onExecutorRemoved("3")
    manager.onExecutorRemoved("4")
    assert(manager.getExecutorIds.size === 8)

    // Add succeeds again, now that we are no longer at the upper limit
    // Number of executors added restarts at 1
    assert(manager.addExecutors() === 1)
    assert(manager.addExecutors() === 1) // upper limit reached again
    assert(manager.addExecutors() === 0)
    assert(manager.getExecutorIds.size === 8)
    manager.onExecutorRemoved("5")
    manager.onExecutorRemoved("6")
    manager.onExecutorAdded("13")
    manager.onExecutorAdded("14")
    assert(manager.getExecutorIds.size === 8)
    assert(manager.addExecutors() === 1)
    assert(manager.addExecutors() === 1) // upper limit reached again
    assert(manager.addExecutors() === 0)
    manager.onExecutorAdded("15")
    manager.onExecutorAdded("16")
    assert(manager.getExecutorIds.size === 10)
    sc.stop()
  }

  test("starting/canceling add timer") {
    val addThresholdSeconds = 30L
    val sc = createSparkContext(2, 10, addThresholdSeconds = addThresholdSeconds)
    val manager = sc.executorAllocationManager.get

    assert(manager.getAddTime === NOT_SET)
    val firstAddTime = manager.onSchedulerBacklogged()
    assert(manager.getAddTime !== NOT_SET)
    Thread.sleep(100)
    assert(manager.onSchedulerBacklogged() === firstAddTime) // timer is already started
    assert(manager.onSchedulerBacklogged() === firstAddTime)
    assert(manager.onSchedulerBacklogged() <
      System.currentTimeMillis() + addThresholdSeconds * 1000)
    manager.onSchedulerQueueEmpty()

    // Restart add timer
    assert(manager.getAddTime === NOT_SET)
    val secondAddTime = manager.onSchedulerBacklogged()
    assert(manager.getAddTime !== NOT_SET)
    assert(manager.onSchedulerBacklogged() === secondAddTime)
    assert(firstAddTime !== secondAddTime)
  }

  test("starting/canceling remove timers") {
    val removeThresholdSeconds = 30L
    val sc = createSparkContext(2, 10, removeThresholdSeconds = removeThresholdSeconds)
    val manager = sc.executorAllocationManager.get

    assert(manager.getRemoveTimes.isEmpty)
    val firstRemoveTime = manager.onExecutorIdle("1")
    assert(manager.getRemoveTimes.size === 1)
    assert(manager.getRemoveTimes.contains("1"))
    Thread.sleep(100)
    assert(manager.onExecutorIdle("1") === firstRemoveTime) // timer is already started
    assert(manager.onExecutorIdle("1") === firstRemoveTime)
    assert(manager.onExecutorIdle("2") !== firstRemoveTime) // different executor
    assert(manager.onExecutorIdle("3") !== firstRemoveTime)
    assert(manager.getRemoveTimes.size === 3)
    assert(manager.getRemoveTimes.contains("2"))
    assert(manager.getRemoveTimes.contains("3"))
    assert(manager.getRemoveTimes("1") <
      System.currentTimeMillis() + removeThresholdSeconds * 1000)

    // Restart remove timer
    manager.onExecutorBusy("1")
    assert(manager.getRemoveTimes.size === 2)
    val secondRemoveTime = manager.onExecutorIdle("1")
    assert(manager.getRemoveTimes.size === 3)
    assert(manager.onExecutorIdle("1") === secondRemoveTime)
    assert(firstRemoveTime !== secondRemoveTime)
    assert(manager.getRemoveTimes("2") === manager.onExecutorIdle("2"))
    assert(manager.getRemoveTimes("3") === manager.onExecutorIdle("3"))
  }

  test("listeners trigger add executors correctly") {
    val sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(manager.getAddTime === NOT_SET)

    // Starting a stage should start the add timer
    val numTasks = 10
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, numTasks)))
    assert(manager.getAddTime !== NOT_SET)

    // Starting a subset of the tasks should not cancel the add timer
    val taskInfos = (0 to numTasks - 1).map { i => createTaskInfo(i, i, "executor-1") }
    taskInfos.tail.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, info)) }
    assert(manager.getAddTime !== NOT_SET)

    // Starting all remaining tasks should cancel the add timer
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, taskInfos.head))
    assert(manager.getAddTime === NOT_SET)

    // Start two different stages
    // The add timer should be canceled only if all tasks in both stages start running
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, numTasks)))
    sc.listenerBus.postToAll(SparkListenerStageSubmitted(createStageInfo(2, numTasks)))
    assert(manager.getAddTime !== NOT_SET)
    taskInfos.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(1, 0, info)) }
    assert(manager.getAddTime !== NOT_SET)
    taskInfos.foreach { info => sc.listenerBus.postToAll(SparkListenerTaskStart(2, 0, info)) }
    assert(manager.getAddTime === NOT_SET)
  }

  test("listeners trigger remove executors correctly") {
    val sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(manager.getRemoveTimes.isEmpty)

    // Added executors should start the remove timers for each executor
    (1 to 5).map("executor-" + _).foreach(manager.onExecutorAdded)
    assert(manager.getRemoveTimes.size === 5)

    // Starting a task cancel the remove timer for that executor
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(1, 1, "executor-1")))
    sc.listenerBus.postToAll(SparkListenerTaskStart(0, 0, createTaskInfo(2, 2, "executor-2")))
    assert(manager.getRemoveTimes.size === 3)
    assert(!manager.getRemoveTimes.contains("executor-1"))
    assert(!manager.getRemoveTimes.contains("executor-2"))

    // Finishing all tasks running on an executor should start the remove timer for that executor
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(0, 0, "executor-1"), new TaskMetrics))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(2, 2, "executor-2"), new TaskMetrics))
    assert(manager.getRemoveTimes.size === 4)
    assert(manager.getRemoveTimes.contains("executor-2"))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(1, 1, "executor-1"), new TaskMetrics))
    assert(manager.getRemoveTimes.size === 5)
    assert(manager.getRemoveTimes.contains("executor-1"))
  }

  test("listeners trigger add and remove executor callbacks correctly") {
    val sc = createSparkContext(2, 10)
    val manager = sc.executorAllocationManager.get
    assert(manager.getExecutorIds.isEmpty)
    assert(manager.getRemoveTimes.isEmpty)

    // New executors have registered
    sc.listenerBus.postToAll(SparkListenerBlockManagerAdded(
      0L, BlockManagerId("executor-1", "host1", 1), 100L))
    assert(manager.getExecutorIds.size === 1)
    assert(manager.getExecutorIds.contains("executor-1"))
    assert(manager.getRemoveTimes.size === 1)
    assert(manager.getRemoveTimes.contains("executor-1"))
    sc.listenerBus.postToAll(SparkListenerBlockManagerAdded(
      0L, BlockManagerId("executor-2", "host2", 1), 100L))
    assert(manager.getExecutorIds.size === 2)
    assert(manager.getExecutorIds.contains("executor-2"))
    assert(manager.getRemoveTimes.size === 2)
    assert(manager.getRemoveTimes.contains("executor-2"))

    // Existing executors have disconnected
    sc.listenerBus.postToAll(SparkListenerBlockManagerRemoved(
      0L, BlockManagerId("executor-1", "host1", 1)))
    assert(manager.getExecutorIds.size === 1)
    assert(!manager.getExecutorIds.contains("executor-1"))
    assert(manager.getRemoveTimes.size === 1)
    assert(!manager.getRemoveTimes.contains("executor-1"))

    // Unknown executor has disconnected
    sc.listenerBus.postToAll(SparkListenerBlockManagerRemoved(
      0L, BlockManagerId("executor-3", "host3", 1)))
    assert(manager.getExecutorIds.size === 1)
    assert(manager.getRemoveTimes.size === 1)
  }

}
