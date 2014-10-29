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

import org.scalatest.{FunSuite, PrivateMethodTester}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId

/**
 * Test add and remove behavior in ExecutorAllocationManager.
 */
class ExecutorAllocationManagerSuite extends FunSuite {
  import ExecutorAllocationManager._
  import ExecutorAllocationManagerSuite._

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
    assert(numExecutorsPending(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(executorIds(manager).isEmpty)
    assert(addTime(manager) === ExecutorAllocationManager.NOT_SET)
    assert(removeTimes(manager).isEmpty)
    sc.stop()
  }

  test("add executors") {
    val sc = createSparkContext(1, 10)
    val manager = sc.executorAllocationManager.get

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
    sc.stop()
  }

  test("remove executors") {
    val sc = createSparkContext(5, 10)
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
    sc.stop()
  }

  test ("interleaving add and remove") {
    val sc = createSparkContext(5, 10)
    val manager = sc.executorAllocationManager.get

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
    sc.stop()
  }

  test("starting/canceling add timer") {
    val schedulerBacklogTimeout = 30L
    val sc = createSparkContext(2, 10, schedulerBacklogTimeout = schedulerBacklogTimeout)
    val manager = sc.executorAllocationManager.get

    // Starting add timer is idempotent
    assert(addTime(manager) === NOT_SET)
    val firstMockTime = 8888L
    onSchedulerBacklogged(manager, firstMockTime)
    val firstAddTime = addTime(manager)
    assert(firstAddTime === firstMockTime + schedulerBacklogTimeout * 1000)
    onSchedulerBacklogged(manager, firstMockTime + 100L) // 100ms has elapsed
    assert(addTime(manager) === firstAddTime)            // timer is already started
    onSchedulerBacklogged(manager, firstMockTime + 200L)
    assert(addTime(manager) === firstAddTime)
    onSchedulerQueueEmpty(manager)

    // Restart add timer
    assert(addTime(manager) === NOT_SET)
    val secondMockTime = firstMockTime + 1000L // 1s has elapsed
    onSchedulerBacklogged(manager, secondMockTime)
    val secondAddTime = addTime(manager)
    assert(secondAddTime === secondMockTime + schedulerBacklogTimeout * 1000)
    onSchedulerBacklogged(manager, secondMockTime + 100L) // 100ms has elapsed
    assert(addTime(manager) === secondAddTime)            // timer is already started
    assert(addTime(manager) !== firstAddTime)
    assert(firstAddTime !== secondAddTime)
  }

  test("starting/canceling remove timers") {
    val executorIdleTimeout = 30L
    val sc = createSparkContext(2, 10, executorIdleTimeout = executorIdleTimeout)
    val manager = sc.executorAllocationManager.get

    // Starting remove timer is idempotent for each executor
    assert(removeTimes(manager).isEmpty)
    val firstMockTime = 14444L
    onExecutorIdle(manager, "1", firstMockTime)
    assert(removeTimes(manager).size === 1)
    assert(removeTimes(manager).contains("1"))
    val firstRemoveTime = removeTimes(manager)("1")
    assert(firstRemoveTime === firstMockTime + executorIdleTimeout * 1000)
    onExecutorIdle(manager, "1", firstMockTime + 100L)    // 100ms has elapsed
    assert(removeTimes(manager)("1") === firstRemoveTime) // timer is already started
    onExecutorIdle(manager, "1", firstMockTime + 200L)
    assert(removeTimes(manager)("1") === firstRemoveTime)
    onExecutorIdle(manager, "2", firstMockTime + 300L)
    assert(removeTimes(manager)("2") !== firstRemoveTime) // different executor
    assert(removeTimes(manager)("2") === firstMockTime + 300L + executorIdleTimeout * 1000)
    onExecutorIdle(manager, "3", firstMockTime + 400L)
    assert(removeTimes(manager)("3") !== firstRemoveTime)
    assert(removeTimes(manager)("3") === firstMockTime + 400L + executorIdleTimeout * 1000)
    assert(removeTimes(manager).size === 3)
    assert(removeTimes(manager).contains("2"))
    assert(removeTimes(manager).contains("3"))

    // Restart remove timer
    onExecutorBusy(manager, "1")
    val secondMockTime = firstMockTime + 1000L // 1s has elapsed
    assert(removeTimes(manager).size === 2)
    onExecutorIdle(manager, "1", secondMockTime)
    assert(removeTimes(manager).size === 3)
    assert(removeTimes(manager).contains("1"))
    val secondRemoveTime = removeTimes(manager)("1")
    assert(secondRemoveTime === secondMockTime + executorIdleTimeout * 1000)
    assert(removeTimes(manager)("1") === secondRemoveTime) // timer is already started
    assert(removeTimes(manager)("1") !== firstRemoveTime)
    assert(firstRemoveTime !== secondRemoveTime)
  }

  test("listeners trigger add executors correctly") {
    val sc = createSparkContext(2, 10)
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
    val sc = createSparkContext(2, 10)
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
    assert(removeTimes(manager).contains("executor-2"))
    sc.listenerBus.postToAll(SparkListenerTaskEnd(
      0, 0, "task-type", Success, createTaskInfo(1, 1, "executor-1"), new TaskMetrics))
    assert(removeTimes(manager).size === 5)
    assert(removeTimes(manager).contains("executor-1"))
  }

  test("listeners trigger add and remove executor callbacks correctly") {
    val sc = createSparkContext(2, 10)
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

}

/**
 * Helper methods for testing ExecutorAllocationManager.
 * This includes methods to access private methods and fields in ExecutorAllocationManager.
 */
private object ExecutorAllocationManagerSuite extends PrivateMethodTester {
  private val _numExecutorsToAdd = PrivateMethod[Int]('numExecutorsToAdd)
  private val _numExecutorsPending = PrivateMethod[Int]('numExecutorsPending)
  private val _executorsPendingToRemove =
    PrivateMethod[collection.Set[String]]('executorsPendingToRemove)
  private val _executorIds = PrivateMethod[collection.Set[String]]('executorIds)
  private val _addTime = PrivateMethod[Long]('addTime)
  private val _removeTimes = PrivateMethod[collection.Map[String, Long]]('removeTimes)
  private val _maybeAddAndRemove = PrivateMethod[Unit]('maybeAddAndRemove)
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

  private def maybeAndAndRemove(
      manager: ExecutorAllocationManager,
      now: Long = System.currentTimeMillis): Unit = {
    manager invokePrivate _maybeAddAndRemove(now)
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

  private def onSchedulerBacklogged(
      manager: ExecutorAllocationManager,
      now: Long = System.currentTimeMillis): Unit = {
    manager invokePrivate _onSchedulerBacklogged(now)
  }

  private def onSchedulerQueueEmpty(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _onSchedulerQueueEmpty()
  }

  private def onExecutorIdle(
      manager: ExecutorAllocationManager,
      id: String,
      now: Long = System.currentTimeMillis): Unit = {
    manager invokePrivate _onExecutorIdle(id, now)
  }

  private def onExecutorBusy(manager: ExecutorAllocationManager, id: String): Unit = {
    manager invokePrivate _onExecutorBusy(id)
  }

  private def createSparkContext(
      minExecutors: Int = 1,
      maxExecutors: Int = 5,
      schedulerBacklogTimeout: Long = 1,
      sustainedSchedulerBacklogTimeout: Long = 1,
      executorIdleTimeout: Long = 3): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", minExecutors + "")
      .set("spark.dynamicAllocation.maxExecutors", maxExecutors + "")
      .set("spark.dynamicAllocation.schedulerBacklogTimeout", schedulerBacklogTimeout + "")
      .set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout",
        sustainedSchedulerBacklogTimeout + "")
      .set("spark.dynamicAllocation.executorIdleTimeout", executorIdleTimeout + "")
    new SparkContext(conf)
  }

  private def createStageInfo(stageId: Int, numTasks: Int): StageInfo = {
    new StageInfo(stageId, 0, "name", numTasks, Seq.empty, "no details")
  }

  private def createTaskInfo(taskId: Int, taskIndex: Int, executorId: String): TaskInfo = {
    new TaskInfo(taskId, taskIndex, 0, 0, executorId, "", TaskLocality.ANY, speculative = false)
  }

}
