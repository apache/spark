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

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.PrivateMethodTester

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.Tests.TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.resource._
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
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
  private val clock = new SystemClock()
  private var rpManager: ResourceProfileManager = _


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
    listenerBus.waitUntilEmpty()
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
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(addTime(manager) === ExecutorAllocationManager.NOT_SET)
  }

  test("add executors default profile") {
    val manager = createManager(createConf(1, 10, 1))
    post(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    // Keep adding until the limit is reached
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 2)
    assert(numExecutorsToAddForDefaultProfile(manager) === 2)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 4)
    assert(numExecutorsToAddForDefaultProfile(manager) === 4)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 4)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 8)
    assert(numExecutorsToAddForDefaultProfile(manager) === 8)
    // reached the limit of 10
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)

    // Register previously requested executors
    onExecutorAddedDefaultProfile(manager, "first")
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    onExecutorAddedDefaultProfile(manager, "second")
    onExecutorAddedDefaultProfile(manager, "third")
    onExecutorAddedDefaultProfile(manager, "fourth")
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    onExecutorAddedDefaultProfile(manager, "first") // duplicates should not count
    onExecutorAddedDefaultProfile(manager, "second")
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)

    // Try adding again
    // This should still fail because the number pending + running is still at the limit
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
  }

  test("add executors multiple profiles") {
    val manager = createManager(createConf(1, 10, 1))
    post(SparkListenerStageSubmitted(createStageInfo(0, 1000, rp = defaultProfile)))
    val rp1 = new ResourceProfileBuilder()
    val execReqs = new ExecutorResourceRequests().cores(4).resource("gpu", 4)
    val taskReqs = new TaskResourceRequests().cpus(1).resource("gpu", 1)
    rp1.require(execReqs).require(taskReqs)
    val rprof1 = rp1.build()
    rpManager.addResourceProfile(rprof1)
    post(SparkListenerStageSubmitted(createStageInfo(1, 1000, rp = rprof1)))
    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    // Keep adding until the limit is reached
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    assert(numExecutorsToAdd(manager, rprof1) === 1)
    assert(numExecutorsTarget(manager, rprof1.id) === 1)
    assert(addExecutorsToTarget(manager, updatesNeeded, rprof1) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 2)
    assert(numExecutorsToAddForDefaultProfile(manager) === 2)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    assert(numExecutorsToAdd(manager, rprof1) === 2)
    assert(numExecutorsTarget(manager, rprof1.id) === 2)
    assert(addExecutorsToTarget(manager, updatesNeeded, rprof1) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 4)
    assert(numExecutorsToAddForDefaultProfile(manager) === 4)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 4)
    assert(numExecutorsToAdd(manager, rprof1) === 4)
    assert(numExecutorsTarget(manager, rprof1.id) === 4)
    assert(addExecutorsToTarget(manager, updatesNeeded, rprof1) === 4)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 8)
    assert(numExecutorsToAddForDefaultProfile(manager) === 8)
    // reached the limit of 10
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    assert(numExecutorsToAdd(manager, rprof1) === 8)
    assert(numExecutorsTarget(manager, rprof1.id) === 8)
    assert(addExecutorsToTarget(manager, updatesNeeded, rprof1) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    assert(numExecutorsToAdd(manager, rprof1) === 1)
    assert(numExecutorsTarget(manager, rprof1.id) === 10)
    assert(addExecutorsToTarget(manager, updatesNeeded, rprof1) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(numExecutorsToAdd(manager, rprof1) === 1)
    assert(numExecutorsTarget(manager, rprof1.id) === 10)

    // Register previously requested executors
    onExecutorAddedDefaultProfile(manager, "first")
    onExecutorAdded(manager, "firstrp1", rprof1)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsTarget(manager, rprof1.id) === 10)
    onExecutorAddedDefaultProfile(manager, "second")
    onExecutorAddedDefaultProfile(manager, "third")
    onExecutorAddedDefaultProfile(manager, "fourth")
    onExecutorAdded(manager, "secondrp1", rprof1)
    onExecutorAdded(manager, "thirdrp1", rprof1)
    onExecutorAdded(manager, "fourthrp1", rprof1)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsTarget(manager, rprof1.id) === 10)
    onExecutorAddedDefaultProfile(manager, "first") // duplicates should not count
    onExecutorAddedDefaultProfile(manager, "second")
    onExecutorAdded(manager, "firstrp1", rprof1)
    onExecutorAdded(manager, "secondrp1", rprof1)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsTarget(manager, rprof1.id) === 10)

    // Try adding again
    // This should still fail because the number pending + running is still at the limit
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    assert(addExecutorsToTarget(manager, updatesNeeded, rprof1) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(numExecutorsToAdd(manager, rprof1) === 1)
    assert(numExecutorsTarget(manager, rprof1.id) === 10)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    assert(addExecutorsToTarget(manager, updatesNeeded, rprof1) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(numExecutorsToAdd(manager, rprof1) === 1)
    assert(numExecutorsTarget(manager, rprof1.id) === 10)
  }

  test("add executors multiple profiles initial num same as needed") {
    // test when the initial number of executors equals the number needed for the first
    // stage using a non default profile to make sure we request the initial number
    // properly. Here initial is 2, each executor in ResourceProfile 1 can have 2 tasks
    // per executor, and start a stage with 4 tasks, which would need 2 executors.
    val clock = new ManualClock(8888L)
    val manager = createManager(createConf(0, 10, 2), clock)
    val rp1 = new ResourceProfileBuilder()
    val execReqs = new ExecutorResourceRequests().cores(2).resource("gpu", 2)
    val taskReqs = new TaskResourceRequests().cpus(1).resource("gpu", 1)
    rp1.require(execReqs).require(taskReqs)
    val rprof1 = rp1.build()
    rpManager.addResourceProfile(rprof1)
    when(client.requestTotalExecutors(any(), any(), any())).thenReturn(true)
    post(SparkListenerStageSubmitted(createStageInfo(1, 4, rp = rprof1)))
    // called once on start and a second time on stage submit with initial number
    verify(client, times(2)).requestTotalExecutors(any(), any(), any())
    assert(numExecutorsTarget(manager, rprof1.id) === 2)
  }

  test("remove executors multiple profiles") {
    val manager = createManager(createConf(5, 10, 5))
    val rp1 = new ResourceProfileBuilder()
    val execReqs = new ExecutorResourceRequests().cores(4).resource("gpu", 4)
    val taskReqs = new TaskResourceRequests().cpus(1).resource("gpu", 1)
    rp1.require(execReqs).require(taskReqs)
    val rprof1 = rp1.build()
    val rp2 = new ResourceProfileBuilder()
    val execReqs2 = new ExecutorResourceRequests().cores(1)
    val taskReqs2 = new TaskResourceRequests().cpus(1)
    rp2.require(execReqs2).require(taskReqs2)
    val rprof2 = rp2.build()
    rpManager.addResourceProfile(rprof1)
    rpManager.addResourceProfile(rprof2)
    post(SparkListenerStageSubmitted(createStageInfo(1, 10, rp = rprof1)))
    post(SparkListenerStageSubmitted(createStageInfo(2, 10, rp = rprof2)))

    (1 to 10).map(_.toString).foreach { id => onExecutorAdded(manager, id, rprof1) }
    (11 to 20).map(_.toString).foreach { id => onExecutorAdded(manager, id, rprof2) }
    (21 to 30).map(_.toString).foreach { id => onExecutorAdded(manager, id, defaultProfile) }

    // Keep removing until the limit is reached
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutor(manager, "1", rprof1.id))
    assert(executorsPendingToRemove(manager).size === 1)
    assert(executorsPendingToRemove(manager).contains("1"))
    assert(removeExecutor(manager, "11", rprof2.id))
    assert(removeExecutor(manager, "2", rprof1.id))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(executorsPendingToRemove(manager).contains("2"))
    assert(executorsPendingToRemove(manager).contains("11"))
    assert(removeExecutor(manager, "21", defaultProfile.id))
    assert(removeExecutor(manager, "3", rprof1.id))
    assert(removeExecutor(manager, "4", rprof1.id))
    assert(executorsPendingToRemove(manager).size === 6)
    assert(executorsPendingToRemove(manager).contains("21"))
    assert(executorsPendingToRemove(manager).contains("3"))
    assert(executorsPendingToRemove(manager).contains("4"))
    assert(removeExecutor(manager, "5", rprof1.id))
    assert(!removeExecutor(manager, "6", rprof1.id)) // reached the limit of 5
    assert(executorsPendingToRemove(manager).size === 7)
    assert(executorsPendingToRemove(manager).contains("5"))
    assert(!executorsPendingToRemove(manager).contains("6"))

    // Kill executors previously requested to remove
    onExecutorRemoved(manager, "1")
    assert(executorsPendingToRemove(manager).size === 6)
    assert(!executorsPendingToRemove(manager).contains("1"))
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")
    assert(executorsPendingToRemove(manager).size === 4)
    assert(!executorsPendingToRemove(manager).contains("2"))
    assert(!executorsPendingToRemove(manager).contains("3"))
    onExecutorRemoved(manager, "2") // duplicates should not count
    onExecutorRemoved(manager, "3")
    assert(executorsPendingToRemove(manager).size === 4)
    onExecutorRemoved(manager, "4")
    onExecutorRemoved(manager, "5")
    assert(executorsPendingToRemove(manager).size === 2)
    assert(executorsPendingToRemove(manager).contains("11"))
    assert(executorsPendingToRemove(manager).contains("21"))

    // Try removing again
    // This should still fail because the number pending + running is still at the limit
    assert(!removeExecutor(manager, "7", rprof1.id))
    assert(executorsPendingToRemove(manager).size === 2)
    assert(!removeExecutor(manager, "8", rprof1.id))
    assert(executorsPendingToRemove(manager).size === 2)

    // make sure rprof2 has the same min limit or 5
    assert(removeExecutor(manager, "12", rprof2.id))
    assert(removeExecutor(manager, "13", rprof2.id))
    assert(removeExecutor(manager, "14", rprof2.id))
    assert(removeExecutor(manager, "15", rprof2.id))
    assert(!removeExecutor(manager, "16", rprof2.id)) // reached the limit of 5
    assert(executorsPendingToRemove(manager).size === 6)
    assert(!executorsPendingToRemove(manager).contains("16"))
    onExecutorRemoved(manager, "11")
    onExecutorRemoved(manager, "12")
    onExecutorRemoved(manager, "13")
    onExecutorRemoved(manager, "14")
    onExecutorRemoved(manager, "15")
    assert(executorsPendingToRemove(manager).size === 1)
  }

  def testAllocationRatio(cores: Int, divisor: Double, expected: Int): Unit = {
    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]
    val conf = createConf(3, 15)
      .set(config.DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO, divisor)
      .set(config.EXECUTOR_CORES, cores)
    val manager = createManager(conf)
    post(SparkListenerStageSubmitted(createStageInfo(0, 20)))
    for (i <- 0 to 5) {
      addExecutorsToTargetForDefaultProfile(manager, updatesNeeded)
      doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    }
    assert(numExecutorsTargetForDefaultProfileId(manager) === expected)
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

    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    // Verify that we're capped at number of tasks in the stage
    assert(numExecutorsTargetForDefaultProfileId(manager) === 0)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(numExecutorsToAddForDefaultProfile(manager) === 2)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 3)
    assert(numExecutorsToAddForDefaultProfile(manager) === 4)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)

    // Verify that running a task doesn't affect the target
    post(SparkListenerStageSubmitted(createStageInfo(1, 3)))
    post(SparkListenerExecutorAdded(
      0L, "executor-1", new ExecutorInfo("host1", 1, Map.empty, Map.empty)))
    post(SparkListenerTaskStart(1, 0, createTaskInfo(0, 0, "executor-1")))
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 6)
    assert(numExecutorsToAddForDefaultProfile(manager) === 2)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)

    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 8)
    assert(numExecutorsToAddForDefaultProfile(manager) === 4)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 8)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)

    // Verify that re-running a task doesn't blow things up
    post(SparkListenerStageSubmitted(createStageInfo(2, 3)))
    post(SparkListenerTaskStart(2, 0, createTaskInfo(0, 0, "executor-1")))
    post(SparkListenerTaskStart(2, 0, createTaskInfo(1, 0, "executor-1")))
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 9)
    assert(numExecutorsToAddForDefaultProfile(manager) === 2)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)

    // Verify that running a task once we're at our limit doesn't blow things up
    post(SparkListenerTaskStart(2, 0, createTaskInfo(0, 1, "executor-1")))
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 10)
  }

  private def speculativeTaskSubmitEventFromTaskIndex(
    stageId: Int,
    stageAttemptId: Int = 0,
    taskIndex: Int = -1,
    partitionId: Int = -1): SparkListenerSpeculativeTaskSubmitted = {
    val event = new SparkListenerSpeculativeTaskSubmitted(stageId, stageAttemptId,
      taskIndex = taskIndex, partitionId = partitionId)
    event
  }

  test("add executors when speculative tasks added") {
    val manager = createManager(createConf(0, 10, 0))

    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    post(SparkListenerStageSubmitted(createStageInfo(1, 2)))
    // Verify that we're capped at number of tasks including the speculative ones in the stage
    post(speculativeTaskSubmitEventFromTaskIndex(1, taskIndex = 0))
    assert(numExecutorsTargetForDefaultProfileId(manager) === 0)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    post(speculativeTaskSubmitEventFromTaskIndex(1, taskIndex = 1))
    post(speculativeTaskSubmitEventFromTaskIndex(1, taskIndex = 2))
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(numExecutorsToAddForDefaultProfile(manager) === 2)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 3)
    assert(numExecutorsToAddForDefaultProfile(manager) === 4)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)

    // Verify that running a task doesn't affect the target
    post(SparkListenerTaskStart(1, 0, createTaskInfo(0, 0, "executor-1")))
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)

    // Verify that running a speculative task doesn't affect the target
    post(SparkListenerTaskStart(1, 0, createTaskInfo(1, 0, "executor-2", true)))
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
  }

  test("SPARK-31418: one stage being unschedulable") {
    val clock = new ManualClock()
    val conf = createConf(0, 5, 0).set(config.EXECUTOR_CORES, 2)
    val manager = createManager(conf, clock = clock)
    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    post(SparkListenerStageSubmitted(createStageInfo(0, 2)))

    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)

    onExecutorAddedDefaultProfile(manager, "0")
    val t1 = createTaskInfo(0, 0, executorId = s"0")
    val t2 = createTaskInfo(1, 1, executorId = s"0")
    post(SparkListenerTaskStart(0, 0, t1))
    post(SparkListenerTaskStart(0, 0, t2))

    assert(numExecutorsTarget(manager, defaultProfile.id) === 1)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 1)

    // Stage 0 becomes unschedulable due to excludeOnFailure
    post(SparkListenerUnschedulableTaskSetAdded(0, 0))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    // Assert that we are getting additional executor to schedule unschedulable tasks
    assert(numExecutorsTarget(manager, defaultProfile.id) === 2)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 2)

    // Add a new executor
    onExecutorAddedDefaultProfile(manager, "1")
    // Now once the task becomes schedulable, clear the unschedulableTaskSets
    post(SparkListenerUnschedulableTaskSetRemoved(0, 0))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 1)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 1)
  }

  test("SPARK-31418: multiple stages being unschedulable") {
    val clock = new ManualClock()
    val conf = createConf(0, 10, 0).set(config.EXECUTOR_CORES, 2)
    val manager = createManager(conf, clock = clock)
    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    post(SparkListenerStageSubmitted(createStageInfo(0, 2)))
    post(SparkListenerStageSubmitted(createStageInfo(1, 2)))
    post(SparkListenerStageSubmitted(createStageInfo(2, 2)))

    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)

    // Add necessary executors
    (0 to 2).foreach(execId => onExecutorAddedDefaultProfile(manager, execId.toString))

    // Start all the tasks
    (0 to 2).foreach {
      i =>
        val t1Info = createTaskInfo(0, (i * 2) + 1, executorId = s"${i / 2}")
        val t2Info = createTaskInfo(1, (i * 2) + 2, executorId = s"${i / 2}")
        post(SparkListenerTaskStart(i, 0, t1Info))
        post(SparkListenerTaskStart(i, 0, t2Info))
    }
    assert(numExecutorsTarget(manager, defaultProfile.id) === 3)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 3)

    // Complete the stage 0 tasks.
    val t1Info = createTaskInfo(0, 0, executorId = s"0")
    val t2Info = createTaskInfo(1, 1, executorId = s"0")
    post(SparkListenerTaskEnd(0, 0, null, Success, t1Info, new ExecutorMetrics, null))
    post(SparkListenerTaskEnd(0, 0, null, Success, t2Info, new ExecutorMetrics, null))
    post(SparkListenerStageCompleted(createStageInfo(0, 2)))

    // Stage 1 and 2 becomes unschedulable now due to excludeOnFailure
    post(SparkListenerUnschedulableTaskSetAdded(1, 0))
    post(SparkListenerUnschedulableTaskSetAdded(2, 0))

    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    // Assert that we are getting additional executor to schedule unschedulable tasks
    assert(numExecutorsTarget(manager, defaultProfile.id) === 4)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 4)

    // Add a new executor
    onExecutorAddedDefaultProfile(manager, "3")

    // Now once the task becomes schedulable, clear the unschedulableTaskSets
    post(SparkListenerUnschedulableTaskSetRemoved(1, 0))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 4)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 5)
  }

  test("SPARK-31418: remove executors after unschedulable tasks end") {
    val clock = new ManualClock()
    val stage = createStageInfo(0, 10)
    val conf = createConf(0, 6, 0).set(config.EXECUTOR_CORES, 2)
    val manager = createManager(conf, clock = clock)
    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    post(SparkListenerStageSubmitted(stage))
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())

    (0 to 4).foreach(execId => onExecutorAddedDefaultProfile(manager, execId.toString))
    (0 to 9).map { i => createTaskInfo(i, i, executorId = s"${i / 2}") }.foreach {
      info => post(SparkListenerTaskStart(0, 0, info))
    }
    assert(numExecutorsTarget(manager, defaultProfile.id) === 5)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 5)

    // 8 tasks (0 - 7) finished
    (0 to 7).map { i => createTaskInfo(i, i, executorId = s"${i / 2}") }.foreach {
      info => post(SparkListenerTaskEnd(0, 0, null, Success, info, new ExecutorMetrics, null))
    }
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 1)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 1)
    (0 to 3).foreach { i => assert(removeExecutorDefaultProfile(manager, i.toString)) }
    (0 to 3).foreach { i => onExecutorRemoved(manager, i.toString) }

    // Now due to executor being excluded, the task becomes unschedulable
    post(SparkListenerUnschedulableTaskSetAdded(0, 0))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 2)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 2)

    // New executor got added
    onExecutorAddedDefaultProfile(manager, "5")

    // Now once the task becomes schedulable, clear the unschedulableTaskSets
    post(SparkListenerUnschedulableTaskSetRemoved(0, 0))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 1)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 1)
    post(SparkListenerTaskEnd(0, 0, null, Success,
      createTaskInfo(9, 9, "4"), new ExecutorMetrics, null))
    // Unschedulable task successfully ran on the new executor provisioned
    post(SparkListenerTaskEnd(0, 0, null, Success,
      createTaskInfo(8, 8, "5"), new ExecutorMetrics, null))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    post(SparkListenerStageCompleted(stage))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 0)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 0)
    assert(removeExecutorDefaultProfile(manager, "4"))
    onExecutorRemoved(manager, "4")
    assert(removeExecutorDefaultProfile(manager, "5"))
    onExecutorRemoved(manager, "5")
  }

  test("SPARK-41192: remove executors when task finished before speculative task scheduled") {
    val clock = new ManualClock()
    val stage = createStageInfo(0, 40)
    val conf = createConf(0, 10, 0).set(config.EXECUTOR_CORES, 4)
    val manager = createManager(conf, clock = clock)
    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    // submit 40 tasks, total executors needed = 40/4 = 10
    post(SparkListenerStageSubmitted(stage))
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 4)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 3)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())

    (0 until 10).foreach(execId => onExecutorAddedDefaultProfile(manager, execId.toString))
    (0 until 40).map { i => createTaskInfo(i, i, executorId = s"${i / 4}")}.foreach {
      info => post(SparkListenerTaskStart(0, 0, info))
    }
    assert(numExecutorsTarget(manager, defaultProfile.id) === 10)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 10)
    // 30 tasks (0 - 29) finished
    (0 until 30).map { i => createTaskInfo(i, i, executorId = s"${i / 4}")}.foreach {
      info => post(SparkListenerTaskEnd(0, 0, null, Success, info, new ExecutorMetrics, null)) }
    // 10 speculative tasks (30 - 39) launch for the remaining tasks
    (30 until 40).foreach { index =>
      post(speculativeTaskSubmitEventFromTaskIndex(0, taskIndex = index))}
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 5)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 5)
    (0 until 5).foreach { i => assert(removeExecutorDefaultProfile(manager, i.toString))}
    (0 until 5).foreach { i => onExecutorRemoved(manager, i.toString)}

    // 5 original tasks (30 - 34) finished before speculative task start,
    // the speculative task will be removed from pending tasks
    // executors needed = (5 + 5) / 4 + 1
    (30 until 35).map { i =>
      createTaskInfo(i, i, executorId = s"${i / 4}")}
      .foreach { info => post(
        SparkListenerTaskEnd(0, 0, null, Success, info, new ExecutorMetrics, null))}
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 3)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 3)

    (40 until 45).map { i =>
      createTaskInfo(i, i - 5, executorId = s"${i / 4}", speculative = true)
    }.foreach {
      info => post(SparkListenerTaskStart(0, 0, info))
    }
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 3)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 3)

    (35 until 39).map { i =>
      createTaskInfo(i, i, executorId = s"${i / 4}")
    }.foreach {
      info => post(SparkListenerTaskEnd(0, 0, null, Success, info, new ExecutorMetrics, null))
    }
    (35 until 39).map { i =>
      createTaskInfo(i + 5, i, executorId = s"${(i + 5) / 4}", speculative = true)
    }.foreach {
      info => post(SparkListenerTaskEnd(0, 0, null, TaskKilled("attempt"),
        info, new ExecutorMetrics, null))
    }
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 1)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 1)
  }

  test("SPARK-30511 remove executors when speculative tasks end") {
    val clock = new ManualClock()
    val stage = createStageInfo(0, 40)
    val conf = createConf(0, 10, 0).set(config.EXECUTOR_CORES, 4)
    val manager = createManager(conf, clock = clock)
    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    post(SparkListenerStageSubmitted(stage))
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 4)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 3)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())

    (0 to 9).foreach(execId => onExecutorAddedDefaultProfile(manager, execId.toString))
    (0 to 39).map { i => createTaskInfo(i, i, executorId = s"${i / 4}")}.foreach {
      info => post(SparkListenerTaskStart(0, 0, info))
    }
    assert(numExecutorsTarget(manager, defaultProfile.id) === 10)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 10)

    // 30 tasks (0 - 29) finished
    (0 to 29).map { i => createTaskInfo(i, i, executorId = s"${i / 4}")}.foreach {
      info => post(SparkListenerTaskEnd(0, 0, null, Success, info, new ExecutorMetrics, null)) }
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 3)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 3)
    (0 to 6).foreach { i => assert(removeExecutorDefaultProfile(manager, i.toString))}
    (0 to 6).foreach { i => onExecutorRemoved(manager, i.toString)}

    // 10 speculative tasks (30 - 39) launch for the remaining tasks
    (30 to 39).foreach { i => post(speculativeTaskSubmitEventFromTaskIndex(0, taskIndex = i))}
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTarget(manager, defaultProfile.id) == 5)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 5)
    (10 to 12).foreach(execId => onExecutorAddedDefaultProfile(manager, execId.toString))
    (40 to 49).map { i =>
      createTaskInfo(taskId = i, taskIndex = i - 10, executorId = s"${i / 4}", speculative = true)}
      .foreach { info => post(SparkListenerTaskStart(0, 0, info))}
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    // At this point, we still have 6 executors running
    assert(numExecutorsTarget(manager, defaultProfile.id) == 5)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 5)

    // 6 speculative tasks (40 - 45) finish before the original tasks, with 4 speculative remaining
    (40 to 45).map { i =>
      createTaskInfo(taskId = i, taskIndex = i - 10, executorId = s"${i / 4}", speculative = true)}
      .foreach {
        info => post(SparkListenerTaskEnd(0, 0, null, Success, info, new ExecutorMetrics, null))}
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 4)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 4)
    assert(removeExecutorDefaultProfile(manager, "10"))
    onExecutorRemoved(manager, "10")
    // At this point, we still have 5 executors running: ["7", "8", "9", "11", "12"]

    // 6 original tasks (30 - 35) are intentionally killed
    (30 to 35).map { i =>
      createTaskInfo(i, i, executorId = s"${i / 4}")}
      .foreach { info => post(
        SparkListenerTaskEnd(0, 0, null, TaskKilled("test"), info, new ExecutorMetrics, null))}
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 2)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 2)
    (7 to 8).foreach { i => assert(removeExecutorDefaultProfile(manager, i.toString))}
    (7 to 8).foreach { i => onExecutorRemoved(manager, i.toString)}
    // At this point, we still have 3 executors running: ["9", "11", "12"]

    // Task 36 finishes before the speculative task 46, task 46 killed
    post(SparkListenerTaskEnd(0, 0, null, Success,
      createTaskInfo(36, 36, executorId = "9"), new ExecutorMetrics, null))
    post(SparkListenerTaskEnd(0, 0, null, TaskKilled("test"),
      createTaskInfo(46, 36, executorId = "11", speculative = true), new ExecutorMetrics, null))

    // We should have 3 original tasks (index 37, 38, 39) running, with corresponding 3 speculative
    // tasks running. Target lowers to 2, but still hold 3 executors ["9", "11", "12"]
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 2)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 2)
    // At this point, we still have 3 executors running: ["9", "11", "12"]

    // Task 37 and 47 succeed at the same time
    post(SparkListenerTaskEnd(0, 0, null, Success,
      createTaskInfo(37, 37, executorId = "9"), new ExecutorMetrics, null))
    post(SparkListenerTaskEnd(0, 0, null, Success,
      createTaskInfo(47, 37, executorId = "11", speculative = true), new ExecutorMetrics, null))

    // We should have 2 original tasks (index 38, 39) running, with corresponding 2 speculative
    // tasks running
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 1)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 1)
    assert(removeExecutorDefaultProfile(manager, "11"))
    onExecutorRemoved(manager, "11")
    // At this point, we still have 2 executors running: ["9", "12"]

    // Task 38 fails and task 49 fails, new speculative task 50 is submitted to speculate on task 39
    post(SparkListenerTaskEnd(0, 0, null, UnknownReason,
      createTaskInfo(38, 38, executorId = "9"), new ExecutorMetrics, null))
    post(SparkListenerTaskEnd(0, 0, null, UnknownReason,
      createTaskInfo(49, 39, executorId = "12", speculative = true), new ExecutorMetrics, null))
    post(speculativeTaskSubmitEventFromTaskIndex(0, taskIndex = 39))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    // maxNeeded = 1, allocate one more to satisfy speculation locality requirement
    assert(numExecutorsTarget(manager, defaultProfile.id) === 2)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 2)
    post(SparkListenerTaskStart(0, 0,
      createTaskInfo(50, 39, executorId = "12", speculative = true)))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 1)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 1)

    // Task 39 and 48 succeed, task 50 killed
    post(SparkListenerTaskEnd(0, 0, null, Success,
      createTaskInfo(39, 39, executorId = "9"), new ExecutorMetrics, null))
    post(SparkListenerTaskEnd(0, 0, null, Success,
      createTaskInfo(48, 38, executorId = "12", speculative = true), new ExecutorMetrics, null))
    post(SparkListenerTaskEnd(0, 0, null, TaskKilled("test"),
      createTaskInfo(50, 39, executorId = "12", speculative = true), new ExecutorMetrics, null))
    post(SparkListenerStageCompleted(stage))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTarget(manager, defaultProfile.id) === 0)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 0)
    assert(removeExecutorDefaultProfile(manager, "9"))
    onExecutorRemoved(manager, "9")
    assert(removeExecutorDefaultProfile(manager, "12"))
    onExecutorRemoved(manager, "12")
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
    assert(totalRunningTasksPerResourceProfile(manager) === 2)

    // submit another attempt for the stage.  We count completions from the first zombie attempt
    val stageAttempt1 = createStageInfo(stage.stageId, 5, attemptId = 1)
    post(SparkListenerStageSubmitted(stageAttempt1))
    post(SparkListenerTaskEnd(0, 0, null, Success, taskInfo1, new ExecutorMetrics, null))
    assert(totalRunningTasksPerResourceProfile(manager) === 1)
    val attemptTaskInfo1 = createTaskInfo(3, 0, "executor-1")
    val attemptTaskInfo2 = createTaskInfo(4, 1, "executor-1")
    post(SparkListenerTaskStart(0, 1, attemptTaskInfo1))
    post(SparkListenerTaskStart(0, 1, attemptTaskInfo2))
    assert(totalRunningTasksPerResourceProfile(manager) === 3)
    post(SparkListenerTaskEnd(0, 1, null, Success, attemptTaskInfo1, new ExecutorMetrics, null))
    assert(totalRunningTasksPerResourceProfile(manager) === 2)
    post(SparkListenerTaskEnd(0, 0, null, Success, taskInfo2, new ExecutorMetrics, null))
    assert(totalRunningTasksPerResourceProfile(manager) === 1)
    post(SparkListenerTaskEnd(0, 1, null, Success, attemptTaskInfo2, new ExecutorMetrics, null))
    assert(totalRunningTasksPerResourceProfile(manager) === 0)
  }

  testRetry("cancel pending executors when no longer needed") {
    val manager = createManager(createConf(0, 10, 0))
    post(SparkListenerStageSubmitted(createStageInfo(2, 5)))

    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    assert(numExecutorsTargetForDefaultProfileId(manager) === 0)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(numExecutorsToAddForDefaultProfile(manager) === 2)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 3)

    val task1Info = createTaskInfo(0, 0, "executor-1")
    post(SparkListenerTaskStart(2, 0, task1Info))

    assert(numExecutorsToAddForDefaultProfile(manager) === 4)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())

    val task2Info = createTaskInfo(1, 0, "executor-1")
    post(SparkListenerTaskStart(2, 0, task2Info))

    task1Info.markFinished(TaskState.FINISHED, System.currentTimeMillis())
    post(SparkListenerTaskEnd(2, 0, null, Success, task1Info, new ExecutorMetrics, null))

    task2Info.markFinished(TaskState.FINISHED, System.currentTimeMillis())
    post(SparkListenerTaskEnd(2, 0, null, Success, task2Info, new ExecutorMetrics, null))

    assert(adjustRequestedExecutors(manager) === -1)
  }

  test("remove executors") {
    val manager = createManager(createConf(5, 10, 5))
    (1 to 10).map(_.toString).foreach { id => onExecutorAddedDefaultProfile(manager, id) }

    // Keep removing until the limit is reached
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutorDefaultProfile(manager, "1"))
    assert(executorsPendingToRemove(manager).size === 1)
    assert(executorsPendingToRemove(manager).contains("1"))
    assert(removeExecutorDefaultProfile(manager, "2"))
    assert(removeExecutorDefaultProfile(manager, "3"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(executorsPendingToRemove(manager).contains("2"))
    assert(executorsPendingToRemove(manager).contains("3"))
    assert(removeExecutorDefaultProfile(manager, "4"))
    assert(removeExecutorDefaultProfile(manager, "5"))
    assert(!removeExecutorDefaultProfile(manager, "6")) // reached the limit of 5
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
    assert(!removeExecutorDefaultProfile(manager, "7"))
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(!removeExecutorDefaultProfile(manager, "8"))
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test("SPARK-33763: metrics to track dynamic allocation (decommissionEnabled=false)") {
    val manager = createManager(createConf(3, 5, 3))
    (1 to 5).map(_.toString).foreach { id => onExecutorAddedDefaultProfile(manager, id) }

    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutorsDefaultProfile(manager, Seq("1", "2")) === Seq("1", "2"))
    assert(executorsPendingToRemove(manager).contains("1"))
    assert(executorsPendingToRemove(manager).contains("2"))

    onExecutorRemoved(manager, "1", "driver requested exit")
    assert(manager.executorAllocationManagerSource.driverKilled.getCount() === 1)
    assert(manager.executorAllocationManagerSource.exitedUnexpectedly.getCount() === 0)

    onExecutorRemoved(manager, "2", "another driver requested exit")
    assert(manager.executorAllocationManagerSource.driverKilled.getCount() === 2)
    assert(manager.executorAllocationManagerSource.exitedUnexpectedly.getCount() === 0)

    onExecutorRemoved(manager, "3", "this will be an unexpected exit")
    assert(manager.executorAllocationManagerSource.driverKilled.getCount() === 2)
    assert(manager.executorAllocationManagerSource.exitedUnexpectedly.getCount() === 1)
  }

  test("SPARK-33763: metrics to track dynamic allocation (decommissionEnabled = true)") {
    val manager = createManager(createConf(3, 5, 3, decommissioningEnabled = true))
    (1 to 5).map(_.toString).foreach { id => onExecutorAddedDefaultProfile(manager, id) }

    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutorsDefaultProfile(manager, Seq("1", "2")) === Seq("1", "2"))
    assert(executorsDecommissioning(manager).contains("1"))
    assert(executorsDecommissioning(manager).contains("2"))

    onExecutorRemoved(manager, "1", ExecutorLossMessage.decommissionFinished)
    assert(manager.executorAllocationManagerSource.gracefullyDecommissioned.getCount() === 1)
    assert(manager.executorAllocationManagerSource.decommissionUnfinished.getCount() === 0)
    assert(manager.executorAllocationManagerSource.exitedUnexpectedly.getCount() === 0)

    onExecutorRemoved(manager, "2", "stopped before gracefully finished")
    assert(manager.executorAllocationManagerSource.gracefullyDecommissioned.getCount() === 1)
    assert(manager.executorAllocationManagerSource.decommissionUnfinished.getCount() === 1)
    assert(manager.executorAllocationManagerSource.exitedUnexpectedly.getCount() === 0)

    onExecutorRemoved(manager, "3", "this will be an unexpected exit")
    assert(manager.executorAllocationManagerSource.gracefullyDecommissioned.getCount() === 1)
    assert(manager.executorAllocationManagerSource.decommissionUnfinished.getCount() === 1)
    assert(manager.executorAllocationManagerSource.exitedUnexpectedly.getCount() === 1)
  }

  test("remove multiple executors") {
    val manager = createManager(createConf(5, 10, 5))
    (1 to 10).map(_.toString).foreach { id => onExecutorAddedDefaultProfile(manager, id) }

    // Keep removing until the limit is reached
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutorsDefaultProfile(manager, Seq("1")) === Seq("1"))
    assert(executorsPendingToRemove(manager).size === 1)
    assert(executorsPendingToRemove(manager).contains("1"))
    assert(removeExecutorsDefaultProfile(manager, Seq("2", "3")) === Seq("2", "3"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(executorsPendingToRemove(manager).contains("2"))
    assert(executorsPendingToRemove(manager).contains("3"))
    assert(executorsPendingToRemove(manager).size === 3)
    assert(removeExecutorDefaultProfile(manager, "4"))
    assert(removeExecutorsDefaultProfile(manager, Seq("5")) === Seq("5"))
    assert(!removeExecutorDefaultProfile(manager, "6")) // reached the limit of 5
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
    assert(!removeExecutorDefaultProfile(manager, "7"))
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(removeExecutorsDefaultProfile(manager, Seq("8")) !== Seq("8"))
    assert(executorsPendingToRemove(manager).isEmpty)
  }

  test ("Removing with various numExecutorsTargetForDefaultProfileId condition") {
    val manager = createManager(createConf(5, 12, 5))

    post(SparkListenerStageSubmitted(createStageInfo(0, 8)))

    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    // Remove when numExecutorsTargetForDefaultProfileId is the same as the current
    // number of executors
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    (1 to 8).foreach(execId => onExecutorAddedDefaultProfile(manager, execId.toString))
    (1 to 8).map { i => createTaskInfo(i, i, s"$i") }.foreach {
      info => post(SparkListenerTaskStart(0, 0, info)) }
    assert(manager.executorMonitor.executorCount === 8)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 8)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 8)
    // won't work since numExecutorsTargetForDefaultProfileId == numExecutors
    assert(!removeExecutorDefaultProfile(manager, "1"))

    // Remove executors when numExecutorsTargetForDefaultProfileId is lower than
    // current number of executors
    (1 to 3).map { i => createTaskInfo(i, i, s"$i") }.foreach { info =>
      post(SparkListenerTaskEnd(0, 0, null, Success, info, new ExecutorMetrics, null))
    }
    adjustRequestedExecutors(manager)
    assert(manager.executorMonitor.executorCount === 8)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 5)
    assert(removeExecutorDefaultProfile(manager, "1"))
    assert(removeExecutorsDefaultProfile(manager, Seq("2", "3"))=== Seq("2", "3"))
    onExecutorRemoved(manager, "1")
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")

    // numExecutorsTargetForDefaultProfileId is lower than minNumExecutors
    post(SparkListenerTaskEnd(0, 0, null, Success, createTaskInfo(4, 4, "4"),
      new ExecutorMetrics, null))
    assert(manager.executorMonitor.executorCount === 5)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) == 4)
    assert(!removeExecutorDefaultProfile(manager, "4")) // lower limit
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0) // upper limit
  }

  test ("interleaving add and remove") {
    // use ManualClock to disable ExecutorAllocationManager.schedule()
    // in order to avoid unexpected update of target executors
    val clock = new ManualClock()
    val manager = createManager(createConf(5, 12, 5), clock)
    post(SparkListenerStageSubmitted(createStageInfo(0, 1000)))

    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    // Add a few executors
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    onExecutorAddedDefaultProfile(manager, "1")
    onExecutorAddedDefaultProfile(manager, "2")
    onExecutorAddedDefaultProfile(manager, "3")
    onExecutorAddedDefaultProfile(manager, "4")
    onExecutorAddedDefaultProfile(manager, "5")
    onExecutorAddedDefaultProfile(manager, "6")
    onExecutorAddedDefaultProfile(manager, "7")
    onExecutorAddedDefaultProfile(manager, "8")
    assert(manager.executorMonitor.executorCount === 8)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 8)


    // Remove when numTargetExecutors is equal to the current number of executors
    assert(!removeExecutorDefaultProfile(manager, "1"))
    assert(removeExecutorsDefaultProfile(manager, Seq("2", "3")) !== Seq("2", "3"))

    // Remove until limit
    onExecutorAddedDefaultProfile(manager, "9")
    onExecutorAddedDefaultProfile(manager, "10")
    onExecutorAddedDefaultProfile(manager, "11")
    onExecutorAddedDefaultProfile(manager, "12")
    assert(manager.executorMonitor.executorCount === 12)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 8)

    assert(removeExecutorDefaultProfile(manager, "1"))
    assert(removeExecutorsDefaultProfile(manager, Seq("2", "3", "4")) === Seq("2", "3", "4"))
    assert(!removeExecutorDefaultProfile(manager, "5")) // lower limit reached
    assert(!removeExecutorDefaultProfile(manager, "6"))
    onExecutorRemoved(manager, "1")
    onExecutorRemoved(manager, "2")
    onExecutorRemoved(manager, "3")
    onExecutorRemoved(manager, "4")
    assert(manager.executorMonitor.executorCount === 8)

    // Add until limit
    assert(!removeExecutorDefaultProfile(manager, "7")) // still at lower limit
    assert((manager, Seq("8")) !== Seq("8"))
    onExecutorAddedDefaultProfile(manager, "13")
    onExecutorAddedDefaultProfile(manager, "14")
    onExecutorAddedDefaultProfile(manager, "15")
    onExecutorAddedDefaultProfile(manager, "16")
    assert(manager.executorMonitor.executorCount === 12)

    // Remove succeeds again, now that we are no longer at the lower limit
    assert(removeExecutorsDefaultProfile(manager, Seq("5", "6", "7")) === Seq("5", "6", "7"))
    assert(removeExecutorDefaultProfile(manager, "8"))
    assert(manager.executorMonitor.executorCount === 12)
    onExecutorRemoved(manager, "5")
    onExecutorRemoved(manager, "6")
    assert(manager.executorMonitor.executorCount === 10)
    assert(numExecutorsToAddForDefaultProfile(manager) === 4)
    onExecutorRemoved(manager, "9")
    onExecutorRemoved(manager, "10")
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 4) // at upper limit
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    onExecutorAddedDefaultProfile(manager, "17")
    onExecutorAddedDefaultProfile(manager, "18")
    assert(manager.executorMonitor.executorCount === 10)
    // still at upper limit
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 0)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    onExecutorAddedDefaultProfile(manager, "19")
    onExecutorAddedDefaultProfile(manager, "20")
    assert(manager.executorMonitor.executorCount === 12)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 12)
  }

  test("starting/canceling add timer") {
    val clock = new ManualClock(8888L)
    val manager = createManager(createConf(2, 10, 2), clock = clock)

    // Starting add timer is idempotent
    assert(addTime(manager) === NOT_SET)
    onSchedulerBacklogged(manager)
    val firstAddTime = addTime(manager)
    assert(firstAddTime === clock.nanoTime() + TimeUnit.SECONDS.toNanos(schedulerBacklogTimeout))
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
    assert(secondAddTime === clock.nanoTime() + TimeUnit.SECONDS.toNanos(schedulerBacklogTimeout))
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
    assert(numExecutorsTargetForDefaultProfileId(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(100L)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(1000L)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 0)
    assert(executorsPendingToRemove(manager).isEmpty)
    clock.advance(10000L)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 0)
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
    assert(numExecutorsTargetForDefaultProfileId(manager) === 0) // timer not exceeded yet
    clock.advance(schedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1) // first timer exceeded
    clock.advance(sustainedSchedulerBacklogTimeout * 1000 / 2)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1) // second timer not exceeded yet
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1 + 2) // second timer exceeded
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1 + 2 + 4) // third timer exceeded

    // Scheduler queue drained
    onSchedulerQueueEmpty(manager)
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 7) // timer is canceled
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 7)

    // Scheduler queue backlogged again
    onSchedulerBacklogged(manager)
    clock.advance(schedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 7 + 1) // timer restarted
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 7 + 1 + 2)
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 7 + 1 + 2 + 4)
    clock.advance(sustainedSchedulerBacklogTimeout * 1000)
    schedule(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 20) // limit reached
  }

  test("mock polling loop remove behavior") {
    val clock = new ManualClock(2020L)
    val manager = createManager(createConf(1, 20, 1), clock = clock)

    // Remove idle executors on timeout
    onExecutorAddedDefaultProfile(manager, "executor-1")
    onExecutorAddedDefaultProfile(manager, "executor-2")
    onExecutorAddedDefaultProfile(manager, "executor-3")
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
    onExecutorAddedDefaultProfile(manager, "executor-4")
    onExecutorAddedDefaultProfile(manager, "executor-5")
    onExecutorAddedDefaultProfile(manager, "executor-6")
    onExecutorAddedDefaultProfile(manager, "executor-7")
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

  test("mock polling loop remove with decommissioning") {
    val clock = new ManualClock(2020L)
    val manager = createManager(createConf(1, 20, 1, true), clock = clock)

    // Remove idle executors on timeout
    onExecutorAddedDefaultProfile(manager, "executor-1")
    onExecutorAddedDefaultProfile(manager, "executor-2")
    onExecutorAddedDefaultProfile(manager, "executor-3")
    assert(executorsDecommissioning(manager).isEmpty)
    assert(executorsPendingToRemove(manager).isEmpty)

    // idle threshold not reached yet
    clock.advance(executorIdleTimeout * 1000 / 2)
    schedule(manager)
    assert(manager.executorMonitor.timedOutExecutors().isEmpty)
    assert(executorsPendingToRemove(manager).isEmpty)
    assert(executorsDecommissioning(manager).isEmpty)

    // idle threshold exceeded
    clock.advance(executorIdleTimeout * 1000)
    assert(manager.executorMonitor.timedOutExecutors().size === 3)
    schedule(manager)
    assert(executorsPendingToRemove(manager).isEmpty) // limit reached (1 executor remaining)
    assert(executorsDecommissioning(manager).size === 2) // limit reached (1 executor remaining)

    // Mark a subset as busy - only idle executors should be removed
    onExecutorAddedDefaultProfile(manager, "executor-4")
    onExecutorAddedDefaultProfile(manager, "executor-5")
    onExecutorAddedDefaultProfile(manager, "executor-6")
    onExecutorAddedDefaultProfile(manager, "executor-7")
    assert(manager.executorMonitor.executorCount === 7)
    assert(executorsPendingToRemove(manager).isEmpty) // no pending to be removed
    assert(executorsDecommissioning(manager).size === 2) // 2 decommissioning
    onExecutorBusy(manager, "executor-4")
    onExecutorBusy(manager, "executor-5")
    onExecutorBusy(manager, "executor-6") // 3 busy and 2 idle (of the 5 active ones)

    // after scheduling, the previously timed out executor should be removed, since
    // there are new active ones.
    schedule(manager)
    assert(executorsDecommissioning(manager).size === 3)

    // advance the clock so that idle executors should time out and move to the pending list
    clock.advance(executorIdleTimeout * 1000)
    schedule(manager)
    assert(executorsPendingToRemove(manager).size === 0)
    assert(executorsDecommissioning(manager).size === 4)
    assert(!executorsDecommissioning(manager).contains("executor-4"))
    assert(!executorsDecommissioning(manager).contains("executor-5"))
    assert(!executorsDecommissioning(manager).contains("executor-6"))

    // Busy executors are now idle and should be removed
    onExecutorIdle(manager, "executor-4")
    onExecutorIdle(manager, "executor-5")
    onExecutorIdle(manager, "executor-6")
    schedule(manager)
    assert(executorsDecommissioning(manager).size === 4)
    clock.advance(executorIdleTimeout * 1000)
    schedule(manager)
    assert(executorsDecommissioning(manager).size === 6) // limit reached (1 executor remaining)
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

    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 4)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 8)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 15)
    (0 until 15).foreach { i =>
      onExecutorAddedDefaultProfile(manager, s"executor-$i")
    }
    assert(manager.executorMonitor.executorCount === 15)
    post(SparkListenerStageCompleted(stage1))

    adjustRequestedExecutors(manager)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 0)

    post(SparkListenerStageSubmitted(createStageInfo(1, 1000)))
    addExecutorsToTargetForDefaultProfile(manager, updatesNeeded)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 16)
  }

  test("avoid ramp down initial executors until first job is submitted") {
    val clock = new ManualClock(10000L)
    val manager = createManager(createConf(2, 5, 3), clock = clock)

    // Verify the initial number of executors
    assert(numExecutorsTargetForDefaultProfileId(manager) === 3)
    schedule(manager)
    // Verify whether the initial number of executors is kept with no pending tasks
    assert(numExecutorsTargetForDefaultProfileId(manager) === 3)

    post(SparkListenerStageSubmitted(createStageInfo(1, 2)))
    clock.advance(100L)

    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) === 2)
    schedule(manager)

    // Verify that current number of executors should be ramp down when first job is submitted
    assert(numExecutorsTargetForDefaultProfileId(manager) === 2)
  }

  test("avoid ramp down initial executors until idle executor is timeout") {
    val clock = new ManualClock(10000L)
    val manager = createManager(createConf(2, 5, 3), clock = clock)

    // Verify the initial number of executors
    assert(numExecutorsTargetForDefaultProfileId(manager) === 3)
    schedule(manager)
    // Verify the initial number of executors is kept when no pending tasks
    assert(numExecutorsTargetForDefaultProfileId(manager) === 3)
    (0 until 3).foreach { i =>
      onExecutorAddedDefaultProfile(manager, s"executor-$i")
    }

    clock.advance(executorIdleTimeout * 1000)

    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) === 0)
    schedule(manager)
    // Verify executor is timeout,numExecutorsTargetForDefaultProfileId is recalculated
    assert(numExecutorsTargetForDefaultProfileId(manager) === 2)
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

    assert(localityAwareTasksForDefaultProfile(manager) === 3)
    val hostToLocal = hostToLocalTaskCount(manager)
    assert(hostToLocalTaskCount(manager) ===
      Map("host1" -> 2, "host2" -> 3, "host3" -> 2, "host4" -> 2))

    val localityPreferences2 = Seq(
      Seq(TaskLocation("host2"), TaskLocation("host3"), TaskLocation("host5")),
      Seq(TaskLocation("host3"), TaskLocation("host4"), TaskLocation("host5")),
      Seq.empty
    )
    val stageInfo2 = createStageInfo(2, 3, localityPreferences2)
    post(SparkListenerStageSubmitted(stageInfo2))

    assert(localityAwareTasksForDefaultProfile(manager) === 5)
    assert(hostToLocalTaskCount(manager) ===
      Map("host1" -> 2, "host2" -> 4, "host3" -> 4, "host4" -> 3, "host5" -> 2))

    post(SparkListenerStageCompleted(stageInfo1))
    assert(localityAwareTasksForDefaultProfile(manager) === 2)
    assert(hostToLocalTaskCount(manager) ===
      Map("host2" -> 1, "host3" -> 2, "host4" -> 1, "host5" -> 2))
  }

  test("SPARK-8366: maxNumExecutorsNeededPerResourceProfile should properly handle failed tasks") {
    val manager = createManager(createConf())
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) === 0)

    post(SparkListenerStageSubmitted(createStageInfo(0, 1)))
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) === 1)

    val taskInfo = createTaskInfo(1, 1, "executor-1")
    post(SparkListenerTaskStart(0, 0, taskInfo))
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) === 1)

    // If the task is failed, we expect it to be resubmitted later.
    val taskEndReason = ExceptionFailure(null, null, null, null, None)
    post(SparkListenerTaskEnd(0, 0, null, taskEndReason, taskInfo, new ExecutorMetrics, null))
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) === 1)
  }

  test("reset the state of allocation manager") {
    val manager = createManager(createConf())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)

    val updatesNeeded =
      new mutable.HashMap[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates]

    // Allocation manager is reset when adding executor requests are sent without reporting back
    // executor added.
    post(SparkListenerStageSubmitted(createStageInfo(0, 10)))

    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 2)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 2)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 4)
    assert(addExecutorsToTargetForDefaultProfile(manager, updatesNeeded) === 1)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)

    manager.reset()
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(manager.executorMonitor.executorCount === 0)

    // Allocation manager is reset when executors are added.
    post(SparkListenerStageSubmitted(createStageInfo(0, 10)))

    addExecutorsToTargetForDefaultProfile(manager, updatesNeeded)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    addExecutorsToTargetForDefaultProfile(manager, updatesNeeded)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    addExecutorsToTargetForDefaultProfile(manager, updatesNeeded)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)

    onExecutorAddedDefaultProfile(manager, "first")
    onExecutorAddedDefaultProfile(manager, "second")
    onExecutorAddedDefaultProfile(manager, "third")
    onExecutorAddedDefaultProfile(manager, "fourth")
    onExecutorAddedDefaultProfile(manager, "fifth")
    assert(manager.executorMonitor.executorCount === 5)

    // Cluster manager lost will make all the live executors lost, so here simulate this behavior
    onExecutorRemoved(manager, "first")
    onExecutorRemoved(manager, "second")
    onExecutorRemoved(manager, "third")
    onExecutorRemoved(manager, "fourth")
    onExecutorRemoved(manager, "fifth")

    manager.reset()
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(manager.executorMonitor.executorCount === 0)

    // Allocation manager is reset when executors are pending to remove
    addExecutorsToTargetForDefaultProfile(manager, updatesNeeded)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    addExecutorsToTargetForDefaultProfile(manager, updatesNeeded)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    addExecutorsToTargetForDefaultProfile(manager, updatesNeeded)
    doUpdateRequest(manager, updatesNeeded.toMap, clock.getTimeMillis())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 5)

    onExecutorAddedDefaultProfile(manager, "first")
    onExecutorAddedDefaultProfile(manager, "second")
    onExecutorAddedDefaultProfile(manager, "third")
    onExecutorAddedDefaultProfile(manager, "fourth")
    onExecutorAddedDefaultProfile(manager, "fifth")
    onExecutorAddedDefaultProfile(manager, "sixth")
    onExecutorAddedDefaultProfile(manager, "seventh")
    onExecutorAddedDefaultProfile(manager, "eighth")
    assert(manager.executorMonitor.executorCount === 8)

    removeExecutorDefaultProfile(manager, "first")
    removeExecutorsDefaultProfile(manager, Seq("second", "third"))
    assert(executorsPendingToRemove(manager) === Set("first", "second", "third"))
    assert(manager.executorMonitor.executorCount === 8)


    // Cluster manager lost will make all the live executors lost, so here simulate this behavior
    onExecutorRemoved(manager, "first")
    onExecutorRemoved(manager, "second")
    onExecutorRemoved(manager, "third")
    onExecutorRemoved(manager, "fourth")
    onExecutorRemoved(manager, "fifth")

    manager.reset()

    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(numExecutorsToAddForDefaultProfile(manager) === 1)
    assert(executorsPendingToRemove(manager) === Set.empty)
    assert(manager.executorMonitor.executorCount === 0)
  }

  test("SPARK-23365 Don't update target num executors when killing idle executors") {
    val clock = new ManualClock()
    val manager = createManager(
      createConf(1, 2, 1),
      clock = clock)

    when(client.requestTotalExecutors(any(), any(), any())).thenReturn(true)
    // test setup -- job with 2 tasks, scale up to two executors
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    post(SparkListenerExecutorAdded(
      clock.getTimeMillis(), "executor-1", new ExecutorInfo("host1", 1, Map.empty, Map.empty)))
    post(SparkListenerStageSubmitted(createStageInfo(0, 2)))
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 2)
    val taskInfo0 = createTaskInfo(0, 0, "executor-1")
    post(SparkListenerTaskStart(0, 0, taskInfo0))
    post(SparkListenerExecutorAdded(
      clock.getTimeMillis(), "executor-2", new ExecutorInfo("host1", 1, Map.empty, Map.empty)))
    val taskInfo1 = createTaskInfo(1, 1, "executor-2")
    post(SparkListenerTaskStart(0, 0, taskInfo1))
    assert(numExecutorsTargetForDefaultProfileId(manager) === 2)

    // have one task finish -- we should adjust the target number of executors down
    // but we should *not* kill any executors yet
    post(SparkListenerTaskEnd(0, 0, null, Success, taskInfo0, new ExecutorMetrics, null))
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) === 1)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 2)
    clock.advance(1000)
    manager invokePrivate _updateAndSyncNumExecutorsTarget(clock.nanoTime())
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    assert(manager.executorMonitor.executorsPendingToRemove().isEmpty)

    // now we cross the idle timeout for executor-1, so we kill it.  the really important
    // thing here is that we do *not* ask the executor allocation client to adjust the target
    // number of executors down
    clock.advance(3000)
    schedule(manager)
    assert(maxNumExecutorsNeededPerResourceProfile(manager, defaultProfile) === 1)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
    // here's the important verify -- we did kill the executors, but did not adjust the target count
    assert(manager.executorMonitor.executorsPendingToRemove() === Set("executor-1"))
  }

  test("SPARK-26758 check executor target number after idle time out ") {
    val clock = new ManualClock(10000L)
    val manager = createManager(createConf(1, 5, 3), clock = clock)
    assert(numExecutorsTargetForDefaultProfileId(manager) === 3)
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
    assert(numExecutorsTargetForDefaultProfileId(manager) === 1)
  }

  private def createConf(
      minExecutors: Int = 1,
      maxExecutors: Int = 5,
      initialExecutors: Int = 1,
      decommissioningEnabled: Boolean = false): SparkConf = {
    val sparkConf = new SparkConf()
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
      // SPARK-22864/SPARK-32287: effectively disable the allocation schedule for the tests so that
      // we won't result in the race condition between thread "spark-dynamic-executor-allocation"
      // and thread "pool-1-thread-1-ScalaTest-running".
      .set(TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED, false)
      .set(DECOMMISSION_ENABLED, decommissioningEnabled)
    sparkConf
  }

  private def createManager(
      conf: SparkConf,
      clock: Clock = new SystemClock()): ExecutorAllocationManager = {
    ResourceProfile.reInitDefaultProfile(conf)

    rpManager = new ResourceProfileManager(conf, listenerBus)
    val manager = new ExecutorAllocationManager(client, listenerBus, conf, clock = clock,
      resourceProfileManager = rpManager, reliableShuffleStorage = false)
    managers += manager
    manager.start()
    manager
  }

  private val execInfo = new ExecutorInfo("host1", 1, Map.empty,
    Map.empty, Map.empty, DEFAULT_RESOURCE_PROFILE_ID)

  private def onExecutorAddedDefaultProfile(
      manager: ExecutorAllocationManager,
      id: String): Unit = {
    post(SparkListenerExecutorAdded(0L, id, execInfo))
  }

  private def onExecutorAdded(
      manager: ExecutorAllocationManager,
      id: String,
      rp: ResourceProfile): Unit = {
    val cores = rp.getExecutorCores.getOrElse(1)
    val execInfo = new ExecutorInfo("host1", cores, Map.empty, Map.empty, Map.empty, rp.id)
    post(SparkListenerExecutorAdded(0L, id, execInfo))
  }

  private def onExecutorRemoved(
      manager: ExecutorAllocationManager,
      id: String,
      reason: String = null): Unit = {
    post(SparkListenerExecutorRemoved(0L, id, reason))
  }

  private def onExecutorBusy(manager: ExecutorAllocationManager, id: String): Unit = {
    val info = new TaskInfo(1, 1, 1, 1, 0, id, "foo.example.com", TaskLocality.PROCESS_LOCAL, false)
    post(SparkListenerTaskStart(1, 1, info))
  }

  private def onExecutorIdle(manager: ExecutorAllocationManager, id: String): Unit = {
    val info = new TaskInfo(1, 1, 1, 1, 0, id, "foo.example.com", TaskLocality.PROCESS_LOCAL, false)
    info.markFinished(TaskState.FINISHED, 1)
    post(SparkListenerTaskEnd(1, 1, "foo", Success, info, new ExecutorMetrics, null))
  }

  private def removeExecutorDefaultProfile(
      manager: ExecutorAllocationManager,
      executorId: String): Boolean = {
    val executorsRemoved = removeExecutorsDefaultProfile(manager, Seq(executorId))
    executorsRemoved.nonEmpty && executorsRemoved(0) == executorId
  }

  private def removeExecutor(
      manager: ExecutorAllocationManager,
      executorId: String,
      rpId: Int): Boolean = {
    val executorsRemoved = removeExecutors(manager, Seq((executorId, rpId)))
    executorsRemoved.nonEmpty && executorsRemoved(0) == executorId
  }

  private def executorsPendingToRemove(manager: ExecutorAllocationManager): Set[String] = {
    manager.executorMonitor.executorsPendingToRemove()
  }

  private def executorsDecommissioning(manager: ExecutorAllocationManager): Set[String] = {
    manager.executorMonitor.executorsDecommissioning()
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
      attemptId: Int = 0,
      rp: ResourceProfile = defaultProfile
    ): StageInfo = {
    new StageInfo(stageId, attemptId, "name", numTasks, Seq.empty, Seq.empty, "no details",
      taskLocalityPreferences = taskLocalityPreferences, resourceProfileId = rp.id)
  }

  private def createTaskInfo(
      taskId: Int,
      taskIndex: Int,
      executorId: String,
      speculative: Boolean = false): TaskInfo = {
    new TaskInfo(taskId, taskIndex, 0, partitionId = taskIndex,
      0, executorId, "", TaskLocality.ANY, speculative)
  }

  /* ------------------------------------------------------- *
   | Helper methods for accessing private methods and fields |
   * ------------------------------------------------------- */

  private val _numExecutorsToAddPerResourceProfileId =
    PrivateMethod[mutable.HashMap[Int, Int]](
      Symbol("numExecutorsToAddPerResourceProfileId"))
  private val _numExecutorsTargetPerResourceProfileId =
    PrivateMethod[mutable.HashMap[Int, Int]](
      Symbol("numExecutorsTargetPerResourceProfileId"))
  private val _maxNumExecutorsNeededPerResourceProfile =
    PrivateMethod[Int](Symbol("maxNumExecutorsNeededPerResourceProfile"))
  private val _addTime = PrivateMethod[Long](Symbol("addTime"))
  private val _schedule = PrivateMethod[Unit](Symbol("schedule"))
  private val _doUpdateRequest = PrivateMethod[Unit](Symbol("doUpdateRequest"))
  private val _updateAndSyncNumExecutorsTarget =
    PrivateMethod[Int](Symbol("updateAndSyncNumExecutorsTarget"))
  private val _addExecutorsToTarget = PrivateMethod[Int](Symbol("addExecutorsToTarget"))
  private val _removeExecutors = PrivateMethod[Seq[String]](Symbol("removeExecutors"))
  private val _onSchedulerBacklogged = PrivateMethod[Unit](Symbol("onSchedulerBacklogged"))
  private val _onSchedulerQueueEmpty = PrivateMethod[Unit](Symbol("onSchedulerQueueEmpty"))
  private val _localityAwareTasksPerResourceProfileId =
    PrivateMethod[mutable.HashMap[Int, Int]](Symbol("numLocalityAwareTasksPerResourceProfileId"))
  private val _rpIdToHostToLocalTaskCount =
    PrivateMethod[Map[Int, Map[String, Int]]](Symbol("rpIdToHostToLocalTaskCount"))
  private val _onSpeculativeTaskSubmitted =
    PrivateMethod[Unit](Symbol("onSpeculativeTaskSubmitted"))
  private val _totalRunningTasksPerResourceProfile =
    PrivateMethod[Int](Symbol("totalRunningTasksPerResourceProfile"))

  private val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(new SparkConf)

  private def numExecutorsToAddForDefaultProfile(manager: ExecutorAllocationManager): Int = {
    numExecutorsToAdd(manager, defaultProfile)
  }

  private def numExecutorsToAdd(
      manager: ExecutorAllocationManager,
      rp: ResourceProfile): Int = {
    val nmap = manager invokePrivate _numExecutorsToAddPerResourceProfileId()
    nmap(rp.id)
  }

  private def updateAndSyncNumExecutorsTarget(
      manager: ExecutorAllocationManager,
      now: Long): Unit = {
    manager invokePrivate _updateAndSyncNumExecutorsTarget(now)
  }

  private def numExecutorsTargetForDefaultProfileId(manager: ExecutorAllocationManager): Int = {
    numExecutorsTarget(manager, defaultProfile.id)
  }

  private def numExecutorsTarget(
      manager: ExecutorAllocationManager,
      rpId: Int): Int = {
    val numMap = manager invokePrivate _numExecutorsTargetPerResourceProfileId()
    numMap(rpId)
  }

  private def addExecutorsToTargetForDefaultProfile(
      manager: ExecutorAllocationManager,
      updatesNeeded: mutable.HashMap[ResourceProfile,
        ExecutorAllocationManager.TargetNumUpdates]
  ): Int = {
    addExecutorsToTarget(manager, updatesNeeded, defaultProfile)
  }

  private def addExecutorsToTarget(
      manager: ExecutorAllocationManager,
      updatesNeeded: mutable.HashMap[ResourceProfile,
        ExecutorAllocationManager.TargetNumUpdates],
      rp: ResourceProfile
  ): Int = {
    val maxNumExecutorsNeeded =
      manager invokePrivate _maxNumExecutorsNeededPerResourceProfile(rp.id)
    manager invokePrivate
      _addExecutorsToTarget(maxNumExecutorsNeeded, rp.id, updatesNeeded)
  }

  private def addTime(manager: ExecutorAllocationManager): Long = {
    manager invokePrivate _addTime()
  }

  private def doUpdateRequest(
      manager: ExecutorAllocationManager,
      updates: Map[ResourceProfile, ExecutorAllocationManager.TargetNumUpdates],
      now: Long): Unit = {
    manager invokePrivate _doUpdateRequest(updates, now)
  }

  private def schedule(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _schedule()
  }

  private def maxNumExecutorsNeededPerResourceProfile(
      manager: ExecutorAllocationManager,
      rp: ResourceProfile): Int = {
    manager invokePrivate _maxNumExecutorsNeededPerResourceProfile(rp.id)
  }

  private def adjustRequestedExecutors(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _updateAndSyncNumExecutorsTarget(0L)
  }

  private def removeExecutorsDefaultProfile(
      manager: ExecutorAllocationManager,
      ids: Seq[String]): Seq[String] = {
    val idsAndProfileIds = ids.map((_, defaultProfile.id))
    manager invokePrivate _removeExecutors(idsAndProfileIds)
  }

  private def removeExecutors(
      manager: ExecutorAllocationManager,
      ids: Seq[(String, Int)]): Seq[String] = {
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

  private def localityAwareTasksForDefaultProfile(manager: ExecutorAllocationManager): Int = {
    val localMap = manager invokePrivate _localityAwareTasksPerResourceProfileId()
    localMap(defaultProfile.id)
  }

  private def totalRunningTasksPerResourceProfile(manager: ExecutorAllocationManager): Int = {
    manager invokePrivate _totalRunningTasksPerResourceProfile(defaultProfile.id)
  }

  private def hostToLocalTaskCount(
      manager: ExecutorAllocationManager): Map[String, Int] = {
    val rpIdToHostLocal = manager invokePrivate _rpIdToHostToLocalTaskCount()
    rpIdToHostLocal(defaultProfile.id)
  }

  private def getResourceProfileIdOfExecutor(manager: ExecutorAllocationManager): Int = {
    defaultProfile.id
  }
}
