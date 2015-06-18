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

package org.apache.spark.scheduler

import scala.collection.mutable

import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

import org.apache.spark._
import org.apache.spark.scheduler.ExecutorBlacklistTracker.ExecutorFailureStatus
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.util.ManualClock

class ExecutorBlacklistTrackerSuite
  extends SparkFunSuite
  with LocalSparkContext
  with BeforeAndAfter {
  import ExecutorBlacklistTrackerSuite._

  before {
    if (sc == null) {
      sc = createSparkContext
    }
  }

  after {
    if (sc !=  null) {
      sc.stop()
      sc = null
    }
  }

  test("add executor to blacklist") {
    // Add 5 executors
    addExecutors(5)
    val tracker = sc.executorBlacklistTracker.get
    assert(numExecutorsRegistered(tracker) === 5)

    // Post 5 TaskEnd event to executor-1 to add executor-1 into blacklist
    (0 until 5).foreach(_ => postTaskEndEvent(TaskResultLost, "executor-1"))

    assert(tracker.getExecutorBlacklist === Set("executor-1"))
    assert(executorIdToTaskFailures(tracker)("executor-1").numFailures === 5)
    assert(executorIdToTaskFailures(tracker)("executor-1").isBlackListed === true)

    // Post 10 TaskEnd event to executor-2 to add executor-2 into blacklist
    (0 until 10).foreach(_ => postTaskEndEvent(TaskResultLost, "executor-2"))
    assert(tracker.getExecutorBlacklist === Set("executor-1", "executor-2"))
    assert(executorIdToTaskFailures(tracker)("executor-2").numFailures === 10)
    assert(executorIdToTaskFailures(tracker)("executor-2").isBlackListed === true)

    // Post 5 TaskEnd event to executor-3 to verify whether executor-3 is blacklisted
    (0 until 5).foreach(_ => postTaskEndEvent(TaskResultLost, "executor-3"))
    // Since the failure number of executor-3 is less than the average blacklist threshold,
    // though exceed the fault threshold, still should not be added into blacklist
    assert(tracker.getExecutorBlacklist === Set("executor-1", "executor-2"))
    assert(executorIdToTaskFailures(tracker)("executor-3").numFailures === 5)
    assert(executorIdToTaskFailures(tracker)("executor-3").isBlackListed === false)

    // Keep post TaskEnd event to executor-3 to add executor-3 into blacklist
    (0 until 2).foreach(_ => postTaskEndEvent(TaskResultLost, "executor-3"))
    assert(tracker.getExecutorBlacklist === Set("executor-1", "executor-2", "executor-3"))
    assert(executorIdToTaskFailures(tracker)("executor-3").numFailures === 7)
    assert(executorIdToTaskFailures(tracker)("executor-3").isBlackListed === true)

    // Post TaskEnd event to executor-4 to verify whether executor-4 could be added into blacklist
    (0 until 10).foreach(_ => postTaskEndEvent(TaskResultLost, "executor-4"))
    // Event executor-4's failure task number is above than blacklist threshold,
    // but the blacklisted executor number is reaching to maximum fraction,
    // so executor-4 still cannot be added into blacklist.
    assert(tracker.getExecutorBlacklist === Set("executor-1", "executor-2", "executor-3"))
    assert(executorIdToTaskFailures(tracker)("executor-4").numFailures === 10)
    assert(executorIdToTaskFailures(tracker)("executor-4").isBlackListed === false)
  }

  test("remove executor from blacklist") {
        // Add 5 executors
    addExecutors(5)
    val tracker = sc.executorBlacklistTracker.get
    val clock = new ManualClock(10000L)
    tracker.setClock(clock)
    assert(numExecutorsRegistered(tracker) === 5)

    // Post 5 TaskEnd event to executor-1 to add executor-1 into blacklist
    (0 until 5).foreach(_ => postTaskEndEvent(TaskResultLost, "executor-1"))

    assert(tracker.getExecutorBlacklist === Set("executor-1"))
    assert(executorIdToTaskFailures(tracker)("executor-1").numFailures === 5)
    assert(executorIdToTaskFailures(tracker)("executor-1").isBlackListed === true)
    assert(executorIdToTaskFailures(tracker)("executor-1").updatedTime === 10000L)

    // Advance the timer
    clock.advance(70 * 1000)
    expireTimeoutExecutorBlacklist(tracker)
    assert(tracker.getExecutorBlacklist === Set.empty)
    assert(executorIdToTaskFailures(tracker)("executor-1").numFailures === 5)
    assert(executorIdToTaskFailures(tracker)("executor-1").isBlackListed === false)
  }

  private def createSparkContext: SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-executor-blacklist-tracker")
      .set("spark.scheduler.blacklist.enabled", "true")
      .set("spark.scheduler.blacklist.executorFaultTimeoutWindowInMinutes", "1")
    val sc = new SparkContext(conf)
    sc
  }

  private def addExecutors(numExecutor: Int): Unit = {
    for (i <- 1 to numExecutor) {
      sc.listenerBus.postToAll(SparkListenerExecutorAdded(
      0L, s"executor-$i", new ExecutorInfo(s"host$i", 1, Map.empty)))
    }
  }

  private def postTaskEndEvent(taskEndReason: TaskEndReason, executorId: String): Unit = {
    val taskInfo = new TaskInfo(0L, 0, 0, 0L, executorId, null, null, false)
    val taskEnd = SparkListenerTaskEnd(0, 0, "", taskEndReason, taskInfo, null)
    sc.listenerBus.postToAll(taskEnd)
  }
}

private object ExecutorBlacklistTrackerSuite extends PrivateMethodTester {
  private val _numExecutorsRegistered = PrivateMethod[Int]('numExecutorsRegistered)
  private val _executorIdToTaskFailures =
    PrivateMethod[mutable.HashMap[String, ExecutorFailureStatus]]('executorIdToTaskFailures)
  private val _expireTimeoutExecutorBlacklist =
    PrivateMethod[Unit]('expireTimeoutExecutorBlacklist)

  private def numExecutorsRegistered(tracker: ExecutorBlacklistTracker): Int = {
    tracker invokePrivate _numExecutorsRegistered()
  }

  private def executorIdToTaskFailures(tracker: ExecutorBlacklistTracker
      ): mutable.HashMap[String, ExecutorFailureStatus] = {
    tracker invokePrivate _executorIdToTaskFailures()
  }

  private def expireTimeoutExecutorBlacklist(tracker: ExecutorBlacklistTracker): Unit = {
    tracker invokePrivate _expireTimeoutExecutorBlacklist()
  }
}
