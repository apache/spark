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

import org.mockito.ArgumentMatchers.isA
import org.mockito.Mockito.{never, verify}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.util.ManualClock

class TaskSetBlacklistSuite extends SparkFunSuite with BeforeAndAfterEach with MockitoSugar {

  private var listenerBusMock: LiveListenerBus = _

  override def beforeEach(): Unit = {
    listenerBusMock = mock[LiveListenerBus]
    super.beforeEach()
  }

  test("Blacklisting tasks, executors, and nodes") {
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.BLACKLIST_ENABLED.key, "true")
    val clock = new ManualClock
    val attemptId = 0
    val taskSetBlacklist = new TaskSetBlacklist(
      listenerBusMock, conf, stageId = 0, stageAttemptId = attemptId, clock = clock)

    clock.setTime(0)
    // We will mark task 0 & 1 failed on both executor 1 & 2.
    // We should blacklist all executors on that host, for all tasks for the stage.  Note the API
    // will return false for isExecutorBacklistedForTaskSet even when the node is blacklisted, so
    // the executor is implicitly blacklisted (this makes sense with how the scheduler uses the
    // blacklist)

    // First, mark task 0 as failed on exec1.
    // task 0 should be blacklisted on exec1, and nowhere else
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "exec1", index = 0, failureReason = "testing")
    for {
      executor <- (1 to 4).map(_.toString)
      index <- 0 until 10
    } {
      val shouldBeBlacklisted = (executor == "exec1" && index == 0)
      assert(taskSetBlacklist.isExecutorBlacklistedForTask(executor, index) === shouldBeBlacklisted)
    }

    assert(!taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec1"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerExecutorBlacklistedForStage]))

    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeBlacklistedForStage]))

    // Mark task 1 failed on exec1 -- this pushes the executor into the blacklist
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "exec1", index = 1, failureReason = "testing")

    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec1"))
    verify(listenerBusMock).post(
      SparkListenerExecutorBlacklistedForStage(0, "exec1", 2, 0, attemptId))

    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeBlacklistedForStage]))

    // Mark one task as failed on exec2 -- not enough for any further blacklisting yet.
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "exec2", index = 0, failureReason = "testing")
    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec1"))

    assert(!taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec2"))

    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeBlacklistedForStage]))

    // Mark another task as failed on exec2 -- now we blacklist exec2, which also leads to
    // blacklisting the entire node.
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "exec2", index = 1, failureReason = "testing")

    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec1"))

    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("exec2"))
    verify(listenerBusMock).post(
      SparkListenerExecutorBlacklistedForStage(0, "exec2", 2, 0, attemptId))

    assert(taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock).post(
      SparkListenerNodeBlacklistedForStage(0, "hostA", 2, 0, attemptId))

    // Make sure the blacklist has the correct per-task && per-executor responses, over a wider
    // range of inputs.
    for {
      executor <- (1 to 4).map(e => s"exec$e")
      index <- 0 until 10
    } {
      withClue(s"exec = $executor; index = $index") {
        val badExec = (executor == "exec1" || executor == "exec2")
        val badIndex = (index == 0 || index == 1)
        assert(
          // this ignores whether the executor is blacklisted entirely for the taskset -- that is
          // intentional, it keeps it fast and is sufficient for usage in the scheduler.
          taskSetBlacklist.isExecutorBlacklistedForTask(executor, index) === (badExec && badIndex))
        assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet(executor) === badExec)
        if (badExec) {
          verify(listenerBusMock).post(
            SparkListenerExecutorBlacklistedForStage(0, executor, 2, 0, attemptId))
        }
      }
    }
    assert(taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    val execToFailures = taskSetBlacklist.execToFailures
    assert(execToFailures.keySet === Set("exec1", "exec2"))

    Seq("exec1", "exec2").foreach { exec =>
      assert(
        execToFailures(exec).taskToFailureCountAndFailureTime === Map(
          0 -> ((1, 0)),
          1 -> ((1, 0))
        )
      )
    }
  }

  test("multiple attempts for the same task count once") {
    // Make sure that for blacklisting tasks, the node counts task attempts, not executors.  But for
    // stage-level blacklisting, we count unique tasks.  The reason for this difference is, with
    // task-attempt blacklisting, we want to make it easy to configure so that you ensure a node
    // is blacklisted before the taskset is completely aborted because of spark.task.maxFailures.
    // But with stage-blacklisting, we want to make sure we're not just counting one bad task
    // that has failed many times.

    val conf = new SparkConf().setMaster("local").setAppName("test")
      .set(config.MAX_TASK_ATTEMPTS_PER_EXECUTOR, 2)
      .set(config.MAX_TASK_ATTEMPTS_PER_NODE, 3)
      .set(config.MAX_FAILURES_PER_EXEC_STAGE, 2)
      .set(config.MAX_FAILED_EXEC_PER_NODE_STAGE, 3)
    val clock = new ManualClock

    val attemptId = 0
    val taskSetBlacklist = new TaskSetBlacklist(
      listenerBusMock, conf, stageId = 0, stageAttemptId = attemptId, clock = clock)

    var time = 0
    clock.setTime(time)
    // Fail a task twice on hostA, exec:1
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    assert(taskSetBlacklist.isExecutorBlacklistedForTask("1", 0))
    assert(!taskSetBlacklist.isNodeBlacklistedForTask("hostA", 0))

    assert(!taskSetBlacklist.isExecutorBlacklistedForTaskSet("1"))
    verify(listenerBusMock, never()).post(
      SparkListenerExecutorBlacklistedForStage(time, "1", 2, 0, attemptId))

    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock, never()).post(
      SparkListenerNodeBlacklistedForStage(time, "hostA", 2, 0, attemptId))

    // Fail the same task once more on hostA, exec:2
    time += 1
    clock.setTime(time)
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "2", index = 0, failureReason = "testing")
    assert(taskSetBlacklist.isNodeBlacklistedForTask("hostA", 0))

    assert(!taskSetBlacklist.isExecutorBlacklistedForTaskSet("2"))
    verify(listenerBusMock, never()).post(
      SparkListenerExecutorBlacklistedForStage(time, "2", 2, 0, attemptId))

    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock, never()).post(
      SparkListenerNodeBlacklistedForStage(time, "hostA", 2, 0, attemptId))

    // Fail another task on hostA, exec:1.  Now that executor has failures on two different tasks,
    // so its blacklisted
    time += 1
    clock.setTime(time)
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "1", index = 1, failureReason = "testing")

    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("1"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorBlacklistedForStage(time, "1", 2, 0, attemptId))

    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeBlacklistedForStage]))

    // Fail a third task on hostA, exec:2, so that exec is blacklisted for the whole task set
    time += 1
    clock.setTime(time)
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "2", index = 2, failureReason = "testing")

    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("2"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorBlacklistedForStage(time, "2", 2, 0, attemptId))

    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeBlacklistedForStage]))

    // Fail a fourth & fifth task on hostA, exec:3.  Now we've got three executors that are
    // blacklisted for the taskset, so blacklist the whole node.
    time += 1
    clock.setTime(time)
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "3", index = 3, failureReason = "testing")
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "3", index = 4, failureReason = "testing")

    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("3"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorBlacklistedForStage(time, "3", 2, 0, attemptId))

    assert(taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock).post(
      SparkListenerNodeBlacklistedForStage(time, "hostA", 3, 0, attemptId))
  }

  test("only blacklist nodes for the task set when all the blacklisted executors are all on " +
    "same host") {
    // we blacklist executors on two different hosts within one taskSet -- make sure that doesn't
    // lead to any node blacklisting
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.BLACKLIST_ENABLED.key, "true")
    val clock = new ManualClock

    val attemptId = 0
    val taskSetBlacklist = new TaskSetBlacklist(
      listenerBusMock, conf, stageId = 0, stageAttemptId = attemptId, clock = clock)
    var time = 0
    clock.setTime(time)
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostA", exec = "1", index = 1, failureReason = "testing")

    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("1"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorBlacklistedForStage(time, "1", 2, 0, attemptId))

    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    verify(listenerBusMock, never()).post(
      SparkListenerNodeBlacklistedForStage(time, "hostA", 2, 0, attemptId))

    time += 1
    clock.setTime(time)
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostB", exec = "2", index = 0, failureReason = "testing")
    taskSetBlacklist.updateBlacklistForFailedTask(
      "hostB", exec = "2", index = 1, failureReason = "testing")
    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("1"))

    assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("2"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorBlacklistedForStage(time, "2", 2, 0, attemptId))

    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostA"))
    assert(!taskSetBlacklist.isNodeBlacklistedForTaskSet("hostB"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeBlacklistedForStage]))
  }

}
