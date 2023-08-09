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
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.util.ManualClock

class TaskSetExcludelistSuite extends SparkFunSuite with MockitoSugar {

  private var listenerBusMock: LiveListenerBus = _

  override def beforeEach(): Unit = {
    listenerBusMock = mock[LiveListenerBus]
    super.beforeEach()
  }

  test("Excluding tasks, executors, and nodes") {
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.EXCLUDE_ON_FAILURE_ENABLED.key, "true")
    val clock = new ManualClock
    val attemptId = 0
    val taskSetExcludelist = new TaskSetExcludelist(
      listenerBusMock, conf, stageId = 0, stageAttemptId = attemptId, clock = clock)

    clock.setTime(0)
    // We will mark task 0 & 1 failed on both executor 1 & 2.
    // We should exclude all executors on that host, for all tasks for the stage.  Note the API
    // will return false for isExecutorBacklistedForTaskSet even when the node is excluded, so
    // the executor is implicitly excluded (this makes sense with how the scheduler uses the
    // exclude)

    // First, mark task 0 as failed on exec1.
    // task 0 should be excluded on exec1, and nowhere else
    taskSetExcludelist.updateExcludedForFailedTask(
      "hostA", exec = "exec1", index = 0, failureReason = "testing")
    for {
      executor <- (1 to 4).map(_.toString)
      index <- 0 until 10
    } {
      val shouldBeExcluded = (executor == "exec1" && index == 0)
      assert(taskSetExcludelist.isExecutorExcludedForTask(executor, index) === shouldBeExcluded)
    }

    assert(!taskSetExcludelist.isExecutorExcludedForTaskSet("exec1"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerExecutorExcludedForStage]))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerExecutorBlacklistedForStage]))

    assert(!taskSetExcludelist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeExcludedForStage]))

    // Mark task 1 failed on exec1 -- this pushes the executor into the exclude
    taskSetExcludelist.updateExcludedForFailedTask(
      "hostA", exec = "exec1", index = 1, failureReason = "testing")

    assert(taskSetExcludelist.isExecutorExcludedForTaskSet("exec1"))
    verify(listenerBusMock).post(
      SparkListenerExecutorExcludedForStage(0, "exec1", 2, 0, attemptId))
    verify(listenerBusMock).post(
      SparkListenerExecutorBlacklistedForStage(0, "exec1", 2, 0, attemptId))


    assert(!taskSetExcludelist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeExcludedForStage]))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeBlacklistedForStage]))

    // Mark one task as failed on exec2 -- not enough for any further excluding yet.
    taskSetExcludelist.updateExcludedForFailedTask(
      "hostA", exec = "exec2", index = 0, failureReason = "testing")
    assert(taskSetExcludelist.isExecutorExcludedForTaskSet("exec1"))

    assert(!taskSetExcludelist.isExecutorExcludedForTaskSet("exec2"))

    assert(!taskSetExcludelist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeExcludedForStage]))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeBlacklistedForStage]))

    // Mark another task as failed on exec2 -- now we exclude exec2, which also leads to
    // excluding the entire node.
    taskSetExcludelist.updateExcludedForFailedTask(
      "hostA", exec = "exec2", index = 1, failureReason = "testing")

    assert(taskSetExcludelist.isExecutorExcludedForTaskSet("exec1"))

    assert(taskSetExcludelist.isExecutorExcludedForTaskSet("exec2"))
    verify(listenerBusMock).post(
      SparkListenerExecutorExcludedForStage(0, "exec2", 2, 0, attemptId))
    verify(listenerBusMock).post(
      SparkListenerExecutorBlacklistedForStage(0, "exec2", 2, 0, attemptId))

    assert(taskSetExcludelist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock).post(
      SparkListenerNodeExcludedForStage(0, "hostA", 2, 0, attemptId))
    verify(listenerBusMock).post(
      SparkListenerNodeBlacklistedForStage(0, "hostA", 2, 0, attemptId))

    // Make sure the exclude has the correct per-task && per-executor responses, over a wider
    // range of inputs.
    for {
      executor <- (1 to 4).map(e => s"exec$e")
      index <- 0 until 10
    } {
      withClue(s"exec = $executor; index = $index") {
        val badExec = (executor == "exec1" || executor == "exec2")
        val badIndex = (index == 0 || index == 1)
        assert(
          // this ignores whether the executor is excluded entirely for the taskset -- that is
          // intentional, it keeps it fast and is sufficient for usage in the scheduler.
          taskSetExcludelist.isExecutorExcludedForTask(executor, index) === (badExec && badIndex))
        assert(taskSetExcludelist.isExecutorExcludedForTaskSet(executor) === badExec)
        if (badExec) {
          verify(listenerBusMock).post(
            SparkListenerExecutorExcludedForStage(0, executor, 2, 0, attemptId))
          verify(listenerBusMock).post(
            SparkListenerExecutorBlacklistedForStage(0, executor, 2, 0, attemptId))
        }
      }
    }
    assert(taskSetExcludelist.isNodeExcludedForTaskSet("hostA"))
    val execToFailures = taskSetExcludelist.execToFailures
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
    // Make sure that for excluding tasks, the node counts task attempts, not executors.  But for
    // stage-level excluding, we count unique tasks.  The reason for this difference is, with
    // task-attempt excluding, we want to make it easy to configure so that you ensure a node
    // is excluded before the taskset is completely aborted because of spark.task.maxFailures.
    // But with stage-excluding, we want to make sure we're not just counting one bad task
    // that has failed many times.

    val conf = new SparkConf().setMaster("local").setAppName("test")
      .set(config.MAX_TASK_ATTEMPTS_PER_EXECUTOR, 2)
      .set(config.MAX_TASK_ATTEMPTS_PER_NODE, 3)
      .set(config.MAX_FAILURES_PER_EXEC_STAGE, 2)
      .set(config.MAX_FAILED_EXEC_PER_NODE_STAGE, 3)
    val clock = new ManualClock

    val attemptId = 0
    val taskSetExcludlist = new TaskSetExcludelist(
      listenerBusMock, conf, stageId = 0, stageAttemptId = attemptId, clock = clock)

    var time = 0
    clock.setTime(time)
    // Fail a task twice on hostA, exec:1
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    assert(taskSetExcludlist.isExecutorExcludedForTask("1", 0))
    assert(!taskSetExcludlist.isNodeExcludedForTask("hostA", 0))

    assert(!taskSetExcludlist.isExecutorExcludedForTaskSet("1"))
    verify(listenerBusMock, never()).post(
      SparkListenerExecutorExcludedForStage(time, "1", 2, 0, attemptId))

    assert(!taskSetExcludlist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock, never()).post(
      SparkListenerNodeExcludedForStage(time, "hostA", 2, 0, attemptId))

    // Fail the same task once more on hostA, exec:2
    time += 1
    clock.setTime(time)
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostA", exec = "2", index = 0, failureReason = "testing")
    assert(taskSetExcludlist.isNodeExcludedForTask("hostA", 0))

    assert(!taskSetExcludlist.isExecutorExcludedForTaskSet("2"))
    verify(listenerBusMock, never()).post(
      SparkListenerExecutorExcludedForStage(time, "2", 2, 0, attemptId))

    assert(!taskSetExcludlist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock, never()).post(
      SparkListenerNodeExcludedForStage(time, "hostA", 2, 0, attemptId))

    // Fail another task on hostA, exec:1.  Now that executor has failures on two different tasks,
    // so its excluded
    time += 1
    clock.setTime(time)
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostA", exec = "1", index = 1, failureReason = "testing")

    assert(taskSetExcludlist.isExecutorExcludedForTaskSet("1"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorExcludedForStage(time, "1", 2, 0, attemptId))

    assert(!taskSetExcludlist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeExcludedForStage]))

    // Fail a third task on hostA, exec:2, so that exec is excluded for the whole task set
    time += 1
    clock.setTime(time)
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostA", exec = "2", index = 2, failureReason = "testing")

    assert(taskSetExcludlist.isExecutorExcludedForTaskSet("2"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorExcludedForStage(time, "2", 2, 0, attemptId))

    assert(!taskSetExcludlist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeExcludedForStage]))

    // Fail a fourth & fifth task on hostA, exec:3.  Now we've got three executors that are
    // excluded for the taskset, so exclude the whole node.
    time += 1
    clock.setTime(time)
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostA", exec = "3", index = 3, failureReason = "testing")
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostA", exec = "3", index = 4, failureReason = "testing")

    assert(taskSetExcludlist.isExecutorExcludedForTaskSet("3"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorExcludedForStage(time, "3", 2, 0, attemptId))

    assert(taskSetExcludlist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock).post(
      SparkListenerNodeExcludedForStage(time, "hostA", 3, 0, attemptId))
  }

  test("only exclude nodes for the task set when all the excluded executors are all on " +
    "same host") {
    // we exclude executors on two different hosts within one taskSet -- make sure that doesn't
    // lead to any node excluding
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.EXCLUDE_ON_FAILURE_ENABLED.key, "true")
    val clock = new ManualClock

    val attemptId = 0
    val taskSetExcludlist = new TaskSetExcludelist(
      listenerBusMock, conf, stageId = 0, stageAttemptId = attemptId, clock = clock)
    var time = 0
    clock.setTime(time)
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostA", exec = "1", index = 1, failureReason = "testing")

    assert(taskSetExcludlist.isExecutorExcludedForTaskSet("1"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorExcludedForStage(time, "1", 2, 0, attemptId))
    verify(listenerBusMock)
      .post(SparkListenerExecutorBlacklistedForStage(time, "1", 2, 0, attemptId))

    assert(!taskSetExcludlist.isNodeExcludedForTaskSet("hostA"))
    verify(listenerBusMock, never()).post(
      SparkListenerNodeExcludedForStage(time, "hostA", 2, 0, attemptId))
    verify(listenerBusMock, never()).post(
      SparkListenerNodeBlacklistedForStage(time, "hostA", 2, 0, attemptId))

    time += 1
    clock.setTime(time)
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostB", exec = "2", index = 0, failureReason = "testing")
    taskSetExcludlist.updateExcludedForFailedTask(
      "hostB", exec = "2", index = 1, failureReason = "testing")
    assert(taskSetExcludlist.isExecutorExcludedForTaskSet("1"))

    assert(taskSetExcludlist.isExecutorExcludedForTaskSet("2"))
    verify(listenerBusMock)
      .post(SparkListenerExecutorExcludedForStage(time, "2", 2, 0, attemptId))
    verify(listenerBusMock)
      .post(SparkListenerExecutorBlacklistedForStage(time, "2", 2, 0, attemptId))

    assert(!taskSetExcludlist.isNodeExcludedForTaskSet("hostA"))
    assert(!taskSetExcludlist.isNodeExcludedForTaskSet("hostB"))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeExcludedForStage]))
    verify(listenerBusMock, never())
      .post(isA(classOf[SparkListenerNodeBlacklistedForStage]))
  }

}
