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

import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.mock.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.util.ManualClock

class BlacklistTrackerSuite extends SparkFunSuite with BeforeAndAfterEach with MockitoSugar {

  val stage1 = 1
  val stage2 = 2

  val partition1 = 1
  val partition2 = 2
  val partition3 = 3

  // Variable name can indicate basic information of taskInfo
  // hostA: executor 1, 2, 4
  // hostB: executor 3
  // The format is "taskInfo_executorId_hostName"
  val taskInfo_1_hostA = new TaskInfo(1L, 1, 1, 0L, "1", "hostA", TaskLocality.ANY, false)
  val taskInfo_2_hostA = new TaskInfo(2L, 1, 1, 0L, "2", "hostA", TaskLocality.ANY, false)
  val taskInfo_3_hostB = new TaskInfo(3L, 3, 1, 0L, "3", "hostB", TaskLocality.ANY, false)

  val clock = new ManualClock(0)

  var blacklistTracker: BlacklistTracker = _

  override def afterEach(): Unit = {
    if (blacklistTracker != null) {
      blacklistTracker.stop()
      blacklistTracker = null
    }
    super.afterEach()
  }

  test("Blacklisting individual tasks") {
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set("spark.ui.enabled", "false")
      .set("spark.scheduler.blacklist.advancedStrategy", "true")
      .set("spark.scheduler.executorTaskBlacklistTime", "1000")
    val scheduler = mock[TaskSchedulerImpl]
    when(scheduler.getExecutorsAliveOnHost("hostA")).thenReturn(Some(Set("1", "2", "4")))
    Set("1", "2", "4").foreach { execId =>
      when(scheduler.getHostForExecutor(execId)).thenReturn("hostA")
    }

    // Task 1 failed on executor 1
    blacklistTracker = new BlacklistTracker(conf, clock)
    blacklistTracker.taskFailed(stage1, partition1, taskInfo_1_hostA)
    for {
      executor <- (1 to 4).map(_.toString)
      partition <- 0 until 10
      stage <- (1 to 2)
    } {
      val exp = (executor == "1" && stage == stage1 && partition == 1)
      assert(blacklistTracker.isExecutorBlacklisted(executor, stage, partition) === exp)
    }
    assert(blacklistTracker.nodeBlacklist() === Set())
    assert(blacklistTracker.nodeBlacklistForStage(stage1) === Set())
    assert(blacklistTracker.nodeBlacklistForStage(stage2) === Set())

    // Task 1 & 2 failed on both executor 1 & 2, so we blacklist all executors on that host,
    // for all tasks for the stage.  Note the api expects multiple checks for each type of
    // blacklist -- this actually fits naturally with its use in the scheduler
    blacklistTracker.taskFailed(stage1, partition1,
      new TaskInfo(2L, 1, 1, 0L, "2", "hostA", TaskLocality.ANY, false))
    blacklistTracker.taskFailed(stage1, partition2,
      new TaskInfo(3L, 2, 1, 0L, "2", "hostA", TaskLocality.ANY, false))
    blacklistTracker.taskFailed(stage1, partition2,
      new TaskInfo(4L, 2, 1, 0L, "1", "hostA", TaskLocality.ANY, false))
    // we don't explicitly return the executors in hostA here, but that is OK
    for {
      executor <- (1 to 4).map(_.toString)
      stage <- (1 to 2)
      partition <- 0 until 10
    } {
      withClue(s"exec = $executor; stage = $stage; part = $partition") {
        val badExec = (executor == "1" || executor == "2")
        val badPart = (partition == 1 || partition == 2)
        val taskExp = (badExec && stage == stage1 && badPart)
        assert(blacklistTracker.isExecutorBlacklisted(executor, stage, partition) === taskExp)
        val executorExp = badExec && stage == stage1
        assert(blacklistTracker.isExecutorBlacklistedForStage(stage, executor) === executorExp)
      }
    }
    assert(blacklistTracker.nodeBlacklistForStage(stage1) === Set("hostA"))
    assert(blacklistTracker.nodeBlacklistForStage(stage2) === Set())
    // we dont' blacklist the nodes or executors till the stages complete
    assert(blacklistTracker.nodeBlacklist() === Set())
    assert(blacklistTracker.executorBlacklist() === Set())

    // when the stage completes successfully, now there is sufficient evidence we've got
    // bad executors and node
    blacklistTracker.taskSetSucceeded(stage1, scheduler)
    assert(blacklistTracker.nodeBlacklist() === Set("hostA"))
    assert(blacklistTracker.executorBlacklist() === Set("1", "2"))

    clock.advance(blacklistTracker.EXECUTOR_RECOVERY_MILLIS + 1)
    blacklistTracker.expireExecutorsInBlackList()
    assert(blacklistTracker.nodeBlacklist() === Set())
    assert(blacklistTracker.executorBlacklist() === Set())
  }

  def trackerFixture: (BlacklistTracker, TaskSchedulerImpl) = {
     val conf = new SparkConf().setAppName("test").setMaster("local")
      .set("spark.ui.enabled", "false")
      .set("spark.scheduler.blacklist.advancedStrategy", "true")
      .set("spark.scheduler.executorTaskBlacklistTime", "1000")
    val scheduler = mock[TaskSchedulerImpl]
    when(scheduler.getExecutorsAliveOnHost("hostA")).thenReturn(Some(Set("1", "2", "4")))
    Set("1", "2", "4").foreach { execId =>
      when(scheduler.getHostForExecutor(execId)).thenReturn("hostA")
    }

    clock.setTime(0)
    blacklistTracker = new BlacklistTracker(conf, clock)
    (blacklistTracker, scheduler)
  }

  test("executors can be blacklisted with only a few failures per stage") {
    val (tracker, scheduler) = trackerFixture
    // for 4 different stages, executor 1 fails a task, then executor 2 succeeds the task,
    // and then the task set is done.  Not enough failures to blacklist the executor *within*
    // any particular taskset, but we still blacklist the executor overall eventually
    (0 until 4).foreach { stage =>
      tracker.taskFailed(stage, 0,
        new TaskInfo(stage, 0, 0, 0, "1", "hostA", TaskLocality.ANY, false))
      tracker.taskSucceeded(stage, 0,
        new TaskInfo(stage, 0, 1, 0, "2", "hostA", TaskLocality.ANY, false))
      tracker.taskSetSucceeded(stage, scheduler)
    }
    assert(tracker.executorBlacklist() === Set("1"))
  }

  // if an executor has many task failures, but the task set ends up failing, don't count it
  // against the executor
  test("executors aren't blacklisted if task sets fail") {
    val (tracker, scheduler) = trackerFixture
    // for 4 different stages, executor 1 fails a task, and then the taskSet fails.
    (0 until 4).foreach { stage =>
      tracker.taskFailed(stage, 0,
        new TaskInfo(stage, 0, 0, 0, "1", "hostA", TaskLocality.ANY, false))
      tracker.taskSetFailed(stage)
    }
    assert(tracker.executorBlacklist() === Set())
  }

  Seq(true, false).foreach { succeedTaskSet =>
    test(s"stage blacklist updates correctly on stage completion ($succeedTaskSet)") {
      // within one taskset, an executor fails a few times, so its blacklisted for the taskset.
      // but if the taskset fails, we don't blacklist the executor after the stage.
      val (tracker, scheduler) = trackerFixture
      val stageId = 1 + (if (succeedTaskSet) 1 else 0)
      (0 until 4).foreach { partition =>
        tracker.taskFailed(stageId, partition, new TaskInfo(stageId * 4 + partition, partition, 0,
          clock.getTimeMillis(), "1", "hostA", TaskLocality.ANY, false))
      }
      assert(tracker.isExecutorBlacklistedForStage(stageId, "1"))
      assert(tracker.executorBlacklist() === Set())
      if (succeedTaskSet) {
        // the task set succeeded elsewhere, so we count those failures against our executor,
        // and blacklist it across stages
        tracker.taskSetSucceeded(stageId, scheduler)
        assert(tracker.executorBlacklist() === Set("1"))
      } else {
        // the task set failed, so we don't count these failures against the executor for other
        // stages
        tracker.taskSetFailed(stageId)
        assert(tracker.executorBlacklist() === Set())
      }
    }
  }

  test("blacklisted executors and nodes get recovered with time") {
    val (tracker, scheduler) = trackerFixture
    (0 until 4).foreach { partition =>
      tracker.taskFailed(0, partition, new TaskInfo(partition, partition, 0, clock.getTimeMillis(),
        "1", "hostA", TaskLocality.ANY, false))
    }
    tracker.taskSetSucceeded(0, scheduler)
    assert(tracker.executorBlacklist() === Set("1"))

    clock.advance(tracker.EXECUTOR_RECOVERY_MILLIS + 1)
    // TODO might want to change this to avoid the need for expiry thread, if that eliminates the
    // need for the locks.  In which case, expiry would happen automatically.
    tracker.expireExecutorsInBlackList()
    assert(tracker.executorBlacklist() === Set())
    // TODO after recovery, count is reset to 0
    // fail one more task, but executor isn't put back into blacklist since count reset to 0
    tracker.taskFailed(1, 0, new TaskInfo(5, 0, 0, clock.getTimeMillis(),
      "1", "hostA", TaskLocality.ANY, false))
    tracker.taskSetSucceeded(1, scheduler)
    assert(tracker.executorBlacklist() === Set())
  }

  test("memory cleaned up as tasksets complete") {
    // We want to make sure that memory used by the blacklist tracker is not O(nTotalTaskSetsRun),
    // that would be really bad for long-lived applications.  This test just requires some knowledge
    // of the internals on what to check (without the overhead of trying to trigger an OOM or
    // something).
    val (tracker, scheduler) = trackerFixture
    // fail a couple of tasks in two stages
    for {
      stage <- 0 until 2
      partition <- 0 until 4
    } {
      val tid = stage * 4 + partition
      // we want to fail on multiple executors, to trigger node blacklist
      val exec = (partition % 2).toString
      tracker.taskFailed(stage, partition, new TaskInfo(tid, partition, 0, clock.getTimeMillis(),
        exec, "hostA", TaskLocality.ANY, false))
    }

    when(scheduler.getHostForExecutor("0")).thenReturn("hostA")
    when(scheduler.getHostForExecutor("1")).thenReturn("hostA")
    when(scheduler.getExecutorsAliveOnHost("hostA")).thenReturn(Some(Set("0", "1")))

    // just make sure our test is even checking something useful -- we expect these data structures
    // to grow for running task sets with failed tasks
    assert(tracker.stageIdToExecToFailures.nonEmpty)
    assert(tracker.stageIdToBlacklistedNodes.nonEmpty)

    // now say stage 0 fails, and stage 1 completes
    tracker.taskSetFailed(0)
    tracker.taskSetSucceeded(1, scheduler)

    // datastructures should be empty again
    assert(tracker.stageIdToExecToFailures.isEmpty)
    assert(tracker.stageIdToBlacklistedNodes.isEmpty)
  }
}
