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

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.util.ManualClock

class BlacklistTrackerSuite extends SparkFunSuite with BeforeAndAfterEach with MockitoSugar
    with LocalSparkContext {

  private val clock = new ManualClock(0)

  private var blacklistTracker: BlacklistTracker = _

  override def afterEach(): Unit = {
    if (blacklistTracker != null) {
      blacklistTracker = null
    }
    super.afterEach()
  }

  val allOptions = (('A' to 'Z').map("host" + _) ++ (1 to 100).map{_.toString}).toSet

  /**
   * Its easier to write our tests as if we could directly look at the sets of nodes & executors in
   * the blacklist.  However the api doesn't expose a set (for thread-safety), so this is a simple
   * way to test something similar, since we know the universe of values that might appear in these
   * sets.
   */
  def assertEquivalentToSet(f: String => Boolean, expected: Set[String]): Unit = {
    allOptions.foreach { opt =>
      val actual = f(opt)
      val exp = expected.contains(opt)
      assert(actual === exp, raw"""for string "$opt" """)
    }
  }

  def mockTaskSchedWithConf(conf: SparkConf): TaskSchedulerImpl = {
    sc = new SparkContext(conf)
    val scheduler = mock[TaskSchedulerImpl]
    when(scheduler.sc).thenReturn(sc)
    when(scheduler.mapOutputTracker).thenReturn(SparkEnv.get.mapOutputTracker)
    scheduler
  }

  test("Blacklisting individual tasks") {
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.BLACKLIST_ENABLED.key, "true")
    val scheduler = mockTaskSchedWithConf(conf)
    // Task 1 failed on executor 1
    blacklistTracker = new BlacklistTracker(conf, clock)
    val taskSet = FakeTask.createTaskSet(10)
    val tsm = new TaskSetManager(scheduler, Some(blacklistTracker), taskSet, 4, clock)
    tsm.updateBlacklistForFailedTask("hostA", "1", 0)
    for {
      executor <- (1 to 4).map(_.toString)
      index <- 0 until 10
    } {
      val exp = (executor == "1"  && index == 0)
      assert(tsm.isExecutorBlacklistedForTask(executor, index) === exp)
    }
    assert(blacklistTracker.nodeBlacklist() === Set())
    assertEquivalentToSet(blacklistTracker.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(tsm.isNodeBlacklistedForTaskSet, Set())
    assertEquivalentToSet(tsm.isExecutorBlacklistedForTaskSet, Set())

    // Task 1 & 2 failed on both executor 1 & 2, so we blacklist all executors on that host,
    // for all tasks for the stage.  Note the api expects multiple checks for each type of
    // blacklist -- this actually fits naturally with its use in the scheduler
    tsm.updateBlacklistForFailedTask("hostA", "1", 1)
    tsm.updateBlacklistForFailedTask("hostA", "2", 0)
    tsm.updateBlacklistForFailedTask("hostA", "2", 1)
    // we don't explicitly return the executors in hostA here, but that is OK
    for {
      executor <- (1 to 4).map(_.toString)
      index <- 0 until 10
    } {
      withClue(s"exec = $executor; index = $index") {
        val badExec = (executor == "1" || executor == "2")
        val badPart = (index == 0 || index == 1)
        val taskExp = (badExec && badPart)
        assert(
          tsm.isExecutorBlacklistedForTask(executor, index) === taskExp)
        val executorExp = badExec
        assert(tsm.isExecutorBlacklistedForTaskSet(executor) === executorExp)
      }
    }
    assertEquivalentToSet(tsm.isNodeBlacklistedForTaskSet, Set("hostA"))
    // we dont' blacklist the nodes or executors till the stages complete
    assert(blacklistTracker.nodeBlacklist() === Set())
    assertEquivalentToSet(blacklistTracker.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(blacklistTracker.isExecutorBlacklisted(_), Set())

    // when the stage completes successfully, now there is sufficient evidence we've got
    // bad executors and node
    blacklistTracker.updateBlacklistForSuccessfulTaskSet(tsm.execToFailures)
    assert(blacklistTracker.nodeBlacklist() === Set("hostA"))
    assertEquivalentToSet(blacklistTracker.isNodeBlacklisted(_), Set("hostA"))
    assertEquivalentToSet(blacklistTracker.isExecutorBlacklisted(_), Set("1", "2"))

    clock.advance(blacklistTracker.BLACKLIST_TIMEOUT_MILLIS + 1)
    blacklistTracker.expireExecutorsInBlacklist()
    assert(blacklistTracker.nodeBlacklist() === Set())
    assertEquivalentToSet(blacklistTracker.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(blacklistTracker.isExecutorBlacklisted(_), Set())
  }

  def trackerFixture: (BlacklistTracker, TaskSchedulerImpl) = {
    trackerFixture()
  }

  def trackerFixture(confs: (String, String)*): (BlacklistTracker, TaskSchedulerImpl) = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.BLACKLIST_ENABLED.key, "true")
    confs.foreach { case (k, v) => conf.set(k, v) }
    val scheduler = mockTaskSchedWithConf(conf)

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
      val taskSet = FakeTask.createTaskSet(1)
      val tsm = new TaskSetManager(scheduler, Some(tracker), taskSet, 4, clock)
      tsm.updateBlacklistForFailedTask("hostA", "1", 0)
      tracker.updateBlacklistForSuccessfulTaskSet(tsm.execToFailures)
    }
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set("1"))
  }

  // if an executor has many task failures, but the task set ends up failing, don't count it
  // against the executor
  test("executors aren't blacklisted if task sets fail") {
    val (tracker, scheduler) = trackerFixture
    // for 4 different stages, executor 1 fails a task, and then the taskSet fails.
    (0 until 4).foreach { stage =>
      val taskSet = FakeTask.createTaskSet(1)
      val tsm = new TaskSetManager(scheduler, Some(tracker), taskSet, 4, clock)
      tsm.updateBlacklistForFailedTask("hostA", "1", 0)
    }
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set())
  }

  Seq(true, false).foreach { succeedTaskSet =>
    test(s"stage blacklist updates correctly on stage completion ($succeedTaskSet)") {
      // within one taskset, an executor fails a few times, so its blacklisted for the taskset.
      // but if the taskset fails, we don't blacklist the executor after the stage.
      val (tracker, scheduler) = trackerFixture
      val stageId = 1 + (if (succeedTaskSet) 1 else 0)
      val taskSet = FakeTask.createTaskSet(4, stageId, 0)
      val tsm = new TaskSetManager(scheduler, Some(tracker), taskSet, 4, clock)
      (0 until 4).foreach { partition =>
        tsm.updateBlacklistForFailedTask("hostA", "1", partition)
      }
      assert(tsm.isExecutorBlacklistedForTaskSet("1"))
      assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set())
      if (succeedTaskSet) {
        // the task set succeeded elsewhere, so we count those failures against our executor,
        // and blacklist it across stages
        tracker.updateBlacklistForSuccessfulTaskSet(tsm.execToFailures)
        assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set("1"))
      } else {
        // the task set failed, so we don't count these failures against the executor for other
        // stages
        assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set())
      }
    }
  }

  test("blacklisted executors and nodes get recovered with time") {
    val (tracker, scheduler) = trackerFixture
    val taskSet0 = FakeTask.createTaskSet(4)
    val tsm0 = new TaskSetManager(scheduler, Some(tracker), taskSet0, 4, clock)
    (0 until 4).foreach { partition =>
      tsm0.updateBlacklistForFailedTask("hostA", "1", partition)
    }
    tracker.updateBlacklistForSuccessfulTaskSet(tsm0.execToFailures)
    assert(tracker.nodeBlacklist() === Set())
    assertEquivalentToSet(tracker.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set("1"))

    val taskSet1 = FakeTask.createTaskSet(4, 1, 0)
    val tsm1 = new TaskSetManager(scheduler, Some(tracker), taskSet1, 4, clock)
    (0 until 4).foreach { partition =>
      tsm1.updateBlacklistForFailedTask("hostA", "2", partition)
    }
    tracker.updateBlacklistForSuccessfulTaskSet(tsm1.execToFailures)
    assert(tracker.nodeBlacklist() === Set("hostA"))
    assertEquivalentToSet(tracker.isNodeBlacklisted(_), Set("hostA"))
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set("1", "2"))

    clock.advance(tracker.BLACKLIST_TIMEOUT_MILLIS + 1)
    tracker.expireExecutorsInBlacklist()
    assert(tracker.nodeBlacklist() === Set())
    assertEquivalentToSet(tracker.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set())

    // fail one more task, but executor isn't put back into blacklist since count reset to 0
    val taskSet2 = FakeTask.createTaskSet(4, 2, 0)
    val tsm2 = new TaskSetManager(scheduler, Some(tracker), taskSet2, 4, clock)
    tsm2.updateBlacklistForFailedTask("hostA", "1", 0)
    tracker.updateBlacklistForSuccessfulTaskSet(tsm2.execToFailures)
    assert(tracker.nodeBlacklist() === Set())
    assertEquivalentToSet(tracker.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set())
  }

  test("blacklist can handle lost executors") {
    // the blacklist should still work if an executor is killed completely.  We should still
    // be able to blacklist the entire node.
    val (tracker, scheduler) = trackerFixture
    val taskSet0 = FakeTask.createTaskSet(4)
    val tsm0 = new TaskSetManager(scheduler, Some(tracker), taskSet0, 4, clock)
    (0 until 3).foreach { partition =>
      tsm0.updateBlacklistForFailedTask("hostA", "1", partition)
    }
    // now lets say that executor 1 dies completely
    // we get a task failure for the last task
    tsm0.updateBlacklistForFailedTask("hostA", "1", 3)
    tracker.updateBlacklistForSuccessfulTaskSet(tsm0.execToFailures)
    assert(tracker.isExecutorBlacklisted("1"))
    clock.advance(tracker.BLACKLIST_TIMEOUT_MILLIS / 2)

    // say another executor gets spun up on that host
    val taskSet1 = FakeTask.createTaskSet(4)
    val tsm1 = new TaskSetManager(scheduler, Some(tracker), taskSet1, 4, clock)
    (0 until 4).foreach { partition =>
      tsm1.updateBlacklistForFailedTask("hostA", "2", partition)
    }
    tracker.updateBlacklistForSuccessfulTaskSet(tsm1.execToFailures)
    // we've now had two bad executors on the hostA, so we should blacklist the entire node
    assert(tracker.isExecutorBlacklisted("1"))
    assert(tracker.isExecutorBlacklisted("2"))
    assert(tracker.isNodeBlacklisted("hostA"))

    clock.advance(tracker.BLACKLIST_TIMEOUT_MILLIS / 2 + 1)
    tracker.expireExecutorsInBlacklist()
    // executor 1 is no longer explicitly blacklisted, since we've gone past its recovery time,
    // but everything else is still blacklisted.
    assert(!tracker.isExecutorBlacklisted("1"))
    assert(tracker.isExecutorBlacklisted("2"))
    assert(tracker.isNodeBlacklisted("hostA"))
    // make sure we don't leak memory
    assert(!tracker.nodeToFailedExecs("hostA").contains("1"))
  }

  test("task failures expire with time") {
    val (tracker, scheduler) = trackerFixture
    def failOneTaskInTaskSet(): Unit = {
      val taskSet = FakeTask.createTaskSet(1)
      val tsm = new TaskSetManager(scheduler, Some(tracker), taskSet, 1, clock)
      tsm.updateBlacklistForFailedTask("hostA", "1", 0)
      tracker.updateBlacklistForSuccessfulTaskSet(tsm.execToFailures)
    }
    failOneTaskInTaskSet()

    // now we advance the clock past the expiry time
    clock.advance(tracker.BLACKLIST_TIMEOUT_MILLIS + 1)
    tracker.expireExecutorsInBlacklist()
    failOneTaskInTaskSet()

    // because we went past the expiry time, nothing should have been blacklisted
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set())

    // now we add one more failure, within the timeout, and it should be counted
    clock.advance(tracker.BLACKLIST_TIMEOUT_MILLIS - 1)
    failOneTaskInTaskSet()
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set("1"))

  }

  test("multiple attempts for the same task count once") {
    // make sure that for blacklisting tasks, the node counts task attempts, not executors.  But for
    // stage-level blacklisting, we count unique tasks.  The reason for this difference is, with
    // task-attempt blacklisting, we want to make it easy to configure so that you ensure a node
    // is blacklisted before the taskset is completely aborted b/c of spark.task.maxFailures.
    // But with stage-blacklisting, we want to make sure we're not just counting one bad task
    // that has failed many times.

    val (tracker, scheduler) = trackerFixture(
      config.MAX_TASK_ATTEMPTS_PER_EXECUTOR.key -> "2",
      config.MAX_TASK_ATTEMPTS_PER_NODE.key -> "3",
      config.MAX_FAILURES_PER_EXEC_STAGE.key -> "2",
      config.MAX_FAILED_EXEC_PER_NODE_STAGE.key -> "3"
    )
    val taskSet0 = FakeTask.createTaskSet(5)
    val tsm0 = new TaskSetManager(scheduler, Some(tracker), taskSet0, 4, clock)
    // fail a task twice on hostA, exec:1
    tsm0.updateBlacklistForFailedTask("hostA", "1", 0)
    tsm0.updateBlacklistForFailedTask("hostA", "1", 0)
    assert(tsm0.isExecutorBlacklistedForTask("1", 0))
    assert(!tsm0.isNodeBlacklistedForTask("hostA", 0))
    assert(!tsm0.isExecutorBlacklistedForTaskSet("1"))
    assert(!tsm0.isNodeBlacklistedForTaskSet("hostA"))

    // fail the same task once more on hostA, exec:2
    tsm0.updateBlacklistForFailedTask("hostA", "2", 0)
    assert(tsm0.isNodeBlacklistedForTask("hostA", 0))
    assert(!tsm0.isExecutorBlacklistedForTaskSet("2"))
    assert(!tsm0.isNodeBlacklistedForTaskSet("hostA"))

    // fail another task on hostA, exec:1.  Now that executor has failures on two different tasks,
    // so its blacklisted
    tsm0.updateBlacklistForFailedTask("hostA", "1", 1)
    tracker.updateBlacklistForSuccessfulTaskSet(tsm0.execToFailures)
    assert(tsm0.isExecutorBlacklistedForTaskSet("1"))
    assert(!tsm0.isNodeBlacklistedForTaskSet("hostA"))

    // fail a third task on hostA, exec:2, so that exec is blacklisted for the whole task set
    tsm0.updateBlacklistForFailedTask("hostA", "2", 2)
    tracker.updateBlacklistForSuccessfulTaskSet(tsm0.execToFailures)
    assert(tsm0.isExecutorBlacklistedForTaskSet("2"))
    assert(!tsm0.isNodeBlacklistedForTaskSet("hostA"))

    // fail a fourth & fifth task on hostA, exec:3.  Now we've got three executors that are
    // blacklisted for the taskset, so blacklist the whole node.
    tsm0.updateBlacklistForFailedTask("hostA", "3", 3)
    tsm0.updateBlacklistForFailedTask("hostA", "3", 4)
    tracker.updateBlacklistForSuccessfulTaskSet(tsm0.execToFailures)
    assert(tsm0.isExecutorBlacklistedForTaskSet("3"))
    assert(tsm0.isNodeBlacklistedForTaskSet("hostA"))
  }

  test("only blacklist nodes when all the blacklisted executors are all on same host (app level)") {
    // we blacklist executors on two different hosts -- make sure that doesn't lead to any
    // node blacklisting
    val (tracker, scheduler) = trackerFixture
    val taskSet0 = FakeTask.createTaskSet(4)
    val tsm0 = new TaskSetManager(scheduler, Some(tracker), taskSet0, 1, clock)
    tsm0.updateBlacklistForFailedTask("hostA", "1", 0)
    tsm0.updateBlacklistForFailedTask("hostA", "1", 1)
    tracker.updateBlacklistForSuccessfulTaskSet(tsm0.execToFailures)
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set("1"))
    assertEquivalentToSet(tracker.isNodeBlacklisted(_), Set())

    val taskSet1 = FakeTask.createTaskSet(4)
    val tsm1 = new TaskSetManager(scheduler, Some(tracker), taskSet1, 1, clock)
    tsm1.updateBlacklistForFailedTask("hostB", "2", 0)
    tsm1.updateBlacklistForFailedTask("hostB", "2", 1)
    tracker.updateBlacklistForSuccessfulTaskSet(tsm1.execToFailures)
    assertEquivalentToSet(tracker.isExecutorBlacklisted(_), Set("1", "2"))
    assertEquivalentToSet(tracker.isNodeBlacklisted(_), Set())
  }

  test("only blacklist nodes when all the blacklisted executors are all on same host (tsm level)") {
    // we blacklist executors on two different hosts within one taskSet -- make sure that doesn't
    // lead to any node blacklisting
    val (tracker, scheduler) = trackerFixture
    val taskSet = FakeTask.createTaskSet(4)
    val tsm = new TaskSetManager(scheduler, Some(tracker), taskSet, 1, clock)
    tsm.updateBlacklistForFailedTask("hostA", "1", 0)
    tsm.updateBlacklistForFailedTask("hostA", "1", 1)
    assertEquivalentToSet(tsm.isExecutorBlacklistedForTaskSet(_), Set("1"))
    assertEquivalentToSet(tsm.isNodeBlacklistedForTaskSet(_), Set())

    tsm.updateBlacklistForFailedTask("hostB", "2", 0)
    tsm.updateBlacklistForFailedTask("hostB", "2", 1)
    assertEquivalentToSet(tsm.isExecutorBlacklistedForTaskSet(_), Set("1", "2"))
    assertEquivalentToSet(tsm.isNodeBlacklistedForTaskSet(_), Set())
  }


  test("blacklist still respects legacy configs") {
    val legacyKey = config.BLACKLIST_LEGACY_TIMEOUT_CONF.key

    {
      val localConf = new SparkConf().setMaster("local")
      assert(!BlacklistTracker.isBlacklistEnabled(localConf))
      localConf.set(legacyKey, "5000")
      assert(BlacklistTracker.isBlacklistEnabled(localConf))
      assert(5000 === BlacklistTracker.getBlacklistTimeout(localConf))

      localConf.set(legacyKey, "0")
      assert(!BlacklistTracker.isBlacklistEnabled(localConf))
    }

    {
      val distConf = new SparkConf().setMaster("yarn-cluster")
      assert(BlacklistTracker.isBlacklistEnabled(distConf))
      assert(60 * 60 * 1000L === BlacklistTracker.getBlacklistTimeout(distConf))
      distConf.set(legacyKey, "5000")
      assert(5000 === BlacklistTracker.getBlacklistTimeout(distConf))
      distConf.set(config.BLACKLIST_TIMEOUT_CONF.key, "10h")
      assert(10 * 60 * 60 * 1000L == BlacklistTracker.getBlacklistTimeout(distConf))
    }
  }
}
