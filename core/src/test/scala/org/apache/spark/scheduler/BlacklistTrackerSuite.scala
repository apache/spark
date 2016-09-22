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

  private var blacklist: BlacklistTracker = _
  private var scheduler: TaskSchedulerImpl = _
  private var conf: SparkConf = _

  override def afterEach(): Unit = {
    if (blacklist != null) {
      blacklist = null
    }
    if (scheduler != null) {
      scheduler.stop()
      scheduler = null
    }
    super.afterEach()
  }

  val allExecutorAndHostIds = (('A' to 'Z').map("host" + _) ++ (1 to 100).map{_.toString}).toSet

  /**
   * Its easier to write our tests as if we could directly look at the sets of nodes & executors in
   * the blacklist.  However the api doesn't expose a set (for thread-safety), so this is a simple
   * way to test something similar, since we know the universe of values that might appear in these
   * sets.
   */
  def assertEquivalentToSet(f: String => Boolean, expected: Set[String]): Unit = {
    allExecutorAndHostIds.foreach { opt =>
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

  def createTaskSetManager(numTasks: Int, stageId: Int = 0): TaskSetManager = {
    val taskSet = FakeTask.createTaskSet(numTasks, stageId = stageId, stageAttemptId = 0)
    new TaskSetManager(scheduler, taskSet, 4, Some(blacklist), clock)
  }

  def configureBlacklistAndScheduler(confs: (String, String)*): Unit = {
    conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.BLACKLIST_ENABLED.key, "true")
    confs.foreach { case (k, v) => conf.set(k, v) }
    scheduler = mockTaskSchedWithConf(conf)

    clock.setTime(0)
    blacklist = new BlacklistTracker(conf, clock)
  }

  test("executors can be blacklisted with only a few failures per stage") {
    configureBlacklistAndScheduler()
    // for 4 different stages, executor 1 fails a task, then executor 2 succeeds the task,
    // and then the task set is done.  Not enough failures to blacklist the executor *within*
    // any particular taskset, but we still blacklist the executor overall eventually.
    // Also, we intentionally have a mix of task successes and failures -- there are even some
    // successes after the executor is blacklisted.  The idea here is those tasks get scheduled
    // before the executor is blacklisted.  We might get successes after blacklisting (because the
    // executor might be flaky but not totally broken).  But successes do not unblacklist the
    // executor.
    val failuresTillBlacklisted = conf.get(config.MAX_FAILURES_PER_EXEC)
    var failuresSoFar = 0
    (0 until failuresTillBlacklisted * 10).foreach { stage =>
      val tsm = createTaskSetManager(numTasks = 1, stageId = stage)
      if (stage % 2 == 0) {
        // fail every other task
        tsm.updateBlacklistForFailedTask("hostA", exec = "1", index = 0)
        failuresSoFar += 1
      }
      blacklist.updateBlacklistForSuccessfulTaskSet(stage, 0, tsm.execToFailures)
      assert(failuresSoFar == stage / 2 + 1)
      if (failuresSoFar < failuresTillBlacklisted) {
        assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
      } else {
        assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1"))
      }
    }
  }

  // if an executor has many task failures, but the task set ends up failing, don't count it
  // against the executor
  test("executors aren't blacklisted if task sets fail") {
    configureBlacklistAndScheduler()
    // for 4 different stages, executor 1 fails a task, and then the taskSet fails.
    (0 until 4).foreach { stage =>
      val tsm = createTaskSetManager(numTasks = 1)
      tsm.updateBlacklistForFailedTask("hostA", exec = "1", index = 0)
    }
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
  }

  Seq(true, false).foreach { succeedTaskSet =>
    test(s"stage blacklist updates correctly on stage completion ($succeedTaskSet)") {
      // within one taskset, an executor fails a few times, so its blacklisted for the taskset.
      // but if the taskset fails, we don't blacklist the executor after the stage.
      configureBlacklistAndScheduler()
      val stageId = 1 + (if (succeedTaskSet) 1 else 0)
      val tsm = createTaskSetManager(numTasks = 4, stageId = stageId)
      (0 until 4).foreach { index =>
        tsm.updateBlacklistForFailedTask("hostA", exec = "1", index = index)
      }
      assert(tsm.isExecutorBlacklistedForTaskSet("1"))
      assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
      if (succeedTaskSet) {
        // the task set succeeded elsewhere, so we count those failures against our executor,
        // and blacklist it across stages
        blacklist.updateBlacklistForSuccessfulTaskSet(stageId, 0, tsm.execToFailures)
        assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1"))
      } else {
        // the task set failed, so we don't count these failures against the executor for other
        // stages
        assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
      }
    }
  }

  test("blacklisted executors and nodes get recovered with time") {
    configureBlacklistAndScheduler()
    val tsm0 = createTaskSetManager(numTasks = 4, stageId = 0)
    (0 until 4).foreach { partition =>
      tsm0.updateBlacklistForFailedTask("hostA", exec = "1", index = partition)
    }
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, tsm0.execToFailures)
    assert(blacklist.nodeBlacklist() === Set())
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1"))

    val tsm1 = createTaskSetManager(numTasks = 4, stageId = 1)
    (0 until 4).foreach { partition =>
      tsm1.updateBlacklistForFailedTask("hostA", exec = "2", index = partition)
    }
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, tsm1.execToFailures)
    assert(blacklist.nodeBlacklist() === Set("hostA"))
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set("hostA"))
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1", "2"))

    clock.advance(blacklist.BLACKLIST_TIMEOUT_MILLIS + 1)
    blacklist.applyBlacklistTimeout()
    assert(blacklist.nodeBlacklist() === Set())
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())

    // fail one more task, but executor isn't put back into blacklist since count reset to 0
    val tsm2 = createTaskSetManager(numTasks = 4, stageId = 2)
    tsm2.updateBlacklistForFailedTask("hostA", exec = "1", index = 0)
    blacklist.updateBlacklistForSuccessfulTaskSet(2, 0, tsm2.execToFailures)
    assert(blacklist.nodeBlacklist() === Set())
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
  }

  test("blacklist can handle lost executors") {
    // The blacklist should still work if an executor is killed completely.  We should still
    // be able to blacklist the entire node.
    configureBlacklistAndScheduler()
    val tsm0 = createTaskSetManager(numTasks = 4, stageId = 0)
    // Lets say that executor 1 dies completely.  We get a task failure for the last task, but
    // the taskset then finishes successfully (elsewhere).
    (0 until 4).foreach { partition =>
      tsm0.updateBlacklistForFailedTask("hostA", exec = "1", index = partition)
    }
    blacklist.handleRemovedExecutor("1")
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, tsm0.execToFailures)
    assert(blacklist.isExecutorBlacklisted("1"))
    clock.advance(blacklist.BLACKLIST_TIMEOUT_MILLIS / 2)

    // Now another executor gets spun up on that host, but it also dies.
    val tsm1 = createTaskSetManager(numTasks = 4, stageId = 1)
    (0 until 4).foreach { partition =>
      tsm1.updateBlacklistForFailedTask("hostA", exec = "2", index = partition)
    }
    blacklist.handleRemovedExecutor("2")
    blacklist.updateBlacklistForSuccessfulTaskSet(1, 0, tsm1.execToFailures)
    // We've now had two bad executors on the hostA, so we should blacklist the entire node.
    assert(blacklist.isExecutorBlacklisted("1"))
    assert(blacklist.isExecutorBlacklisted("2"))
    assert(blacklist.isNodeBlacklisted("hostA"))

    clock.advance(blacklist.BLACKLIST_TIMEOUT_MILLIS / 2 + 1)
    blacklist.applyBlacklistTimeout()
    // executor 1 is no longer explicitly blacklisted, since we've gone past its recovery time,
    // but everything else is still blacklisted.
    assert(!blacklist.isExecutorBlacklisted("1"))
    assert(blacklist.isExecutorBlacklisted("2"))
    assert(blacklist.isNodeBlacklisted("hostA"))
    // make sure we don't leak memory
    assert(!blacklist.executorIdToBlacklistStatus.contains("1"))
    assert(!blacklist.nodeToFailedExecs("hostA").contains("1"))
    clock.advance(blacklist.BLACKLIST_TIMEOUT_MILLIS)
    blacklist.applyBlacklistTimeout()
    assert(!blacklist.nodeIdToBlacklistExpiryTime.contains("hostA"))
  }

  test("task failures expire with time") {
    configureBlacklistAndScheduler()
    var stageId = 0
    def failOneTaskInTaskSet(exec: String): Unit = {
      val tsm = createTaskSetManager(numTasks = 1, stageId = stageId)
      tsm.updateBlacklistForFailedTask("host-" + exec, exec, 0)
      blacklist.updateBlacklistForSuccessfulTaskSet(stageId, 0, tsm.execToFailures)
      stageId += 1
    }
    failOneTaskInTaskSet(exec = "1")
    // We have one sporadic failure on exec 2, but that's it.  Later checks ensure that we never
    // blacklist executor 2 despite this one failure.
    failOneTaskInTaskSet(exec = "2")
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
    assert(blacklist.nextExpiryTime === Long.MaxValue)

    // We advance the clock past the expiry time.
    clock.advance(blacklist.BLACKLIST_TIMEOUT_MILLIS + 1)
    val t0 = clock.getTimeMillis()
    blacklist.applyBlacklistTimeout()
    assert(blacklist.nextExpiryTime === Long.MaxValue)
    failOneTaskInTaskSet(exec = "1")

    // Because the 2nd failure on executor 1 happened past the expiry time, nothing should have been
    // blacklisted.
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())

    // Now we add one more failure, within the timeout, and it should be counted.
    clock.setTime(t0 + blacklist.BLACKLIST_TIMEOUT_MILLIS - 1)
    val t1 = clock.getTimeMillis()
    failOneTaskInTaskSet(exec = "1")
    blacklist.applyBlacklistTimeout()
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1"))
    assert(blacklist.nextExpiryTime === t1 + blacklist.BLACKLIST_TIMEOUT_MILLIS)

    // Fail a second executor, and go over its expiry as well.
    clock.setTime(t1 + blacklist.BLACKLIST_TIMEOUT_MILLIS - 1)
    val t2 = clock.getTimeMillis()
    failOneTaskInTaskSet(exec = "3")
    failOneTaskInTaskSet(exec = "3")
    blacklist.applyBlacklistTimeout()
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1", "3"))
    assert(blacklist.nextExpiryTime === t1 + blacklist.BLACKLIST_TIMEOUT_MILLIS)

    clock.setTime(t1 + blacklist.BLACKLIST_TIMEOUT_MILLIS + 1)
    blacklist.applyBlacklistTimeout()
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("3"))
    assert(blacklist.nextExpiryTime === t2 + blacklist.BLACKLIST_TIMEOUT_MILLIS)

    // Make sure that we update correctly when we go from having blacklisted executors to
    // just having tasks with timeouts.
    clock.setTime(t2 + blacklist.BLACKLIST_TIMEOUT_MILLIS - 1)
    failOneTaskInTaskSet(exec = "4")
    blacklist.applyBlacklistTimeout()
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("3"))
    assert(blacklist.nextExpiryTime === t2 + blacklist.BLACKLIST_TIMEOUT_MILLIS)

    clock.setTime(t2 + blacklist.BLACKLIST_TIMEOUT_MILLIS + 1)
    blacklist.applyBlacklistTimeout()
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
    // we've got one task failure still, but we don't bother setting nextExpiryTime to it, to
    // avoid wasting time checking for expiry of individual task failures.
    assert(blacklist.nextExpiryTime === Long.MaxValue)
  }

  test("multiple attempts for the same task count once") {
    // make sure that for blacklisting tasks, the node counts task attempts, not executors.  But for
    // stage-level blacklisting, we count unique tasks.  The reason for this difference is, with
    // task-attempt blacklisting, we want to make it easy to configure so that you ensure a node
    // is blacklisted before the taskset is completely aborted b/c of spark.task.maxFailures.
    // But with stage-blacklisting, we want to make sure we're not just counting one bad task
    // that has failed many times.

    configureBlacklistAndScheduler(
      config.MAX_TASK_ATTEMPTS_PER_EXECUTOR.key -> "2",
      config.MAX_TASK_ATTEMPTS_PER_NODE.key -> "3",
      config.MAX_FAILURES_PER_EXEC_STAGE.key -> "2",
      config.MAX_FAILED_EXEC_PER_NODE_STAGE.key -> "3"
    )
    val tsm = createTaskSetManager(numTasks = 5, stageId = 0)
    // fail a task twice on hostA, exec:1
    tsm.updateBlacklistForFailedTask("hostA", exec = "1", index = 0)
    tsm.updateBlacklistForFailedTask("hostA", exec = "1", index = 0)
    assert(tsm.isExecutorBlacklistedForTask("1", 0))
    assert(!tsm.isNodeBlacklistedForTask("hostA", 0))
    assert(!tsm.isExecutorBlacklistedForTaskSet("1"))
    assert(!tsm.isNodeBlacklistedForTaskSet("hostA"))

    // fail the same task once more on hostA, exec:2
    tsm.updateBlacklistForFailedTask("hostA", exec = "2", index = 0)
    assert(tsm.isNodeBlacklistedForTask("hostA", 0))
    assert(!tsm.isExecutorBlacklistedForTaskSet("2"))
    assert(!tsm.isNodeBlacklistedForTaskSet("hostA"))

    // fail another task on hostA, exec:1.  Now that executor has failures on two different tasks,
    // so its blacklisted
    tsm.updateBlacklistForFailedTask("hostA", exec = "1", index = 1)
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, tsm.execToFailures)
    assert(tsm.isExecutorBlacklistedForTaskSet("1"))
    assert(!tsm.isNodeBlacklistedForTaskSet("hostA"))

    // fail a third task on hostA, exec:2, so that exec is blacklisted for the whole task set
    tsm.updateBlacklistForFailedTask("hostA", exec = "2", index = 2)
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, tsm.execToFailures)
    assert(tsm.isExecutorBlacklistedForTaskSet("2"))
    assert(!tsm.isNodeBlacklistedForTaskSet("hostA"))

    // fail a fourth & fifth task on hostA, exec:3.  Now we've got three executors that are
    // blacklisted for the taskset, so blacklist the whole node.
    tsm.updateBlacklistForFailedTask("hostA", exec = "3", index = 3)
    tsm.updateBlacklistForFailedTask("hostA", exec = "3", index = 4)
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, tsm.execToFailures)
    assert(tsm.isExecutorBlacklistedForTaskSet("3"))
    assert(tsm.isNodeBlacklistedForTaskSet("hostA"))
  }

  test("only blacklist nodes for the application when all the blacklisted executors are all on " +
    "same host") {
    // we blacklist executors on two different hosts -- make sure that doesn't lead to any
    // node blacklisting
    configureBlacklistAndScheduler()
    val tsm0 = createTaskSetManager(numTasks = 4, stageId = 0)
    tsm0.updateBlacklistForFailedTask("hostA", exec = "1", index = 0)
    tsm0.updateBlacklistForFailedTask("hostA", exec = "1", index = 1)
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, tsm0.execToFailures)
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1"))
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())

    val tsm1 = createTaskSetManager(numTasks = 4, stageId = 1)
    tsm1.updateBlacklistForFailedTask("hostB", exec = "2", index = 0)
    tsm1.updateBlacklistForFailedTask("hostB", exec = "2", index = 1)
    blacklist.updateBlacklistForSuccessfulTaskSet(1, 0, tsm1.execToFailures)
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1", "2"))
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())
  }

  test("only blacklist nodes for the task set when all the blacklisted executors are all on " +
    "same host") {
    // we blacklist executors on two different hosts within one taskSet -- make sure that doesn't
    // lead to any node blacklisting
    configureBlacklistAndScheduler()
    val tsm = createTaskSetManager(numTasks = 4, stageId = 0)
    tsm.updateBlacklistForFailedTask("hostA", exec = "1", index = 0)
    tsm.updateBlacklistForFailedTask("hostA", exec = "1", index = 1)
    assertEquivalentToSet(tsm.isExecutorBlacklistedForTaskSet(_), Set("1"))
    assertEquivalentToSet(tsm.isNodeBlacklistedForTaskSet(_), Set())

    tsm.updateBlacklistForFailedTask("hostB", exec = "2", index = 0)
    tsm.updateBlacklistForFailedTask("hostB", exec = "2", index = 1)
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

  test("check blacklist configuration invariants") {
    val conf = new SparkConf().setMaster("yarn-cluster")
    Seq(
      (2, 2),
      (2, 3),
      (3, 3)
    ).foreach { case (maxTaskFailures, maxNodeAttempts) =>
      conf.set("spark.task.maxFailures", maxTaskFailures.toString)
      conf.set(config.MAX_TASK_ATTEMPTS_PER_NODE.key, maxNodeAttempts.toString)
      val excMsg = intercept[IllegalArgumentException] {
        BlacklistTracker.validateBlacklistConfs(conf)
      }.getMessage()
      assert(excMsg === s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key} " +
        s"( = ${maxNodeAttempts}) was >= spark.task.maxFailures " +
        s"( = ${maxTaskFailures} ).  Though blacklisting is enabled, with this configuration, " +
        s"Spark will not be robust to one bad node.  Decrease " +
        s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key }, increase spark.task.maxFailures, or disable " +
        s"blacklisting with ${config.BLACKLIST_ENABLED.key}")
    }

    conf.remove("spark.task.maxFailures")
    conf.remove(config.MAX_TASK_ATTEMPTS_PER_NODE)

    Seq(
      config.MAX_TASK_ATTEMPTS_PER_EXECUTOR,
      config.MAX_TASK_ATTEMPTS_PER_NODE,
      config.MAX_FAILURES_PER_EXEC_STAGE,
      config.MAX_FAILED_EXEC_PER_NODE_STAGE,
      config.MAX_FAILURES_PER_EXEC,
      config.MAX_FAILED_EXEC_PER_NODE,
      config.BLACKLIST_TIMEOUT_CONF
    ).foreach { config =>
      conf.set(config.key, "0")
      val excMsg = intercept[IllegalArgumentException] {
        BlacklistTracker.validateBlacklistConfs(conf)
      }.getMessage()
      assert(excMsg.contains(s"${config.key} was 0, but must be > 0."))
      conf.remove(config)
    }
  }
}
