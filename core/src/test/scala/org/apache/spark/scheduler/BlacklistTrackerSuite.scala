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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfterEach
import org.scalatest.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.util.ManualClock

class BlacklistTrackerSuite extends SparkFunSuite with BeforeAndAfterEach with MockitoSugar
    with LocalSparkContext {

  private val clock = new ManualClock(0)

  private var blacklist: BlacklistTracker = _
  private var listenerBusMock: LiveListenerBus = _
  private var scheduler: TaskSchedulerImpl = _
  private var conf: SparkConf = _

  override def beforeEach(): Unit = {
    conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.BLACKLIST_ENABLED.key, "true")
    scheduler = mockTaskSchedWithConf(conf)

    clock.setTime(0)

    listenerBusMock = mock[LiveListenerBus]
    blacklist = new BlacklistTracker(listenerBusMock, conf, None, clock)
  }

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

  // All executors and hosts used in tests should be in this set, so that [[assertEquivalentToSet]]
  // works.  Its OK if its got extraneous entries
  val allExecutorAndHostIds = {
    (('A' to 'Z')++ (1 to 100).map(_.toString))
      .flatMap{ suffix =>
        Seq(s"host$suffix", s"host-$suffix")
      }
  }.toSet

  /**
   * Its easier to write our tests as if we could directly look at the sets of nodes & executors in
   * the blacklist.  However the api doesn't expose a set, so this is a simple way to test
   * something similar, since we know the universe of values that might appear in these sets.
   */
  def assertEquivalentToSet(f: String => Boolean, expected: Set[String]): Unit = {
    allExecutorAndHostIds.foreach { id =>
      val actual = f(id)
      val exp = expected.contains(id)
      assert(actual === exp, raw"""for string "$id" """)
    }
  }

  def mockTaskSchedWithConf(conf: SparkConf): TaskSchedulerImpl = {
    sc = new SparkContext(conf)
    val scheduler = mock[TaskSchedulerImpl]
    when(scheduler.sc).thenReturn(sc)
    when(scheduler.mapOutputTracker).thenReturn(
      SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster])
    scheduler
  }

  def createTaskSetBlacklist(stageId: Int = 0): TaskSetBlacklist = {
    new TaskSetBlacklist(listenerBusMock, conf, stageId, stageAttemptId = 0, clock = clock)
  }

  test("executors can be blacklisted with only a few failures per stage") {
    // For many different stages, executor 1 fails a task, then executor 2 succeeds the task,
    // and then the task set is done.  Not enough failures to blacklist the executor *within*
    // any particular taskset, but we still blacklist the executor overall eventually.
    // Also, we intentionally have a mix of task successes and failures -- there are even some
    // successes after the executor is blacklisted.  The idea here is those tasks get scheduled
    // before the executor is blacklisted.  We might get successes after blacklisting (because the
    // executor might be flaky but not totally broken).  But successes should not unblacklist the
    // executor.
    val failuresUntilBlacklisted = conf.get(config.MAX_FAILURES_PER_EXEC)
    var failuresSoFar = 0
    (0 until failuresUntilBlacklisted * 10).foreach { stageId =>
      val taskSetBlacklist = createTaskSetBlacklist(stageId)
      if (stageId % 2 == 0) {
        // fail one task in every other taskset
        taskSetBlacklist.updateBlacklistForFailedTask(
          "hostA", exec = "1", index = 0, failureReason = "testing")
        failuresSoFar += 1
      }
      blacklist.updateBlacklistForSuccessfulTaskSet(stageId, 0, taskSetBlacklist.execToFailures)
      assert(failuresSoFar == stageId / 2 + 1)
      if (failuresSoFar < failuresUntilBlacklisted) {
        assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
      } else {
        assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1"))
        verify(listenerBusMock).post(
          SparkListenerExecutorBlacklisted(0, "1", failuresUntilBlacklisted))
      }
    }
  }

  // If an executor has many task failures, but the task set ends up failing, it shouldn't be
  // counted against the executor.
  test("executors aren't blacklisted as a result of tasks in failed task sets") {
    val failuresUntilBlacklisted = conf.get(config.MAX_FAILURES_PER_EXEC)
    // for many different stages, executor 1 fails a task, and then the taskSet fails.
    (0 until failuresUntilBlacklisted * 10).foreach { stage =>
      val taskSetBlacklist = createTaskSetBlacklist(stage)
      taskSetBlacklist.updateBlacklistForFailedTask(
        "hostA", exec = "1", index = 0, failureReason = "testing")
    }
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
  }

  Seq(true, false).foreach { succeedTaskSet =>
    val label = if (succeedTaskSet) "success" else "failure"
    test(s"stage blacklist updates correctly on stage $label") {
      // Within one taskset, an executor fails a few times, so it's blacklisted for the taskset.
      // But if the taskset fails, we shouldn't blacklist the executor after the stage.
      val taskSetBlacklist = createTaskSetBlacklist(0)
      // We trigger enough failures for both the taskset blacklist, and the application blacklist.
      val numFailures = math.max(conf.get(config.MAX_FAILURES_PER_EXEC),
        conf.get(config.MAX_FAILURES_PER_EXEC_STAGE))
      (0 until numFailures).foreach { index =>
        taskSetBlacklist.updateBlacklistForFailedTask(
          "hostA", exec = "1", index = index, failureReason = "testing")
      }
      assert(taskSetBlacklist.isExecutorBlacklistedForTaskSet("1"))
      assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
      if (succeedTaskSet) {
        // The task set succeeded elsewhere, so we should count those failures against our executor,
        // and it should be blacklisted for the entire application.
        blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, taskSetBlacklist.execToFailures)
        assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1"))
        verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(0, "1", numFailures))
      } else {
        // The task set failed, so we don't count these failures against the executor for other
        // stages.
        assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
      }
    }
  }

  test("blacklisted executors and nodes get recovered with time") {
    val taskSetBlacklist0 = createTaskSetBlacklist(stageId = 0)
    // Fail 4 tasks in one task set on executor 1, so that executor gets blacklisted for the whole
    // application.
    (0 until 4).foreach { partition =>
      taskSetBlacklist0.updateBlacklistForFailedTask(
        "hostA", exec = "1", index = partition, failureReason = "testing")
    }
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, taskSetBlacklist0.execToFailures)
    assert(blacklist.nodeBlacklist() === Set())
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1"))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(0, "1", 4))

    val taskSetBlacklist1 = createTaskSetBlacklist(stageId = 1)
    // Fail 4 tasks in one task set on executor 2, so that executor gets blacklisted for the whole
    // application.  Since that's the second executor that is blacklisted on the same node, we also
    // blacklist that node.
    (0 until 4).foreach { partition =>
      taskSetBlacklist1.updateBlacklistForFailedTask(
        "hostA", exec = "2", index = partition, failureReason = "testing")
    }
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, taskSetBlacklist1.execToFailures)
    assert(blacklist.nodeBlacklist() === Set("hostA"))
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set("hostA"))
    verify(listenerBusMock).post(SparkListenerNodeBlacklisted(0, "hostA", 2))
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1", "2"))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(0, "2", 4))

    // Advance the clock and then make sure hostA and executors 1 and 2 have been removed from the
    // blacklist.
    val timeout = blacklist.BLACKLIST_TIMEOUT_MILLIS + 1
    clock.advance(timeout)
    blacklist.applyBlacklistTimeout()
    assert(blacklist.nodeBlacklist() === Set())
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
    verify(listenerBusMock).post(SparkListenerExecutorUnblacklisted(timeout, "2"))
    verify(listenerBusMock).post(SparkListenerExecutorUnblacklisted(timeout, "1"))
    verify(listenerBusMock).post(SparkListenerNodeUnblacklisted(timeout, "hostA"))

    // Fail one more task, but executor isn't put back into blacklist since the count of failures
    // on that executor should have been reset to 0.
    val taskSetBlacklist2 = createTaskSetBlacklist(stageId = 2)
    taskSetBlacklist2.updateBlacklistForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    blacklist.updateBlacklistForSuccessfulTaskSet(2, 0, taskSetBlacklist2.execToFailures)
    assert(blacklist.nodeBlacklist() === Set())
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
  }

  test("blacklist can handle lost executors") {
    // The blacklist should still work if an executor is killed completely.  We should still
    // be able to blacklist the entire node.
    val taskSetBlacklist0 = createTaskSetBlacklist(stageId = 0)
    // Lets say that executor 1 dies completely.  We get some task failures, but
    // the taskset then finishes successfully (elsewhere).
    (0 until 4).foreach { partition =>
      taskSetBlacklist0.updateBlacklistForFailedTask(
        "hostA", exec = "1", index = partition, failureReason = "testing")
    }
    blacklist.handleRemovedExecutor("1")
    blacklist.updateBlacklistForSuccessfulTaskSet(
      stageId = 0,
      stageAttemptId = 0,
      taskSetBlacklist0.execToFailures)
    assert(blacklist.isExecutorBlacklisted("1"))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(0, "1", 4))
    val t1 = blacklist.BLACKLIST_TIMEOUT_MILLIS / 2
    clock.advance(t1)

    // Now another executor gets spun up on that host, but it also dies.
    val taskSetBlacklist1 = createTaskSetBlacklist(stageId = 1)
    (0 until 4).foreach { partition =>
      taskSetBlacklist1.updateBlacklistForFailedTask(
        "hostA", exec = "2", index = partition, failureReason = "testing")
    }
    blacklist.handleRemovedExecutor("2")
    blacklist.updateBlacklistForSuccessfulTaskSet(
      stageId = 1,
      stageAttemptId = 0,
      taskSetBlacklist1.execToFailures)
    // We've now had two bad executors on the hostA, so we should blacklist the entire node.
    assert(blacklist.isExecutorBlacklisted("1"))
    assert(blacklist.isExecutorBlacklisted("2"))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(t1, "2", 4))
    assert(blacklist.isNodeBlacklisted("hostA"))
    verify(listenerBusMock).post(SparkListenerNodeBlacklisted(t1, "hostA", 2))

    // Advance the clock so that executor 1 should no longer be explicitly blacklisted, but
    // everything else should still be blacklisted.
    val t2 = blacklist.BLACKLIST_TIMEOUT_MILLIS / 2 + 1
    clock.advance(t2)
    blacklist.applyBlacklistTimeout()
    assert(!blacklist.isExecutorBlacklisted("1"))
    verify(listenerBusMock).post(SparkListenerExecutorUnblacklisted(t1 + t2, "1"))
    assert(blacklist.isExecutorBlacklisted("2"))
    assert(blacklist.isNodeBlacklisted("hostA"))
    // make sure we don't leak memory
    assert(!blacklist.executorIdToBlacklistStatus.contains("1"))
    assert(!blacklist.nodeToBlacklistedExecs("hostA").contains("1"))
    // Advance the timeout again so now hostA should be removed from the blacklist.
    clock.advance(t1)
    blacklist.applyBlacklistTimeout()
    assert(!blacklist.nodeIdToBlacklistExpiryTime.contains("hostA"))
    verify(listenerBusMock).post(SparkListenerNodeUnblacklisted(t1 + t2 + t1, "hostA"))
    // Even though unblacklisting a node implicitly unblacklists all of its executors,
    // there will be no SparkListenerExecutorUnblacklisted sent here.
  }

  test("task failures expire with time") {
    // Verifies that 2 failures within the timeout period cause an executor to be blacklisted, but
    // if task failures are spaced out by more than the timeout period, the first failure is timed
    // out, and the executor isn't blacklisted.
    var stageId = 0

    def failOneTaskInTaskSet(exec: String): Unit = {
      val taskSetBlacklist = createTaskSetBlacklist(stageId = stageId)
      taskSetBlacklist.updateBlacklistForFailedTask("host-" + exec, exec, 0, "testing")
      blacklist.updateBlacklistForSuccessfulTaskSet(stageId, 0, taskSetBlacklist.execToFailures)
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
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(t1, "1", 2))
    assert(blacklist.nextExpiryTime === t1 + blacklist.BLACKLIST_TIMEOUT_MILLIS)

    // Add failures on executor 3, make sure it gets put on the blacklist.
    clock.setTime(t1 + blacklist.BLACKLIST_TIMEOUT_MILLIS - 1)
    val t2 = clock.getTimeMillis()
    failOneTaskInTaskSet(exec = "3")
    failOneTaskInTaskSet(exec = "3")
    blacklist.applyBlacklistTimeout()
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1", "3"))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(t2, "3", 2))
    assert(blacklist.nextExpiryTime === t1 + blacklist.BLACKLIST_TIMEOUT_MILLIS)

    // Now we go past the timeout for executor 1, so it should be dropped from the blacklist.
    clock.setTime(t1 + blacklist.BLACKLIST_TIMEOUT_MILLIS + 1)
    blacklist.applyBlacklistTimeout()
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("3"))
    verify(listenerBusMock).post(SparkListenerExecutorUnblacklisted(clock.getTimeMillis(), "1"))
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
    verify(listenerBusMock).post(SparkListenerExecutorUnblacklisted(clock.getTimeMillis(), "3"))
    // we've got one task failure still, but we don't bother setting nextExpiryTime to it, to
    // avoid wasting time checking for expiry of individual task failures.
    assert(blacklist.nextExpiryTime === Long.MaxValue)
  }

  test("task failure timeout works as expected for long-running tasksets") {
    // This ensures that we don't trigger spurious blacklisting for long tasksets, when the taskset
    // finishes long after the task failures.  We create two tasksets, each with one failure.
    // Individually they shouldn't cause any blacklisting since there is only one failure.
    // Furthermore, we space the failures out so far that even when both tasksets have completed,
    // we still don't trigger any blacklisting.
    val taskSetBlacklist1 = createTaskSetBlacklist(stageId = 1)
    val taskSetBlacklist2 = createTaskSetBlacklist(stageId = 2)
    // Taskset1 has one failure immediately
    taskSetBlacklist1.updateBlacklistForFailedTask("host-1", "1", 0, "testing")
    // Then we have a *long* delay, much longer than the timeout, before any other failures or
    // taskset completion
    clock.advance(blacklist.BLACKLIST_TIMEOUT_MILLIS * 5)
    // After the long delay, we have one failure on taskset 2, on the same executor
    taskSetBlacklist2.updateBlacklistForFailedTask("host-1", "1", 0, "testing")
    // Finally, we complete both tasksets.  Its important here to complete taskset2 *first*.  We
    // want to make sure that when taskset 1 finishes, even though we've now got two task failures,
    // we realize that the task failure we just added was well before the timeout.
    clock.advance(1)
    blacklist.updateBlacklistForSuccessfulTaskSet(stageId = 2, 0, taskSetBlacklist2.execToFailures)
    clock.advance(1)
    blacklist.updateBlacklistForSuccessfulTaskSet(stageId = 1, 0, taskSetBlacklist1.execToFailures)

    // Make sure nothing was blacklisted
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set())
  }

  test("only blacklist nodes for the application when enough executors have failed on that " +
    "specific host") {
    // we blacklist executors on two different hosts -- make sure that doesn't lead to any
    // node blacklisting
    val taskSetBlacklist0 = createTaskSetBlacklist(stageId = 0)
    taskSetBlacklist0.updateBlacklistForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    taskSetBlacklist0.updateBlacklistForFailedTask(
      "hostA", exec = "1", index = 1, failureReason = "testing")
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, taskSetBlacklist0.execToFailures)
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1"))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(0, "1", 2))
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())

    val taskSetBlacklist1 = createTaskSetBlacklist(stageId = 1)
    taskSetBlacklist1.updateBlacklistForFailedTask(
      "hostB", exec = "2", index = 0, failureReason = "testing")
    taskSetBlacklist1.updateBlacklistForFailedTask(
      "hostB", exec = "2", index = 1, failureReason = "testing")
    blacklist.updateBlacklistForSuccessfulTaskSet(1, 0, taskSetBlacklist1.execToFailures)
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1", "2"))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(0, "2", 2))
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set())

    // Finally, blacklist another executor on the same node as the original blacklisted executor,
    // and make sure this time we *do* blacklist the node.
    val taskSetBlacklist2 = createTaskSetBlacklist(stageId = 0)
    taskSetBlacklist2.updateBlacklistForFailedTask(
      "hostA", exec = "3", index = 0, failureReason = "testing")
    taskSetBlacklist2.updateBlacklistForFailedTask(
      "hostA", exec = "3", index = 1, failureReason = "testing")
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, taskSetBlacklist2.execToFailures)
    assertEquivalentToSet(blacklist.isExecutorBlacklisted(_), Set("1", "2", "3"))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(0, "3", 2))
    assertEquivalentToSet(blacklist.isNodeBlacklisted(_), Set("hostA"))
    verify(listenerBusMock).post(SparkListenerNodeBlacklisted(0, "hostA", 2))
  }

  test("blacklist still respects legacy configs") {
    val conf = new SparkConf().setMaster("local")
    assert(!BlacklistTracker.isBlacklistEnabled(conf))
    conf.set(config.BLACKLIST_LEGACY_TIMEOUT_CONF, 5000L)
    assert(BlacklistTracker.isBlacklistEnabled(conf))
    assert(5000 === BlacklistTracker.getBlacklistTimeout(conf))
    // the new conf takes precedence, though
    conf.set(config.BLACKLIST_TIMEOUT_CONF, 1000L)
    assert(1000 === BlacklistTracker.getBlacklistTimeout(conf))

    // if you explicitly set the legacy conf to 0, that also would disable blacklisting
    conf.set(config.BLACKLIST_LEGACY_TIMEOUT_CONF, 0L)
    assert(!BlacklistTracker.isBlacklistEnabled(conf))
    // but again, the new conf takes precedence
    conf.set(config.BLACKLIST_ENABLED, true)
    assert(BlacklistTracker.isBlacklistEnabled(conf))
    assert(1000 === BlacklistTracker.getBlacklistTimeout(conf))
  }

  test("check blacklist configuration invariants") {
    val conf = new SparkConf().setMaster("yarn-cluster")
    Seq(
      (2, 2),
      (2, 3)
    ).foreach { case (maxTaskFailures, maxNodeAttempts) =>
      conf.set(config.TASK_MAX_FAILURES, maxTaskFailures)
      conf.set(config.MAX_TASK_ATTEMPTS_PER_NODE.key, maxNodeAttempts.toString)
      val excMsg = intercept[IllegalArgumentException] {
        BlacklistTracker.validateBlacklistConfs(conf)
      }.getMessage()
      assert(excMsg === s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key} " +
        s"( = ${maxNodeAttempts}) was >= ${config.TASK_MAX_FAILURES.key} " +
        s"( = ${maxTaskFailures} ).  Though blacklisting is enabled, with this configuration, " +
        s"Spark will not be robust to one bad node.  Decrease " +
        s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key}, increase ${config.TASK_MAX_FAILURES.key}, " +
        s"or disable blacklisting with ${config.BLACKLIST_ENABLED.key}")
    }

    conf.remove(config.TASK_MAX_FAILURES)
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

  test("blacklisting kills executors, configured by BLACKLIST_KILL_ENABLED") {
    val allocationClientMock = mock[ExecutorAllocationClient]
    when(allocationClientMock.killExecutors(any(), any(), any(), any())).thenReturn(Seq("called"))
    when(allocationClientMock.killExecutorsOnHost("hostA")).thenAnswer { (_: InvocationOnMock) =>
      // To avoid a race between blacklisting and killing, it is important that the nodeBlacklist
      // is updated before we ask the executor allocation client to kill all the executors
      // on a particular host.
      if (blacklist.nodeBlacklist.contains("hostA")) {
        true
      } else {
        throw new IllegalStateException("hostA should be on the blacklist")
      }
    }
    blacklist = new BlacklistTracker(listenerBusMock, conf, Some(allocationClientMock), clock)

    // Disable auto-kill. Blacklist an executor and make sure killExecutors is not called.
    conf.set(config.BLACKLIST_KILL_ENABLED, false)

    val taskSetBlacklist0 = createTaskSetBlacklist(stageId = 0)
    // Fail 4 tasks in one task set on executor 1, so that executor gets blacklisted for the whole
    // application.
    (0 until 4).foreach { partition =>
      taskSetBlacklist0.updateBlacklistForFailedTask(
        "hostA", exec = "1", index = partition, failureReason = "testing")
    }
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, taskSetBlacklist0.execToFailures)

    verify(allocationClientMock, never).killExecutor(any())

    val taskSetBlacklist1 = createTaskSetBlacklist(stageId = 1)
    // Fail 4 tasks in one task set on executor 2, so that executor gets blacklisted for the whole
    // application.  Since that's the second executor that is blacklisted on the same node, we also
    // blacklist that node.
    (0 until 4).foreach { partition =>
      taskSetBlacklist1.updateBlacklistForFailedTask(
        "hostA", exec = "2", index = partition, failureReason = "testing")
    }
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, taskSetBlacklist1.execToFailures)

    verify(allocationClientMock, never).killExecutors(any(), any(), any(), any())
    verify(allocationClientMock, never).killExecutorsOnHost(any())

    // Enable auto-kill. Blacklist an executor and make sure killExecutors is called.
    conf.set(config.BLACKLIST_KILL_ENABLED, true)
    blacklist = new BlacklistTracker(listenerBusMock, conf, Some(allocationClientMock), clock)

    val taskSetBlacklist2 = createTaskSetBlacklist(stageId = 0)
    // Fail 4 tasks in one task set on executor 1, so that executor gets blacklisted for the whole
    // application.
    (0 until 4).foreach { partition =>
      taskSetBlacklist2.updateBlacklistForFailedTask(
        "hostA", exec = "1", index = partition, failureReason = "testing")
    }
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, taskSetBlacklist2.execToFailures)

    verify(allocationClientMock).killExecutors(Seq("1"), false, false, true)

    val taskSetBlacklist3 = createTaskSetBlacklist(stageId = 1)
    // Fail 4 tasks in one task set on executor 2, so that executor gets blacklisted for the whole
    // application.  Since that's the second executor that is blacklisted on the same node, we also
    // blacklist that node.
    (0 until 4).foreach { partition =>
      taskSetBlacklist3.updateBlacklistForFailedTask(
        "hostA", exec = "2", index = partition, failureReason = "testing")
    }
    blacklist.updateBlacklistForSuccessfulTaskSet(0, 0, taskSetBlacklist3.execToFailures)

    verify(allocationClientMock).killExecutors(Seq("2"), false, false, true)
    verify(allocationClientMock).killExecutorsOnHost("hostA")
  }

  test("fetch failure blacklisting kills executors, configured by BLACKLIST_KILL_ENABLED") {
    val allocationClientMock = mock[ExecutorAllocationClient]
    when(allocationClientMock.killExecutors(any(), any(), any(), any())).thenReturn(Seq("called"))
    when(allocationClientMock.killExecutorsOnHost("hostA")).thenAnswer { (_: InvocationOnMock) =>
      // To avoid a race between blacklisting and killing, it is important that the nodeBlacklist
      // is updated before we ask the executor allocation client to kill all the executors
      // on a particular host.
      if (blacklist.nodeBlacklist.contains("hostA")) {
        true
      } else {
        throw new IllegalStateException("hostA should be on the blacklist")
      }
    }

    conf.set(config.BLACKLIST_FETCH_FAILURE_ENABLED, true)
    blacklist = new BlacklistTracker(listenerBusMock, conf, Some(allocationClientMock), clock)

    // Disable auto-kill. Blacklist an executor and make sure killExecutors is not called.
    conf.set(config.BLACKLIST_KILL_ENABLED, false)
    blacklist.updateBlacklistForFetchFailure("hostA", exec = "1")

    verify(allocationClientMock, never).killExecutors(any(), any(), any(), any())
    verify(allocationClientMock, never).killExecutorsOnHost(any())

    assert(blacklist.nodeToBlacklistedExecs.contains("hostA"))
    assert(blacklist.nodeToBlacklistedExecs("hostA").contains("1"))

    // Enable auto-kill. Blacklist an executor and make sure killExecutors is called.
    conf.set(config.BLACKLIST_KILL_ENABLED, true)
    blacklist = new BlacklistTracker(listenerBusMock, conf, Some(allocationClientMock), clock)
    clock.advance(1000)
    blacklist.updateBlacklistForFetchFailure("hostA", exec = "1")

    verify(allocationClientMock).killExecutors(Seq("1"), false, false, true)
    verify(allocationClientMock, never).killExecutorsOnHost(any())

    assert(blacklist.executorIdToBlacklistStatus.contains("1"))
    assert(blacklist.executorIdToBlacklistStatus("1").node === "hostA")
    assert(blacklist.executorIdToBlacklistStatus("1").expiryTime ===
      1000 + blacklist.BLACKLIST_TIMEOUT_MILLIS)
    assert(blacklist.nextExpiryTime === 1000 + blacklist.BLACKLIST_TIMEOUT_MILLIS)
    assert(blacklist.nodeIdToBlacklistExpiryTime.isEmpty)
    assert(blacklist.nodeToBlacklistedExecs.contains("hostA"))
    assert(blacklist.nodeToBlacklistedExecs("hostA").contains("1"))

    // Enable external shuffle service to see if all the executors on this node will be killed.
    conf.set(config.SHUFFLE_SERVICE_ENABLED, true)
    clock.advance(1000)
    blacklist.updateBlacklistForFetchFailure("hostA", exec = "2")

    verify(allocationClientMock, never).killExecutors(Seq("2"), true, true)
    verify(allocationClientMock).killExecutorsOnHost("hostA")

    assert(blacklist.nodeIdToBlacklistExpiryTime.contains("hostA"))
    assert(blacklist.nodeIdToBlacklistExpiryTime("hostA") ===
      2000 + blacklist.BLACKLIST_TIMEOUT_MILLIS)
    assert(blacklist.nextExpiryTime === 1000 + blacklist.BLACKLIST_TIMEOUT_MILLIS)
  }
}
