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
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.util.ManualClock

class HealthTrackerSuite extends SparkFunSuite with MockitoSugar with LocalSparkContext {

  private val clock = new ManualClock(0)

  private var healthTracker: HealthTracker = _
  private var listenerBusMock: LiveListenerBus = _
  private var scheduler: TaskSchedulerImpl = _
  private var conf: SparkConf = _

  override def beforeEach(): Unit = {
    conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.EXCLUDE_ON_FAILURE_ENABLED.key, "true")
    scheduler = mockTaskSchedWithConf(conf)

    clock.setTime(0)

    listenerBusMock = mock[LiveListenerBus]
    healthTracker = new HealthTracker(listenerBusMock, conf, None, clock)
  }

  override def afterEach(): Unit = {
    if (healthTracker != null) {
      healthTracker = null
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
    ('A' to 'Z')
      .flatMap { suffix =>
        Seq(s"host$suffix", s"host-$suffix")
      }
  }.toSet ++ (1 to 100).map(_.toString)

  /**
   * Its easier to write our tests as if we could directly look at the sets of nodes & executors in
   * the exclude.  However the api doesn't expose a set, so this is a simple way to test
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

  def createTaskSetExcludelist(stageId: Int = 0): TaskSetExcludelist = {
    new TaskSetExcludelist(listenerBusMock, conf, stageId, stageAttemptId = 0, clock = clock)
  }

  test("executors can be excluded with only a few failures per stage") {
    // For many different stages, executor 1 fails a task, then executor 2 succeeds the task,
    // and then the task set is done.  Not enough failures to exclude the executor *within*
    // any particular taskset, but we still exclude the executor overall eventually.
    // Also, we intentionally have a mix of task successes and failures -- there are even some
    // successes after the executor is excluded.  The idea here is those tasks get scheduled
    // before the executor is excluded.  We might get successes after excluding (because the
    // executor might be flaky but not totally broken).  But successes should not unexclude the
    // executor.
    val failuresUntilExcludeed = conf.get(config.MAX_FAILURES_PER_EXEC)
    var failuresSoFar = 0
    (0 until failuresUntilExcludeed * 10).foreach { stageId =>
      val taskSetExclude = createTaskSetExcludelist(stageId)
      if (stageId % 2 == 0) {
        // fail one task in every other taskset
        taskSetExclude.updateExcludedForFailedTask(
          "hostA", exec = "1", index = 0, failureReason = "testing")
        failuresSoFar += 1
      }
      healthTracker.updateExcludedForSuccessfulTaskSet(stageId, 0, taskSetExclude.execToFailures)
      assert(failuresSoFar == stageId / 2 + 1)
      if (failuresSoFar < failuresUntilExcludeed) {
        assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())
      } else {
        assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("1"))
        verify(listenerBusMock).post(
          SparkListenerExecutorExcluded(0, "1", failuresUntilExcludeed))
        verify(listenerBusMock).post(
          SparkListenerExecutorBlacklisted(0, "1", failuresUntilExcludeed))
      }
    }
  }

  // If an executor has many task failures, but the task set ends up failing, it shouldn't be
  // counted against the executor.
  test("executors aren't excluded as a result of tasks in failed task sets") {
    val failuresUntilExcludeed = conf.get(config.MAX_FAILURES_PER_EXEC)
    // for many different stages, executor 1 fails a task, and then the taskSet fails.
    (0 until failuresUntilExcludeed * 10).foreach { stage =>
      val taskSetExclude = createTaskSetExcludelist(stage)
      taskSetExclude.updateExcludedForFailedTask(
        "hostA", exec = "1", index = 0, failureReason = "testing")
    }
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())
  }

  Seq(true, false).foreach { succeedTaskSet =>
    val label = if (succeedTaskSet) "success" else "failure"
    test(s"stage exclude updates correctly on stage $label") {
      // Within one taskset, an executor fails a few times, so it's excluded for the taskset.
      // But if the taskset fails, we shouldn't exclude the executor after the stage.
      val taskSetExclude = createTaskSetExcludelist(0)
      // We trigger enough failures for both the taskset exclude, and the application exclude.
      val numFailures = math.max(conf.get(config.MAX_FAILURES_PER_EXEC),
        conf.get(config.MAX_FAILURES_PER_EXEC_STAGE))
      (0 until numFailures).foreach { index =>
        taskSetExclude.updateExcludedForFailedTask(
          "hostA", exec = "1", index = index, failureReason = "testing")
      }
      assert(taskSetExclude.isExecutorExcludedForTaskSet("1"))
      assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())
      if (succeedTaskSet) {
        // The task set succeeded elsewhere, so we should count those failures against our executor,
        // and it should be excluded for the entire application.
        healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude.execToFailures)
        assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("1"))
        verify(listenerBusMock).post(SparkListenerExecutorExcluded(0, "1", numFailures))
      } else {
        // The task set failed, so we don't count these failures against the executor for other
        // stages.
        assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())
      }
    }
  }

  test("excluded executors and nodes get recovered with time") {
    val taskSetExclude0 = createTaskSetExcludelist(stageId = 0)
    // Fail 4 tasks in one task set on executor 1, so that executor gets excluded for the whole
    // application.
    (0 until 4).foreach { partition =>
      taskSetExclude0.updateExcludedForFailedTask(
        "hostA", exec = "1", index = partition, failureReason = "testing")
    }
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude0.execToFailures)
    assert(healthTracker.excludedNodeList() === Set())
    assertEquivalentToSet(healthTracker.isNodeExcluded(_), Set())
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("1"))
    verify(listenerBusMock).post(SparkListenerExecutorExcluded(0, "1", 4))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(0, "1", 4))

    val taskSetExclude1 = createTaskSetExcludelist(stageId = 1)
    // Fail 4 tasks in one task set on executor 2, so that executor gets excluded for the whole
    // application.  Since that's the second executor that is excluded on the same node, we also
    // exclude that node.
    (0 until 4).foreach { partition =>
      taskSetExclude1.updateExcludedForFailedTask(
        "hostA", exec = "2", index = partition, failureReason = "testing")
    }
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude1.execToFailures)
    assert(healthTracker.excludedNodeList() === Set("hostA"))
    assertEquivalentToSet(healthTracker.isNodeExcluded(_), Set("hostA"))
    verify(listenerBusMock).post(SparkListenerNodeExcluded(0, "hostA", 2))
    verify(listenerBusMock).post(SparkListenerNodeBlacklisted(0, "hostA", 2))
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("1", "2"))
    verify(listenerBusMock).post(SparkListenerExecutorExcluded(0, "2", 4))
    verify(listenerBusMock).post(SparkListenerExecutorBlacklisted(0, "2", 4))

    // Advance the clock and then make sure hostA and executors 1 and 2 have been removed from the
    // exclude.
    val timeout = healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS + 1
    clock.advance(timeout)
    healthTracker.applyExcludeOnFailureTimeout()
    assert(healthTracker.excludedNodeList() === Set())
    assertEquivalentToSet(healthTracker.isNodeExcluded(_), Set())
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())
    verify(listenerBusMock).post(SparkListenerExecutorUnexcluded(timeout, "2"))
    verify(listenerBusMock).post(SparkListenerExecutorUnexcluded(timeout, "1"))
    verify(listenerBusMock).post(SparkListenerExecutorUnblacklisted(timeout, "2"))
    verify(listenerBusMock).post(SparkListenerExecutorUnblacklisted(timeout, "1"))
    verify(listenerBusMock).post(SparkListenerNodeUnexcluded(timeout, "hostA"))

    // Fail one more task, but executor isn't put back into exclude since the count of failures
    // on that executor should have been reset to 0.
    val taskSetExclude2 = createTaskSetExcludelist(stageId = 2)
    taskSetExclude2.updateExcludedForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    healthTracker.updateExcludedForSuccessfulTaskSet(2, 0, taskSetExclude2.execToFailures)
    assert(healthTracker.excludedNodeList() === Set())
    assertEquivalentToSet(healthTracker.isNodeExcluded(_), Set())
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())
  }

  test("exclude can handle lost executors") {
    // The exclude should still work if an executor is killed completely.  We should still
    // be able to exclude the entire node.
    val taskSetExclude0 = createTaskSetExcludelist(stageId = 0)
    // Lets say that executor 1 dies completely.  We get some task failures, but
    // the taskset then finishes successfully (elsewhere).
    (0 until 4).foreach { partition =>
      taskSetExclude0.updateExcludedForFailedTask(
        "hostA", exec = "1", index = partition, failureReason = "testing")
    }
    healthTracker.handleRemovedExecutor("1")
    healthTracker.updateExcludedForSuccessfulTaskSet(
      stageId = 0,
      stageAttemptId = 0,
      taskSetExclude0.execToFailures)
    assert(healthTracker.isExecutorExcluded("1"))
    verify(listenerBusMock).post(SparkListenerExecutorExcluded(0, "1", 4))
    val t1 = healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS / 2
    clock.advance(t1)

    // Now another executor gets spun up on that host, but it also dies.
    val taskSetExclude1 = createTaskSetExcludelist(stageId = 1)
    (0 until 4).foreach { partition =>
      taskSetExclude1.updateExcludedForFailedTask(
        "hostA", exec = "2", index = partition, failureReason = "testing")
    }
    healthTracker.handleRemovedExecutor("2")
    healthTracker.updateExcludedForSuccessfulTaskSet(
      stageId = 1,
      stageAttemptId = 0,
      taskSetExclude1.execToFailures)
    // We've now had two bad executors on the hostA, so we should exclude the entire node.
    assert(healthTracker.isExecutorExcluded("1"))
    assert(healthTracker.isExecutorExcluded("2"))
    verify(listenerBusMock).post(SparkListenerExecutorExcluded(t1, "2", 4))
    assert(healthTracker.isNodeExcluded("hostA"))
    verify(listenerBusMock).post(SparkListenerNodeExcluded(t1, "hostA", 2))

    // Advance the clock so that executor 1 should no longer be explicitly excluded, but
    // everything else should still be excluded.
    val t2 = healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS / 2 + 1
    clock.advance(t2)
    healthTracker.applyExcludeOnFailureTimeout()
    assert(!healthTracker.isExecutorExcluded("1"))
    verify(listenerBusMock).post(SparkListenerExecutorUnexcluded(t1 + t2, "1"))
    assert(healthTracker.isExecutorExcluded("2"))
    assert(healthTracker.isNodeExcluded("hostA"))
    // make sure we don't leak memory
    assert(!healthTracker.executorIdToExcludedStatus.contains("1"))
    assert(!healthTracker.nodeToExcludedExecs("hostA").contains("1"))
    // Advance the timeout again so now hostA should be removed from the exclude.
    clock.advance(t1)
    healthTracker.applyExcludeOnFailureTimeout()
    assert(!healthTracker.nodeIdToExcludedExpiryTime.contains("hostA"))
    verify(listenerBusMock).post(SparkListenerNodeUnexcluded(t1 + t2 + t1, "hostA"))
    // Even though unexcluding a node implicitly unexcludes all of its executors,
    // there will be no SparkListenerExecutorUnexcluded sent here.
  }

  test("task failures expire with time") {
    // Verifies that 2 failures within the timeout period cause an executor to be excluded, but
    // if task failures are spaced out by more than the timeout period, the first failure is timed
    // out, and the executor isn't excluded.
    var stageId = 0

    def failOneTaskInTaskSet(exec: String): Unit = {
      val taskSetExclude = createTaskSetExcludelist(stageId = stageId)
      taskSetExclude.updateExcludedForFailedTask("host-" + exec, exec, 0, "testing")
      healthTracker.updateExcludedForSuccessfulTaskSet(stageId, 0, taskSetExclude.execToFailures)
      stageId += 1
    }

    failOneTaskInTaskSet(exec = "1")
    // We have one sporadic failure on exec 2, but that's it.  Later checks ensure that we never
    // exclude executor 2 despite this one failure.
    failOneTaskInTaskSet(exec = "2")
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())
    assert(healthTracker.nextExpiryTime === Long.MaxValue)

    // We advance the clock past the expiry time.
    clock.advance(healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS + 1)
    val t0 = clock.getTimeMillis()
    healthTracker.applyExcludeOnFailureTimeout()
    assert(healthTracker.nextExpiryTime === Long.MaxValue)
    failOneTaskInTaskSet(exec = "1")

    // Because the 2nd failure on executor 1 happened past the expiry time, nothing should have been
    // excluded.
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())

    // Now we add one more failure, within the timeout, and it should be counted.
    clock.setTime(t0 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS - 1)
    val t1 = clock.getTimeMillis()
    failOneTaskInTaskSet(exec = "1")
    healthTracker.applyExcludeOnFailureTimeout()
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("1"))
    verify(listenerBusMock).post(SparkListenerExecutorExcluded(t1, "1", 2))
    assert(healthTracker.nextExpiryTime === t1 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS)

    // Add failures on executor 3, make sure it gets put on the exclude.
    clock.setTime(t1 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS - 1)
    val t2 = clock.getTimeMillis()
    failOneTaskInTaskSet(exec = "3")
    failOneTaskInTaskSet(exec = "3")
    healthTracker.applyExcludeOnFailureTimeout()
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("1", "3"))
    verify(listenerBusMock).post(SparkListenerExecutorExcluded(t2, "3", 2))
    assert(healthTracker.nextExpiryTime === t1 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS)

    // Now we go past the timeout for executor 1, so it should be dropped from the exclude.
    clock.setTime(t1 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS + 1)
    healthTracker.applyExcludeOnFailureTimeout()
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("3"))
    verify(listenerBusMock).post(SparkListenerExecutorUnexcluded(clock.getTimeMillis(), "1"))
    assert(healthTracker.nextExpiryTime === t2 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS)

    // Make sure that we update correctly when we go from having excluded executors to
    // just having tasks with timeouts.
    clock.setTime(t2 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS - 1)
    failOneTaskInTaskSet(exec = "4")
    healthTracker.applyExcludeOnFailureTimeout()
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("3"))
    assert(healthTracker.nextExpiryTime === t2 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS)

    clock.setTime(t2 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS + 1)
    healthTracker.applyExcludeOnFailureTimeout()
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())
    verify(listenerBusMock).post(SparkListenerExecutorUnexcluded(clock.getTimeMillis(), "3"))
    // we've got one task failure still, but we don't bother setting nextExpiryTime to it, to
    // avoid wasting time checking for expiry of individual task failures.
    assert(healthTracker.nextExpiryTime === Long.MaxValue)
  }

  test("task failure timeout works as expected for long-running tasksets") {
    // This ensures that we don't trigger spurious excluding for long tasksets, when the taskset
    // finishes long after the task failures.  We create two tasksets, each with one failure.
    // Individually they shouldn't cause any excluding since there is only one failure.
    // Furthermore, we space the failures out so far that even when both tasksets have completed,
    // we still don't trigger any excluding.
    val taskSetExclude1 = createTaskSetExcludelist(stageId = 1)
    val taskSetExclude2 = createTaskSetExcludelist(stageId = 2)
    // Taskset1 has one failure immediately
    taskSetExclude1.updateExcludedForFailedTask("host-1", "1", 0, "testing")
    // Then we have a *long* delay, much longer than the timeout, before any other failures or
    // taskset completion
    clock.advance(healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS * 5)
    // After the long delay, we have one failure on taskset 2, on the same executor
    taskSetExclude2.updateExcludedForFailedTask("host-1", "1", 0, "testing")
    // Finally, we complete both tasksets.  Its important here to complete taskset2 *first*.  We
    // want to make sure that when taskset 1 finishes, even though we've now got two task failures,
    // we realize that the task failure we just added was well before the timeout.
    clock.advance(1)
    healthTracker.updateExcludedForSuccessfulTaskSet(stageId = 2, 0, taskSetExclude2.execToFailures)
    clock.advance(1)
    healthTracker.updateExcludedForSuccessfulTaskSet(stageId = 1, 0, taskSetExclude1.execToFailures)

    // Make sure nothing was excluded
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set())
  }

  test("only exclude nodes for the application when enough executors have failed on that " +
    "specific host") {
    // we exclude executors on two different hosts -- make sure that doesn't lead to any
    // node excluding
    val taskSetExclude0 = createTaskSetExcludelist(stageId = 0)
    taskSetExclude0.updateExcludedForFailedTask(
      "hostA", exec = "1", index = 0, failureReason = "testing")
    taskSetExclude0.updateExcludedForFailedTask(
      "hostA", exec = "1", index = 1, failureReason = "testing")
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude0.execToFailures)
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("1"))
    verify(listenerBusMock).post(SparkListenerExecutorExcluded(0, "1", 2))
    assertEquivalentToSet(healthTracker.isNodeExcluded(_), Set())

    val taskSetExclude1 = createTaskSetExcludelist(stageId = 1)
    taskSetExclude1.updateExcludedForFailedTask(
      "hostB", exec = "2", index = 0, failureReason = "testing")
    taskSetExclude1.updateExcludedForFailedTask(
      "hostB", exec = "2", index = 1, failureReason = "testing")
    healthTracker.updateExcludedForSuccessfulTaskSet(1, 0, taskSetExclude1.execToFailures)
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("1", "2"))
    verify(listenerBusMock).post(SparkListenerExecutorExcluded(0, "2", 2))
    assertEquivalentToSet(healthTracker.isNodeExcluded(_), Set())

    // Finally, exclude another executor on the same node as the original excluded executor,
    // and make sure this time we *do* exclude the node.
    val taskSetExclude2 = createTaskSetExcludelist(stageId = 0)
    taskSetExclude2.updateExcludedForFailedTask(
      "hostA", exec = "3", index = 0, failureReason = "testing")
    taskSetExclude2.updateExcludedForFailedTask(
      "hostA", exec = "3", index = 1, failureReason = "testing")
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude2.execToFailures)
    assertEquivalentToSet(healthTracker.isExecutorExcluded(_), Set("1", "2", "3"))
    verify(listenerBusMock).post(SparkListenerExecutorExcluded(0, "3", 2))
    assertEquivalentToSet(healthTracker.isNodeExcluded(_), Set("hostA"))
    verify(listenerBusMock).post(SparkListenerNodeExcluded(0, "hostA", 2))
  }

  test("exclude still respects legacy configs") {
    val conf = new SparkConf().setMaster("local")
    assert(!HealthTracker.isExcludeOnFailureEnabled(conf))
    conf.set(config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF, 5000L)
    assert(HealthTracker.isExcludeOnFailureEnabled(conf))
    assert(5000 === HealthTracker.getExcludeOnFailureTimeout(conf))
    // the new conf takes precedence, though
    conf.set(config.EXCLUDE_ON_FAILURE_TIMEOUT_CONF, 1000L)
    assert(1000 === HealthTracker.getExcludeOnFailureTimeout(conf))

    // if you explicitly set the legacy conf to 0, that also would disable excluding
    conf.set(config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF, 0L)
    assert(!HealthTracker.isExcludeOnFailureEnabled(conf))
    // but again, the new conf takes precedence
    conf.set(config.EXCLUDE_ON_FAILURE_ENABLED, true)
    assert(HealthTracker.isExcludeOnFailureEnabled(conf))
    assert(1000 === HealthTracker.getExcludeOnFailureTimeout(conf))
  }

  test("SPARK-49252: check exclusion enabling config on the application level") {
    val conf = new SparkConf().setMaster("local")
    assert(!HealthTracker.isExcludeOnFailureEnabled(conf))
    conf.set(config.EXCLUDE_ON_FAILURE_ENABLED, true)
    assert(HealthTracker.isExcludeOnFailureEnabled(conf))
    // Turn off taskset level exclusion, application level healthtracker should still be enabled.
    conf.set(config.EXCLUDE_ON_FAILURE_ENABLED_TASK_AND_STAGE, false)
    assert(HealthTracker.isExcludeOnFailureEnabled(conf))
    // Turn off the application level exclusion specifically, this overrides the global setting.
    conf.set(config.EXCLUDE_ON_FAILURE_ENABLED_APPLICATION, false)
    conf.set(config.EXCLUDE_ON_FAILURE_ENABLED_TASK_AND_STAGE, false)
    assert(!HealthTracker.isExcludeOnFailureEnabled(conf))
    // Turn on application level exclusion, health tracker should be enabled.
    conf.set(config.EXCLUDE_ON_FAILURE_ENABLED_APPLICATION, true)
    assert(HealthTracker.isExcludeOnFailureEnabled(conf))
  }

  test("check exclude configuration invariants") {
    val conf = new SparkConf().setMaster("yarn").set(config.SUBMIT_DEPLOY_MODE, "cluster")
    Seq(
      (2, 2),
      (2, 3)
    ).foreach { case (maxTaskFailures, maxNodeAttempts) =>
      conf.set(config.TASK_MAX_FAILURES, maxTaskFailures)
      conf.set(config.MAX_TASK_ATTEMPTS_PER_NODE.key, maxNodeAttempts.toString)
      val excMsg = intercept[IllegalArgumentException] {
        HealthTracker.validateExcludeOnFailureConfs(conf)
      }.getMessage()
      assert(excMsg === s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key} " +
        s"( = ${maxNodeAttempts}) was >= ${config.TASK_MAX_FAILURES.key} " +
        s"( = ${maxTaskFailures} ). Though excludeOnFailure is enabled, with this " +
        s"configuration, Spark will not be robust to one bad node. Decrease " +
        s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key}, increase ${config.TASK_MAX_FAILURES.key}, " +
        s"or disable excludeOnFailure with ${config.EXCLUDE_ON_FAILURE_ENABLED.key}")
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
      config.EXCLUDE_ON_FAILURE_TIMEOUT_CONF
    ).foreach { config =>
      conf.set(config.key, "0")
      val excMsg = intercept[IllegalArgumentException] {
        HealthTracker.validateExcludeOnFailureConfs(conf)
      }.getMessage()
      assert(excMsg.contains(s"${config.key} was 0, but must be > 0."))
      conf.remove(config)
    }
  }

  test("excluding kills executors, configured by EXCLUDE_ON_FAILURE_KILL_ENABLED") {
    val allocationClientMock = mock[ExecutorAllocationClient]
    when(allocationClientMock.killExecutors(any(), any(), any(), any())).thenReturn(Seq("called"))
    when(allocationClientMock.killExecutorsOnHost("hostA")).thenAnswer { (_: InvocationOnMock) =>
      // To avoid a race between excluding and killing, it is important that the nodeExclude
      // is updated before we ask the executor allocation client to kill all the executors
      // on a particular host.
      if (healthTracker.excludedNodeList().contains("hostA")) {
        true
      } else {
        throw new IllegalStateException("hostA should be on the exclude")
      }
    }
    healthTracker = new HealthTracker(listenerBusMock, conf, Some(allocationClientMock), clock)

    // Disable auto-kill. Exclude an executor and make sure killExecutors is not called.
    conf.set(config.EXCLUDE_ON_FAILURE_KILL_ENABLED, false)

    val taskSetExclude0 = createTaskSetExcludelist(stageId = 0)
    // Fail 4 tasks in one task set on executor 1, so that executor gets excluded for the whole
    // application.
    (0 until 4).foreach { partition =>
      taskSetExclude0.updateExcludedForFailedTask(
        "hostA", exec = "1", index = partition, failureReason = "testing")
    }
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude0.execToFailures)

    verify(allocationClientMock, never).killExecutor(any())

    val taskSetExclude1 = createTaskSetExcludelist(stageId = 1)
    // Fail 4 tasks in one task set on executor 2, so that executor gets excluded for the whole
    // application.  Since that's the second executor that is excluded on the same node, we also
    // exclude that node.
    (0 until 4).foreach { partition =>
      taskSetExclude1.updateExcludedForFailedTask(
        "hostA", exec = "2", index = partition, failureReason = "testing")
    }
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude1.execToFailures)

    verify(allocationClientMock, never).killExecutors(any(), any(), any(), any())
    verify(allocationClientMock, never).killExecutorsOnHost(any())

    // Enable auto-kill. Exclude an executor and make sure killExecutors is called.
    conf.set(config.EXCLUDE_ON_FAILURE_KILL_ENABLED, true)
    healthTracker = new HealthTracker(listenerBusMock, conf, Some(allocationClientMock), clock)

    val taskSetExclude2 = createTaskSetExcludelist(stageId = 0)
    // Fail 4 tasks in one task set on executor 1, so that executor gets excluded for the whole
    // application.
    (0 until 4).foreach { partition =>
      taskSetExclude2.updateExcludedForFailedTask(
        "hostA", exec = "1", index = partition, failureReason = "testing")
    }
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude2.execToFailures)

    verify(allocationClientMock).killExecutors(Seq("1"), false, false, true)

    val taskSetExclude3 = createTaskSetExcludelist(stageId = 1)
    // Fail 4 tasks in one task set on executor 2, so that executor gets excluded for the whole
    // application.  Since that's the second executor that is excluded on the same node, we also
    // exclude that node.
    (0 until 4).foreach { partition =>
      taskSetExclude3.updateExcludedForFailedTask(
        "hostA", exec = "2", index = partition, failureReason = "testing")
    }
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude3.execToFailures)

    verify(allocationClientMock).killExecutors(Seq("2"), false, false, true)
    verify(allocationClientMock).killExecutorsOnHost("hostA")
  }

  test("excluding decommission and kills executors when enabled") {
    val allocationClientMock = mock[ExecutorAllocationClient]

    // verify we decommission when configured
    conf.set(config.EXCLUDE_ON_FAILURE_KILL_ENABLED, true)
    conf.set(config.DECOMMISSION_ENABLED.key, "true")
    conf.set(config.EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED.key, "true")
    conf.set(config.MAX_FAILURES_PER_EXEC.key, "1")
    conf.set(config.MAX_FAILED_EXEC_PER_NODE.key, "2")
    healthTracker = new HealthTracker(listenerBusMock, conf, Some(allocationClientMock), clock)

    // Fail 4 tasks in one task set on executor 1, so that executor gets excluded for the whole
    // application.
    val taskSetExclude2 = createTaskSetExcludelist(stageId = 0)
    (0 until 4).foreach { partition =>
      taskSetExclude2.updateExcludedForFailedTask(
        "hostA", exec = "1", index = partition, failureReason = "testing")
    }
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude2.execToFailures)

    val msg1 =
      "Killing excluded executor id 1 since spark.excludeOnFailure.killExcludedExecutors is set." +
      " (actually decommissioning)"

    verify(allocationClientMock).decommissionExecutor(
      "1", ExecutorDecommissionInfo(msg1), false)

    val taskSetExclude3 = createTaskSetExcludelist(stageId = 1)
    // Fail 4 tasks in one task set on executor 2, so that executor gets excluded for the whole
    // application.  Since that's the second executor that is excluded on the same node, we also
    // exclude that node.
    (0 until 4).foreach { partition =>
      taskSetExclude3.updateExcludedForFailedTask(
        "hostA", exec = "2", index = partition, failureReason = "testing")
    }
    healthTracker.updateExcludedForSuccessfulTaskSet(0, 0, taskSetExclude3.execToFailures)

    val msg2 =
      "Killing excluded executor id 2 since spark.excludeOnFailure.killExcludedExecutors is set." +
      " (actually decommissioning)"
    verify(allocationClientMock).decommissionExecutor(
      "2", ExecutorDecommissionInfo(msg2), false, false)
    verify(allocationClientMock).decommissionExecutorsOnHost("hostA")
  }

  test("fetch failure excluding kills executors, configured by EXCLUDE_ON_FAILURE_KILL_ENABLED") {
    val allocationClientMock = mock[ExecutorAllocationClient]
    when(allocationClientMock.killExecutors(any(), any(), any(), any())).thenReturn(Seq("called"))
    when(allocationClientMock.killExecutorsOnHost("hostA")).thenAnswer { (_: InvocationOnMock) =>
      // To avoid a race between excluding and killing, it is important that the nodeExclude
      // is updated before we ask the executor allocation client to kill all the executors
      // on a particular host.
      if (healthTracker.excludedNodeList().contains("hostA")) {
        true
      } else {
        throw new IllegalStateException("hostA should be on the exclude")
      }
    }

    conf.set(config.EXCLUDE_ON_FAILURE_FETCH_FAILURE_ENABLED, true)
    healthTracker = new HealthTracker(listenerBusMock, conf, Some(allocationClientMock), clock)

    // Disable auto-kill. Exclude an executor and make sure killExecutors is not called.
    conf.set(config.EXCLUDE_ON_FAILURE_KILL_ENABLED, false)
    healthTracker.updateExcludedForFetchFailure("hostA", exec = "1")

    verify(allocationClientMock, never).killExecutors(any(), any(), any(), any())
    verify(allocationClientMock, never).killExecutorsOnHost(any())

    assert(healthTracker.nodeToExcludedExecs.contains("hostA"))
    assert(healthTracker.nodeToExcludedExecs("hostA").contains("1"))

    // Enable auto-kill. Exclude an executor and make sure killExecutors is called.
    conf.set(config.EXCLUDE_ON_FAILURE_KILL_ENABLED, true)
    healthTracker = new HealthTracker(listenerBusMock, conf, Some(allocationClientMock), clock)
    clock.advance(1000)
    healthTracker.updateExcludedForFetchFailure("hostA", exec = "1")

    verify(allocationClientMock).killExecutors(Seq("1"), false, false, true)
    verify(allocationClientMock, never).killExecutorsOnHost(any())

    assert(healthTracker.executorIdToExcludedStatus.contains("1"))
    assert(healthTracker.executorIdToExcludedStatus("1").node === "hostA")
    assert(healthTracker.executorIdToExcludedStatus("1").expiryTime ===
      1000 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS)
    assert(healthTracker.nextExpiryTime === 1000 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS)
    assert(healthTracker.nodeIdToExcludedExpiryTime.isEmpty)
    assert(healthTracker.nodeToExcludedExecs.contains("hostA"))
    assert(healthTracker.nodeToExcludedExecs("hostA").contains("1"))

    // Enable external shuffle service to see if all the executors on this node will be killed.
    conf.set(config.SHUFFLE_SERVICE_ENABLED, true)
    clock.advance(1000)
    healthTracker.updateExcludedForFetchFailure("hostA", exec = "2")

    verify(allocationClientMock, never).killExecutors(Seq("2"), true, true)
    verify(allocationClientMock).killExecutorsOnHost("hostA")

    assert(healthTracker.nodeIdToExcludedExpiryTime.contains("hostA"))
    assert(healthTracker.nodeIdToExcludedExpiryTime("hostA") ===
      2000 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS)
    assert(healthTracker.nextExpiryTime === 1000 + healthTracker.EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS)
  }
}
