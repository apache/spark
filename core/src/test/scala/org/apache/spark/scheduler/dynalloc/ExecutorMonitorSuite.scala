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

package org.apache.spark.scheduler.dynalloc

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.codahale.metrics.Counter
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, mock, when}

import org.apache.spark._
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceProfile.{DEFAULT_RESOURCE_PROFILE_ID, UNKNOWN_RESOURCE_PROFILE_ID}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage._
import org.apache.spark.util.ManualClock

class ExecutorMonitorSuite extends SparkFunSuite {

  private val idleTimeoutNs = TimeUnit.SECONDS.toNanos(60L)
  private val storageTimeoutNs = TimeUnit.SECONDS.toNanos(120L)
  private val shuffleTimeoutNs = TimeUnit.SECONDS.toNanos(240L)

  private val conf = new SparkConf()
    .set(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, "60s")
    .set(DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT.key, "120s")
    .set(DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT.key, "240s")
    .set(SHUFFLE_SERVICE_ENABLED, true)

  private var monitor: ExecutorMonitor = _
  private var client: ExecutorAllocationClient = _
  private var clock: ManualClock = _

  private val execInfo = new ExecutorInfo("host1", 1, Map.empty,
    Map.empty, Map.empty, DEFAULT_RESOURCE_PROFILE_ID)

  // List of known executors. Allows easily mocking which executors are alive without
  // having to use mockito APIs directly in each test.
  private val knownExecs = mutable.HashSet[String]()

  private def allocationManagerSource(): ExecutorAllocationManagerSource = {
    val metricSource = mock(classOf[ExecutorAllocationManagerSource])
    when(metricSource.driverKilled).thenReturn(new Counter)
    when(metricSource.decommissionUnfinished).thenReturn(new Counter)
    when(metricSource.gracefullyDecommissioned).thenReturn(new Counter)
    when(metricSource.exitedUnexpectedly).thenReturn(new Counter)
    metricSource
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    knownExecs.clear()
    clock = new ManualClock()
    client = mock(classOf[ExecutorAllocationClient])
    when(client.isExecutorActive(any())).thenAnswer { invocation =>
      knownExecs.contains(invocation.getArguments()(0).asInstanceOf[String])
    }
    monitor = new ExecutorMonitor(conf, client, null, clock, allocationManagerSource())
  }

  test("basic executor timeout") {
    knownExecs += "1"
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    assert(monitor.executorCount === 1)
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))
    assert(monitor.executorCountWithResourceProfile(DEFAULT_RESOURCE_PROFILE_ID) === 1)
    assert(monitor.getResourceProfileId("1") === DEFAULT_RESOURCE_PROFILE_ID)
  }

  test("SPARK-4951, SPARK-26927: handle out of order task start events") {
    knownExecs ++= Set("1", "2")

    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("1", 1)))
    assert(monitor.executorCount === 1)
    assert(monitor.executorCountWithResourceProfile(UNKNOWN_RESOURCE_PROFILE_ID) === 1)

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    assert(monitor.executorCount === 1)
    assert(monitor.executorCountWithResourceProfile(UNKNOWN_RESOURCE_PROFILE_ID) === 0)
    assert(monitor.executorCountWithResourceProfile(DEFAULT_RESOURCE_PROFILE_ID) === 1)
    assert(monitor.getResourceProfileId("1") === DEFAULT_RESOURCE_PROFILE_ID)

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "2", execInfo))
    assert(monitor.executorCount === 2)
    assert(monitor.executorCountWithResourceProfile(DEFAULT_RESOURCE_PROFILE_ID) === 2)
    assert(monitor.getResourceProfileId("2") === DEFAULT_RESOURCE_PROFILE_ID)

    monitor.onExecutorRemoved(SparkListenerExecutorRemoved(clock.getTimeMillis(), "2", null))
    assert(monitor.executorCount === 1)
    assert(monitor.executorCountWithResourceProfile(DEFAULT_RESOURCE_PROFILE_ID) === 1)

    knownExecs -= "2"

    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("2", 2)))
    assert(monitor.executorCount === 1)
    assert(monitor.executorCountWithResourceProfile(DEFAULT_RESOURCE_PROFILE_ID) === 1)

    monitor.onExecutorRemoved(SparkListenerExecutorRemoved(clock.getTimeMillis(), "1", null))
    assert(monitor.executorCount === 0)
    assert(monitor.executorCountWithResourceProfile(DEFAULT_RESOURCE_PROFILE_ID) === 0)
  }

  test("track tasks running on executor") {
    knownExecs += "1"

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("1", 1)))
    assert(!monitor.isExecutorIdle("1"))

    // Start/end a few tasks and make sure the executor does not go idle.
    (2 to 10).foreach { i =>
      monitor.onTaskStart(SparkListenerTaskStart(i, 1, taskInfo("1", 1)))
      assert(!monitor.isExecutorIdle("1"))

      monitor.onTaskEnd(SparkListenerTaskEnd(i, 1, "foo", Success, taskInfo("1", 1),
        new ExecutorMetrics, null))
      assert(!monitor.isExecutorIdle("1"))
    }

    monitor.onTaskEnd(SparkListenerTaskEnd(1, 1, "foo", Success, taskInfo("1", 1),
      new ExecutorMetrics, null))
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(clock.nanoTime()).isEmpty)
    assert(monitor.timedOutExecutors(clock.nanoTime() + idleTimeoutNs + 1) === Seq("1"))
  }

  test("use appropriate time out depending on whether blocks are stored") {
    knownExecs += "1"
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 0, "1"))
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.NONE))
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))

    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("1", 1)))
    assert(!monitor.isExecutorIdle("1"))
    monitor.onBlockUpdated(rddUpdate(1, 0, "1"))
    assert(!monitor.isExecutorIdle("1"))
    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.NONE))
    assert(!monitor.isExecutorIdle("1"))
  }

  test("keeps track of stored blocks for each rdd and split") {
    knownExecs ++= Set("1", "2")

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))

    monitor.onBlockUpdated(rddUpdate(1, 0, "1"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 1, "1"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(2, 0, "1"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.NONE))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onUnpersistRDD(SparkListenerUnpersistRDD(1))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    // Make sure that if we get an unpersist event much later, which moves an executor from having
    // cached blocks to no longer having cached blocks, it will time out based on the time it
    // originally went idle.
    clock.setTime(idleDeadline)
    monitor.onUnpersistRDD(SparkListenerUnpersistRDD(2))
    assert(monitor.timedOutExecutors(clock.nanoTime()) === Seq("1"))
  }

  test("handle timeouts correctly with multiple executors") {
    knownExecs ++= Set("1", "2", "3")

    // start exec 1 at 0s (should idle time out at 60s)
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    assert(monitor.isExecutorIdle("1"))

    // start exec 2 at 30s, store a block (should idle time out at 150s)
    clock.setTime(TimeUnit.SECONDS.toMillis(30))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "2", execInfo))
    monitor.onBlockUpdated(rddUpdate(1, 0, "2"))
    assert(monitor.isExecutorIdle("2"))
    assert(!monitor.timedOutExecutors(idleDeadline).contains("2"))

    // start exec 3 at 60s (should idle timeout at 120s, exec 1 should time out)
    clock.setTime(TimeUnit.SECONDS.toMillis(60))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "3", execInfo))
    assert(monitor.timedOutExecutors(clock.nanoTime()) === Seq("1"))

    // store block on exec 3 (should now idle time out at 180s)
    monitor.onBlockUpdated(rddUpdate(1, 0, "3"))
    assert(monitor.isExecutorIdle("3"))
    assert(!monitor.timedOutExecutors(idleDeadline).contains("3"))

    // advance to 140s, remove block from exec 3 (time out immediately)
    clock.setTime(TimeUnit.SECONDS.toMillis(140))
    monitor.onBlockUpdated(rddUpdate(1, 0, "3", level = StorageLevel.NONE))
    assert(monitor.timedOutExecutors(clock.nanoTime()).toSet === Set("1", "3"))

    // advance to 150s, now exec 2 should time out
    clock.setTime(TimeUnit.SECONDS.toMillis(150))
    assert(monitor.timedOutExecutors(clock.nanoTime()).toSet === Set("1", "2", "3"))
  }

  test("SPARK-38019: timedOutExecutors should be deterministic") {
    knownExecs ++= Set("1", "2", "3")

    // start exec 1, 2, 3 at 0s (should idle time out at 60s)
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    assert(monitor.isExecutorIdle("1"))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "2", execInfo))
    assert(monitor.isExecutorIdle("2"))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "3", execInfo))
    assert(monitor.isExecutorIdle("3"))

    clock.setTime(TimeUnit.SECONDS.toMillis(150))
    assert(monitor.timedOutExecutors().map(_._1) === Seq("1", "2", "3"))
  }

  test("SPARK-27677: don't track blocks stored on disk when using shuffle service") {
    knownExecs += "1"
    // First make sure that blocks on disk are counted when no shuffle service is available.
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.DISK_ONLY))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) ===  Seq("1"))

    conf.set(SHUFFLE_SERVICE_ENABLED, true).set(SHUFFLE_SERVICE_FETCH_RDD_ENABLED, true)
    monitor = new ExecutorMonitor(conf, client, null, clock, allocationManagerSource())

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.MEMORY_ONLY))
    monitor.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.MEMORY_ONLY))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) ===  Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.DISK_ONLY))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) ===  Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.DISK_ONLY))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))

    // Tag the block as being both in memory and on disk, which may happen after it was
    // evicted and then restored into memory. Since it's still on disk the executor should
    // still be eligible for removal.
    monitor.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.MEMORY_AND_DISK))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))
  }

  test("track executors pending for removal") {
    knownExecs ++= Set("1", "2", "3")

    val execInfoRp1 = new ExecutorInfo("host1", 1, Map.empty,
      Map.empty, Map.empty, 1, None, None)

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "2", execInfo))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "3", execInfoRp1))
    clock.setTime(idleDeadline)
    assert(monitor.timedOutExecutors().toSet === Set(("1", 0), ("2", 0), ("3", 1)))
    assert(monitor.pendingRemovalCount === 0)

    // Notify that only a subset of executors was killed, to mimic the case where the scheduler
    // refuses to kill an executor that is busy for whatever reason the monitor hasn't detected yet.
    monitor.executorsKilled(Seq("1"))
    assert(monitor.timedOutExecutors().toSet === Set(("2", 0), ("3", 1)))
    assert(monitor.pendingRemovalCount === 1)

    // Check the timed out executors again so that we're sure they're still timed out when no
    // events happen. This ensures that the monitor doesn't lose track of them.
    assert(monitor.timedOutExecutors().toSet === Set(("2", 0), ("3", 1)))

    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("2", 1)))
    assert(monitor.timedOutExecutors().toSet === Set(("3", 1)))

    monitor.executorsKilled(Seq("3"))
    assert(monitor.pendingRemovalCount === 2)

    monitor.onTaskEnd(SparkListenerTaskEnd(1, 1, "foo", Success, taskInfo("2", 1),
      new ExecutorMetrics, null))
    assert(monitor.timedOutExecutors().isEmpty)
    clock.advance(idleDeadline)
    assert(monitor.timedOutExecutors().toSet === Set(("2", 0)))
  }

  test("shuffle block tracking") {
    val bus = mockListenerBus()
    conf.set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true).set(SHUFFLE_SERVICE_ENABLED, false)
    monitor = new ExecutorMonitor(conf, client, bus, clock, allocationManagerSource())

    // 3 jobs: 2 and 3 share a shuffle, 1 has a separate shuffle.
    val stage1 = stageInfo(1, shuffleId = 0)
    val stage2 = stageInfo(2)

    val stage3 = stageInfo(3, shuffleId = 1)
    val stage4 = stageInfo(4)

    val stage5 = stageInfo(5, shuffleId = 1)
    val stage6 = stageInfo(6)

    // Start jobs 1 and 2. Finish a task on each, but don't finish the jobs. This should prevent the
    // executor from going idle since there are active shuffles.
    monitor.onJobStart(SparkListenerJobStart(1, clock.getTimeMillis(), Seq(stage1, stage2)))
    monitor.onJobStart(SparkListenerJobStart(2, clock.getTimeMillis(), Seq(stage3, stage4)))

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))

    // First a failed task, to make sure it does not count.
    monitor.onTaskStart(SparkListenerTaskStart(1, 0, taskInfo("1", 1)))
    monitor.onTaskEnd(SparkListenerTaskEnd(1, 0, "foo", TaskResultLost, taskInfo("1", 1),
      new ExecutorMetrics, null))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))

    monitor.onTaskStart(SparkListenerTaskStart(1, 0, taskInfo("1", 1)))
    monitor.onTaskEnd(SparkListenerTaskEnd(1, 0, "foo", Success, taskInfo("1", 1),
      new ExecutorMetrics, null))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)

    monitor.onTaskStart(SparkListenerTaskStart(3, 0, taskInfo("1", 1)))
    monitor.onTaskEnd(SparkListenerTaskEnd(3, 0, "foo", Success, taskInfo("1", 1),
      new ExecutorMetrics, null))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)

    // Finish the jobs, now the executor should be idle, but with the shuffle timeout, since the
    // shuffles are not active.
    monitor.onJobEnd(SparkListenerJobEnd(1, clock.getTimeMillis(), JobSucceeded))
    assert(!monitor.isExecutorIdle("1"))

    monitor.onJobEnd(SparkListenerJobEnd(2, clock.getTimeMillis(), JobSucceeded))
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline).isEmpty)
    assert(monitor.timedOutExecutors(shuffleDeadline) === Seq("1"))

    // Start job 3. Since it shares a shuffle with job 2, the executor should not be considered
    // idle anymore, even if no tasks are run.
    monitor.onJobStart(SparkListenerJobStart(3, clock.getTimeMillis(), Seq(stage5, stage6)))
    assert(!monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(shuffleDeadline).isEmpty)

    monitor.onJobEnd(SparkListenerJobEnd(3, clock.getTimeMillis(), JobSucceeded))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(shuffleDeadline) === Seq("1"))

    // Clean up the shuffles, executor now should now time out at the idle deadline.
    monitor.shuffleCleaned(0)
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    monitor.shuffleCleaned(1)
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))
  }


  test("SPARK-28839: Avoids NPE in context cleaner when shuffle service is on") {
    val bus = mockListenerBus()
    conf.set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true).set(SHUFFLE_SERVICE_ENABLED, true)
    monitor = new ExecutorMonitor(conf, client, bus, clock, allocationManagerSource()) {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        throw new IllegalStateException("No event should be sent.")
      }
    }
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    monitor.shuffleCleaned(0)
  }

  test("shuffle tracking with multiple executors and concurrent jobs") {
    val bus = mockListenerBus()
    conf.set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true).set(SHUFFLE_SERVICE_ENABLED, false)
    monitor = new ExecutorMonitor(conf, client, bus, clock, allocationManagerSource())

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "2", execInfo))

    // Two separate jobs with separate shuffles. The first job will only run tasks on
    // executor 1, the second on executor 2. Ensures that jobs finishing don't affect
    // executors that are active in other jobs.

    val stage1 = stageInfo(1, shuffleId = 0)
    val stage2 = stageInfo(2)
    monitor.onJobStart(SparkListenerJobStart(1, clock.getTimeMillis(), Seq(stage1, stage2)))

    val stage3 = stageInfo(3, shuffleId = 1)
    val stage4 = stageInfo(4)
    monitor.onJobStart(SparkListenerJobStart(2, clock.getTimeMillis(), Seq(stage3, stage4)))

    monitor.onTaskStart(SparkListenerTaskStart(1, 0, taskInfo("1", 1)))
    monitor.onTaskEnd(SparkListenerTaskEnd(1, 0, "foo", Success, taskInfo("1", 1),
     new ExecutorMetrics, null))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("2"))

    monitor.onTaskStart(SparkListenerTaskStart(3, 0, taskInfo("2", 1)))
    monitor.onTaskEnd(SparkListenerTaskEnd(3, 0, "foo", Success, taskInfo("2", 1),
      new ExecutorMetrics, null))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)

    monitor.onJobEnd(SparkListenerJobEnd(1, clock.getTimeMillis(), JobSucceeded))
    assert(monitor.isExecutorIdle("1"))
    assert(!monitor.isExecutorIdle("2"))

    monitor.onJobEnd(SparkListenerJobEnd(2, clock.getTimeMillis(), JobSucceeded))
    assert(monitor.isExecutorIdle("2"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)

    monitor.shuffleCleaned(0)
    monitor.shuffleCleaned(1)
    assert(monitor.timedOutExecutors(idleDeadline).toSet === Set("1", "2"))
  }

  test("SPARK-28455: avoid overflow in timeout calculation") {
    conf
      .set(DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT, Long.MaxValue)
      .set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true)
      .set(SHUFFLE_SERVICE_ENABLED, false)
    monitor = new ExecutorMonitor(conf, client, null, clock, allocationManagerSource())

    // Generate events that will make executor 1 be idle, while still holding shuffle data.
    // The executor should not be eligible for removal since the timeout is basically "infinite".
    val stage = stageInfo(1, shuffleId = 0)
    monitor.onJobStart(SparkListenerJobStart(1, clock.getTimeMillis(), Seq(stage)))
    clock.advance(1000L)
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    monitor.onTaskStart(SparkListenerTaskStart(1, 0, taskInfo("1", 1)))
    monitor.onTaskEnd(SparkListenerTaskEnd(1, 0, "foo", Success, taskInfo("1", 1),
      new ExecutorMetrics, null))
    monitor.onJobEnd(SparkListenerJobEnd(1, clock.getTimeMillis(), JobSucceeded))

    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
  }

  test("SPARK-37688: ignore SparkListenerBlockUpdated event if executor was not active") {
    conf
      .set(DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT, Long.MaxValue)
      .set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true)
      .set(SHUFFLE_SERVICE_ENABLED, false)
    monitor = new ExecutorMonitor(conf, client, null, clock, allocationManagerSource())

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
    monitor.onExecutorRemoved(SparkListenerExecutorRemoved(clock.getTimeMillis(), "1",
      "heartbeats timeout"))
    monitor.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.MEMORY_AND_DISK))

    assert(monitor.executorCount == 0 )
  }

  for (isShuffleTrackingEnabled <- Seq(true, false)) {
    test(s"SPARK-43398: executor timeout should be max of shuffle and rdd timeout with" +
      s" shuffleTrackingEnabled as $isShuffleTrackingEnabled") {
      conf
        .set(DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT.key, "240s")
        .set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, isShuffleTrackingEnabled)
        .set(SHUFFLE_SERVICE_ENABLED, false)
      monitor = new ExecutorMonitor(conf, client, null, clock, allocationManagerSource())

      monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", execInfo))
      knownExecs += "1"
      val stage1 = stageInfo(1, shuffleId = 0)
      monitor.onJobStart(SparkListenerJobStart(1, clock.getTimeMillis(), Seq(stage1)))
      monitor.onBlockUpdated(rddUpdate(1, 0, "1"))
      val t1 = taskInfo("1", 1)
      monitor.onTaskStart(SparkListenerTaskStart(1, 1, t1))
      monitor.onTaskEnd(SparkListenerTaskEnd(1, 1, "foo", Success, t1, new ExecutorMetrics, null))
      monitor.onJobEnd(SparkListenerJobEnd(1, clock.getTimeMillis(), JobSucceeded))

      if (isShuffleTrackingEnabled) {
        assert(monitor.timedOutExecutors(storageDeadline).isEmpty)
        assert(monitor.timedOutExecutors(shuffleDeadline) == Seq("1"))
      } else {
        assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
        assert(monitor.timedOutExecutors(storageDeadline) == Seq("1"))
      }
    }
  }

  private def idleDeadline: Long = clock.nanoTime() + idleTimeoutNs + 1
  private def storageDeadline: Long = clock.nanoTime() + storageTimeoutNs + 1
  private def shuffleDeadline: Long = clock.nanoTime() + shuffleTimeoutNs + 1

  private def stageInfo(id: Int, shuffleId: Int = -1): StageInfo = {
    new StageInfo(id, 0, s"stage$id", 1, Nil, Nil, "",
      shuffleDepId = if (shuffleId >= 0) Some(shuffleId) else None,
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
  }

  private def taskInfo(
      execId: String,
      id: Int,
      speculative: Boolean = false,
      duration: Long = -1L): TaskInfo = {
    val start = if (duration > 0) clock.getTimeMillis() - duration else clock.getTimeMillis()
    val task = new TaskInfo(
      id, id, 1, id, start, execId, "foo.example.com",
      TaskLocality.PROCESS_LOCAL, speculative)
    if (duration > 0) {
      task.markFinished(TaskState.FINISHED, math.max(1, clock.getTimeMillis()))
    }
    task
  }

  private def rddUpdate(
      rddId: Int,
      splitIndex: Int,
      execId: String,
      level: StorageLevel = StorageLevel.MEMORY_ONLY): SparkListenerBlockUpdated = {
    SparkListenerBlockUpdated(
      BlockUpdatedInfo(BlockManagerId(execId, "1.example.com", 42),
        RDDBlockId(rddId, splitIndex), level, 1L, 0L))
  }

  /**
   * Mock the listener bus *only* for the functionality needed by the shuffle tracking code.
   * Any other event sent through the mock bus will fail.
   */
  private def mockListenerBus(): LiveListenerBus = {
    val bus = mock(classOf[LiveListenerBus])
    doAnswer { invocation =>
      monitor.onOtherEvent(invocation.getArguments()(0).asInstanceOf[SparkListenerEvent])
    }.when(bus).post(any())
    bus
  }

}
