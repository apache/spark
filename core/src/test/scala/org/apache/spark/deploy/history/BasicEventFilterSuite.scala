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

package org.apache.spark.deploy.history

import org.apache.spark.{storage, SparkContext, SparkFunSuite, Success, TaskState}
import org.apache.spark.deploy.history.EventFilter.FilterStatistics
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler._
import org.apache.spark.status.ListenerEventsTestHelper._
import org.apache.spark.storage.{BlockManagerId, RDDBlockId, StorageLevel}

class BasicEventFilterSuite extends SparkFunSuite {
  import BasicEventFilterSuite._

  test("filter out events for finished jobs") {
    // assume finished job 1 with stage 1, tasks (1, 2), rdds (1, 2)
    // live job 2 with stage 2 with tasks (3, 4) & rdds (3, 4),
    // and stage 3 with tasks (5, 6) & rdds (5, 6)
    val liveJobs = Set(2)
    val liveStages = Set(2, 3)
    val liveTasks = Set(3L, 4L, 5L, 6L)
    val liveRDDs = Set(3, 4, 5, 6)
    val liveExecutors: Set[String] = Set("1", "2")
    val filterStats = FilterStatistics(
      // counts finished job 1
      liveJobs.size + 1,
      liveJobs.size,
      // counts finished stage 1 for job 1
      liveStages.size + 1,
      liveStages.size,
      // counts finished tasks (1, 2) for job 1
      liveTasks.size + 2,
      liveTasks.size)

    val filter = new BasicEventFilter(filterStats, liveJobs, liveStages, liveTasks, liveRDDs,
      liveExecutors)
    val acceptFn = filter.acceptFn().lift

    // Verifying with finished job 1
    val rddsForStage1 = createRddsWithId(1 to 2)
    val stage1 = createStage(1, rddsForStage1, Nil)
    val tasksForStage1 = createTasks(Seq(1L, 2L), liveExecutors.toArray, 0)
    tasksForStage1.foreach { task => task.markFinished(TaskState.FINISHED, 5) }

    val jobStartEventForJob1 = SparkListenerJobStart(1, 0, Seq(stage1))
    val jobEndEventForJob1 = SparkListenerJobEnd(1, 0, JobSucceeded)
    val stageSubmittedEventsForJob1 = SparkListenerStageSubmitted(stage1)
    val stageCompletedEventsForJob1 = SparkListenerStageCompleted(stage1)
    val unpersistRDDEventsForJob1 = (1 to 2).map(SparkListenerUnpersistRDD)

    // job events for finished job should be rejected
    assert(Some(false) === acceptFn(jobStartEventForJob1))
    assert(Some(false) === acceptFn(jobEndEventForJob1))

    // stage events for finished job should be rejected
    // NOTE: it doesn't filter out stage events which are also related to the executor
    assertFilterStageEvents(
      acceptFn,
      stageSubmittedEventsForJob1,
      stageCompletedEventsForJob1,
      unpersistRDDEventsForJob1,
      SparkListenerSpeculativeTaskSubmitted(stage1.stageId, stageAttemptId = 1),
      Some(false))

    // task events for finished job should be rejected
    assertFilterTaskEvents(acceptFn, tasksForStage1, stage1, Some(false))

    // Verifying with live job 2
    val rddsForStage2 = createRddsWithId(3 to 4)
    val stage2 = createStage(2, rddsForStage2, Nil)
    val tasksForStage2 = createTasks(Seq(3L, 4L), liveExecutors.toArray, 0)
    tasksForStage1.foreach { task => task.markFinished(TaskState.FINISHED, 5) }

    val jobStartEventForJob2 = SparkListenerJobStart(2, 0, Seq(stage2))
    val stageSubmittedEventsForJob2 = SparkListenerStageSubmitted(stage2)
    val stageCompletedEventsForJob2 = SparkListenerStageCompleted(stage2)
    val unpersistRDDEventsForJob2 = rddsForStage2.map { rdd => SparkListenerUnpersistRDD(rdd.id) }

    // job events for live job should be accepted
    assert(Some(true) === acceptFn(jobStartEventForJob2))

    // stage events for live job should be accepted
    assertFilterStageEvents(
      acceptFn,
      stageSubmittedEventsForJob2,
      stageCompletedEventsForJob2,
      unpersistRDDEventsForJob2,
      SparkListenerSpeculativeTaskSubmitted(stage2.stageId, stageAttemptId = 1),
      Some(true))

    // task events for live job should be accepted
    assertFilterTaskEvents(acceptFn, tasksForStage2, stage2, Some(true))
  }

  test("accept all events for block manager addition/removal on driver") {
    val filter = new BasicEventFilter(EMPTY_STATS, Set.empty, Set.empty, Set.empty, Set.empty,
      Set.empty)
    val acceptFn = filter.acceptFn().lift

    val bmId = BlockManagerId(SparkContext.DRIVER_IDENTIFIER, "host1", 1)
    assert(Some(true) === acceptFn(SparkListenerBlockManagerAdded(0, bmId, 1)))
    assert(Some(true) === acceptFn(SparkListenerBlockManagerRemoved(1, bmId)))
    assert(Some(true) === acceptFn(SparkListenerBlockUpdated(
      storage.BlockUpdatedInfo(bmId, RDDBlockId(1, 1), StorageLevel.DISK_ONLY, 0, 10))))
  }

  test("filter out events for dead executors") {
    // assume executor 1 was dead, and live executor 2 is available
    val liveExecutors: Set[String] = Set("2")

    val filter = new BasicEventFilter(EMPTY_STATS, Set.empty, Set.empty, Set.empty, Set.empty,
      liveExecutors)
    val acceptFn = filter.acceptFn().lift

    // events for dead executor should be rejected
    assert(Some(false) === acceptFn(createExecutorAddedEvent(1)))
    // though the name of event is stage executor metrics, AppStatusListener only deals with
    // live executors
    assert(Some(false) === acceptFn(
      SparkListenerStageExecutorMetrics(1.toString, 0, 0, new ExecutorMetrics)))
    assert(Some(false) === acceptFn(SparkListenerExecutorBlacklisted(0, 1.toString, 1)))
    assert(Some(false) === acceptFn(SparkListenerExecutorUnblacklisted(0, 1.toString)))
    assert(Some(false) === acceptFn(SparkListenerExecutorExcluded(0, 1.toString, 1)))
    assert(Some(false) === acceptFn(SparkListenerExecutorUnexcluded(0, 1.toString)))
    assert(Some(false) === acceptFn(createExecutorRemovedEvent(1)))
    val bmId = BlockManagerId(1.toString, "host1", 1)
    assert(Some(false) === acceptFn(SparkListenerBlockManagerAdded(0, bmId, 1)))
    assert(Some(false) === acceptFn(SparkListenerBlockManagerRemoved(1, bmId)))
    assert(Some(false) === acceptFn(SparkListenerBlockUpdated(
      storage.BlockUpdatedInfo(bmId, RDDBlockId(1, 1), StorageLevel.DISK_ONLY, 0, 10))))

    // events for live executor should be accepted
    assert(Some(true) === acceptFn(createExecutorAddedEvent(2)))
    assert(Some(true) === acceptFn(
      SparkListenerStageExecutorMetrics(2.toString, 0, 0, new ExecutorMetrics)))
    assert(Some(true) === acceptFn(SparkListenerExecutorBlacklisted(0, 2.toString, 1)))
    assert(Some(true) === acceptFn(SparkListenerExecutorUnblacklisted(0, 2.toString)))
    assert(None === acceptFn(SparkListenerNodeBlacklisted(0, "host1", 1)))
    assert(None === acceptFn(SparkListenerNodeUnblacklisted(0, "host1")))
    assert(Some(true) === acceptFn(SparkListenerExecutorExcluded(0, 2.toString, 1)))
    assert(Some(true) === acceptFn(SparkListenerExecutorUnexcluded(0, 2.toString)))
    assert(Some(true) === acceptFn(createExecutorRemovedEvent(2)))
    val bmId2 = BlockManagerId(2.toString, "host1", 1)
    assert(Some(true) === acceptFn(SparkListenerBlockManagerAdded(0, bmId2, 1)))
    assert(Some(true) === acceptFn(SparkListenerBlockManagerRemoved(1, bmId2)))
    assert(Some(true) === acceptFn(SparkListenerBlockUpdated(
      storage.BlockUpdatedInfo(bmId2, RDDBlockId(1, 1), StorageLevel.DISK_ONLY, 0, 10))))
  }

  test("other events should be left to other filters") {
    val filter = new BasicEventFilter(EMPTY_STATS, Set.empty, Set.empty, Set.empty, Set.empty,
      Set.empty)
    val acceptFn = filter.acceptFn().lift

    assert(None === acceptFn(SparkListenerEnvironmentUpdate(Map.empty)))
    assert(None === acceptFn(SparkListenerApplicationStart("1", Some("1"), 0, "user", None)))
    assert(None === acceptFn(SparkListenerApplicationEnd(1)))
    assert(None === acceptFn(SparkListenerNodeExcluded(0, "host1", 1)))
    assert(None === acceptFn(SparkListenerNodeUnexcluded(0, "host1")))
    assert(None === acceptFn(SparkListenerLogStart("testVersion")))
  }

  private def assertFilterStageEvents(
      acceptFn: SparkListenerEvent => Option[Boolean],
      stageSubmitted: SparkListenerStageSubmitted,
      stageCompleted: SparkListenerStageCompleted,
      unpersistRDDs: Seq[SparkListenerUnpersistRDD],
      taskSpeculativeSubmitted: SparkListenerSpeculativeTaskSubmitted,
      expectedVal: Option[Boolean]): Unit = {
    assert(acceptFn(stageSubmitted) === expectedVal)
    assert(acceptFn(stageCompleted) === expectedVal)
    unpersistRDDs.foreach { event =>
      assert(acceptFn(event) === expectedVal)
    }
    assert(acceptFn(taskSpeculativeSubmitted) === expectedVal)
  }

  private def assertFilterTaskEvents(
      acceptFn: SparkListenerEvent => Option[Boolean],
      taskInfos: Seq[TaskInfo],
      stageInfo: StageInfo,
      expectedVal: Option[Boolean]): Unit = {
    taskInfos.foreach { task =>
      val taskStartEvent = SparkListenerTaskStart(stageInfo.stageId, 0, task)
      assert(acceptFn(taskStartEvent) === expectedVal)

      val taskGettingResultEvent = SparkListenerTaskGettingResult(task)
      assert(acceptFn(taskGettingResultEvent) === expectedVal)

      val taskEndEvent = SparkListenerTaskEnd(stageInfo.stageId, 0, "taskType",
        Success, task, new ExecutorMetrics, null)
      assert(acceptFn(taskEndEvent) === expectedVal)
    }
  }
}

object BasicEventFilterSuite {
  val EMPTY_STATS = FilterStatistics(0, 0, 0, 0, 0, 0)
}
