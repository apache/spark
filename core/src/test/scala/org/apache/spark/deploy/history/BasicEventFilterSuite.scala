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

import org.apache.spark.{storage, SparkFunSuite, Success, TaskState}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler._
import org.apache.spark.status.ListenerEventsTestHelper
import org.apache.spark.storage.{BlockManagerId, RDDBlockId, StorageLevel}

class BasicEventFilterSuite extends SparkFunSuite {
  import ListenerEventsTestHelper._

  test("filter out events for finished jobs") {
    // assume finished job 1 with stage 1, tasks (1, 2), rdds (1, 2)
    // live job 2 with stages 2, tasks (3, 4), rdds (3, 4)
    val liveJobToStages: Map[Int, Seq[Int]] = Map(2 -> Seq(2, 3))
    val stageToTasks: Map[Int, Set[Long]] = Map(2 -> Set(3, 4), 3 -> Set(5, 6))
    val stageToRDDs: Map[Int, Seq[Int]] = Map(2 -> Seq(3, 4), 3 -> Seq(5, 6))
    val liveExecutors: Set[String] = Set("1", "2")

    val filter = new BasicEventFilter(liveJobToStages, stageToTasks, stageToRDDs, liveExecutors)

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

    // job events for finished job should be filtered out
    assertFilterJobEvents(filter, jobStartEventForJob1, jobEndEventForJob1, Some(false))

    // stage events for finished job should be filtered out
    // NOTE: it doesn't filter out stage events which are also related to the executor
    assertFilterStageEvents(
      filter,
      stageSubmittedEventsForJob1,
      stageCompletedEventsForJob1,
      unpersistRDDEventsForJob1,
      SparkListenerSpeculativeTaskSubmitted(stage1.stageId, stageAttemptId = 1),
      Some(false))

    // task events for finished job should be filtered out
    assertFilterTaskEvents(filter, tasksForStage1, stage1, Some(false))

    // Verifying with live job 2
    val rddsForStage2 = createRddsWithId(3 to 4)
    val stage2 = createStage(2, rddsForStage2, Nil)
    val tasksForStage2 = createTasks(Seq(3L, 4L), liveExecutors.toArray, 0)
    tasksForStage1.foreach { task => task.markFinished(TaskState.FINISHED, 5) }

    val jobStartEventForJob2 = SparkListenerJobStart(2, 0, Seq(stage2))
    val stageSubmittedEventsForJob2 = SparkListenerStageSubmitted(stage2)
    val stageCompletedEventsForJob2 = SparkListenerStageCompleted(stage2)
    val unpersistRDDEventsForJob2 = rddsForStage2.map { rdd => SparkListenerUnpersistRDD(rdd.id) }

    // job events for live job should be filtered in
    assert(filter.filterJobStart(jobStartEventForJob2) === Some(true))

    // stage events for live job should be filtered in
    assertFilterStageEvents(
      filter,
      stageSubmittedEventsForJob2,
      stageCompletedEventsForJob2,
      unpersistRDDEventsForJob2,
      SparkListenerSpeculativeTaskSubmitted(stage2.stageId, stageAttemptId = 1),
      Some(true))

    // task events for live job should be filtered in
    assertFilterTaskEvents(filter, tasksForStage2, stage2, Some(true))
  }

  test("filter out events for dead executors") {
    // assume executor 1 was dead, and live executor 2 is available
    val liveExecutors: Set[String] = Set("2")

    val filter = new BasicEventFilter(Map.empty, Map.empty, Map.empty, liveExecutors)

    // events for dead executor should be filtered out
    assert(filter.filterExecutorAdded(createExecutorAddedEvent(1)) === Some(false))
    // though the name of event is stage executor metrics, AppStatusListener only deals with
    // live executors
    assert(filter.filterStageExecutorMetrics(
      SparkListenerStageExecutorMetrics(1.toString, 0, 0, new ExecutorMetrics)) ===
      Some(false))
    assert(filter.filterExecutorBlacklisted(SparkListenerExecutorBlacklisted(0, 1.toString, 1))
      === Some(false))
    assert(filter.filterExecutorUnblacklisted(SparkListenerExecutorUnblacklisted(0, 1.toString))
      === Some(false))
    assert(filter.filterExecutorRemoved(createExecutorRemovedEvent(1)) === Some(false))

    // events for live executor should be filtered in
    assert(filter.filterExecutorAdded(createExecutorAddedEvent(2)) === Some(true))
    assert(filter.filterStageExecutorMetrics(
      SparkListenerStageExecutorMetrics(2.toString, 0, 0, new ExecutorMetrics)) ===
      Some(true))
    assert(filter.filterExecutorBlacklisted(SparkListenerExecutorBlacklisted(0, 2.toString, 1))
      === Some(true))
    assert(filter.filterExecutorUnblacklisted(SparkListenerExecutorUnblacklisted(0, 2.toString))
      === Some(true))
    assert(filter.filterExecutorRemoved(createExecutorRemovedEvent(2)) === Some(true))
  }

  test("other events should be left to other filters") {
    def assertNone(predicate: => Option[Boolean]): Unit = {
      assert(predicate === None)
    }

    val filter = new BasicEventFilter(Map.empty, Map.empty, Map.empty, Set.empty)

    assertNone(filter.filterEnvironmentUpdate(SparkListenerEnvironmentUpdate(Map.empty)))
    assertNone(filter.filterApplicationStart(
      SparkListenerApplicationStart("1", Some("1"), 0, "user", None)))
    assertNone(filter.filterApplicationEnd(SparkListenerApplicationEnd(1)))
    val bmId = BlockManagerId("1", "host1", 1)
    assertNone(filter.filterBlockManagerAdded(SparkListenerBlockManagerAdded(0, bmId, 1)))
    assertNone(filter.filterBlockManagerRemoved(SparkListenerBlockManagerRemoved(1, bmId)))
    assertNone(filter.filterBlockUpdated(SparkListenerBlockUpdated(
      storage.BlockUpdatedInfo(bmId, RDDBlockId(1, 1), StorageLevel.DISK_ONLY, 0, 10))))
    assertNone(filter.filterNodeBlacklisted(SparkListenerNodeBlacklisted(0, "host1", 1)))
    assertNone(filter.filterNodeUnblacklisted(SparkListenerNodeUnblacklisted(0, "host1")))
    assertNone(filter.filterOtherEvent(SparkListenerLogStart("testVersion")))
  }

  private def assertFilterJobEvents(
      filter: BasicEventFilter,
      jobStart: SparkListenerJobStart,
      jobEnd: SparkListenerJobEnd,
      expectedVal: Option[Boolean]): Unit = {
    assert(filter.filterJobStart(jobStart) === expectedVal)
    assert(filter.filterJobEnd(jobEnd) === expectedVal)
  }

  private def assertFilterStageEvents(
      filter: BasicEventFilter,
      stageSubmitted: SparkListenerStageSubmitted,
      stageCompleted: SparkListenerStageCompleted,
      unpersistRDDs: Seq[SparkListenerUnpersistRDD],
      taskSpeculativeSubmitted: SparkListenerSpeculativeTaskSubmitted,
      expectedVal: Option[Boolean]): Unit = {
    assert(filter.filterStageSubmitted(stageSubmitted) === expectedVal)
    assert(filter.filterStageCompleted(stageCompleted) === expectedVal)
    unpersistRDDs.foreach { event =>
      assert(filter.filterUnpersistRDD(event) === expectedVal)
    }
    assert(filter.filterSpeculativeTaskSubmitted(taskSpeculativeSubmitted) === expectedVal)
  }

  private def assertFilterTaskEvents(
      filter: BasicEventFilter,
      taskInfos: Seq[TaskInfo],
      stageInfo: StageInfo,
      expectedVal: Option[Boolean]): Unit = {
    taskInfos.foreach { task =>
      val taskStartEvent = SparkListenerTaskStart(stageInfo.stageId, 0, task)
      assert(filter.filterTaskStart(taskStartEvent) === expectedVal)

      val taskGettingResultEvent = SparkListenerTaskGettingResult(task)
      assert(filter.filterTaskGettingResult(taskGettingResultEvent) === expectedVal)

      val taskEndEvent = SparkListenerTaskEnd(stageInfo.stageId, 0, "taskType",
        Success, task, new ExecutorMetrics, null)
      assert(filter.filterTaskEnd(taskEndEvent) === expectedVal)
    }
  }
}
