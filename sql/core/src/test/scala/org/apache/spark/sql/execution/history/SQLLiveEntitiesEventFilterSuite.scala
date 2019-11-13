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

package org.apache.spark.sql.execution.history

import org.apache.spark.{SparkFunSuite, Success, TaskState}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.status.ListenerEventsTestHelper.{createRddsWithId, createStage, createTasks}

class SQLLiveEntitiesEventFilterSuite extends SparkFunSuite {
  test("filter in events for jobs related to live SQL execution") {
    // assume finished job 1 with stage 1, task (1, 2), rdds (1, 2) and finished sql execution id 1
    // live job 2 with stages 2, tasks (3, 4), rdds (3, 4) and job 2 belongs to the live
    // sql execution id 2

    val liveExecutionToJobs: Map[Long, Set[Int]] = Map(2L -> Set(2))
    val liveJobToStages: Map[Int, Seq[Int]] = Map(2 -> Seq(2, 3))
    val stageToTasks: Map[Int, Set[Long]] = Map(2 -> Set(3L, 4L), 3 -> Set(5L, 6L))
    val stageToRDDs: Map[Int, Seq[Int]] = Map(2 -> Seq(3, 4), 3 -> Seq(5, 6))
    val liveExecutors: Set[String] = Set("1", "2")

    val filter = new SQLLiveEntitiesEventFilter(liveExecutionToJobs, liveJobToStages,
      stageToTasks, stageToRDDs)

    // Verifying with finished SQL execution 1
    assert(filter.filterOtherEvent(SparkListenerSQLExecutionStart(1, "description1", "details1",
      "plan", null, 0)) === Some(false))
    assert(filter.filterOtherEvent(SparkListenerSQLExecutionEnd(1, 0)) === Some(false))
    assert(filter.filterOtherEvent(SparkListenerSQLAdaptiveExecutionUpdate(1, "plan", null))
      === Some(false))
    assert(filter.filterOtherEvent(SparkListenerDriverAccumUpdates(1, Seq.empty)) === Some(false))

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

    // job events for finished job should be considered as "don't know"
    assert(filter.filterJobStart(jobStartEventForJob1) === None)
    assert(filter.filterJobEnd(jobEndEventForJob1) === None)

    // stage events for finished job should be considered as "don't know"
    assert(filter.filterStageSubmitted(stageSubmittedEventsForJob1) === None)
    assert(filter.filterStageCompleted(stageCompletedEventsForJob1) === None)
    unpersistRDDEventsForJob1.foreach { event =>
      assert(filter.filterUnpersistRDD(event) === None)
    }

    val taskSpeculativeTaskSubmittedEvent = SparkListenerSpeculativeTaskSubmitted(stage1.stageId,
      stageAttemptId = 1)
    assert(filter.filterSpeculativeTaskSubmitted(taskSpeculativeTaskSubmittedEvent) === None)

    // task events for finished job should be considered as "don't know"
    tasksForStage1.foreach { task =>
      val taskStartEvent = SparkListenerTaskStart(stage1.stageId, 0, task)
      assert(filter.filterTaskStart(taskStartEvent) === None)

      val taskGettingResultEvent = SparkListenerTaskGettingResult(task)
      assert(filter.filterTaskGettingResult(taskGettingResultEvent) === None)

      val taskEndEvent = SparkListenerTaskEnd(stage1.stageId, 0, "taskType",
        Success, task, new ExecutorMetrics, null)
      assert(filter.filterTaskEnd(taskEndEvent) === None)
    }

    // Verifying with live SQL execution 2
    assert(filter.filterOtherEvent(SparkListenerSQLExecutionStart(2, "description2", "details2",
      "plan", null, 0)) === Some(true))
    assert(filter.filterOtherEvent(SparkListenerSQLExecutionEnd(2, 0)) === Some(true))
    assert(filter.filterOtherEvent(SparkListenerSQLAdaptiveExecutionUpdate(2, "plan", null))
      === Some(true))
    assert(filter.filterOtherEvent(SparkListenerDriverAccumUpdates(2, Seq.empty)) === Some(true))

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
    assert(filter.filterStageSubmitted(stageSubmittedEventsForJob2) === Some(true))
    assert(filter.filterStageCompleted(stageCompletedEventsForJob2) === Some(true))
    unpersistRDDEventsForJob2.foreach { event =>
      assert(filter.filterUnpersistRDD(event) === Some(true))
    }

    val taskSpeculativeTaskSubmittedEvent2 = SparkListenerSpeculativeTaskSubmitted(stage2.stageId,
      stageAttemptId = 1)
    assert(filter.filterSpeculativeTaskSubmitted(taskSpeculativeTaskSubmittedEvent2) === Some(true))

    // task events for live job should be filtered in
    tasksForStage2.foreach { task =>
      val taskStartEvent = SparkListenerTaskStart(stage2.stageId, 0, task)
      assert(filter.filterTaskStart(taskStartEvent) === Some(true))

      val taskGettingResultEvent = SparkListenerTaskGettingResult(task)
      assert(filter.filterTaskGettingResult(taskGettingResultEvent) === Some(true))

      val taskEndEvent = SparkListenerTaskEnd(stage1.stageId, 0, "taskType",
        Success, task, new ExecutorMetrics, null)
      assert(filter.filterTaskEnd(taskEndEvent) === Some(true))
    }
  }
}
