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

    val liveSQLExecutions = Set(2L)
    val liveJobs = Set(2)
    val liveStages = Set(2, 3)
    val liveTasks = Set(3L, 4L, 5L, 6L)
    val liveRDDs = Set(3, 4, 5, 6)
    val liveExecutors: Set[String] = Set("1", "2")

    val filter = new SQLLiveEntitiesEventFilter(liveSQLExecutions, liveJobs, liveStages, liveTasks,
      liveRDDs)
    val acceptFn = filter.acceptFn().lift

    // Verifying with finished SQL execution 1
    assert(Some(false) === acceptFn(SparkListenerSQLExecutionStart(1, Some(1),
      "description1", "details1", "plan", null, 0, Map.empty)))
    assert(Some(false) === acceptFn(SparkListenerSQLExecutionEnd(1, 0)))
    assert(Some(false) === acceptFn(SparkListenerSQLAdaptiveExecutionUpdate(1, "plan", null)))
    assert(Some(false) === acceptFn(SparkListenerDriverAccumUpdates(1, Seq.empty)))

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
    assert(None === acceptFn(jobStartEventForJob1))
    assert(None === acceptFn(jobEndEventForJob1))

    // stage events for finished job should be considered as "don't know"
    assert(None === acceptFn(stageSubmittedEventsForJob1))
    assert(None === acceptFn(stageCompletedEventsForJob1))
    unpersistRDDEventsForJob1.foreach { event =>
      assert(None === acceptFn(event))
    }

    val taskSpeculativeTaskSubmittedEvent = SparkListenerSpeculativeTaskSubmitted(stage1.stageId,
      stageAttemptId = 1)
    assert(None === acceptFn(taskSpeculativeTaskSubmittedEvent))

    // task events for finished job should be considered as "don't know"
    tasksForStage1.foreach { task =>
      val taskStartEvent = SparkListenerTaskStart(stage1.stageId, 0, task)
      assert(None === acceptFn(taskStartEvent))

      val taskGettingResultEvent = SparkListenerTaskGettingResult(task)
      assert(None === acceptFn(taskGettingResultEvent))

      val taskEndEvent = SparkListenerTaskEnd(stage1.stageId, 0, "taskType",
        Success, task, new ExecutorMetrics, null)
      assert(None === acceptFn(taskEndEvent))
    }

    // Verifying with live SQL execution 2
    assert(Some(true) === acceptFn(SparkListenerSQLExecutionStart(2, Some(2),
      "description2", "details2", "plan", null, 0, Map.empty)))
    assert(Some(true) === acceptFn(SparkListenerSQLExecutionEnd(2, 0)))
    assert(Some(true) === acceptFn(SparkListenerSQLAdaptiveExecutionUpdate(2, "plan", null)))
    assert(Some(true) === acceptFn(SparkListenerDriverAccumUpdates(2, Seq.empty)))

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
    assert(Some(true) === acceptFn(stageSubmittedEventsForJob2))
    assert(Some(true) === acceptFn(stageCompletedEventsForJob2))
    unpersistRDDEventsForJob2.foreach { event =>
      assert(Some(true) === acceptFn(event))
    }

    val taskSpeculativeTaskSubmittedEvent2 = SparkListenerSpeculativeTaskSubmitted(stage2.stageId,
      stageAttemptId = 1)
    assert(Some(true) === acceptFn(taskSpeculativeTaskSubmittedEvent2))

    // task events for live job should be accepted
    tasksForStage2.foreach { task =>
      val taskStartEvent = SparkListenerTaskStart(stage2.stageId, 0, task)
      assert(Some(true) === acceptFn(taskStartEvent))

      val taskGettingResultEvent = SparkListenerTaskGettingResult(task)
      assert(Some(true) === acceptFn(taskGettingResultEvent))

      val taskEndEvent = SparkListenerTaskEnd(stage1.stageId, 0, "taskType",
        Success, task, new ExecutorMetrics, null)
      assert(Some(true) === acceptFn(taskEndEvent))
    }
  }
}
