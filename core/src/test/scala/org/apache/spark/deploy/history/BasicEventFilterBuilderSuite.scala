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

import org.apache.spark.{SparkFunSuite, Success, TaskResultLost, TaskState}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._
import org.apache.spark.status.ListenerEventsTestHelper

class BasicEventFilterBuilderSuite extends SparkFunSuite {
  import ListenerEventsTestHelper._

  override protected def beforeEach(): Unit = {
    ListenerEventsTestHelper.reset()
  }

  test("track live jobs") {
    var time = 0L

    val listener = new BasicEventFilterBuilder
    listener.onOtherEvent(SparkListenerLogStart("TestSparkVersion"))

    // Start the application.
    time += 1
    listener.onApplicationStart(SparkListenerApplicationStart(
      "name",
      Some("id"),
      time,
      "user",
      Some("attempt"),
      None))

    // Start a couple of executors.
    time += 1
    val execIds = Array("1", "2")
    execIds.foreach { id =>
      listener.onExecutorAdded(createExecutorAddedEvent(id, time))
    }

    // Start a job with 2 stages / 4 tasks each
    time += 1

    val rddsForStage0 = createRdds(2)
    val rddsForStage1 = createRdds(2)

    val stage0 = createStage(rddsForStage0, Nil)
    val stage1 = createStage(rddsForStage1, Seq(stage0.stageId))
    val stages = Seq(stage0, stage1)

    val jobProps = createJobProps()
    listener.onJobStart(SparkListenerJobStart(1, time, stages, jobProps))

    // Submit stage 0
    time += 1
    stages.head.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stages.head, jobProps))

    // Start tasks from stage 0
    time += 1

    val s0Tasks = ListenerEventsTestHelper.createTasks(4, execIds, time)
    s0Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId,
        stages.head.attemptNumber(), task))
    }

    // Fail one of the tasks, re-start it.
    time += 1
    s0Tasks.head.markFinished(TaskState.FAILED, time)
    listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptNumber,
      "taskType", TaskResultLost, s0Tasks.head, new ExecutorMetrics, null))

    time += 1
    val reattempt = createTaskWithNewAttempt(s0Tasks.head, time)
    listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId, stages.head.attemptNumber,
      reattempt))

    // Succeed all tasks in stage 0.
    val pending = s0Tasks.drop(1) ++ Seq(reattempt)

    time += 1
    pending.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptNumber,
        "taskType", Success, task, new ExecutorMetrics, TaskMetrics.empty))
    }

    // End stage 0.
    time += 1
    stages.head.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stages.head))

    assert(listener.liveJobs === Set(1))
    assert(listener.liveStages === Set(0))
    // stage 1 not yet submitted - RDDs for stage 1 is not available
    assert(listener.liveRDDs === rddsForStage0.map(_.id).toSet)
    assert(listener.liveTasks === (s0Tasks ++ Seq(reattempt)).map(_.taskId).toSet)

    // Submit stage 1.
    time += 1
    stages.last.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stages.last, jobProps))

    // Start and fail all tasks of stage 1.
    time += 1
    val s1Tasks = createTasks(4, execIds, time)
    s1Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stages.last.stageId,
        stages.last.attemptNumber,
        task))
    }

    time += 1
    s1Tasks.foreach { task =>
      task.markFinished(TaskState.FAILED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stages.last.stageId, stages.last.attemptNumber,
        "taskType", TaskResultLost, task, new ExecutorMetrics, null))
    }

    // Fail stage 1.
    time += 1
    stages.last.completionTime = Some(time)
    stages.last.failureReason = Some("uh oh")
    listener.onStageCompleted(SparkListenerStageCompleted(stages.last))

    // - Re-submit stage 1, all tasks, and succeed them and the stage.
    val oldS1 = stages.last
    val newS1 = new StageInfo(oldS1.stageId, oldS1.attemptNumber + 1, oldS1.name, oldS1.numTasks,
      oldS1.rddInfos, oldS1.parentIds, oldS1.details, oldS1.taskMetrics,
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)

    time += 1
    newS1.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(newS1, jobProps))

    val newS1Tasks = createTasks(4, execIds, time)

    newS1Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(newS1.stageId, newS1.attemptNumber, task))
    }

    time += 1
    newS1Tasks.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(newS1.stageId, newS1.attemptNumber, "taskType",
        Success, task, new ExecutorMetrics, null))
    }

    time += 1
    newS1.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(newS1))

    assert(listener.liveJobs === Set(1))
    assert(listener.liveStages === Set(0, 1))
    // stage 0 and 1 are finished but it stores the information regarding stage
    assert(listener.liveRDDs === (rddsForStage0.map(_.id) ++ rddsForStage1.map(_.id)).toSet)
    assert(listener.liveTasks ===
      (s0Tasks ++ Seq(reattempt) ++ s1Tasks ++ newS1Tasks).map(_.taskId).toSet)

    // Start next job.
    time += 1

    val rddsForStage2 = createRdds(2)
    val rddsForStage3 = createRdds(2)

    val stage3 = createStage(rddsForStage2, Nil)
    val stage4 = createStage(rddsForStage3, Seq(stage3.stageId))
    val stagesForJob2 = Seq(stage3, stage4)

    listener.onJobStart(SparkListenerJobStart(2, time, stagesForJob2, jobProps))

    // End job 1.
    time += 1
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    // everything related to job 1 should be cleaned up, but not for job 2
    assert(listener.liveJobs === Set(2))
    assert(listener.liveStages.isEmpty)
    // no RDD information available as these stages are not submitted yet
    assert(listener.liveRDDs.isEmpty)
    // stageToTasks has no information for job 2, as no task has been started
    assert(listener.liveTasks.isEmpty)
  }

  test("track live executors") {
    var time = 0L

    val listener = new BasicEventFilterBuilder
    listener.onOtherEvent(SparkListenerLogStart("TestSparkVersion"))

    // Start the application.
    time += 1
    listener.onApplicationStart(SparkListenerApplicationStart(
      "name",
      Some("id"),
      time,
      "user",
      Some("attempt"),
      None))

    // Start a couple of executors.
    time += 1
    val execIds = (1 to 3).map(_.toString)
    execIds.foreach { id =>
      listener.onExecutorAdded(createExecutorAddedEvent(id, time))
    }

    // End one of executors.
    time += 1
    listener.onExecutorRemoved(createExecutorRemovedEvent(execIds.head, time))

    assert(listener.liveExecutors === execIds.drop(1).toSet)
  }
}
