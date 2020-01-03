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

    val rddsForStage1 = createRdds(2)
    val rddsForStage2 = createRdds(2)

    val stage1 = createStage(rddsForStage1, Nil)
    val stage2 = createStage(rddsForStage2, Seq(stage1.stageId))
    val stages = Seq(stage1, stage2)

    val jobProps = createJobProps()
    listener.onJobStart(SparkListenerJobStart(1, time, stages, jobProps))

    // Submit stage 1
    time += 1
    stages.head.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stages.head, jobProps))

    // Start tasks from stage 1
    time += 1

    val s1Tasks = ListenerEventsTestHelper.createTasks(4, execIds, time)
    s1Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId,
        stages.head.attemptNumber(), task))
    }

    // Fail one of the tasks, re-start it.
    time += 1
    s1Tasks.head.markFinished(TaskState.FAILED, time)
    listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptNumber,
      "taskType", TaskResultLost, s1Tasks.head, new ExecutorMetrics, null))

    time += 1
    val reattempt = createTaskWithNewAttempt(s1Tasks.head, time)
    listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId, stages.head.attemptNumber,
      reattempt))

    // Succeed all tasks in stage 1.
    val pending = s1Tasks.drop(1) ++ Seq(reattempt)

    val s1Metrics = TaskMetrics.empty
    s1Metrics.setExecutorCpuTime(2L)
    s1Metrics.setExecutorRunTime(4L)

    time += 1
    pending.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptNumber,
        "taskType", Success, task, new ExecutorMetrics, s1Metrics))
    }

    // End stage 1.
    time += 1
    stages.head.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stages.head))

    assert(listener.liveJobToStages.keys.toSeq === Seq(1))
    assert(listener.liveJobToStages(1) === Seq(0, 1))
    assert(listener.stageToRDDs.keys.toSeq === Seq(0))
    assert(listener.stageToRDDs(0) === rddsForStage1.map(_.id))
    // stage 1 not yet submitted
    assert(listener.stageToTasks.keys.toSeq === Seq(0))
    assert(listener.stageToTasks(0) === (s1Tasks ++ Seq(reattempt)).map(_.taskId).toSet)

    // Submit stage 2.
    time += 1
    stages.last.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stages.last, jobProps))

    // Start and fail all tasks of stage 2.
    time += 1
    val s2Tasks = createTasks(4, execIds, time)
    s2Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stages.last.stageId,
        stages.last.attemptNumber,
        task))
    }

    time += 1
    s2Tasks.foreach { task =>
      task.markFinished(TaskState.FAILED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stages.last.stageId, stages.last.attemptNumber,
        "taskType", TaskResultLost, task, new ExecutorMetrics, null))
    }

    // Fail stage 2.
    time += 1
    stages.last.completionTime = Some(time)
    stages.last.failureReason = Some("uh oh")
    listener.onStageCompleted(SparkListenerStageCompleted(stages.last))

    // - Re-submit stage 2, all tasks, and succeed them and the stage.
    val oldS2 = stages.last
    val newS2 = new StageInfo(oldS2.stageId, oldS2.attemptNumber + 1, oldS2.name, oldS2.numTasks,
      oldS2.rddInfos, oldS2.parentIds, oldS2.details, oldS2.taskMetrics)

    time += 1
    newS2.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(newS2, jobProps))

    val newS2Tasks = createTasks(4, execIds, time)

    newS2Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(newS2.stageId, newS2.attemptNumber, task))
    }

    time += 1
    newS2Tasks.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(newS2.stageId, newS2.attemptNumber, "taskType",
        Success, task, new ExecutorMetrics, null))
    }

    time += 1
    newS2.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(newS2))

    assert(listener.liveJobToStages.keys.toSeq === Seq(1))
    assert(listener.liveJobToStages(1) === Seq(0, 1))
    assert(listener.stageToRDDs.keys === Set(0, 1))
    assert(listener.stageToRDDs(0) === rddsForStage1.map(_.id))
    assert(listener.stageToRDDs(1) === rddsForStage2.map(_.id))
    assert(listener.stageToTasks.keys.toSet === Set(0, 1))
    // stage 0 is finished but it stores the information regarding stage
    assert(listener.stageToTasks(0) === (s1Tasks ++ Seq(reattempt)).map(_.taskId).toSet)
    // stage 1 is newly added
    assert(listener.stageToTasks(1) === (s2Tasks ++ newS2Tasks).map(_.taskId).toSet)

    // Start next job.
    time += 1

    val rddsForStage3 = createRdds(2)
    val rddsForStage4 = createRdds(2)

    val stage3 = createStage(rddsForStage3, Nil)
    val stage4 = createStage(rddsForStage4, Seq(stage3.stageId))
    val stagesForJob2 = Seq(stage3, stage4)

    listener.onJobStart(SparkListenerJobStart(2, time, stagesForJob2, jobProps))

    // End job 1.
    time += 1
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    // everything related to job 1 should be cleaned up, but not for job 2
    assert(listener.liveJobToStages.keys.toSet === Set(2))
    assert(listener.stageToRDDs.isEmpty)
    // stageToTasks has no information for job 2, as no task has been started
    assert(listener.stageToTasks.isEmpty)
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
