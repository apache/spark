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
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerLogStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.execution.{SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.status.ListenerEventsTestHelper

class SQLEventFilterBuilderSuite extends SparkFunSuite {
  import ListenerEventsTestHelper._

  override protected def beforeEach(): Unit = {
    ListenerEventsTestHelper.reset()
  }

  test("track live SQL executions") {
    var time = 0L

    val listener = new SQLEventFilterBuilder

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

    // Start SQL Execution
    listener.onOtherEvent(SparkListenerSQLExecutionStart(1, "desc1", "details1", "plan",
      new SparkPlanInfo("node", "str", Seq.empty, Map.empty, Seq.empty), time))

    time += 1

    // job 1, 2: coupled with SQL execution 1, finished
    val jobInfoForJob1 = pushJobEventsWithoutJobEnd(listener, 1, execIds, Some("1"), time)
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    val jobInfoForJob2 = pushJobEventsWithoutJobEnd(listener, 2, execIds, Some("1"), time)
    listener.onJobEnd(SparkListenerJobEnd(2, time, JobSucceeded))

    // job 3: not coupled with SQL execution 1, finished
    pushJobEventsWithoutJobEnd(listener, 3, execIds, None, time)
    listener.onJobEnd(SparkListenerJobEnd(3, time, JobSucceeded))

    // job 4: not coupled with SQL execution 1, not finished
    pushJobEventsWithoutJobEnd(listener, 4, execIds, None, time)
    listener.onJobEnd(SparkListenerJobEnd(4, time, JobSucceeded))

    assert(listener.liveExecutionToJobs.keys === Set(1))
    assert(listener.liveExecutionToJobs(1) === Set(1, 2))

    // only SQL executions related jobs are tracked
    assert(listener.jobToStages.keys === Set(1, 2))
    assert(listener.jobToStages(1).toSet === jobInfoForJob1.stageIds.toSet)
    assert(listener.jobToStages(2).toSet === jobInfoForJob2.stageIds.toSet)

    assert(listener.stageToTasks.keys ===
      (jobInfoForJob1.stageIds ++ jobInfoForJob2.stageIds).toSet)
    listener.stageToTasks.foreach { case (stageId, tasks) =>
      val expectedTasks = jobInfoForJob1.stageToTaskIds.getOrElse(stageId,
        jobInfoForJob2.stageToTaskIds.getOrElse(stageId, null))
      assert(tasks === expectedTasks.toSet)
    }

    assert(listener.stageToRDDs.keys ===
      (jobInfoForJob1.stageIds ++ jobInfoForJob2.stageIds).toSet)
    listener.stageToRDDs.foreach { case (stageId, rdds) =>
      val expectedRDDs = jobInfoForJob1.stageToRddIds.getOrElse(stageId,
        jobInfoForJob2.stageToRddIds.getOrElse(stageId, null))
      assert(rdds.toSet === expectedRDDs.toSet)
    }

    // End SQL execution
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(1, 0))

    assert(listener.liveExecutionToJobs.isEmpty)
    assert(listener.jobToStages.isEmpty)
    assert(listener.stageToTasks.isEmpty)
    assert(listener.stageToRDDs.isEmpty)
  }

  case class JobInfo(
      stageIds: Seq[Int],
      stageToTaskIds: Map[Int, Seq[Long]],
      stageToRddIds: Map[Int, Seq[Int]])

  private def pushJobEventsWithoutJobEnd(
      listener: SQLEventFilterBuilder,
      jobId: Int,
      execIds: Array[String],
      sqlExecId: Option[String],
      time: Long): JobInfo = {
    // Start a job with 1 stages / 4 tasks each
    val rddsForStage = createRdds(2)
    val stage = createStage(rddsForStage, Nil)

    val jobProps = createJobProps()
    sqlExecId.foreach { id => jobProps.setProperty(SQLExecution.EXECUTION_ID_KEY, id) }

    listener.onJobStart(SparkListenerJobStart(jobId, time, Seq(stage), jobProps))

    // Submit stage
    stage.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage, jobProps))

    // Start tasks from stage
    val s1Tasks = ListenerEventsTestHelper.createTasks(4, execIds, time)
    s1Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stage.stageId,
        stage.attemptNumber(), task))
    }

    // Succeed all tasks in stage.
    val s1Metrics = TaskMetrics.empty
    s1Metrics.setExecutorCpuTime(2L)
    s1Metrics.setExecutorRunTime(4L)

    s1Tasks.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stage.stageId, stage.attemptNumber,
        "taskType", Success, task, new ExecutorMetrics, s1Metrics))
    }

    // End stage.
    stage.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stage))

    JobInfo(Seq(stage.stageId), Map(stage.stageId -> s1Tasks.map(_.taskId)),
      Map(stage.stageId -> rddsForStage.map(_.id)))
  }
}
