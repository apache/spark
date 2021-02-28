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

package org.apache.spark.status

import java.util.Properties

import scala.collection.immutable.Map

import org.apache.spark.{AccumulatorSuite, SparkContext, Success, TaskState}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart, StageInfo, TaskInfo, TaskLocality}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{RDDInfo, StorageLevel}

object ListenerEventsTestHelper {

  private var taskIdTracker = -1L
  private var rddIdTracker = -1
  private var stageIdTracker = -1

  def reset(): Unit = {
    taskIdTracker = -1L
    rddIdTracker = -1
    stageIdTracker = -1
  }

  def createJobProps(): Properties = {
    val jobProps = new Properties()
    jobProps.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, "jobDescription")
    jobProps.setProperty(SparkContext.SPARK_JOB_GROUP_ID, "jobGroup")
    jobProps.setProperty(SparkContext.SPARK_SCHEDULER_POOL, "schedPool")
    jobProps
  }

  def createRddsWithId(ids: Seq[Int]): Seq[RDDInfo] = {
    ids.map { rddId =>
      new RDDInfo(rddId, s"rdd${rddId}", 2, StorageLevel.NONE, false, Nil)
    }
  }

  def createRdds(count: Int): Seq[RDDInfo] = {
    (1 to count).map { _ =>
      val rddId = nextRddId()
      new RDDInfo(rddId, s"rdd${rddId}", 2, StorageLevel.NONE, false, Nil)
    }
  }

  def createStage(id: Int, rdds: Seq[RDDInfo], parentIds: Seq[Int]): StageInfo = {
    new StageInfo(id, 0, s"stage${id}", 4, rdds, parentIds, s"details${id}",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
  }

  def createStage(rdds: Seq[RDDInfo], parentIds: Seq[Int]): StageInfo = {
    createStage(nextStageId(), rdds, parentIds)
  }

  def createTasks(ids: Seq[Long], execs: Array[String], time: Long): Seq[TaskInfo] = {
    ids.zipWithIndex.map { case (id, idx) =>
      val exec = execs(idx % execs.length)
      new TaskInfo(id, idx, 1, time, exec, s"$exec.example.com",
        TaskLocality.PROCESS_LOCAL, idx % 2 == 0)
    }
  }

  def createTasks(count: Int, execs: Array[String], time: Long): Seq[TaskInfo] = {
    createTasks((1 to count).map { _ => nextTaskId() }, execs, time)
  }

  def createTaskWithNewAttempt(orig: TaskInfo, time: Long): TaskInfo = {
    // Task reattempts have a different ID, but the same index as the original.
    new TaskInfo(nextTaskId(), orig.index, orig.attemptNumber + 1, time, orig.executorId,
      s"${orig.executorId}.example.com", TaskLocality.PROCESS_LOCAL, orig.speculative)
  }

  def createTaskStartEvent(
      taskInfo: TaskInfo,
      stageId: Int,
      attemptId: Int): SparkListenerTaskStart = {
    SparkListenerTaskStart(stageId, attemptId, taskInfo)
  }

  /** Create a stage submitted event for the specified stage Id. */
  def createStageSubmittedEvent(stageId: Int): SparkListenerStageSubmitted = {
    SparkListenerStageSubmitted(new StageInfo(stageId, 0, stageId.toString, 0,
      Seq.empty, Seq.empty, "details",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
  }

  /** Create a stage completed event for the specified stage Id. */
  def createStageCompletedEvent(stageId: Int): SparkListenerStageCompleted = {
    SparkListenerStageCompleted(new StageInfo(stageId, 0, stageId.toString, 0,
      Seq.empty, Seq.empty, "details",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
  }

  def createExecutorAddedEvent(executorId: Int): SparkListenerExecutorAdded = {
    createExecutorAddedEvent(executorId.toString, 0)
  }

  /** Create an executor added event for the specified executor Id. */
  def createExecutorAddedEvent(executorId: String, time: Long): SparkListenerExecutorAdded = {
    SparkListenerExecutorAdded(time, executorId,
      new ExecutorInfo("host1", 1, Map.empty, Map.empty))
  }

  def createExecutorRemovedEvent(executorId: Int): SparkListenerExecutorRemoved = {
    createExecutorRemovedEvent(executorId.toString, 10L)
  }

  /** Create an executor added event for the specified executor Id. */
  def createExecutorRemovedEvent(executorId: String, time: Long): SparkListenerExecutorRemoved = {
    SparkListenerExecutorRemoved(time, executorId, "test")
  }

  /** Create an executor metrics update event, with the specified executor metrics values. */
  def createExecutorMetricsUpdateEvent(
      stageId: Int,
      executorId: Int,
      executorMetrics: Array[Long]): SparkListenerExecutorMetricsUpdate = {
    val taskMetrics = TaskMetrics.empty
    taskMetrics.incDiskBytesSpilled(111)
    taskMetrics.incMemoryBytesSpilled(222)
    val accum = Array((333L, 1, 1, taskMetrics.accumulators().map(AccumulatorSuite.makeInfo)))
    val executorUpdates = Map((stageId, 0) -> new ExecutorMetrics(executorMetrics))
    SparkListenerExecutorMetricsUpdate(executorId.toString, accum, executorUpdates)
  }

  case class JobInfo(
      stageIds: Seq[Int],
      stageToTaskIds: Map[Int, Seq[Long]],
      stageToRddIds: Map[Int, Seq[Int]])

  def pushJobEventsWithoutJobEnd(
      listener: SparkListener,
      jobId: Int,
      jobProps: Properties,
      execIds: Array[String],
      time: Long): JobInfo = {
    // Start a job with 1 stage / 4 tasks each
    val rddsForStage = createRdds(2)
    val stage = createStage(rddsForStage, Nil)

    listener.onJobStart(SparkListenerJobStart(jobId, time, Seq(stage), jobProps))

    // Submit stage
    stage.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage, jobProps))

    // Start tasks from stage
    val s1Tasks = createTasks(4, execIds, time)
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

  private def nextTaskId(): Long = {
    taskIdTracker += 1
    taskIdTracker
  }

  private def nextRddId(): Int = {
    rddIdTracker += 1
    rddIdTracker
  }

  private def nextStageId(): Int = {
    stageIdTracker += 1
    stageIdTracker
  }
}
