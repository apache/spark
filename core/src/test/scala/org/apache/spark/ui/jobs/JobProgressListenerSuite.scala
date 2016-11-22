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

package org.apache.spark.ui.jobs

import java.util.Properties

import org.scalatest.Matchers

import org.apache.spark._
import org.apache.spark.{LocalSparkContext, SparkConf, Success}
import org.apache.spark.executor._
import org.apache.spark.scheduler._
import org.apache.spark.ui.jobs.UIData.TaskUIData
import org.apache.spark.util.{AccumulatorContext, Utils}

class JobProgressListenerSuite extends SparkFunSuite with LocalSparkContext with Matchers {

  val jobSubmissionTime = 1421191042750L
  val jobCompletionTime = 1421191296660L

  private def createStageStartEvent(stageId: Int) = {
    val stageInfo = new StageInfo(stageId, 0, stageId.toString, 0, null, null, "")
    SparkListenerStageSubmitted(stageInfo)
  }

  private def createStageEndEvent(stageId: Int, failed: Boolean = false) = {
    val stageInfo = new StageInfo(stageId, 0, stageId.toString, 0, null, null, "")
    if (failed) {
      stageInfo.failureReason = Some("Failed!")
    }
    SparkListenerStageCompleted(stageInfo)
  }

  private def createJobStartEvent(
      jobId: Int,
      stageIds: Seq[Int],
      jobGroup: Option[String] = None): SparkListenerJobStart = {
    val stageInfos = stageIds.map { stageId =>
      new StageInfo(stageId, 0, stageId.toString, 0, null, null, "")
    }
    val properties: Option[Properties] = jobGroup.map { groupId =>
      val props = new Properties()
      props.setProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
      props
    }
    SparkListenerJobStart(jobId, jobSubmissionTime, stageInfos, properties.orNull)
  }

  private def createJobEndEvent(jobId: Int, failed: Boolean = false) = {
    val result = if (failed) JobFailed(new Exception("dummy failure")) else JobSucceeded
    SparkListenerJobEnd(jobId, jobCompletionTime, result)
  }

  private def runJob(listener: SparkListener, jobId: Int, shouldFail: Boolean = false) {
    val stagesThatWontBeRun = jobId * 200 to jobId * 200 + 10
    val stageIds = jobId * 100 to jobId * 100 + 50
    listener.onJobStart(createJobStartEvent(jobId, stageIds ++ stagesThatWontBeRun))
    for (stageId <- stageIds) {
      listener.onStageSubmitted(createStageStartEvent(stageId))
      listener.onStageCompleted(createStageEndEvent(stageId, failed = stageId % 2 == 0))
    }
    listener.onJobEnd(createJobEndEvent(jobId, shouldFail))
  }

  private def assertActiveJobsStateIsEmpty(listener: JobProgressListener) {
    listener.getSizesOfActiveStateTrackingCollections.foreach { case (fieldName, size) =>
      assert(size === 0, s"$fieldName was not empty")
    }
  }

  test("test LRU eviction of stages") {
    def runWithListener(listener: JobProgressListener) : Unit = {
      for (i <- 1 to 50) {
        listener.onStageSubmitted(createStageStartEvent(i))
        listener.onStageCompleted(createStageEndEvent(i))
      }
      assertActiveJobsStateIsEmpty(listener)
    }
    val conf = new SparkConf()
    conf.set("spark.ui.retainedStages", 5.toString)
    var listener = new JobProgressListener(conf)

    // Test with 5 retainedStages
    runWithListener(listener)
    listener.completedStages.size should be (5)
    listener.completedStages.map(_.stageId).toSet should be (Set(50, 49, 48, 47, 46))

    // Test with 0 retainedStages
    conf.set("spark.ui.retainedStages", 0.toString)
    listener = new JobProgressListener(conf)
    runWithListener(listener)
    listener.completedStages.size should be (0)
  }

  test("test clearing of stageIdToActiveJobs") {
    val conf = new SparkConf()
    conf.set("spark.ui.retainedStages", 5.toString)
    val listener = new JobProgressListener(conf)
    val jobId = 0
    val stageIds = 1 to 50
    // Start a job with 50 stages
    listener.onJobStart(createJobStartEvent(jobId, stageIds))
    for (stageId <- stageIds) {
      listener.onStageSubmitted(createStageStartEvent(stageId))
    }
    listener.stageIdToActiveJobIds.size should be > 0

    // Complete the stages and job
    for (stageId <- stageIds) {
      listener.onStageCompleted(createStageEndEvent(stageId, failed = false))
    }
    listener.onJobEnd(createJobEndEvent(jobId, false))
    assertActiveJobsStateIsEmpty(listener)
    listener.stageIdToActiveJobIds.size should be (0)
  }

  test("test clearing of jobGroupToJobIds") {
    def runWithListener(listener: JobProgressListener): Unit = {
      // Run 50 jobs, each with one stage
      for (jobId <- 0 to 50) {
        listener.onJobStart(createJobStartEvent(jobId, Seq(0), jobGroup = Some(jobId.toString)))
        listener.onStageSubmitted(createStageStartEvent(0))
        listener.onStageCompleted(createStageEndEvent(0, failed = false))
        listener.onJobEnd(createJobEndEvent(jobId, false))
      }
      assertActiveJobsStateIsEmpty(listener)
    }
    val conf = new SparkConf()
    conf.set("spark.ui.retainedJobs", 5.toString)

    var listener = new JobProgressListener(conf)
    runWithListener(listener)
    // This collection won't become empty, but it should be bounded by spark.ui.retainedJobs
    listener.jobGroupToJobIds.size should be (5)

    // Test with 0 jobs
    conf.set("spark.ui.retainedJobs", 0.toString)
    listener = new JobProgressListener(conf)
    runWithListener(listener)
    listener.jobGroupToJobIds.size should be (0)
  }

  test("test LRU eviction of jobs") {
    val conf = new SparkConf()
    conf.set("spark.ui.retainedStages", 5.toString)
    conf.set("spark.ui.retainedJobs", 5.toString)
    val listener = new JobProgressListener(conf)

    // Run a bunch of jobs to get the listener into a state where we've exceeded both the
    // job and stage retention limits:
    for (jobId <- 1 to 10) {
      runJob(listener, jobId, shouldFail = false)
    }
    for (jobId <- 200 to 210) {
      runJob(listener, jobId, shouldFail = true)
    }
    assertActiveJobsStateIsEmpty(listener)
    // Snapshot the sizes of various soft- and hard-size-limited collections:
    val softLimitSizes = listener.getSizesOfSoftSizeLimitedCollections
    val hardLimitSizes = listener.getSizesOfHardSizeLimitedCollections
    // Run some more jobs:
    for (jobId <- 11 to 50) {
      runJob(listener, jobId, shouldFail = false)
      // We shouldn't exceed the hard / soft limit sizes after the jobs have finished:
      listener.getSizesOfSoftSizeLimitedCollections should be (softLimitSizes)
      listener.getSizesOfHardSizeLimitedCollections should be (hardLimitSizes)
    }

    listener.completedJobs.size should be (5)
    listener.completedJobs.map(_.jobId).toSet should be (Set(50, 49, 48, 47, 46))

    for (jobId <- 51 to 100) {
      runJob(listener, jobId, shouldFail = true)
      // We shouldn't exceed the hard / soft limit sizes after the jobs have finished:
      listener.getSizesOfSoftSizeLimitedCollections should be (softLimitSizes)
      listener.getSizesOfHardSizeLimitedCollections should be (hardLimitSizes)
    }
    assertActiveJobsStateIsEmpty(listener)

    // Completed and failed jobs each their own size limits, so this should still be the same:
    listener.completedJobs.size should be (5)
    listener.completedJobs.map(_.jobId).toSet should be (Set(50, 49, 48, 47, 46))
    listener.failedJobs.size should be (5)
    listener.failedJobs.map(_.jobId).toSet should be (Set(100, 99, 98, 97, 96))
  }

  test("test executor id to summary") {
    val conf = new SparkConf()
    val listener = new JobProgressListener(conf)
    val taskMetrics = TaskMetrics.empty
    val shuffleReadMetrics = taskMetrics.createTempShuffleReadMetrics()
    assert(listener.stageIdToData.size === 0)

    // finish this task, should get updated shuffleRead
    shuffleReadMetrics.incRemoteBytesRead(1000)
    taskMetrics.mergeShuffleReadMetrics()
    var taskInfo = new TaskInfo(1234L, 0, 1, 0L, "exe-1", "host1", TaskLocality.NODE_LOCAL, false)
    taskInfo.finishTime = 1
    var task = new ShuffleMapTask(0)
    val taskType = Utils.getFormattedClassName(task)
    listener.onTaskEnd(
      SparkListenerTaskEnd(task.stageId, 0, taskType, Success, taskInfo, taskMetrics))
    assert(listener.stageIdToData.getOrElse((0, 0), fail())
      .executorSummary.getOrElse("exe-1", fail()).shuffleRead === 1000)

    // finish a task with unknown executor-id, nothing should happen
    taskInfo =
      new TaskInfo(1234L, 0, 1, 1000L, "exe-unknown", "host1", TaskLocality.NODE_LOCAL, true)
    taskInfo.finishTime = 1
    task = new ShuffleMapTask(0)
    listener.onTaskEnd(
      SparkListenerTaskEnd(task.stageId, 0, taskType, Success, taskInfo, taskMetrics))
    assert(listener.stageIdToData.size === 1)

    // finish this task, should get updated duration
    taskInfo = new TaskInfo(1235L, 0, 1, 0L, "exe-1", "host1", TaskLocality.NODE_LOCAL, false)
    taskInfo.finishTime = 1
    task = new ShuffleMapTask(0)
    listener.onTaskEnd(
      SparkListenerTaskEnd(task.stageId, 0, taskType, Success, taskInfo, taskMetrics))
    assert(listener.stageIdToData.getOrElse((0, 0), fail())
      .executorSummary.getOrElse("exe-1", fail()).shuffleRead === 2000)

    // finish this task, should get updated duration
    taskInfo = new TaskInfo(1236L, 0, 2, 0L, "exe-2", "host1", TaskLocality.NODE_LOCAL, false)
    taskInfo.finishTime = 1
    task = new ShuffleMapTask(0)
    listener.onTaskEnd(
      SparkListenerTaskEnd(task.stageId, 0, taskType, Success, taskInfo, taskMetrics))
    assert(listener.stageIdToData.getOrElse((0, 0), fail())
      .executorSummary.getOrElse("exe-2", fail()).shuffleRead === 1000)
  }

  test("test task success vs failure counting for different task end reasons") {
    val conf = new SparkConf()
    val listener = new JobProgressListener(conf)
    val metrics = TaskMetrics.empty
    val taskInfo = new TaskInfo(1234L, 0, 3, 0L, "exe-1", "host1", TaskLocality.NODE_LOCAL, false)
    taskInfo.finishTime = 1
    val task = new ShuffleMapTask(0)
    val taskType = Utils.getFormattedClassName(task)

    // Go through all the failure cases to make sure we are counting them as failures.
    val taskFailedReasons = Seq(
      Resubmitted,
      new FetchFailed(null, 0, 0, 0, "ignored"),
      ExceptionFailure("Exception", "description", null, null, None),
      TaskResultLost,
      ExecutorLostFailure("0", true, Some("Induced failure")),
      UnknownReason)
    var failCount = 0
    for (reason <- taskFailedReasons) {
      listener.onTaskEnd(
        SparkListenerTaskEnd(task.stageId, 0, taskType, reason, taskInfo, metrics))
      failCount += 1
      assert(listener.stageIdToData((task.stageId, 0)).numCompleteTasks === 0)
      assert(listener.stageIdToData((task.stageId, 0)).numFailedTasks === failCount)
    }

    // Make sure killed tasks are accounted for correctly.
    listener.onTaskEnd(
      SparkListenerTaskEnd(task.stageId, 0, taskType, TaskKilled, taskInfo, metrics))
    assert(listener.stageIdToData((task.stageId, 0)).numKilledTasks === 1)

    // Make sure we count success as success.
    listener.onTaskEnd(
      SparkListenerTaskEnd(task.stageId, 1, taskType, Success, taskInfo, metrics))
    assert(listener.stageIdToData((task.stageId, 1)).numCompleteTasks === 1)
    assert(listener.stageIdToData((task.stageId, 0)).numFailedTasks === failCount)
  }

  test("test update metrics") {
    val conf = new SparkConf()
    val listener = new JobProgressListener(conf)

    val taskType = Utils.getFormattedClassName(new ShuffleMapTask(0))
    val execId = "exe-1"

    def makeTaskMetrics(base: Int): TaskMetrics = {
      val taskMetrics = TaskMetrics.empty
      val shuffleReadMetrics = taskMetrics.createTempShuffleReadMetrics()
      val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
      val inputMetrics = taskMetrics.inputMetrics
      val outputMetrics = taskMetrics.outputMetrics
      shuffleReadMetrics.incRemoteBytesRead(base + 1)
      shuffleReadMetrics.incLocalBytesRead(base + 9)
      shuffleReadMetrics.incRemoteBlocksFetched(base + 2)
      taskMetrics.mergeShuffleReadMetrics()
      shuffleWriteMetrics.incBytesWritten(base + 3)
      taskMetrics.setExecutorRunTime(base + 4)
      taskMetrics.incDiskBytesSpilled(base + 5)
      taskMetrics.incMemoryBytesSpilled(base + 6)
      inputMetrics.setBytesRead(base + 7)
      outputMetrics.setBytesWritten(base + 8)
      taskMetrics
    }

    def makeTaskInfo(taskId: Long, finishTime: Int = 0): TaskInfo = {
      val taskInfo = new TaskInfo(taskId, 0, 1, 0L, execId, "host1", TaskLocality.NODE_LOCAL,
        false)
      taskInfo.finishTime = finishTime
      taskInfo
    }

    listener.onTaskStart(SparkListenerTaskStart(0, 0, makeTaskInfo(1234L)))
    listener.onTaskStart(SparkListenerTaskStart(0, 0, makeTaskInfo(1235L)))
    listener.onTaskStart(SparkListenerTaskStart(1, 0, makeTaskInfo(1236L)))
    listener.onTaskStart(SparkListenerTaskStart(1, 0, makeTaskInfo(1237L)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate(execId, Array(
      (1234L, 0, 0, makeTaskMetrics(0).accumulators().map(AccumulatorSuite.makeInfo)),
      (1235L, 0, 0, makeTaskMetrics(100).accumulators().map(AccumulatorSuite.makeInfo)),
      (1236L, 1, 0, makeTaskMetrics(200).accumulators().map(AccumulatorSuite.makeInfo)))))

    var stage0Data = listener.stageIdToData.get((0, 0)).get
    var stage1Data = listener.stageIdToData.get((1, 0)).get
    assert(stage0Data.shuffleReadTotalBytes == 220)
    assert(stage1Data.shuffleReadTotalBytes == 410)
    assert(stage0Data.shuffleWriteBytes == 106)
    assert(stage1Data.shuffleWriteBytes == 203)
    assert(stage0Data.executorRunTime == 108)
    assert(stage1Data.executorRunTime == 204)
    assert(stage0Data.diskBytesSpilled == 110)
    assert(stage1Data.diskBytesSpilled == 205)
    assert(stage0Data.memoryBytesSpilled == 112)
    assert(stage1Data.memoryBytesSpilled == 206)
    assert(stage0Data.inputBytes == 114)
    assert(stage1Data.inputBytes == 207)
    assert(stage0Data.outputBytes == 116)
    assert(stage1Data.outputBytes == 208)

    assert(
      stage0Data.taskData.get(1234L).get.metrics.get.shuffleReadMetrics.totalBlocksFetched == 2)
    assert(
      stage0Data.taskData.get(1235L).get.metrics.get.shuffleReadMetrics.totalBlocksFetched == 102)
    assert(
      stage1Data.taskData.get(1236L).get.metrics.get.shuffleReadMetrics.totalBlocksFetched == 202)

    // task that was included in a heartbeat
    listener.onTaskEnd(SparkListenerTaskEnd(0, 0, taskType, Success, makeTaskInfo(1234L, 1),
      makeTaskMetrics(300)))
    // task that wasn't included in a heartbeat
    listener.onTaskEnd(SparkListenerTaskEnd(1, 0, taskType, Success, makeTaskInfo(1237L, 1),
      makeTaskMetrics(400)))

    stage0Data = listener.stageIdToData.get((0, 0)).get
    stage1Data = listener.stageIdToData.get((1, 0)).get
    // Task 1235 contributed (100+1)+(100+9) = 210 shuffle bytes, and task 1234 contributed
    // (300+1)+(300+9) = 610 total shuffle bytes, so the total for the stage is 820.
    assert(stage0Data.shuffleReadTotalBytes == 820)
    // Task 1236 contributed 410 shuffle bytes, and task 1237 contributed 810 shuffle bytes.
    assert(stage1Data.shuffleReadTotalBytes == 1220)
    assert(stage0Data.shuffleWriteBytes == 406)
    assert(stage1Data.shuffleWriteBytes == 606)
    assert(stage0Data.executorRunTime == 408)
    assert(stage1Data.executorRunTime == 608)
    assert(stage0Data.diskBytesSpilled == 410)
    assert(stage1Data.diskBytesSpilled == 610)
    assert(stage0Data.memoryBytesSpilled == 412)
    assert(stage1Data.memoryBytesSpilled == 612)
    assert(stage0Data.inputBytes == 414)
    assert(stage1Data.inputBytes == 614)
    assert(stage0Data.outputBytes == 416)
    assert(stage1Data.outputBytes == 616)
    assert(
      stage0Data.taskData.get(1234L).get.metrics.get.shuffleReadMetrics.totalBlocksFetched == 302)
    assert(
      stage1Data.taskData.get(1237L).get.metrics.get.shuffleReadMetrics.totalBlocksFetched == 402)
  }

  test("drop internal and sql accumulators") {
    val taskInfo = new TaskInfo(0, 0, 0, 0, "", "", TaskLocality.ANY, false)
    val internalAccum =
      AccumulableInfo(id = 1, name = Some("internal"), None, None, true, false, None)
    val sqlAccum = AccumulableInfo(
      id = 2,
      name = Some("sql"),
      update = None,
      value = None,
      internal = false,
      countFailedValues = false,
      metadata = Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER))
    val userAccum = AccumulableInfo(
      id = 3,
      name = Some("user"),
      update = None,
      value = None,
      internal = false,
      countFailedValues = false,
      metadata = None)
    taskInfo.setAccumulables(List(internalAccum, sqlAccum, userAccum))

    val newTaskInfo = TaskUIData.dropInternalAndSQLAccumulables(taskInfo)
    assert(newTaskInfo.accumulables === Seq(userAccum))
  }
}
