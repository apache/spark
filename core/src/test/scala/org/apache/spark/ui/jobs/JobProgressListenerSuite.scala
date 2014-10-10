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

import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.apache.spark._
import org.apache.spark.{LocalSparkContext, SparkConf, Success}
import org.apache.spark.executor._
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

class JobProgressListenerSuite extends FunSuite with LocalSparkContext with Matchers {

  test("test LRU eviction of stages") {
    val conf = new SparkConf()
    conf.set("spark.ui.retainedStages", 5.toString)
    val listener = new JobProgressListener(conf)

    def createStageStartEvent(stageId: Int) = {
      val stageInfo = new StageInfo(stageId, stageId.toString, 0, null, "")
      SparkListenerStageSubmitted(stageInfo)
    }

    def createStageEndEvent(stageId: Int) = {
      val stageInfo = new StageInfo(stageId, stageId.toString, 0, null, "")
      SparkListenerStageCompleted(stageInfo)
    }

    for (i <- 1 to 50) {
      listener.onStageSubmitted(createStageStartEvent(i))
      listener.onStageCompleted(createStageEndEvent(i))
    }

    listener.completedStages.size should be (5)
    listener.completedStages.count(_.stageId == 50) should be (1)
    listener.completedStages.count(_.stageId == 49) should be (1)
    listener.completedStages.count(_.stageId == 48) should be (1)
    listener.completedStages.count(_.stageId == 47) should be (1)
    listener.completedStages.count(_.stageId == 46) should be (1)
  }

  test("test executor id to summary") {
    val conf = new SparkConf()
    val listener = new JobProgressListener(conf)
    val taskMetrics = new TaskMetrics()
    val shuffleReadMetrics = new ShuffleReadMetrics()
    assert(listener.stageIdToData.size === 0)

    // finish this task, should get updated shuffleRead
    shuffleReadMetrics.remoteBytesRead = 1000
    taskMetrics.setShuffleReadMetrics(Some(shuffleReadMetrics))
    var taskInfo = new TaskInfo(1234L, 0, 1, 0L, "exe-1", "host1", TaskLocality.NODE_LOCAL, false)
    taskInfo.finishTime = 1
    var task = new ShuffleMapTask(0)
    val taskType = Utils.getFormattedClassName(task)
    listener.onTaskEnd(SparkListenerTaskEnd(task.stageId, taskType, Success, taskInfo, taskMetrics))
    assert(listener.stageIdToData.getOrElse(0, fail()).executorSummary.getOrElse("exe-1", fail())
      .shuffleRead === 1000)

    // finish a task with unknown executor-id, nothing should happen
    taskInfo =
      new TaskInfo(1234L, 0, 1, 1000L, "exe-unknown", "host1", TaskLocality.NODE_LOCAL, true)
    taskInfo.finishTime = 1
    task = new ShuffleMapTask(0)
    listener.onTaskEnd(SparkListenerTaskEnd(task.stageId, taskType, Success, taskInfo, taskMetrics))
    assert(listener.stageIdToData.size === 1)

    // finish this task, should get updated duration
    taskInfo = new TaskInfo(1235L, 0, 1, 0L, "exe-1", "host1", TaskLocality.NODE_LOCAL, false)
    taskInfo.finishTime = 1
    task = new ShuffleMapTask(0)
    listener.onTaskEnd(SparkListenerTaskEnd(task.stageId, taskType, Success, taskInfo, taskMetrics))
    assert(listener.stageIdToData.getOrElse(0, fail()).executorSummary.getOrElse("exe-1", fail())
      .shuffleRead === 2000)

    // finish this task, should get updated duration
    taskInfo = new TaskInfo(1236L, 0, 2, 0L, "exe-2", "host1", TaskLocality.NODE_LOCAL, false)
    taskInfo.finishTime = 1
    task = new ShuffleMapTask(0)
    listener.onTaskEnd(SparkListenerTaskEnd(task.stageId, taskType, Success, taskInfo, taskMetrics))
    assert(listener.stageIdToData.getOrElse(0, fail()).executorSummary.getOrElse("exe-2", fail())
      .shuffleRead === 1000)
  }

  test("test task success vs failure counting for different task end reasons") {
    val conf = new SparkConf()
    val listener = new JobProgressListener(conf)
    val metrics = new TaskMetrics()
    val taskInfo = new TaskInfo(1234L, 0, 3, 0L, "exe-1", "host1", TaskLocality.NODE_LOCAL, false)
    taskInfo.finishTime = 1
    val task = new ShuffleMapTask(0)
    val taskType = Utils.getFormattedClassName(task)

    // Go through all the failure cases to make sure we are counting them as failures.
    val taskFailedReasons = Seq(
      Resubmitted,
      new FetchFailed(null, 0, 0, 0),
      new ExceptionFailure("Exception", "description", null, None),
      TaskResultLost,
      TaskKilled,
      ExecutorLostFailure,
      UnknownReason)
    var failCount = 0
    for (reason <- taskFailedReasons) {
      listener.onTaskEnd(SparkListenerTaskEnd(task.stageId, taskType, reason, taskInfo, metrics))
      failCount += 1
      assert(listener.stageIdToData(task.stageId).numCompleteTasks === 0)
      assert(listener.stageIdToData(task.stageId).numFailedTasks === failCount)
    }

    // Make sure we count success as success.
    listener.onTaskEnd(SparkListenerTaskEnd(task.stageId, taskType, Success, taskInfo, metrics))
    assert(listener.stageIdToData(task.stageId).numCompleteTasks === 1)
    assert(listener.stageIdToData(task.stageId).numFailedTasks === failCount)
  }

  test("test update metrics") {
    val conf = new SparkConf()
    val listener = new JobProgressListener(conf)

    val taskType = Utils.getFormattedClassName(new ShuffleMapTask(0))
    val execId = "exe-1"

    def makeTaskMetrics(base: Int) = {
      val taskMetrics = new TaskMetrics()
      val shuffleReadMetrics = new ShuffleReadMetrics()
      val shuffleWriteMetrics = new ShuffleWriteMetrics()
      taskMetrics.setShuffleReadMetrics(Some(shuffleReadMetrics))
      taskMetrics.shuffleWriteMetrics = Some(shuffleWriteMetrics)
      shuffleReadMetrics.remoteBytesRead = base + 1
      shuffleReadMetrics.remoteBlocksFetched = base + 2
      shuffleWriteMetrics.shuffleBytesWritten = base + 3
      taskMetrics.executorRunTime = base + 4
      taskMetrics.diskBytesSpilled = base + 5
      taskMetrics.memoryBytesSpilled = base + 6
      val inputMetrics = new InputMetrics(DataReadMethod.Hadoop)
      taskMetrics.inputMetrics = Some(inputMetrics)
      inputMetrics.bytesRead = base + 7
      taskMetrics
    }

    def makeTaskInfo(taskId: Long, finishTime: Int = 0) = {
      val taskInfo = new TaskInfo(taskId, 0, 1, 0L, execId, "host1", TaskLocality.NODE_LOCAL,
        false)
      taskInfo.finishTime = finishTime
      taskInfo
    }

    listener.onTaskStart(SparkListenerTaskStart(0, makeTaskInfo(1234L)))
    listener.onTaskStart(SparkListenerTaskStart(0, makeTaskInfo(1235L)))
    listener.onTaskStart(SparkListenerTaskStart(1, makeTaskInfo(1236L)))
    listener.onTaskStart(SparkListenerTaskStart(1, makeTaskInfo(1237L)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate(execId, Array(
      (1234L, 0, makeTaskMetrics(0)),
      (1235L, 0, makeTaskMetrics(100)),
      (1236L, 1, makeTaskMetrics(200)))))

    var stage0Data = listener.stageIdToData.get(0).get
    var stage1Data = listener.stageIdToData.get(1).get
    assert(stage0Data.shuffleReadBytes == 102)
    assert(stage1Data.shuffleReadBytes == 201)
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
    assert(stage0Data.taskData.get(1234L).get.taskMetrics.get.shuffleReadMetrics.get
      .totalBlocksFetched == 2)
    assert(stage0Data.taskData.get(1235L).get.taskMetrics.get.shuffleReadMetrics.get
      .totalBlocksFetched == 102)
    assert(stage1Data.taskData.get(1236L).get.taskMetrics.get.shuffleReadMetrics.get
      .totalBlocksFetched == 202)

    // task that was included in a heartbeat
    listener.onTaskEnd(SparkListenerTaskEnd(0, taskType, Success, makeTaskInfo(1234L, 1),
      makeTaskMetrics(300)))
    // task that wasn't included in a heartbeat
    listener.onTaskEnd(SparkListenerTaskEnd(1, taskType, Success, makeTaskInfo(1237L, 1),
      makeTaskMetrics(400)))

    stage0Data = listener.stageIdToData.get(0).get
    stage1Data = listener.stageIdToData.get(1).get
    assert(stage0Data.shuffleReadBytes == 402)
    assert(stage1Data.shuffleReadBytes == 602)
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
    assert(stage0Data.taskData.get(1234L).get.taskMetrics.get.shuffleReadMetrics.get
      .totalBlocksFetched == 302)
    assert(stage1Data.taskData.get(1237L).get.taskMetrics.get.shuffleReadMetrics.get
      .totalBlocksFetched == 402)
  }
}
