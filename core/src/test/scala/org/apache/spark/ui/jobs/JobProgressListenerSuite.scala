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
import org.apache.spark.executor.{ShuffleReadMetrics, TaskMetrics}
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
    taskMetrics.updateShuffleReadMetrics(shuffleReadMetrics)
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
}
