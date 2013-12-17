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
import org.apache.spark.scheduler._
import org.apache.spark.{LocalSparkContext, SparkContext, Success}
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.executor.{ShuffleReadMetrics, TaskMetrics}

class JobProgressListenerSuite extends FunSuite with LocalSparkContext {
  test("test executor id to summary") {
    val sc = new SparkContext("local", "test")
    val listener = new JobProgressListener(sc)
    val taskMetrics = new TaskMetrics()
    val shuffleReadMetrics = new ShuffleReadMetrics()

    // nothing in it
    assert(listener.stageIdToExecutorSummaries.size == 0)

    // finish this task, should get updated shuffleRead
    shuffleReadMetrics.remoteBytesRead = 1000
    taskMetrics.shuffleReadMetrics = Some(shuffleReadMetrics)
    var taskInfo = new TaskInfo(1234L, 0, 0L, "exe-1", "host1", TaskLocality.NODE_LOCAL)
    taskInfo.finishTime = 1
    listener.onTaskEnd(new SparkListenerTaskEnd(
      new ShuffleMapTask(0, null, null, 0, null), Success, taskInfo, taskMetrics))
    assert(listener.stageIdToExecutorSummaries.getOrElse(0, fail()).getOrElse("exe-1", fail())
      .shuffleRead == 1000)

    // finish a task with unknown executor-id, nothing should happen
    taskInfo = new TaskInfo(1234L, 0, 1000L, "exe-unknown", "host1", TaskLocality.NODE_LOCAL)
    taskInfo.finishTime = 1
    listener.onTaskEnd(new SparkListenerTaskEnd(
      new ShuffleMapTask(0, null, null, 0, null), Success, taskInfo, taskMetrics))
    assert(listener.stageIdToExecutorSummaries.size == 1)

    // finish this task, should get updated duration
    shuffleReadMetrics.remoteBytesRead = 1000
    taskMetrics.shuffleReadMetrics = Some(shuffleReadMetrics)
    taskInfo = new TaskInfo(1235L, 0, 0L, "exe-1", "host1", TaskLocality.NODE_LOCAL)
    taskInfo.finishTime = 1
    listener.onTaskEnd(new SparkListenerTaskEnd(
      new ShuffleMapTask(0, null, null, 0, null), Success, taskInfo, taskMetrics))
    assert(listener.stageIdToExecutorSummaries.getOrElse(0, fail()).getOrElse("exe-1", fail())
      .shuffleRead == 2000)

    // finish this task, should get updated duration
    shuffleReadMetrics.remoteBytesRead = 1000
    taskMetrics.shuffleReadMetrics = Some(shuffleReadMetrics)
    taskInfo = new TaskInfo(1236L, 0, 0L, "exe-2", "host1", TaskLocality.NODE_LOCAL)
    taskInfo.finishTime = 1
    listener.onTaskEnd(new SparkListenerTaskEnd(
      new ShuffleMapTask(0, null, null, 0, null), Success, taskInfo, taskMetrics))
    assert(listener.stageIdToExecutorSummaries.getOrElse(0, fail()).getOrElse("exe-2", fail())
      .shuffleRead == 1000)
  }
}
