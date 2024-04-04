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

package org.apache.spark.sql.connect.execution

import java.util.Properties

import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkFunSuite, Success}
import org.apache.spark.executor.{ExecutorMetrics, InputMetrics, TaskMetrics}
import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerTaskStart, StageInfo, TaskInfo}

class ConnectProgressExecutionListenerSuite extends SparkFunSuite with MockitoSugar {

  def mockStage(stageId: Int, numTasks: Int): StageInfo = {
    val result = mock[StageInfo]
    when(result.stageId).thenReturn(stageId)
    when(result.numTasks).thenReturn(numTasks)
    result
  }

  val testTag = "testTag"
  val testStage1 = mockStage(1, 1)
  val testStage2 = mockStage(2, 1)

  val testStage1Task1 = mock[TaskInfo]
  val testStage1Task1ExecutorMetrics = mock[ExecutorMetrics]
  val testStage1Task1Metrics = mock[TaskMetrics]

  val inputMetrics = mock[InputMetrics]
  when(inputMetrics.bytesRead).thenReturn(500)
  when(testStage1Task1Metrics.inputMetrics).thenReturn(inputMetrics)

  val testStage2Task1 = mock[TaskInfo]
//
  val testProperties = new Properties()
  testProperties.setProperty("spark.job.tags", s"otherTag,$testTag,anotherTag")

  val testJobStart = SparkListenerJobStart(1, 1, Seq(testStage1, testStage2), testProperties)
  val testTaskStart = SparkListenerTaskStart(1, 1, testStage1Task1)

  test("onJobStart with no matching tags") {
    val listener = new ConnectProgressExecutionListener
    listener.onJobStart(testJobStart)
    assert(listener.trackedTags.isEmpty)
  }

  test("onJobStart with a registered tag") {
    val listener = new ConnectProgressExecutionListener
    listener.registerJobTag(testTag)
    assert(listener.trackedTags.size == 1)

    // Trigger the event
    listener.onJobStart(testJobStart)
    val t = listener.trackedTags(testTag)

    t.yieldWhenDirty((stages, inflight) => {
      assert(stages.map(_.numTasks).sum == 2)
      assert(stages.map(_.completedTasks).sum == 0)
      assert(stages.size == 2)
      assert(stages.map(_.inputBytesRead).sum == 0)
      assert(inflight == 0)
    })
  }

  test("taskDone") {
    val listener = new ConnectProgressExecutionListener
    listener.registerJobTag(testTag)
    listener.onJobStart(testJobStart)

    // Finish the tasks
    val taskEnd = SparkListenerTaskEnd(
      1,
      1,
      "taskType",
      Success,
      testStage1Task1,
      testStage1Task1ExecutorMetrics,
      testStage1Task1Metrics)

    val t = listener.trackedTags(testTag)
    var yielded = false
    t.yieldWhenDirty { (stages, inflight) =>
      assert(stages.map(_.numTasks).sum == 2)
      assert(stages.map(_.completedTasks).sum == 0)
      assert(stages.size == 2)
      assert(
        stages
          .map(_.completed match {
            case true => 1
            case false => 0
          })
          .sum == 0)
      yielded = true
    }
    assert(yielded, "Must updated with results")

    yielded = false
    listener.onTaskEnd(taskEnd)
    t.yieldWhenDirty { (stages, inflight) =>
      assert(stages.map(_.numTasks).sum == 2)
      assert(stages.map(_.completedTasks).sum == 1)
      assert(stages.size == 2)
      assert(stages.map(_.inputBytesRead).sum == 500)
      assert(
        stages
          .map(_.completed match {
            case true => 1
            case false => 0
          })
          .sum == 0)
      yielded = true
    }
    assert(yielded, "Must updated with results")
    yielded = false
    t.yieldWhenDirty { (stages, inflight) =>
      yielded = true
    }
    assert(!yielded, "Must not update if not dirty")

    val stageEnd = SparkListenerStageCompleted(testStage1)
    listener.onStageCompleted(stageEnd)
    t.yieldWhenDirty { (stages, inflight) =>
      assert(stages.map(_.numTasks).sum == 2)
      assert(stages.map(_.completedTasks).sum == 1)
      assert(stages.size == 2)
      assert(stages.map(_.inputBytesRead).sum == 500)
      assert(
        stages
          .map(_.completed match {
            case true => 1
            case false => 0
          })
          .sum == 1)
      yielded = true
    }
    assert(yielded, "Must updated with results")
  }

}
