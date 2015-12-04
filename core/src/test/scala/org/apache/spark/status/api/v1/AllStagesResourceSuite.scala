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

package org.apache.spark.status.api.v1

import java.util.Date
import scala.collection.mutable.HashMap

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.{StageInfo, TaskInfo, TaskLocality}
import org.apache.spark.ui.jobs.UIData.{StageUIData, TaskUIData}


class AllStagesResourceSuite extends SparkFunSuite {

  test("test firstTaskLaunchedTime, there are no tasks") {

    val status = StageStatus.PENDING
    val stageInfo = new StageInfo(
      1, 1, "stage 1", 10, Seq.empty, Seq.empty, "details abc", Seq.empty)
    val includeDetails = false

    val noTasks = new StageUIData()

    var actual = AllStagesResource.stageUiToStageData(
      status, stageInfo, noTasks, includeDetails)

    assert(actual.firstTaskLaunchedTime == None)
  }

  test("test firstTaskLaunchedTime, there are tasks but none launched") {

    val status = StageStatus.ACTIVE
    val stageInfo = new StageInfo(
      1, 1, "stage 1", 10, Seq.empty, Seq.empty, "details abc", Seq.empty)
    val includeDetails = false

    // generate some tasks, launched time is minus
    val taskNoLaunched1 = new TaskUIData(
      new TaskInfo(1, 1, 1, -100, "", "", TaskLocality.ANY, false), None, None)
    val taskNoLaunched2 = new TaskUIData(
      new TaskInfo(1, 1, 1, -200, "", "", TaskLocality.ANY, false), None, None)
    val taskNoLaunched3 = new TaskUIData(
      new TaskInfo(1, 1, 1, -300, "", "", TaskLocality.ANY, false), None, None)

    // construct hashmap
    var taskDataNoLaunched = new HashMap[Long, TaskUIData]
    taskDataNoLaunched.put(1, taskNoLaunched1)
    taskDataNoLaunched.put(2, taskNoLaunched2)
    taskDataNoLaunched.put(3, taskNoLaunched3)

    val tasksNoLaunched = new StageUIData()
    tasksNoLaunched.taskData = taskDataNoLaunched

    val actual = AllStagesResource.stageUiToStageData(
      status, stageInfo, tasksNoLaunched, includeDetails)

    assert(actual.firstTaskLaunchedTime == None)
  }

  test("test firstTaskLaunchedTime, there are tasks and some launched") {

    val status = StageStatus.COMPLETE
    val stageInfo = new StageInfo(
      1, 1, "stage 1", 10, Seq.empty, Seq.empty, "details abc", Seq.empty)
    val includeDetails = false

    // generate some tasks, min lauched time 1449255596000L
    val taskLaunched1 = new TaskUIData(
      new TaskInfo(1, 1, 1, 1449255596000L, "", "", TaskLocality.ANY, false), None, None)
    val taskLaunched2 = new TaskUIData(
      new TaskInfo(1, 1, 1, 1449255597000L, "", "", TaskLocality.ANY, false), None, None)
    val taskLaunched3 = new TaskUIData(
      new TaskInfo(1, 1, 1, -100, "", "", TaskLocality.ANY, false), None, None)

    // construct hashmap
    var taskDataLaunched = new HashMap[Long, TaskUIData]
    taskDataLaunched.put(1, taskLaunched1)
    taskDataLaunched.put(2, taskLaunched2)
    taskDataLaunched.put(3, taskLaunched3)

    val tasksLaunched = new StageUIData()
    tasksLaunched.taskData = taskDataLaunched

    val actual = AllStagesResource.stageUiToStageData(
      status, stageInfo, tasksLaunched, includeDetails)

    assert(actual.firstTaskLaunchedTime == Some(new Date(1449255596000L)))
  }

}
