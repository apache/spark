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

import scala.collection.mutable.LinkedHashMap

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.{StageInfo, TaskInfo, TaskLocality}
import org.apache.spark.ui.jobs.UIData.{StageUIData, TaskUIData}

class AllStagesResourceSuite extends SparkFunSuite {

  def getFirstTaskLaunchTime(taskLaunchTimes: Seq[Long]): Option[Date] = {
    val tasks = new LinkedHashMap[Long, TaskUIData]
    taskLaunchTimes.zipWithIndex.foreach { case (time, idx) =>
      tasks(idx.toLong) = TaskUIData(
        new TaskInfo(idx, idx, 1, time, "", "", TaskLocality.ANY, false), None)
    }

    val stageUiData = new StageUIData()
    stageUiData.taskData = tasks
    val status = StageStatus.ACTIVE
    val stageInfo = new StageInfo(
      1, 1, "stage 1", 10, Seq.empty, Seq.empty, "details abc")
    val stageData = AllStagesResource.stageUiToStageData(status, stageInfo, stageUiData, false)

    stageData.firstTaskLaunchedTime
  }

  test("firstTaskLaunchedTime when there are no tasks") {
    val result = getFirstTaskLaunchTime(Seq())
    assert(result == None)
  }

  test("firstTaskLaunchedTime when there are tasks but none launched") {
    val result = getFirstTaskLaunchTime(Seq(-100L, -200L, -300L))
    assert(result == None)
  }

  test("firstTaskLaunchedTime when there are tasks and some launched") {
    val result = getFirstTaskLaunchTime(Seq(-100L, 1449255596000L, 1449255597000L))
    assert(result == Some(new Date(1449255596000L)))
  }

}
