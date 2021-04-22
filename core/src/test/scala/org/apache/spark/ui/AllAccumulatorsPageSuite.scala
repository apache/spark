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

package org.apache.spark.ui

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ListBuffer

import org.mockito.Mockito.{mock, RETURNS_SMART_NULLS}

import org.apache.spark._
import org.apache.spark.internal.config.Status._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ AccumulableInfo => UIAccumulableInfo, StageData, StageStatus}
import org.apache.spark.ui.accm._

class AllAccumulatorsPageSuite extends SparkFunSuite with LocalSparkContext {

  val conf = new SparkConf(false).set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
  val statusStore = AppStatusStore.createLiveStore(conf)

  test("check contents of tempMap") {
    val mockStageData1 = new StageData(
      status = StageStatus.COMPLETE,
      stageId = 1,
      attemptId = 1,
      numTasks = 1,
      numActiveTasks = 1,
      numCompleteTasks = 1,
      numFailedTasks = 1,
      numKilledTasks = 1,
      numCompletedIndices = 1,

      submissionTime = None,
      firstTaskLaunchedTime = None,
      completionTime = None,
      failureReason = None,

      executorDeserializeTime = 1L,
      executorDeserializeCpuTime = 1L,
      executorRunTime = 1L,
      executorCpuTime = 1L,
      resultSize = 1L,
      jvmGcTime = 1L,
      resultSerializationTime = 1L,
      memoryBytesSpilled = 1L,
      diskBytesSpilled = 1L,
      peakExecutionMemory = 1L,
      inputBytes = 1L,
      inputRecords = 1L,
      outputBytes = 1L,
      outputRecords = 1L,
      shuffleRemoteBlocksFetched = 1L,
      shuffleLocalBlocksFetched = 1L,
      shuffleFetchWaitTime = 1L,
      shuffleRemoteBytesRead = 1L,
      shuffleRemoteBytesReadToDisk = 1L,
      shuffleLocalBytesRead = 1L,
      shuffleReadBytes = 1L,
      shuffleReadRecords = 1L,
      shuffleWriteBytes = 1L,
      shuffleWriteTime = 1L,
      shuffleWriteRecords = 1L,

      name = "stage1",
      description = Some("description"),
      details = "detail",
      schedulingPool = "pool1",

      rddIds = Seq(1),
      accumulatorUpdates = Seq(new UIAccumulableInfo(0L, "test_acc1", None, "000100"),
        new UIAccumulableInfo(1L, "test_acc2", None, "000200")),
      tasks = None,
      executorSummary = None,
      killedTasksSummary = Map.empty,
      ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID,
      peakExecutorMetrics = None,
      taskMetricsDistributions = None,
      executorMetricsDistributions = None
    )
    val mockStageData2 = new StageData(
      status = StageStatus.COMPLETE,
      stageId = 2,
      attemptId = 1,
      numTasks = 1,
      numActiveTasks = 1,
      numCompleteTasks = 1,
      numFailedTasks = 1,
      numKilledTasks = 1,
      numCompletedIndices = 1,

      submissionTime = None,
      firstTaskLaunchedTime = None,
      completionTime = None,
      failureReason = None,

      executorDeserializeTime = 1L,
      executorDeserializeCpuTime = 1L,
      executorRunTime = 1L,
      executorCpuTime = 1L,
      resultSize = 1L,
      jvmGcTime = 1L,
      resultSerializationTime = 1L,
      memoryBytesSpilled = 1L,
      diskBytesSpilled = 1L,
      peakExecutionMemory = 1L,
      inputBytes = 1L,
      inputRecords = 1L,
      outputBytes = 1L,
      outputRecords = 1L,
      shuffleRemoteBlocksFetched = 1L,
      shuffleLocalBlocksFetched = 1L,
      shuffleFetchWaitTime = 1L,
      shuffleRemoteBytesRead = 1L,
      shuffleRemoteBytesReadToDisk = 1L,
      shuffleLocalBytesRead = 1L,
      shuffleReadBytes = 1L,
      shuffleReadRecords = 1L,
      shuffleWriteBytes = 1L,
      shuffleWriteTime = 1L,
      shuffleWriteRecords = 1L,

      name = "stage1",
      description = Some("description"),
      details = "detail",
      schedulingPool = "pool1",

      rddIds = Seq(1),
      accumulatorUpdates = Seq(new UIAccumulableInfo(0L, "test_acc1", None, "000300"),
        new UIAccumulableInfo(1L, "test_acc2", None, "000300")),
      tasks = None,
      executorSummary = None,
      killedTasksSummary = Map.empty,
      ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID,
      peakExecutorMetrics = None,
      taskMetricsDistributions = None,
      executorMetricsDistributions = None
    )
    val mockStageData3 = new StageData(
      status = StageStatus.COMPLETE,
      stageId = 2,
      attemptId = 1,
      numTasks = 1,
      numActiveTasks = 1,
      numCompleteTasks = 1,
      numFailedTasks = 1,
      numKilledTasks = 1,
      numCompletedIndices = 1,

      submissionTime = None,
      firstTaskLaunchedTime = None,
      completionTime = None,
      failureReason = None,

      executorDeserializeTime = 1L,
      executorDeserializeCpuTime = 1L,
      executorRunTime = 1L,
      executorCpuTime = 1L,
      resultSize = 1L,
      jvmGcTime = 1L,
      resultSerializationTime = 1L,
      memoryBytesSpilled = 1L,
      diskBytesSpilled = 1L,
      peakExecutionMemory = 1L,
      inputBytes = 1L,
      inputRecords = 1L,
      outputBytes = 1L,
      outputRecords = 1L,
      shuffleRemoteBlocksFetched = 1L,
      shuffleLocalBlocksFetched = 1L,
      shuffleFetchWaitTime = 1L,
      shuffleRemoteBytesRead = 1L,
      shuffleRemoteBytesReadToDisk = 1L,
      shuffleLocalBytesRead = 1L,
      shuffleReadBytes = 1L,
      shuffleReadRecords = 1L,
      shuffleWriteBytes = 1L,
      shuffleWriteTime = 1L,
      shuffleWriteRecords = 1L,

      name = "stage1",
      description = Some("description"),
      details = "detail",
      schedulingPool = "pool1",

      rddIds = Seq(1),
      accumulatorUpdates = Seq(new UIAccumulableInfo(1L, "test_acc2", None, "000600")),
      tasks = None,
      executorSummary = None,
      killedTasksSummary = Map.empty,
      ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID,
      peakExecutorMetrics = None,
      taskMetricsDistributions = None,
      executorMetricsDistributions = None
    )

    val basePath = ""
    val tab = mock(classOf[AccumulatorsTab], RETURNS_SMART_NULLS)
    val page = new AllAccumulatorsPage(tab, statusStore)

    val stages = Array(mockStageData1, mockStageData2, mockStageData3)

    stages.foreach(stage => {
      stage.accumulatorUpdates.foreach(acc => {
        page.addStageAcc(acc, basePath, stage.stageId, stage.attemptId)
      })
    })

    var tempMapMock = SortedMap[String, ListBuffer[AccumulatorInfo]]()

    stages.foreach(stage => {
      stage.accumulatorUpdates.foreach(acc => {
        val accNameLinkUri =
          s"$basePath/accumulators/accumulator/?accumulator_name=" +
            s"${acc.name}&attempt=${stage.attemptId}"

        val stageLinkUri =
          s"$basePath/accumulators/stage/?accumulator_name=" +
            s"${acc.name}&id=${stage.stageId}&attempt=${stage.attemptId}"

        def addAccToMap(): ListBuffer[AccumulatorInfo] = {
          tempMapMock += (acc.name -> new ListBuffer[AccumulatorInfo]())
          tempMapMock(acc.name)
        }
        val newList = tempMapMock.getOrElse(acc.name, addAccToMap()):+
          AccumulatorInfo(stage.stageId, acc.name, acc.value, stageLinkUri, accNameLinkUri)
        tempMapMock+=(acc.name -> newList)
      })
    })
    assert(tempMapMock == page.displayMap())
  }
}
