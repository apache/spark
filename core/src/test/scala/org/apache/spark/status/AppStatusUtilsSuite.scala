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

import java.util.Date

import org.apache.spark.SparkFunSuite
import org.apache.spark.status.api.v1.{TaskData, TaskMetrics}

class AppStatusUtilsSuite extends SparkFunSuite {

  test("schedulerDelay") {
    val runningTask = new TaskData(
      taskId = 0,
      index = 0,
      attempt = 0,
      partitionId = 0,
      launchTime = new Date(1L),
      resultFetchStart = None,
      duration = Some(100L),
      executorId = "1",
      host = "localhost",
      status = "RUNNING",
      taskLocality = "PROCESS_LOCAL",
      speculative = false,
      accumulatorUpdates = Nil,
      errorMessage = None,
      taskMetrics = Some(new TaskMetrics(
        executorDeserializeTime = 0L,
        executorDeserializeCpuTime = 0L,
        executorRunTime = 0L,
        executorCpuTime = 0L,
        resultSize = 0L,
        jvmGcTime = 0L,
        resultSerializationTime = 0L,
        memoryBytesSpilled = 0L,
        diskBytesSpilled = 0L,
        peakExecutionMemory = 0L,
        inputMetrics = null,
        outputMetrics = null,
        shuffleReadMetrics = null,
        shuffleWriteMetrics = null)),
      executorLogs = null,
      schedulerDelay = 0L,
      gettingResultTime = 0L)
    assert(AppStatusUtils.schedulerDelay(runningTask) === 0L)

    val finishedTask = new TaskData(
      taskId = 0,
      index = 0,
      attempt = 0,
      partitionId = 0,
      launchTime = new Date(1L),
      resultFetchStart = None,
      duration = Some(100L),
      executorId = "1",
      host = "localhost",
      status = "SUCCESS",
      taskLocality = "PROCESS_LOCAL",
      speculative = false,
      accumulatorUpdates = Nil,
      errorMessage = None,
      taskMetrics = Some(new TaskMetrics(
        executorDeserializeTime = 5L,
        executorDeserializeCpuTime = 3L,
        executorRunTime = 90L,
        executorCpuTime = 10L,
        resultSize = 100L,
        jvmGcTime = 10L,
        resultSerializationTime = 2L,
        memoryBytesSpilled = 0L,
        diskBytesSpilled = 0L,
        peakExecutionMemory = 100L,
        inputMetrics = null,
        outputMetrics = null,
        shuffleReadMetrics = null,
        shuffleWriteMetrics = null)),
      executorLogs = null,
      schedulerDelay = 0L,
      gettingResultTime = 0L)
    assert(AppStatusUtils.schedulerDelay(finishedTask) === 3L)
  }
}
