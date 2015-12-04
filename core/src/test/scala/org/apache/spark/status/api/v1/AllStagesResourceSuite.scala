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

import org.scalatest.Matchers
import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.ui.jobs.UIData.StageUIData

class AllStagesResourceSuite extends SparkFunSuite with Matchers {

  test("test test") {
    val s = "abc"
    assert(s === "abc")
  }

  test("test class instantiation") {
    val status = StageStatus.COMPLETE
    val stageInfo = new StageInfo(
      1, 1, "stage 1", 10, Seq.empty, Seq.empty, "details abc", Seq.empty)
    val stageUiData = new StageUIData()
    val includeDetails = false

    val actual = AllStagesResource.stageUiToStageData(
      status, stageInfo, stageUiData, includeDetails)
    val expected = new StageData(status, 1, 1, 0, 0, 0, 0, None, None, None,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "stage 1", "details abc", "", Seq.empty, None, None)

    assert(actual.status == expected.status)
    assert(actual.stageId == expected.stageId)
    assert(actual.attemptId == expected.attemptId)
    assert(actual.numActiveTasks == expected.numActiveTasks)
    assert(actual.numCompleteTasks == expected.numCompleteTasks)
    assert(actual.executorRunTime == expected.executorRunTime)
    assert(actual.submissionTime == expected.submissionTime)
    assert(actual.firstTaskLaunchedTime == expected.firstTaskLaunchedTime)
    assert(actual.completionTime == expected.completionTime)
    assert(actual.inputBytes == expected.inputBytes)
    assert(actual.inputRecords == expected.inputRecords)
    assert(actual.outputBytes == expected.outputBytes)
    assert(actual.outputRecords == expected.outputRecords)
    assert(actual.shuffleReadBytes == expected.shuffleReadBytes)
    assert(actual.shuffleReadRecords == expected.shuffleReadRecords)
    assert(actual.shuffleWriteBytes == expected.shuffleWriteBytes)
    assert(actual.shuffleWriteRecords == expected.shuffleWriteRecords)
    assert(actual.memoryBytesSpilled == expected.memoryBytesSpilled)
    assert(actual.diskBytesSpilled == expected.diskBytesSpilled)
    assert(actual.name == expected.name)
    assert(actual.details == expected.details)
    assert(actual.schedulingPool == expected.schedulingPool)
    assert(actual.accumulatorUpdates == expected.accumulatorUpdates)
    assert(actual.tasks == expected.tasks)
    assert(actual.executorSummary == expected.executorSummary)


//    println(actual.toString)
//    println(expected.toString)

//    actual should equal(expected)


//    val expectedDup = new StageData(status, 1, 1, 0, 0, 0, 0, None, None, None,
//      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "stage 1", "details abc", "", Seq.empty, None, None)

    // below is false
    // expected should equal (expectedDup)

    // below is false
    // assert(expectedDup equals expected)

    // below is false
    // assert(expectedDup == expected)

  }

}

