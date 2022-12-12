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

package org.apache.spark.status.protobuf

import java.util.Date

import org.apache.spark.{JobExecutionStatus, SparkFunSuite}
import org.apache.spark.status.JobDataWrapper
import org.apache.spark.status.api.v1.JobData

class KVStoreProtobufSerializerSuite extends SparkFunSuite {
  private val serializer = new KVStoreProtobufSerializer()

  test("Job data") {
    val input = new JobDataWrapper(
      new JobData(
        jobId = 1,
        name = "test",
        description = Some("test description"),
        submissionTime = Some(new Date(123456L)),
        completionTime = Some(new Date(654321L)),
        stageIds = Seq(1, 2, 3, 4),
        jobGroup = Some("group"),
        status = JobExecutionStatus.UNKNOWN,
        numTasks = 2,
        numActiveTasks = 3,
        numCompletedTasks = 4,
        numSkippedTasks = 5,
        numFailedTasks = 6,
        numKilledTasks = 7,
        numCompletedIndices = 8,
        numActiveStages = 9,
        numCompletedStages = 10,
        numSkippedStages = 11,
        numFailedStages = 12,
        killedTasksSummary = Map("a" -> 1, "b" -> 2)),
      Set(1, 2),
      Some(999)
    )

    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[JobDataWrapper])
    assert(result.info.jobId == input.info.jobId)
    assert(result.info.description == input.info.description)
    assert(result.info.submissionTime == input.info.submissionTime)
    assert(result.info.completionTime == input.info.completionTime)
    assert(result.info.stageIds == input.info.stageIds)
    assert(result.info.jobGroup == input.info.jobGroup)
    assert(result.info.status == input.info.status)
    assert(result.info.numTasks == input.info.numTasks)
    assert(result.info.numActiveTasks == input.info.numActiveTasks)
    assert(result.info.numCompletedTasks == input.info.numCompletedTasks)
    assert(result.info.numSkippedTasks == input.info.numSkippedTasks)
    assert(result.info.numFailedTasks == input.info.numFailedTasks)
    assert(result.info.numKilledTasks == input.info.numKilledTasks)
    assert(result.info.numCompletedIndices == input.info.numCompletedIndices)
    assert(result.info.numActiveStages == input.info.numActiveStages)
    assert(result.info.numCompletedStages == input.info.numCompletedStages)
    assert(result.info.numSkippedStages == input.info.numSkippedStages)
    assert(result.info.numFailedStages == input.info.numFailedStages)
    assert(result.info.killedTasksSummary == input.info.killedTasksSummary)
    assert(result.skippedStages == input.skippedStages)
    assert(result.sqlExecutionId == input.sqlExecutionId)
  }
}

