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

package org.apache.spark.scheduler

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException, TaskState}

class TaskInfoSuite extends SparkFunSuite {

  private def newTaskInfo(): TaskInfo = new TaskInfo(
    taskId = 42L,
    index = 0,
    attemptNumber = 0,
    partitionId = 0,
    launchTime = 100L,
    executorId = "exec-1",
    host = "host-1",
    taskLocality = TaskLocality.PROCESS_LOCAL,
    speculative = false)

  test("duration is not available before the task finishes") {
    val info = newTaskInfo()
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        info.duration
      },
      condition = "UNSUPPORTED_CALL.TASK_NOT_FINISHED",
      sqlState = Some("0A000"),
      parameters = Map(
        "className" -> "org.apache.spark.scheduler.TaskInfo",
        "methodName" -> "duration"))
  }

  test("duration is available once the task has finished") {
    val info = newTaskInfo()
    info.markFinished(TaskState.FINISHED, 300L)
    assert(info.duration === 200L)
  }
}
