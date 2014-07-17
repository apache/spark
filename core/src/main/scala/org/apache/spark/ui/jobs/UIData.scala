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

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection.mutable.HashMap

private[jobs] object UIData {

  class ExecutorSummary {
    var taskTime : Long = 0
    var failedTasks : Int = 0
    var succeededTasks : Int = 0
    var inputBytes : Long = 0
    var shuffleRead : Long = 0
    var shuffleWrite : Long = 0
    var memoryBytesSpilled : Long = 0
    var diskBytesSpilled : Long = 0
  }

  class StageUIData {
    var numActiveTasks: Int = _
    var numCompleteTasks: Int = _
    var numFailedTasks: Int = _

    var executorRunTime: Long = _

    var inputBytes: Long = _
    var shuffleReadBytes: Long = _
    var shuffleWriteBytes: Long = _
    var memoryBytesSpilled: Long = _
    var diskBytesSpilled: Long = _

    var schedulingPool: String = ""
    var description: Option[String] = None

    var taskData = new HashMap[Long, TaskUIData]
    var executorSummary = new HashMap[String, ExecutorSummary]
  }

  case class TaskUIData(
      taskInfo: TaskInfo,
      taskMetrics: Option[TaskMetrics] = None,
      errorMessage: Option[String] = None)
}
