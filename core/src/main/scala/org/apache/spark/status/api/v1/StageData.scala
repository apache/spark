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

import org.apache.spark.status.api.StageStatus

import scala.collection.Map

case class StageData(
  status: StageStatus,
  stageId: Int,
  numActiveTasks: Int ,
  numCompleteTasks: Int,
  numFailedTasks: Int,

  executorRunTime: Long,

  inputBytes: Long,
  inputRecords: Long,
  outputBytes: Long,
  outputRecords: Long,
  shuffleReadBytes: Long,
  shuffleReadRecords: Long,
  shuffleWriteBytes: Long,
  shuffleWriteRecords: Long,
  memoryBytesSpilled: Long,
  diskBytesSpilled: Long,

  name: String,
  details: String,
  schedulingPool: String,

  //TODO what to do about accumulables?
  tasks: Option[Map[Long, TaskData]],
  executorSummary:Option[Map[String,ExecutorStageSummary]]
)

