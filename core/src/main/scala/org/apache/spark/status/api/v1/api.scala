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

import scala.collection.Map

import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.StageStatus

case class ApplicationInfo(
  id: String,
  name: String,
  startTime: Date,
  endTime: Date,
  sparkUser: String,
  completed: Boolean = false
)

case class ExecutorStageSummary(
  taskTime : Long,
  failedTasks : Int,
  succeededTasks : Int,
  inputBytes : Long,
  outputBytes : Long,
  shuffleRead : Long,
  shuffleWrite : Long,
  memoryBytesSpilled : Long,
  diskBytesSpilled : Long
)

case class ExecutorSummary(
  id: String,
  hostPort: String,
  rddBlocks: Int,
  memoryUsed: Long,
  diskUsed: Long,
  activeTasks: Int,
  failedTasks: Int,
  completedTasks: Int,
  totalTasks: Int,
  totalDuration: Long,
  totalInputBytes: Long,
  totalShuffleRead: Long,
  totalShuffleWrite: Long,
  maxMemory: Long,
  executorLogs: Map[String, String]
)

case class JobData(
  jobId: Int,
  name: String,
  description: Option[String],
  submissionTime: Option[Date],
  completionTime: Option[Date],
  stageIds: Seq[Int],
  jobGroup: Option[String],
  status: JobExecutionStatus,
  numTasks: Int,
  numActiveTasks: Int,
  numCompletedTasks: Int,
  numSkippedTasks: Int,
  numFailedTasks: Int,
  numActiveStages: Int,
  numCompletedStages: Int,
  numSkippedStages: Int,
  numFailedStages: Int
)

// Q: should Tachyon size go in here as well?  currently the UI only shows it on the overall storage
// page ... does anybody pay attention to it?
case class RDDStorageInfo(
  id: Int,
  name: String,
  numPartitions: Int,
  numCachedPartitions: Int,
  storageLevel: String,
  memoryUsed: Long,
  diskUsed: Long,
  dataDistribution: Option[Seq[RDDDataDistribution]],
  partitions: Option[Seq[RDDPartitionInfo]]
)

case class RDDDataDistribution(
  address: String,
  memoryUsed: Long,
  memoryRemaining: Long,
  diskUsed: Long
)

case class RDDPartitionInfo(
  blockName: String,
  storageLevel: String,
  memoryUsed: Long,
  diskUsed: Long,
  executors: Seq[String]
)

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

case class TaskData(
  taskId: Long,
  index: Int,
  attempt: Int,
  launchTime: Date,
  executorId: String,
  host: String,
  taskLocality: String,
  speculative: Boolean,
  errorMessage: Option[String] = None,
  taskMetrics: Option[TaskMetrics] = None
)

case class TaskMetrics(
  executorDeserializeTime: Long,
  executorRunTime: Long,
  resultSize: Long,
  jvmGcTime: Long,
  resultSerializationTime: Long,
  memoryBytesSpilled: Long,
  diskBytesSpilled: Long,
  inputMetrics: Option[InputMetrics],
  outputMetrics: Option[OutputMetrics],
  shuffleReadMetrics: Option[ShuffleReadMetrics],
  shuffleWriteMetrics: Option[ShuffleWriteMetrics]
)

case class InputMetrics(
  bytesRead: Long,
  recordsRead: Long
)

case class OutputMetrics(
  bytesWritten: Long,
  recordsWritten: Long
)

case class ShuffleReadMetrics(
  remoteBlocksFetched: Int,
  localBlocksFetched: Int,
  fetchWaitTime: Long,
  remoteBytesRead: Long,
  totalBlocksFetched: Int,
  recordsRead: Long
)

case class ShuffleWriteMetrics(
  bytesWritten: Long,
  writeTime: Long,
  recordsWritten: Long
)
