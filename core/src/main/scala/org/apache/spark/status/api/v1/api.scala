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

class ApplicationInfo private[spark](
    val id: String,
    val name: String,
    val coresGranted: Option[Int],
    val maxCores: Option[Int],
    val coresPerExecutor: Option[Int],
    val memoryPerExecutorMB: Option[Int],
    val attempts: Seq[ApplicationAttemptInfo])

class ApplicationAttemptInfo private[spark](
    val attemptId: Option[String],
    val startTime: Date,
    val endTime: Date,
    val sparkUser: String,
    val completed: Boolean = false)

class ExecutorStageSummary private[spark](
    val taskTime : Long,
    val failedTasks : Int,
    val succeededTasks : Int,
    val inputBytes : Long,
    val outputBytes : Long,
    val shuffleRead : Long,
    val shuffleWrite : Long,
    val memoryBytesSpilled : Long,
    val diskBytesSpilled : Long)

class ExecutorSummary private[spark](
    val id: String,
    val hostPort: String,
    val rddBlocks: Int,
    val memoryUsed: Long,
    val diskUsed: Long,
    val activeTasks: Int,
    val failedTasks: Int,
    val completedTasks: Int,
    val totalTasks: Int,
    val totalDuration: Long,
    val totalInputBytes: Long,
    val totalShuffleRead: Long,
    val totalShuffleWrite: Long,
    val maxMemory: Long,
    val executorLogs: Map[String, String])

class JobData private[spark](
    val jobId: Int,
    val name: String,
    val description: Option[String],
    val submissionTime: Option[Date],
    val completionTime: Option[Date],
    val stageIds: Seq[Int],
    val jobGroup: Option[String],
    val status: JobExecutionStatus,
    val numTasks: Int,
    val numActiveTasks: Int,
    val numCompletedTasks: Int,
    val numSkippedTasks: Int,
    val numFailedTasks: Int,
    val numActiveStages: Int,
    val numCompletedStages: Int,
    val numSkippedStages: Int,
    val numFailedStages: Int)

// Q: should Tachyon size go in here as well?  currently the UI only shows it on the overall storage
// page ... does anybody pay attention to it?
class RDDStorageInfo private[spark](
    val id: Int,
    val name: String,
    val numPartitions: Int,
    val numCachedPartitions: Int,
    val storageLevel: String,
    val memoryUsed: Long,
    val diskUsed: Long,
    val dataDistribution: Option[Seq[RDDDataDistribution]],
    val partitions: Option[Seq[RDDPartitionInfo]])

class RDDDataDistribution private[spark](
    val address: String,
    val memoryUsed: Long,
    val memoryRemaining: Long,
    val diskUsed: Long)

class RDDPartitionInfo private[spark](
    val blockName: String,
    val storageLevel: String,
    val memoryUsed: Long,
    val diskUsed: Long,
    val executors: Seq[String])

class StageData private[spark](
    val status: StageStatus,
    val stageId: Int,
    val attemptId: Int,
    val numActiveTasks: Int ,
    val numCompleteTasks: Int,
    val numFailedTasks: Int,

    val executorRunTime: Long,

    val inputBytes: Long,
    val inputRecords: Long,
    val outputBytes: Long,
    val outputRecords: Long,
    val shuffleReadBytes: Long,
    val shuffleReadRecords: Long,
    val shuffleWriteBytes: Long,
    val shuffleWriteRecords: Long,
    val memoryBytesSpilled: Long,
    val diskBytesSpilled: Long,

    val name: String,
    val details: String,
    val schedulingPool: String,

    val accumulatorUpdates: Seq[AccumulableInfo],
    val tasks: Option[Map[Long, TaskData]],
    val executorSummary: Option[Map[String, ExecutorStageSummary]])

class TaskData private[spark](
    val taskId: Long,
    val index: Int,
    val attempt: Int,
    val launchTime: Date,
    val executorId: String,
    val host: String,
    val taskLocality: String,
    val speculative: Boolean,
    val accumulatorUpdates: Seq[AccumulableInfo],
    val errorMessage: Option[String] = None,
    val taskMetrics: Option[TaskMetrics] = None)

class TaskMetrics private[spark](
    val executorDeserializeTime: Long,
    val executorRunTime: Long,
    val resultSize: Long,
    val jvmGcTime: Long,
    val resultSerializationTime: Long,
    val memoryBytesSpilled: Long,
    val diskBytesSpilled: Long,
    val inputMetrics: Option[InputMetrics],
    val outputMetrics: Option[OutputMetrics],
    val shuffleReadMetrics: Option[ShuffleReadMetrics],
    val shuffleWriteMetrics: Option[ShuffleWriteMetrics])

class InputMetrics private[spark](
    val bytesRead: Long,
    val recordsRead: Long)

class OutputMetrics private[spark](
    val bytesWritten: Long,
    val recordsWritten: Long)

class ShuffleReadMetrics private[spark](
    val remoteBlocksFetched: Int,
    val localBlocksFetched: Int,
    val fetchWaitTime: Long,
    val remoteBytesRead: Long,
    val totalBlocksFetched: Int,
    val recordsRead: Long)

class ShuffleWriteMetrics private[spark](
    val bytesWritten: Long,
    val writeTime: Long,
    val recordsWritten: Long)

class TaskMetricDistributions private[spark](
    val quantiles: IndexedSeq[Double],

    val executorDeserializeTime: IndexedSeq[Double],
    val executorRunTime: IndexedSeq[Double],
    val resultSize: IndexedSeq[Double],
    val jvmGcTime: IndexedSeq[Double],
    val resultSerializationTime: IndexedSeq[Double],
    val memoryBytesSpilled: IndexedSeq[Double],
    val diskBytesSpilled: IndexedSeq[Double],

    val inputMetrics: Option[InputMetricDistributions],
    val outputMetrics: Option[OutputMetricDistributions],
    val shuffleReadMetrics: Option[ShuffleReadMetricDistributions],
    val shuffleWriteMetrics: Option[ShuffleWriteMetricDistributions])

class InputMetricDistributions private[spark](
    val bytesRead: IndexedSeq[Double],
    val recordsRead: IndexedSeq[Double])

class OutputMetricDistributions private[spark](
    val bytesWritten: IndexedSeq[Double],
    val recordsWritten: IndexedSeq[Double])

class ShuffleReadMetricDistributions private[spark](
    val readBytes: IndexedSeq[Double],
    val readRecords: IndexedSeq[Double],
    val remoteBlocksFetched: IndexedSeq[Double],
    val localBlocksFetched: IndexedSeq[Double],
    val fetchWaitTime: IndexedSeq[Double],
    val remoteBytesRead: IndexedSeq[Double],
    val totalBlocksFetched: IndexedSeq[Double])

class ShuffleWriteMetricDistributions private[spark](
    val writeBytes: IndexedSeq[Double],
    val writeRecords: IndexedSeq[Double],
    val writeTime: IndexedSeq[Double])

class AccumulableInfo private[spark](
    val id: Long,
    val name: String,
    val update: Option[String],
    val value: String)
