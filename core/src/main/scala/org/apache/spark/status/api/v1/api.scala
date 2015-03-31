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
import org.apache.spark.util.{SparkEnum, SparkEnumCompanion}

class ApplicationInfo(
  val id: String,
  val name: String,
  val startTime: Date,
  val endTime: Date,
  val sparkUser: String,
  val completed: Boolean = false
)

class ExecutorStageSummary(
  val taskTime : Long,
  val failedTasks : Int,
  val succeededTasks : Int,
  val inputBytes : Long,
  val outputBytes : Long,
  val shuffleRead : Long,
  val shuffleWrite : Long,
  val memoryBytesSpilled : Long,
  val diskBytesSpilled : Long
)

class ExecutorSummary(
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
  val executorLogs: Map[String, String]
)

class JobData(
  val jobId: Int,
  val name: String,
  val description: Option[String],
  val submissionTime: Option[Date],
  val completionTime: Option[Date],
  val stageIds: Seq[Int],
  val jobGroup: Option[String],
  val status: JobStatus,
  val numTasks: Int,
  val numActiveTasks: Int,
  val numCompletedTasks: Int,
  val numSkippedTasks: Int,
  val numFailedTasks: Int,
  val numActiveStages: Int,
  val numCompletedStages: Int,
  val numSkippedStages: Int,
  val numFailedStages: Int
)

// Q: should Tachyon size go in here as well?  currently the UI only shows it on the overall storage
// page ... does anybody pay attention to it?
class RDDStorageInfo(
  val id: Int,
  val name: String,
  val numPartitions: Int,
  val numCachedPartitions: Int,
  val storageLevel: String,
  val memoryUsed: Long,
  val diskUsed: Long,
  val dataDistribution: Option[Seq[RDDDataDistribution]],
  val partitions: Option[Seq[RDDPartitionInfo]]
)

class RDDDataDistribution(
  val address: String,
  val memoryUsed: Long,
  val memoryRemaining: Long,
  val diskUsed: Long
)

class RDDPartitionInfo(
  val blockName: String,
  val storageLevel: String,
  val memoryUsed: Long,
  val diskUsed: Long,
  val executors: Seq[String]
)

class StageData(
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
  val executorSummary:Option[Map[String,ExecutorStageSummary]]
)

class TaskData(
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
  val taskMetrics: Option[TaskMetrics] = None
)

class TaskMetrics(
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
  val shuffleWriteMetrics: Option[ShuffleWriteMetrics]
)

class InputMetrics(
  val bytesRead: Long,
  val recordsRead: Long
)

class OutputMetrics(
  val bytesWritten: Long,
  val recordsWritten: Long
)

class ShuffleReadMetrics(
  val remoteBlocksFetched: Int,
  val localBlocksFetched: Int,
  val fetchWaitTime: Long,
  val remoteBytesRead: Long,
  val totalBlocksFetched: Int,
  val recordsRead: Long
)

class ShuffleWriteMetrics(
  val bytesWritten: Long,
  val writeTime: Long,
  val recordsWritten: Long
)

class TaskMetricDistributions(
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
  val shuffleWriteMetrics: Option[ShuffleWriteMetricDistributions]
)

class InputMetricDistributions(
  val bytesRead: IndexedSeq[Double],
  val recordsRead: IndexedSeq[Double]
)

class OutputMetricDistributions(
  val bytesWritten: IndexedSeq[Double],
  val recordsWritten: IndexedSeq[Double]
)


class ShuffleReadMetricDistributions(
  val readBytes: IndexedSeq[Double],
  val readRecords: IndexedSeq[Double],
  val remoteBlocksFetched: IndexedSeq[Double],
  val localBlocksFetched: IndexedSeq[Double],
  val fetchWaitTime: IndexedSeq[Double],
  val remoteBytesRead: IndexedSeq[Double],
  val totalBlocksFetched: IndexedSeq[Double]
)

class ShuffleWriteMetricDistributions(
  val writeBytes: IndexedSeq[Double],
  val writeRecords: IndexedSeq[Double],
  val writeTime: IndexedSeq[Double]
)

class AccumulableInfo (
  val id: Long,
  val name: String,
  val update: Option[String],
  val value: String) {

  override def equals(other: Any): Boolean = other match {
    case acc: AccumulableInfo =>
      this.id == acc.id && this.name == acc.name &&
        this.value == acc.value
    case _ => false
  }
}


private[spark] trait JerseyEnum[T <: SparkEnum] extends SparkEnumCompanion[T] {
  def fromString(s: String): T = {
    parseIgnoreCase(s).getOrElse { throw new IllegalArgumentException(
      s"Illegal type=$s. Supported type values: ${values.map{_.toString}}")}
  }
}

sealed abstract class JobStatus extends SparkEnum
object JobStatus extends JerseyEnum[JobStatus] {
  final val RUNNING = {
    case object RUNNING extends JobStatus
    RUNNING
  }
  final val SUCCEEDED = {
    case object SUCCEEDED extends JobStatus
    SUCCEEDED
  }
  final val FAILED = {
    case object FAILED extends JobStatus
    FAILED
  }
  final val UNKNOWN = {
    case object UNKNOWN extends JobStatus
    UNKNOWN
  }

  val values = Seq(
    RUNNING,
    SUCCEEDED,
    FAILED,
    UNKNOWN
  )

  private[spark] def fromInternalStatus(s: JobExecutionStatus): JobStatus = {
    JobStatus.parse(s.name()).get
  }
}

sealed abstract class StageStatus extends SparkEnum
object StageStatus extends JerseyEnum[StageStatus] {
  final val Active = {
    case object Active extends StageStatus
    Active
  }

  final val Complete = {
    case object Complete extends StageStatus
    Complete
  }

  final val Failed = {
    case object Failed extends StageStatus
    Failed
  }

  final val Pending = {
    case object Pending extends StageStatus
    Pending
  }

  val values = Seq(
    Active,
    Complete,
    Failed,
    Pending
  )
}

sealed abstract class ApplicationStatus extends SparkEnum
object ApplicationStatus extends JerseyEnum[SparkEnum] {
  final val COMPLETED = {
    case object COMPLETED extends ApplicationStatus
    COMPLETED
  }

  final val RUNNING = {
    case object RUNNING extends ApplicationStatus
    RUNNING
  }

  val values = Seq(
    COMPLETED,
    RUNNING
  )
}
