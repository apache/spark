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

import java.lang.{Long => JLong}
import java.util.Date

import scala.xml.{NodeSeq, Text}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.metrics.ExecutorMetricType

case class ApplicationInfo private[spark](
    id: String,
    name: String,
    coresGranted: Option[Int],
    maxCores: Option[Int],
    coresPerExecutor: Option[Int],
    memoryPerExecutorMB: Option[Int],
    attempts: Seq[ApplicationAttemptInfo])

@JsonIgnoreProperties(
  value = Array("startTimeEpoch", "endTimeEpoch", "lastUpdatedEpoch"),
  allowGetters = true)
case class ApplicationAttemptInfo private[spark](
    attemptId: Option[String],
    startTime: Date,
    endTime: Date,
    lastUpdated: Date,
    duration: Long,
    sparkUser: String,
    completed: Boolean = false,
    appSparkVersion: String) {

  def getStartTimeEpoch: Long = startTime.getTime

  def getEndTimeEpoch: Long = endTime.getTime

  def getLastUpdatedEpoch: Long = lastUpdated.getTime

}

class ExecutorStageSummary private[spark](
    val taskTime : Long,
    val failedTasks : Int,
    val succeededTasks : Int,
    val killedTasks : Int,
    val inputBytes : Long,
    val inputRecords : Long,
    val outputBytes : Long,
    val outputRecords : Long,
    val shuffleRead : Long,
    val shuffleReadRecords : Long,
    val shuffleWrite : Long,
    val shuffleWriteRecords : Long,
    val memoryBytesSpilled : Long,
    val diskBytesSpilled : Long,
    val isBlacklistedForStage: Boolean)

class ExecutorSummary private[spark](
    val id: String,
    val hostPort: String,
    val isActive: Boolean,
    val rddBlocks: Int,
    val memoryUsed: Long,
    val diskUsed: Long,
    val totalCores: Int,
    val maxTasks: Int,
    val activeTasks: Int,
    val failedTasks: Int,
    val completedTasks: Int,
    val totalTasks: Int,
    val totalDuration: Long,
    val totalGCTime: Long,
    val totalInputBytes: Long,
    val totalShuffleRead: Long,
    val totalShuffleWrite: Long,
    val isBlacklisted: Boolean,
    val maxMemory: Long,
    val addTime: Date,
    val removeTime: Option[Date],
    val removeReason: Option[String],
    val executorLogs: Map[String, String],
    val memoryMetrics: Option[MemoryMetrics],
    val blacklistedInStages: Set[Int],
    @JsonSerialize(using = classOf[ExecutorMetricsJsonSerializer])
    @JsonDeserialize(using = classOf[ExecutorMetricsJsonDeserializer])
    val peakMemoryMetrics: Option[ExecutorMetrics],
    val attributes: Map[String, String])

class MemoryMetrics private[spark](
    val usedOnHeapStorageMemory: Long,
    val usedOffHeapStorageMemory: Long,
    val totalOnHeapStorageMemory: Long,
    val totalOffHeapStorageMemory: Long)

/** deserializer for peakMemoryMetrics: convert map to ExecutorMetrics */
private[spark] class ExecutorMetricsJsonDeserializer
    extends JsonDeserializer[Option[ExecutorMetrics]] {
  override def deserialize(
      jsonParser: JsonParser,
      deserializationContext: DeserializationContext): Option[ExecutorMetrics] = {
    val metricsMap = jsonParser.readValueAs[Option[Map[String, Long]]](
      new TypeReference[Option[Map[String, java.lang.Long]]] {})
    metricsMap.map(metrics => new ExecutorMetrics(metrics))
  }
}
/** serializer for peakMemoryMetrics: convert ExecutorMetrics to map with metric name as key */
private[spark] class ExecutorMetricsJsonSerializer
    extends JsonSerializer[Option[ExecutorMetrics]] {
  override def serialize(
      metrics: Option[ExecutorMetrics],
      jsonGenerator: JsonGenerator,
      serializerProvider: SerializerProvider): Unit = {
    metrics.foreach { m: ExecutorMetrics =>
      val metricsMap = ExecutorMetricType.metricToOffset.map { case (metric, _) =>
        metric -> m.getMetricValue(metric)
      }
      jsonGenerator.writeObject(metricsMap)
    }
  }

  override def isEmpty(provider: SerializerProvider, value: Option[ExecutorMetrics]): Boolean =
    value.isEmpty
}

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
    val numKilledTasks: Int,
    val numCompletedIndices: Int,
    val numActiveStages: Int,
    val numCompletedStages: Int,
    val numSkippedStages: Int,
    val numFailedStages: Int,
    val killedTasksSummary: Map[String, Int])

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
    val diskUsed: Long,
    @JsonDeserialize(contentAs = classOf[JLong])
    val onHeapMemoryUsed: Option[Long],
    @JsonDeserialize(contentAs = classOf[JLong])
    val offHeapMemoryUsed: Option[Long],
    @JsonDeserialize(contentAs = classOf[JLong])
    val onHeapMemoryRemaining: Option[Long],
    @JsonDeserialize(contentAs = classOf[JLong])
    val offHeapMemoryRemaining: Option[Long])

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
    val numTasks: Int,
    val numActiveTasks: Int,
    val numCompleteTasks: Int,
    val numFailedTasks: Int,
    val numKilledTasks: Int,
    val numCompletedIndices: Int,

    val executorRunTime: Long,
    val executorCpuTime: Long,
    val submissionTime: Option[Date],
    val firstTaskLaunchedTime: Option[Date],
    val completionTime: Option[Date],
    val failureReason: Option[String],

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
    val description: Option[String],
    val details: String,
    val schedulingPool: String,

    val rddIds: Seq[Int],
    val accumulatorUpdates: Seq[AccumulableInfo],
    val tasks: Option[Map[Long, TaskData]],
    val executorSummary: Option[Map[String, ExecutorStageSummary]],
    val killedTasksSummary: Map[String, Int])

class TaskData private[spark](
    val taskId: Long,
    val index: Int,
    val attempt: Int,
    val launchTime: Date,
    val resultFetchStart: Option[Date],
    @JsonDeserialize(contentAs = classOf[JLong])
    val duration: Option[Long],
    val executorId: String,
    val host: String,
    val status: String,
    val taskLocality: String,
    val speculative: Boolean,
    val accumulatorUpdates: Seq[AccumulableInfo],
    val errorMessage: Option[String] = None,
    val taskMetrics: Option[TaskMetrics] = None,
    val executorLogs: Map[String, String],
    val schedulerDelay: Long,
    val gettingResultTime: Long)

class TaskMetrics private[spark](
    val executorDeserializeTime: Long,
    val executorDeserializeCpuTime: Long,
    val executorRunTime: Long,
    val executorCpuTime: Long,
    val resultSize: Long,
    val jvmGcTime: Long,
    val resultSerializationTime: Long,
    val memoryBytesSpilled: Long,
    val diskBytesSpilled: Long,
    val peakExecutionMemory: Long,
    val inputMetrics: InputMetrics,
    val outputMetrics: OutputMetrics,
    val shuffleReadMetrics: ShuffleReadMetrics,
    val shuffleWriteMetrics: ShuffleWriteMetrics)

class InputMetrics private[spark](
    val bytesRead: Long,
    val recordsRead: Long)

class OutputMetrics private[spark](
    val bytesWritten: Long,
    val recordsWritten: Long)

class ShuffleReadMetrics private[spark](
    val remoteBlocksFetched: Long,
    val localBlocksFetched: Long,
    val fetchWaitTime: Long,
    val remoteBytesRead: Long,
    val remoteBytesReadToDisk: Long,
    val localBytesRead: Long,
    val recordsRead: Long)

class ShuffleWriteMetrics private[spark](
    val bytesWritten: Long,
    val writeTime: Long,
    val recordsWritten: Long)

class TaskMetricDistributions private[spark](
    val quantiles: IndexedSeq[Double],

    val executorDeserializeTime: IndexedSeq[Double],
    val executorDeserializeCpuTime: IndexedSeq[Double],
    val executorRunTime: IndexedSeq[Double],
    val executorCpuTime: IndexedSeq[Double],
    val resultSize: IndexedSeq[Double],
    val jvmGcTime: IndexedSeq[Double],
    val resultSerializationTime: IndexedSeq[Double],
    val gettingResultTime: IndexedSeq[Double],
    val schedulerDelay: IndexedSeq[Double],
    val peakExecutionMemory: IndexedSeq[Double],
    val memoryBytesSpilled: IndexedSeq[Double],
    val diskBytesSpilled: IndexedSeq[Double],

    val inputMetrics: InputMetricDistributions,
    val outputMetrics: OutputMetricDistributions,
    val shuffleReadMetrics: ShuffleReadMetricDistributions,
    val shuffleWriteMetrics: ShuffleWriteMetricDistributions)

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
    val remoteBytesReadToDisk: IndexedSeq[Double],
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

class VersionInfo private[spark](
  val spark: String)

class ApplicationEnvironmentInfo private[spark] (
    val runtime: RuntimeInfo,
    val sparkProperties: Seq[(String, String)],
    val hadoopProperties: Seq[(String, String)],
    val systemProperties: Seq[(String, String)],
    val classpathEntries: Seq[(String, String)])

class RuntimeInfo private[spark](
    val javaVersion: String,
    val javaHome: String,
    val scalaVersion: String)

case class StackTrace(elems: Seq[String]) {
  override def toString: String = elems.mkString

  def html: NodeSeq = {
    val withNewLine = elems.foldLeft(NodeSeq.Empty) { (acc, elem) =>
      if (acc.isEmpty) {
        acc :+ Text(elem)
      } else {
        acc :+ <br /> :+ Text(elem)
      }
    }

    withNewLine
  }

  def mkString(start: String, sep: String, end: String): String = {
    elems.mkString(start, sep, end)
  }
}

case class ThreadStackTrace(
    val threadId: Long,
    val threadName: String,
    val threadState: Thread.State,
    val stackTrace: StackTrace,
    val blockedByThreadId: Option[Long],
    val blockedByLock: String,
    val holdingLocks: Seq[String])
