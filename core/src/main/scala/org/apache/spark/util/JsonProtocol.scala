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

package org.apache.spark.util

import java.util.{Properties, UUID}

import scala.collection.Map
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonNode
import org.json4s.jackson.JsonMethods.compact

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.internal.config._
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.rdd.{DeterministicLevel, RDDOperationScope}
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceProfile, TaskResourceRequest}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils.weakIntern

/**
 * Helper class for passing configuration options to JsonProtocol.
 * We use this instead of passing SparkConf directly because it lets us avoid
 * repeated re-parsing of configuration values on each read.
 */
private[spark] class JsonProtocolOptions(conf: SparkConf) {
  val includeTaskMetricsAccumulators: Boolean =
    conf.get(EVENT_LOG_INCLUDE_TASK_METRICS_ACCUMULATORS)
}

/**
 * Serializes SparkListener events to/from JSON.  This protocol provides strong backwards-
 * and forwards-compatibility guarantees: any version of Spark should be able to read JSON output
 * written by any other version, including newer versions.
 *
 * JsonProtocolSuite contains backwards-compatibility tests which check that the current version of
 * JsonProtocol is able to read output written by earlier versions.  We do not currently have tests
 * for reading newer JSON output with older Spark versions.
 *
 * To ensure that we provide these guarantees, follow these rules when modifying these methods:
 *
 *  - Never delete any JSON fields.
 *  - Any new JSON fields should be optional; use `jsonOption` when reading these fields
 *    in `*FromJson` methods.
 */
private[spark] object JsonProtocol extends JsonUtils {
  // TODO: Remove this file and put JSON serialization into each individual class.

  private[util]
  val defaultOptions: JsonProtocolOptions = new JsonProtocolOptions(new SparkConf(false))

  /** ------------------------------------------------- *
   * JSON serialization methods for SparkListenerEvents |
   * -------------------------------------------------- */

  // Only for use in tests. Production code should use the two-argument overload defined below.
  def sparkEventToJsonString(event: SparkListenerEvent): String = {
    sparkEventToJsonString(event, defaultOptions)
  }

  def sparkEventToJsonString(event: SparkListenerEvent, options: JsonProtocolOptions): String = {
    toJsonString { generator =>
      writeSparkEventToJson(event, generator, options)
    }
  }

  def writeSparkEventToJson(
      event: SparkListenerEvent,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
        stageSubmittedToJson(stageSubmitted, g, options)
      case stageCompleted: SparkListenerStageCompleted =>
        stageCompletedToJson(stageCompleted, g, options)
      case taskStart: SparkListenerTaskStart =>
        taskStartToJson(taskStart, g, options)
      case taskGettingResult: SparkListenerTaskGettingResult =>
        taskGettingResultToJson(taskGettingResult, g, options)
      case taskEnd: SparkListenerTaskEnd =>
        taskEndToJson(taskEnd, g, options)
      case jobStart: SparkListenerJobStart =>
        jobStartToJson(jobStart, g, options)
      case jobEnd: SparkListenerJobEnd =>
        jobEndToJson(jobEnd, g)
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        environmentUpdateToJson(environmentUpdate, g)
      case blockManagerAdded: SparkListenerBlockManagerAdded =>
        blockManagerAddedToJson(blockManagerAdded, g)
      case blockManagerRemoved: SparkListenerBlockManagerRemoved =>
        blockManagerRemovedToJson(blockManagerRemoved, g)
      case unpersistRDD: SparkListenerUnpersistRDD =>
        unpersistRDDToJson(unpersistRDD, g)
      case applicationStart: SparkListenerApplicationStart =>
        applicationStartToJson(applicationStart, g)
      case applicationEnd: SparkListenerApplicationEnd =>
        applicationEndToJson(applicationEnd, g)
      case executorAdded: SparkListenerExecutorAdded =>
        executorAddedToJson(executorAdded, g)
      case executorRemoved: SparkListenerExecutorRemoved =>
        executorRemovedToJson(executorRemoved, g)
      case logStart: SparkListenerLogStart =>
        logStartToJson(logStart, g)
      case metricsUpdate: SparkListenerExecutorMetricsUpdate =>
        executorMetricsUpdateToJson(metricsUpdate, g)
      case stageExecutorMetrics: SparkListenerStageExecutorMetrics =>
        stageExecutorMetricsToJson(stageExecutorMetrics, g)
      case blockUpdate: SparkListenerBlockUpdated =>
        blockUpdateToJson(blockUpdate, g)
      case resourceProfileAdded: SparkListenerResourceProfileAdded =>
        resourceProfileAddedToJson(resourceProfileAdded, g)
      case _ =>
        mapper.writeValue(g, event)
    }
  }

  def stageSubmittedToJson(
      stageSubmitted: SparkListenerStageSubmitted,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageSubmitted)
    g.writeFieldName("Stage Info")
    // SPARK-42205: don't log accumulables in start events:
    stageInfoToJson(stageSubmitted.stageInfo, g, options, includeAccumulables = false)
    Option(stageSubmitted.properties).foreach { properties =>
      g.writeFieldName("Properties")
      propertiesToJson(properties, g)
    }
    g.writeEndObject()
  }

  def stageCompletedToJson(
      stageCompleted: SparkListenerStageCompleted,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageCompleted)
    g.writeFieldName("Stage Info")
    stageInfoToJson(stageCompleted.stageInfo, g, options, includeAccumulables = true)
    g.writeEndObject()
  }

  def taskStartToJson(
      taskStart: SparkListenerTaskStart,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskStart)
    g.writeNumberField("Stage ID", taskStart.stageId)
    g.writeNumberField("Stage Attempt ID", taskStart.stageAttemptId)
    g.writeFieldName("Task Info")
    // SPARK-42205: don't log accumulables in start events:
    taskInfoToJson(taskStart.taskInfo, g, options, includeAccumulables = false)
    g.writeEndObject()
  }

  def taskGettingResultToJson(
      taskGettingResult: SparkListenerTaskGettingResult,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    val taskInfo = taskGettingResult.taskInfo
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskGettingResult)
    g.writeFieldName("Task Info")
    // SPARK-42205: don't log accumulables in "task getting result" events:
    taskInfoToJson(taskInfo, g, options, includeAccumulables = false)
    g.writeEndObject()
  }

  def taskEndToJson(
      taskEnd: SparkListenerTaskEnd,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskEnd)
    g.writeNumberField("Stage ID", taskEnd.stageId)
    g.writeNumberField("Stage Attempt ID", taskEnd.stageAttemptId)
    g.writeStringField("Task Type", taskEnd.taskType)
    g.writeFieldName("Task End Reason")
    taskEndReasonToJson(taskEnd.reason, g)
    g.writeFieldName("Task Info")
    taskInfoToJson(taskEnd.taskInfo, g, options, includeAccumulables = true)
    g.writeFieldName("Task Executor Metrics")
    executorMetricsToJson(taskEnd.taskExecutorMetrics, g)
    Option(taskEnd.taskMetrics).foreach { m =>
      g.writeFieldName("Task Metrics")
      taskMetricsToJson(m, g)
    }
    g.writeEndObject()
  }

  def jobStartToJson(
      jobStart: SparkListenerJobStart,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.jobStart)
    g.writeNumberField("Job ID", jobStart.jobId)
    g.writeNumberField("Submission Time", jobStart.time)
    g.writeArrayFieldStart("Stage Infos")  // Added in Spark 1.2.0
    // SPARK-42205: here, we purposely include accumulables so that we accurately log all
    // available information about stages that may have already completed by the time
    // the job was submitted: it is technically possible for a stage to belong to multiple
    // concurrent jobs, so this situation can arise even without races occurring between
    // event logging and stage completion.
    jobStart.stageInfos.foreach(stageInfoToJson(_, g, options, includeAccumulables = true))
    g.writeEndArray()
    g.writeArrayFieldStart("Stage IDs")
    jobStart.stageIds.foreach(g.writeNumber)
    g.writeEndArray()
    Option(jobStart.properties).foreach { properties =>
      g.writeFieldName("Properties")
      propertiesToJson(properties, g)
    }

    g.writeEndObject()
  }

  def jobEndToJson(jobEnd: SparkListenerJobEnd, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.jobEnd)
    g.writeNumberField("Job ID", jobEnd.jobId)
    g.writeNumberField("Completion Time", jobEnd.time)
    g.writeFieldName("Job Result")
    jobResultToJson(jobEnd.jobResult, g)
    g.writeEndObject()
  }

  def environmentUpdateToJson(
      environmentUpdate: SparkListenerEnvironmentUpdate,
      g: JsonGenerator): Unit = {
    val environmentDetails = environmentUpdate.environmentDetails
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.environmentUpdate)
    writeMapField("JVM Information", environmentDetails("JVM Information").toMap, g)
    writeMapField("Spark Properties", environmentDetails("Spark Properties").toMap, g)
    writeMapField("Hadoop Properties", environmentDetails("Hadoop Properties").toMap, g)
    writeMapField("System Properties", environmentDetails("System Properties").toMap, g)
    writeMapField("Metrics Properties", environmentDetails("Metrics Properties").toMap, g)
    writeMapField("Classpath Entries", environmentDetails("Classpath Entries").toMap, g)
    g.writeEndObject()
  }

  def blockManagerAddedToJson(
      blockManagerAdded: SparkListenerBlockManagerAdded,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockManagerAdded)
    g.writeFieldName("Block Manager ID")
    blockManagerIdToJson(blockManagerAdded.blockManagerId, g)
    g.writeNumberField("Maximum Memory", blockManagerAdded.maxMem)
    g.writeNumberField("Timestamp", blockManagerAdded.time)
    blockManagerAdded.maxOnHeapMem.foreach(g.writeNumberField("Maximum Onheap Memory", _))
    blockManagerAdded.maxOffHeapMem.foreach(g.writeNumberField("Maximum Offheap Memory", _))
    g.writeEndObject()
  }

  def blockManagerRemovedToJson(
      blockManagerRemoved: SparkListenerBlockManagerRemoved,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockManagerRemoved)
    g.writeFieldName("Block Manager ID")
    blockManagerIdToJson(blockManagerRemoved.blockManagerId, g)
    g.writeNumberField("Timestamp", blockManagerRemoved.time)
    g.writeEndObject()
  }

  def unpersistRDDToJson(unpersistRDD: SparkListenerUnpersistRDD, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.unpersistRDD)
    g.writeNumberField("RDD ID", unpersistRDD.rddId)
    g.writeEndObject()
  }

  def applicationStartToJson(
      applicationStart: SparkListenerApplicationStart,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.applicationStart)
    g.writeStringField("App Name", applicationStart.appName)
    applicationStart.appId.foreach(g.writeStringField("App ID", _))
    g.writeNumberField("Timestamp", applicationStart.time)
    g.writeStringField("User", applicationStart.sparkUser)
    applicationStart.appAttemptId.foreach(g.writeStringField("App Attempt ID", _))
    applicationStart.driverLogs.foreach(writeMapField("Driver Logs", _, g))
    applicationStart.driverAttributes.foreach(writeMapField("Driver Attributes", _, g))
    g.writeEndObject()
  }

  def applicationEndToJson(
      applicationEnd: SparkListenerApplicationEnd,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.applicationEnd)
    g.writeNumberField("Timestamp", applicationEnd.time)
    applicationEnd.exitCode.foreach(exitCode => g.writeNumberField("ExitCode", exitCode))
    g.writeEndObject()
  }

  def resourceProfileAddedToJson(
      profileAdded: SparkListenerResourceProfileAdded,
      g: JsonGenerator
    ): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.resourceProfileAdded)
    g.writeNumberField("Resource Profile Id", profileAdded.resourceProfile.id)
    g.writeFieldName("Executor Resource Requests")
    executorResourceRequestMapToJson(profileAdded.resourceProfile.executorResources, g)
    g.writeFieldName("Task Resource Requests")
    taskResourceRequestMapToJson(profileAdded.resourceProfile.taskResources, g)
    g.writeEndObject()
  }

  def executorAddedToJson(executorAdded: SparkListenerExecutorAdded, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.executorAdded)
    g.writeNumberField("Timestamp", executorAdded.time)
    g.writeStringField("Executor ID", executorAdded.executorId)
    g.writeFieldName("Executor Info")
    executorInfoToJson(executorAdded.executorInfo, g)
    g.writeEndObject()
  }

  def executorRemovedToJson(
      executorRemoved: SparkListenerExecutorRemoved,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.executorRemoved)
    g.writeNumberField("Timestamp", executorRemoved.time)
    g.writeStringField("Executor ID", executorRemoved.executorId)
    g.writeStringField("Removed Reason", executorRemoved.reason)
    g.writeEndObject()
  }

  def logStartToJson(logStart: SparkListenerLogStart, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.logStart)
    g.writeStringField("Spark Version", SPARK_VERSION)
    g.writeEndObject()
  }

  def executorMetricsUpdateToJson(
      metricsUpdate: SparkListenerExecutorMetricsUpdate,
      g: JsonGenerator): Unit = {
    val execId = metricsUpdate.execId
    val accumUpdates = metricsUpdate.accumUpdates
    val executorUpdates = metricsUpdate.executorUpdates
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.metricsUpdate)
    g.writeStringField("Executor ID", execId)
    g.writeArrayFieldStart("Metrics Updated")
    accumUpdates.foreach { case (taskId, stageId, stageAttemptId, updates) =>
      g.writeStartObject()
      g.writeNumberField("Task ID", taskId)
      g.writeNumberField("Stage ID", stageId)
      g.writeNumberField("Stage Attempt ID", stageAttemptId)
      g.writeArrayFieldStart("Accumulator Updates")
      updates.foreach(accumulableInfoToJson(_, g))
      g.writeEndArray()
      g.writeEndObject()
    }
    g.writeEndArray()
    g.writeArrayFieldStart("Executor Metrics Updated")
    executorUpdates.foreach {
      case ((stageId, stageAttemptId), metrics) =>
        g.writeStartObject()
        g.writeNumberField("Stage ID", stageId)
        g.writeNumberField("Stage Attempt ID", stageAttemptId)
        g.writeFieldName("Executor Metrics")
        executorMetricsToJson(metrics, g)
        g.writeEndObject()
    }
    g.writeEndArray()
    g.writeEndObject()
  }

  def stageExecutorMetricsToJson(
      metrics: SparkListenerStageExecutorMetrics,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageExecutorMetrics)
    g.writeStringField("Executor ID", metrics.execId)
    g.writeNumberField("Stage ID", metrics.stageId)
    g.writeNumberField("Stage Attempt ID", metrics.stageAttemptId)
    g.writeFieldName("Executor Metrics")
    executorMetricsToJson(metrics.executorMetrics, g)
    g.writeEndObject()
  }

  def blockUpdateToJson(blockUpdate: SparkListenerBlockUpdated, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockUpdate)
    g.writeFieldName("Block Updated Info")
    blockUpdatedInfoToJson(blockUpdate.blockUpdatedInfo, g)
    g.writeEndObject()
  }

  /** ------------------------------------------------------------------- *
   * JSON serialization methods for classes SparkListenerEvents depend on |
   * -------------------------------------------------------------------- */

  def stageInfoToJson(
      stageInfo: StageInfo,
      g: JsonGenerator,
      options: JsonProtocolOptions,
      includeAccumulables: Boolean): Unit = {
    g.writeStartObject()
    g.writeNumberField("Stage ID", stageInfo.stageId)
    g.writeNumberField("Stage Attempt ID", stageInfo.attemptNumber())
    g.writeStringField("Stage Name", stageInfo.name)
    g.writeNumberField ("Number of Tasks", stageInfo.numTasks)
    g.writeArrayFieldStart("RDD Info")
    stageInfo.rddInfos.foreach(rddInfoToJson(_, g))
    g.writeEndArray()
    g.writeArrayFieldStart("Parent IDs")
    stageInfo.parentIds.foreach(g.writeNumber)
    g.writeEndArray()
    g.writeStringField("Details", stageInfo.details)
    stageInfo.submissionTime.foreach(g.writeNumberField("Submission Time", _))
    stageInfo.completionTime.foreach(g.writeNumberField("Completion Time", _))
    stageInfo.failureReason.foreach(g.writeStringField("Failure Reason", _))
    g.writeFieldName("Accumulables")
    if (includeAccumulables) {
      accumulablesToJson(
        stageInfo.accumulables.values,
        g,
        includeTaskMetricsAccumulators = options.includeTaskMetricsAccumulators)
    } else {
      g.writeStartArray()
      g.writeEndArray()
    }
    g.writeNumberField("Resource Profile Id", stageInfo.resourceProfileId)
    g.writeBooleanField("Shuffle Push Enabled", stageInfo.isShufflePushEnabled)
    g.writeNumberField("Shuffle Push Mergers Count", stageInfo.shuffleMergerCount)
    g.writeEndObject()
  }

  def taskInfoToJson(
      taskInfo: TaskInfo,
      g: JsonGenerator,
      options: JsonProtocolOptions,
      includeAccumulables: Boolean): Unit = {
    g.writeStartObject()
    g.writeNumberField("Task ID", taskInfo.taskId)
    g.writeNumberField("Index", taskInfo.index)
    g.writeNumberField("Attempt", taskInfo.attemptNumber)
    g.writeNumberField("Partition ID", taskInfo.partitionId)
    g.writeNumberField("Launch Time", taskInfo.launchTime)
    g.writeStringField("Executor ID", taskInfo.executorId)
    g.writeStringField("Host", taskInfo.host)
    g.writeStringField("Locality", taskInfo.taskLocality.toString)
    g.writeBooleanField("Speculative", taskInfo.speculative)
    g.writeNumberField("Getting Result Time", taskInfo.gettingResultTime)
    g.writeNumberField("Finish Time", taskInfo.finishTime)
    g.writeBooleanField("Failed", taskInfo.failed)
    g.writeBooleanField("Killed", taskInfo.killed)
    g.writeFieldName("Accumulables")
    if (includeAccumulables) {
      accumulablesToJson(
        taskInfo.accumulables,
        g,
        includeTaskMetricsAccumulators = options.includeTaskMetricsAccumulators)
    } else {
      g.writeStartArray()
      g.writeEndArray()
    }
    g.writeEndObject()
  }

  private[util] val accumulableExcludeList = Set(InternalAccumulator.UPDATED_BLOCK_STATUSES)

  private[this] val taskMetricAccumulableNames = TaskMetrics.empty.nameToAccums.keySet.toSet

  def accumulablesToJson(
      accumulables: Iterable[AccumulableInfo],
      g: JsonGenerator,
    includeTaskMetricsAccumulators: Boolean = true): Unit = {
    g.writeStartArray()
    accumulables
        .filterNot { acc =>
          acc.name.exists(accumulableExcludeList.contains) ||
          (!includeTaskMetricsAccumulators && acc.name.exists(taskMetricAccumulableNames.contains))
        }
        .toList
        .sortBy(_.id)
        .foreach(a => accumulableInfoToJson(a, g))
    g.writeEndArray()
  }

  def accumulableInfoToJson(accumulableInfo: AccumulableInfo, g: JsonGenerator): Unit = {
    val name = accumulableInfo.name
    g.writeStartObject()
    g.writeNumberField("ID", accumulableInfo.id)
    name.foreach(g.writeStringField("Name", _))
    accumulableInfo.update.foreach { v =>
      accumValueToJson(name, v, g, fieldName = Some("Update"))
    }
    accumulableInfo.value.foreach { v =>
      accumValueToJson(name, v, g, fieldName = Some("Value"))
    }
    g.writeBooleanField("Internal", accumulableInfo.internal)
    g.writeBooleanField("Count Failed Values", accumulableInfo.countFailedValues)
    accumulableInfo.metadata.foreach(g.writeStringField("Metadata", _))
    g.writeEndObject()
  }

  /**
   * Serialize the value of an accumulator to JSON.
   *
   * For accumulators representing internal task metrics, this looks up the relevant
   * [[AccumulatorParam]] to serialize the value accordingly. For all other accumulators,
   * this will simply serialize the value as a string.
   *
   * The behavior here must match that of [[accumValueFromJson]]. Exposed for testing.
   */
  private[util] def accumValueToJson(
      name: Option[String],
      value: Any,
      g: JsonGenerator,
      fieldName: Option[String] = None): Unit = {
    if (name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))) {
      value match {
        case v: Int =>
          fieldName.foreach(g.writeFieldName)
          g.writeNumber(v)
        case v: Long =>
          fieldName.foreach(g.writeFieldName)
          g.writeNumber(v)
        // We only have 3 kind of internal accumulator types, so if it's not int or long, it must be
        // the blocks accumulator, whose type is `java.util.List[(BlockId, BlockStatus)]`
        case v: java.util.List[_] =>
          fieldName.foreach(g.writeFieldName)
          g.writeStartArray()
          v.asScala.foreach {
            case (id: BlockId, status: BlockStatus) =>
              g.writeStartObject()
              g.writeStringField("Block ID", id.toString)
              g.writeFieldName("Status")
              blockStatusToJson(status, g)
              g.writeEndObject()
            case _ =>
              // Ignore unsupported types. A user may put `METRICS_PREFIX` in the name. We should
              // not crash.
          }
          g.writeEndArray()
        case _ =>
          // Ignore unsupported types. A user may put `METRICS_PREFIX` in the name. We should not
          // crash.
      }
    } else {
      // For all external accumulators, just use strings
      fieldName.foreach(g.writeFieldName)
      g.writeString(value.toString)
    }
  }

  def taskMetricsToJson(taskMetrics: TaskMetrics, g: JsonGenerator): Unit = {
    def writeShufflePushReadMetrics(): Unit = {
      g.writeStartObject()
      g.writeNumberField("Corrupt Merged Block Chunks",
        taskMetrics.shuffleReadMetrics.corruptMergedBlockChunks)
      g.writeNumberField("Merged Fetch Fallback Count",
        taskMetrics.shuffleReadMetrics.mergedFetchFallbackCount)
      g.writeNumberField("Merged Remote Blocks Fetched",
        taskMetrics.shuffleReadMetrics.remoteMergedBlocksFetched)
      g.writeNumberField("Merged Local Blocks Fetched",
        taskMetrics.shuffleReadMetrics.localMergedBlocksFetched)
      g.writeNumberField("Merged Remote Chunks Fetched",
        taskMetrics.shuffleReadMetrics.remoteMergedChunksFetched)
      g.writeNumberField("Merged Local Chunks Fetched",
        taskMetrics.shuffleReadMetrics.localMergedChunksFetched)
      g.writeNumberField("Merged Remote Bytes Read",
        taskMetrics.shuffleReadMetrics.remoteMergedBytesRead)
      g.writeNumberField("Merged Local Bytes Read",
        taskMetrics.shuffleReadMetrics.localMergedBytesRead)
      g.writeNumberField("Merged Remote Requests Duration",
        taskMetrics.shuffleReadMetrics.remoteMergedReqsDuration)
      g.writeEndObject()
    }
    def writeShuffleReadMetrics(): Unit = {
      g.writeStartObject()
      g.writeNumberField(
        "Remote Blocks Fetched", taskMetrics.shuffleReadMetrics.remoteBlocksFetched)
      g.writeNumberField("Local Blocks Fetched", taskMetrics.shuffleReadMetrics.localBlocksFetched)
      g.writeNumberField("Fetch Wait Time", taskMetrics.shuffleReadMetrics.fetchWaitTime)
      g.writeNumberField("Remote Bytes Read", taskMetrics.shuffleReadMetrics.remoteBytesRead)
      g.writeNumberField(
        "Remote Bytes Read To Disk", taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk)
      g.writeNumberField("Local Bytes Read", taskMetrics.shuffleReadMetrics.localBytesRead)
      g.writeNumberField("Total Records Read", taskMetrics.shuffleReadMetrics.recordsRead)
      g.writeNumberField("Remote Requests Duration",
        taskMetrics.shuffleReadMetrics.remoteReqsDuration)
      g.writeFieldName("Shuffle Push Read Metrics")
      writeShufflePushReadMetrics()
      g.writeEndObject()
    }
    def writeShuffleWriteMetrics(): Unit = {
      g.writeStartObject()
      g.writeNumberField("Shuffle Bytes Written", taskMetrics.shuffleWriteMetrics.bytesWritten)
      g.writeNumberField("Shuffle Write Time", taskMetrics.shuffleWriteMetrics.writeTime)
      g.writeNumberField("Shuffle Records Written", taskMetrics.shuffleWriteMetrics.recordsWritten)
      g.writeEndObject()
    }
    def writeInputMetrics(): Unit = {
      g.writeStartObject()
      g.writeNumberField("Bytes Read", taskMetrics.inputMetrics.bytesRead)
      g.writeNumberField("Records Read", taskMetrics.inputMetrics.recordsRead)
      g.writeEndObject()
    }
    def writeOutputMetrics(): Unit = {
      g.writeStartObject()
      g.writeNumberField("Bytes Written", taskMetrics.outputMetrics.bytesWritten)
      g.writeNumberField("Records Written", taskMetrics.outputMetrics.recordsWritten)
      g.writeEndObject()
    }
    def writeUpdatedBlocks(): Unit = {
      g.writeStartArray()
      taskMetrics.updatedBlockStatuses.foreach { case (id, status) =>
        g.writeStartObject()
        g.writeStringField("Block ID", id.toString)
        g.writeFieldName("Status")
        blockStatusToJson(status, g)
        g.writeEndObject()
      }
      g.writeEndArray()
    }

    g.writeStartObject()
    g.writeNumberField("Executor Deserialize Time", taskMetrics.executorDeserializeTime)
    g.writeNumberField("Executor Deserialize CPU Time", taskMetrics.executorDeserializeCpuTime)
    g.writeNumberField("Executor Run Time", taskMetrics.executorRunTime)
    g.writeNumberField("Executor CPU Time", taskMetrics.executorCpuTime)
    g.writeNumberField("Peak Execution Memory", taskMetrics.peakExecutionMemory)
    g.writeNumberField("Peak On Heap Execution Memory", taskMetrics.peakOnHeapExecutionMemory)
    g.writeNumberField("Peak Off Heap Execution Memory", taskMetrics.peakOffHeapExecutionMemory)
    g.writeNumberField("Result Size", taskMetrics.resultSize)
    g.writeNumberField("JVM GC Time", taskMetrics.jvmGCTime)
    g.writeNumberField("Result Serialization Time", taskMetrics.resultSerializationTime)
    g.writeNumberField("Memory Bytes Spilled", taskMetrics.memoryBytesSpilled)
    g.writeNumberField("Disk Bytes Spilled", taskMetrics.diskBytesSpilled)
    g.writeFieldName("Shuffle Read Metrics")
    writeShuffleReadMetrics()
    g.writeFieldName("Shuffle Write Metrics")
    writeShuffleWriteMetrics()
    g.writeFieldName("Input Metrics")
    writeInputMetrics()
    g.writeFieldName("Output Metrics")
    writeOutputMetrics()
    g.writeFieldName("Updated Blocks")
    writeUpdatedBlocks()
    g.writeEndObject()
  }

  /** Convert executor metrics to JSON. */
  def executorMetricsToJson(executorMetrics: ExecutorMetrics, g: JsonGenerator): Unit = {
    g.writeStartObject()
    ExecutorMetricType.metricToOffset.foreach { case (m, _) =>
      g.writeNumberField(m, executorMetrics.getMetricValue(m))
    }
    g.writeEndObject()
  }

  def taskEndReasonToJson(taskEndReason: TaskEndReason, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Reason", Utils.getFormattedClassName(taskEndReason))
    taskEndReason match {
      case fetchFailed: FetchFailed =>
        Option(fetchFailed.bmAddress).foreach { id =>
          g.writeFieldName("Block Manager Address")
          blockManagerIdToJson(id, g)
        }
        g.writeNumberField("Shuffle ID", fetchFailed.shuffleId)
        g.writeNumberField("Map ID", fetchFailed.mapId)
        g.writeNumberField("Map Index", fetchFailed.mapIndex)
        g.writeNumberField("Reduce ID", fetchFailed.reduceId)
        g.writeStringField("Message", fetchFailed.message)
      case exceptionFailure: ExceptionFailure =>
        g.writeStringField("Class Name", exceptionFailure.className)
        g.writeStringField("Description", exceptionFailure.description)
        g.writeFieldName("Stack Trace")
        stackTraceToJson(exceptionFailure.stackTrace, g)
        g.writeStringField("Full Stack Trace", exceptionFailure.fullStackTrace)
        g.writeFieldName("Accumulator Updates")
        accumulablesToJson(exceptionFailure.accumUpdates, g)
      case taskCommitDenied: TaskCommitDenied =>
        g.writeNumberField("Job ID", taskCommitDenied.jobID)
        g.writeNumberField("Partition ID", taskCommitDenied.partitionID)
        g.writeNumberField("Attempt Number", taskCommitDenied.attemptNumber)
      case ExecutorLostFailure(executorId, exitCausedByApp, reason) =>
        g.writeStringField("Executor ID", executorId)
        g.writeBooleanField("Exit Caused By App", exitCausedByApp)
        reason.foreach(g.writeStringField("Loss Reason", _))
      case taskKilled: TaskKilled =>
        g.writeStringField("Kill Reason", taskKilled.reason)
        g.writeArrayFieldStart("Accumulator Updates")
        taskKilled.accumUpdates.foreach { info =>
          accumulableInfoToJson(info, g)
        }
        g.writeEndArray()
      case _ =>
        // no extra fields to write
    }
    g.writeEndObject()
  }

  def blockManagerIdToJson(blockManagerId: BlockManagerId, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Executor ID", blockManagerId.executorId)
    g.writeStringField("Host", blockManagerId.host)
    g.writeNumberField("Port", blockManagerId.port)
    g.writeEndObject()
  }

  def jobResultToJson(jobResult: JobResult, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Result", Utils.getFormattedClassName(jobResult))
    jobResult match {
      case jobFailed: JobFailed =>
        g.writeFieldName("Exception")
        exceptionToJson(jobFailed.exception, g)
      case JobSucceeded =>
        // Nothing else to write in case of success
    }
    g.writeEndObject()
  }

  def rddInfoToJson(rddInfo: RDDInfo, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeNumberField("RDD ID", rddInfo.id)
    g.writeStringField("Name", rddInfo.name)
    rddInfo.scope.foreach { s =>
      g.writeStringField("Scope", s.toJson)
    }
    g.writeStringField("Callsite", rddInfo.callSite)
    g.writeArrayFieldStart("Parent IDs")
    rddInfo.parentIds.foreach(g.writeNumber)
    g.writeEndArray()
    g.writeFieldName("Storage Level")
    storageLevelToJson(rddInfo.storageLevel, g)
    g.writeBooleanField("Barrier", rddInfo.isBarrier)
    g.writeStringField("DeterministicLevel", rddInfo.outputDeterministicLevel.toString)
    g.writeNumberField("Number of Partitions", rddInfo.numPartitions)
    g.writeNumberField("Number of Cached Partitions", rddInfo.numCachedPartitions)
    g.writeNumberField("Memory Size", rddInfo.memSize)
    g.writeNumberField("Disk Size", rddInfo.diskSize)
    g.writeEndObject()
  }

  def storageLevelToJson(storageLevel: StorageLevel, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeBooleanField("Use Disk", storageLevel.useDisk)
    g.writeBooleanField("Use Memory", storageLevel.useMemory)
    g.writeBooleanField("Use Off Heap", storageLevel.useOffHeap)
    g.writeBooleanField("Deserialized", storageLevel.deserialized)
    g.writeNumberField("Replication", storageLevel.replication)
    g.writeEndObject()
  }

  def blockStatusToJson(blockStatus: BlockStatus, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeFieldName("Storage Level")
    storageLevelToJson(blockStatus.storageLevel, g)
    g.writeNumberField("Memory Size", blockStatus.memSize)
    g.writeNumberField("Disk Size", blockStatus.diskSize)
    g.writeEndObject()
  }

  def executorInfoToJson(executorInfo: ExecutorInfo, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Host", executorInfo.executorHost)
    g.writeNumberField("Total Cores", executorInfo.totalCores)
    writeMapField("Log Urls", executorInfo.logUrlMap, g)
    writeMapField("Attributes", executorInfo.attributes, g)
    g.writeObjectFieldStart("Resources")
    // TODO(SPARK-39658): here we are taking a Json4s JValue and are converting it to
    // a JSON string then are combining that string with Jackson-generated JSON. This is
    // done because ResourceInformation.toJson is a public class and exposes Json4s
    // JValues as part of its public API. We should reconsider the design of that interface
    // and explore whether we can avoid exposing third-party symbols in this public API.
    executorInfo.resourcesInfo.foreach { case (k, v) =>
      g.writeFieldName(k)
      g.writeRawValue(compact(v.toJson()))
    }
    g.writeEndObject()
    g.writeNumberField("Resource Profile Id", executorInfo.resourceProfileId)
    executorInfo.registrationTime.foreach(g.writeNumberField("Registration Time", _))
    executorInfo.requestTime.foreach(g.writeNumberField("Request Time", _))
    g.writeEndObject()
  }

  def blockUpdatedInfoToJson(blockUpdatedInfo: BlockUpdatedInfo, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeFieldName("Block Manager ID")
    blockManagerIdToJson(blockUpdatedInfo.blockManagerId, g)
    g.writeStringField("Block ID", blockUpdatedInfo.blockId.toString)
    g.writeFieldName("Storage Level")
    storageLevelToJson(blockUpdatedInfo.storageLevel, g)
    g.writeNumberField("Memory Size", blockUpdatedInfo.memSize)
    g.writeNumberField("Disk Size", blockUpdatedInfo.diskSize)
    g.writeEndObject()
  }

  def executorResourceRequestToJson(execReq: ExecutorResourceRequest, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Resource Name", execReq.resourceName)
    g.writeNumberField("Amount", execReq.amount)
    g.writeStringField("Discovery Script", execReq.discoveryScript)
    g.writeStringField("Vendor", execReq.vendor)
    g.writeEndObject()
  }

  def executorResourceRequestMapToJson(
      m: Map[String, ExecutorResourceRequest],
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    m.foreach { case (k, execReq) =>
      g.writeFieldName(k)
      executorResourceRequestToJson(execReq, g)
    }
    g.writeEndObject()
  }

  def taskResourceRequestToJson(taskReq: TaskResourceRequest, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Resource Name", taskReq.resourceName)
    g.writeNumberField("Amount", taskReq.amount)
    g.writeEndObject()
  }

  def taskResourceRequestMapToJson(m: Map[String, TaskResourceRequest], g: JsonGenerator): Unit = {
    g.writeStartObject()
    m.foreach { case (k, taskReq) =>
      g.writeFieldName(k)
      taskResourceRequestToJson(taskReq, g)
    }
    g.writeEndObject()
  }

  /** ------------------------------ *
   * Util JSON serialization methods |
   * ------------------------------- */

  def writeMapField(name: String, m: Map[String, String], g: JsonGenerator): Unit = {
    g.writeObjectFieldStart(name)
    m.foreach { case (k, v) => g.writeStringField(k, v) }
    g.writeEndObject()
  }

  def propertiesToJson(properties: Properties, g: JsonGenerator): Unit = {
    g.writeStartObject()
    properties.asScala.foreach { case (k, v) => g.writeStringField(k, v) }
    g.writeEndObject()
  }

  def UUIDToJson(id: UUID, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeNumberField("Least Significant Bits", id.getLeastSignificantBits)
    g.writeNumberField("Most Significant Bits", id.getMostSignificantBits)
    g.writeEndObject()
  }

  def stackTraceToJson(stackTrace: Array[StackTraceElement], g: JsonGenerator): Unit = {
    g.writeStartArray()
    stackTrace.foreach { line =>
      g.writeStartObject()
      g.writeStringField("Declaring Class", line.getClassName)
      g.writeStringField("Method Name", line.getMethodName)
      g.writeStringField("File Name", line.getFileName)
      g.writeNumberField("Line Number", line.getLineNumber)
      g.writeEndObject()
    }
    g.writeEndArray()
  }

  def exceptionToJson(exception: Exception, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Message", exception.getMessage)
    g.writeFieldName("Stack Trace")
    stackTraceToJson(exception.getStackTrace, g)
    g.writeEndObject()
  }


  /** --------------------------------------------------- *
   * JSON deserialization methods for SparkListenerEvents |
   * ---------------------------------------------------- */

  private object SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES {
    val stageSubmitted = Utils.getFormattedClassName(SparkListenerStageSubmitted)
    val stageCompleted = Utils.getFormattedClassName(SparkListenerStageCompleted)
    val taskStart = Utils.getFormattedClassName(SparkListenerTaskStart)
    val taskGettingResult = Utils.getFormattedClassName(SparkListenerTaskGettingResult)
    val taskEnd = Utils.getFormattedClassName(SparkListenerTaskEnd)
    val jobStart = Utils.getFormattedClassName(SparkListenerJobStart)
    val jobEnd = Utils.getFormattedClassName(SparkListenerJobEnd)
    val environmentUpdate = Utils.getFormattedClassName(SparkListenerEnvironmentUpdate)
    val blockManagerAdded = Utils.getFormattedClassName(SparkListenerBlockManagerAdded)
    val blockManagerRemoved = Utils.getFormattedClassName(SparkListenerBlockManagerRemoved)
    val unpersistRDD = Utils.getFormattedClassName(SparkListenerUnpersistRDD)
    val applicationStart = Utils.getFormattedClassName(SparkListenerApplicationStart)
    val applicationEnd = Utils.getFormattedClassName(SparkListenerApplicationEnd)
    val executorAdded = Utils.getFormattedClassName(SparkListenerExecutorAdded)
    val executorRemoved = Utils.getFormattedClassName(SparkListenerExecutorRemoved)
    val logStart = Utils.getFormattedClassName(SparkListenerLogStart)
    val metricsUpdate = Utils.getFormattedClassName(SparkListenerExecutorMetricsUpdate)
    val stageExecutorMetrics = Utils.getFormattedClassName(SparkListenerStageExecutorMetrics)
    val blockUpdate = Utils.getFormattedClassName(SparkListenerBlockUpdated)
    val resourceProfileAdded = Utils.getFormattedClassName(SparkListenerResourceProfileAdded)
  }

  def sparkEventFromJson(json: String): SparkListenerEvent = {
    sparkEventFromJson(mapper.readTree(json))
  }

  def sparkEventFromJson(json: JsonNode): SparkListenerEvent = {
    import SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES._

    json.get("Event").asText match {
      case `stageSubmitted` => stageSubmittedFromJson(json)
      case `stageCompleted` => stageCompletedFromJson(json)
      case `taskStart` => taskStartFromJson(json)
      case `taskGettingResult` => taskGettingResultFromJson(json)
      case `taskEnd` => taskEndFromJson(json)
      case `jobStart` => jobStartFromJson(json)
      case `jobEnd` => jobEndFromJson(json)
      case `environmentUpdate` => environmentUpdateFromJson(json)
      case `blockManagerAdded` => blockManagerAddedFromJson(json)
      case `blockManagerRemoved` => blockManagerRemovedFromJson(json)
      case `unpersistRDD` => unpersistRDDFromJson(json)
      case `applicationStart` => applicationStartFromJson(json)
      case `applicationEnd` => applicationEndFromJson(json)
      case `executorAdded` => executorAddedFromJson(json)
      case `executorRemoved` => executorRemovedFromJson(json)
      case `logStart` => logStartFromJson(json)
      case `metricsUpdate` => executorMetricsUpdateFromJson(json)
      case `stageExecutorMetrics` => stageExecutorMetricsFromJson(json)
      case `blockUpdate` => blockUpdateFromJson(json)
      case `resourceProfileAdded` => resourceProfileAddedFromJson(json)
      case other => mapper.readValue(json.toString, Utils.classForName(other))
        .asInstanceOf[SparkListenerEvent]
    }
  }

  def stageSubmittedFromJson(json: JsonNode): SparkListenerStageSubmitted = {
    val stageInfo = stageInfoFromJson(json.get("Stage Info"))
    val properties = propertiesFromJson(json.get("Properties"))
    SparkListenerStageSubmitted(stageInfo, properties)
  }

  def stageCompletedFromJson(json: JsonNode): SparkListenerStageCompleted = {
    val stageInfo = stageInfoFromJson(json.get("Stage Info"))
    SparkListenerStageCompleted(stageInfo)
  }

  def taskStartFromJson(json: JsonNode): SparkListenerTaskStart = {
    val stageId = json.get("Stage ID").extractInt
    val stageAttemptId =
      jsonOption(json.get("Stage Attempt ID")).map(_.extractInt).getOrElse(0)
    val taskInfo = taskInfoFromJson(json.get("Task Info"))
    SparkListenerTaskStart(stageId, stageAttemptId, taskInfo)
  }

  def taskGettingResultFromJson(json: JsonNode): SparkListenerTaskGettingResult = {
    val taskInfo = taskInfoFromJson(json.get("Task Info"))
    SparkListenerTaskGettingResult(taskInfo)
  }

  /** Extract the executor metrics from JSON. */
  def executorMetricsFromJson(maybeJson: JsonNode): ExecutorMetrics = {
    // Executor metrics might be absent in JSON from very old Spark versions.
    // In this case we return zero values for each metric.
    val metrics =
      ExecutorMetricType.metricToOffset.map { case (metric, _) =>
        val metricValueJson = jsonOption(maybeJson).flatMap(json => jsonOption(json.get(metric)))
        metric -> metricValueJson.map(_.extractLong).getOrElse(0L)
      }
    new ExecutorMetrics(metrics.toMap)
  }

  def taskEndFromJson(json: JsonNode): SparkListenerTaskEnd = {
    val stageId = json.get("Stage ID").extractInt
    val stageAttemptId =
      jsonOption(json.get("Stage Attempt ID")).map(_.extractInt).getOrElse(0)
    val taskType = json.get("Task Type").extractString
    val taskEndReason = taskEndReasonFromJson(json.get("Task End Reason"))
    val taskInfo = taskInfoFromJson(json.get("Task Info"))
    val executorMetrics = executorMetricsFromJson(json.get("Task Executor Metrics"))
    val taskMetrics = taskMetricsFromJson(json.get("Task Metrics"))
    SparkListenerTaskEnd(stageId, stageAttemptId, taskType, taskEndReason, taskInfo,
      executorMetrics, taskMetrics)
  }

  def jobStartFromJson(json: JsonNode): SparkListenerJobStart = {
    val jobId = json.get("Job ID").extractInt
    val submissionTime =
      jsonOption(json.get("Submission Time")).map(_.extractLong).getOrElse(-1L)
    val stageIds =
      json.get("Stage IDs").extractElements.map(_.extractInt).toArray.toImmutableArraySeq
    val properties = propertiesFromJson(json.get("Properties"))
    // The "Stage Infos" field was added in Spark 1.2.0
    val stageInfos = jsonOption(json.get("Stage Infos"))
      .map(_.extractElements.map(stageInfoFromJson).toArray.toImmutableArraySeq).getOrElse {
        stageIds.map { id =>
          new StageInfo(id, 0, "unknown", 0, Seq.empty, Seq.empty, "unknown",
            resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
        }
      }
    SparkListenerJobStart(jobId, submissionTime, stageInfos, properties)
  }

  def jobEndFromJson(json: JsonNode): SparkListenerJobEnd = {
    val jobId = json.get("Job ID").extractInt
    val completionTime =
      jsonOption(json.get("Completion Time")).map(_.extractLong).getOrElse(-1L)
    val jobResult = jobResultFromJson(json.get("Job Result"))
    SparkListenerJobEnd(jobId, completionTime, jobResult)
  }

  def resourceProfileAddedFromJson(json: JsonNode): SparkListenerResourceProfileAdded = {
    val profId = json.get("Resource Profile Id").extractInt
    val executorReqs = executorResourceRequestMapFromJson(json.get("Executor Resource Requests"))
    val taskReqs = taskResourceRequestMapFromJson(json.get("Task Resource Requests"))
    val rp = new ResourceProfile(executorReqs.toMap, taskReqs.toMap)
    rp.setResourceProfileId(profId)
    SparkListenerResourceProfileAdded(rp)
  }

  def executorResourceRequestFromJson(json: JsonNode): ExecutorResourceRequest = {
    val rName = json.get("Resource Name").extractString
    val amount = json.get("Amount").extractLong
    val discoveryScript = json.get("Discovery Script").extractString
    val vendor = json.get("Vendor").extractString
    new ExecutorResourceRequest(rName, amount, discoveryScript, vendor)
  }

  def taskResourceRequestFromJson(json: JsonNode): TaskResourceRequest = {
    val rName = json.get("Resource Name").extractString
    val amount = json.get("Amount").extractDouble
    new TaskResourceRequest(rName, amount)
  }

  def taskResourceRequestMapFromJson(json: JsonNode): Map[String, TaskResourceRequest] = {
    json.fields().asScala.collect { case field =>
      val req = taskResourceRequestFromJson(field.getValue)
      (field.getKey, req)
    }.toMap
  }

  def executorResourceRequestMapFromJson(json: JsonNode): Map[String, ExecutorResourceRequest] = {
    json.fields().asScala.collect { case field =>
      val req = executorResourceRequestFromJson(field.getValue)
      (field.getKey, req)
    }.toMap
  }

  def environmentUpdateFromJson(json: JsonNode): SparkListenerEnvironmentUpdate = {
    // For compatible with previous event logs
    val hadoopProperties = jsonOption(json.get("Hadoop Properties")).map(mapFromJson(_).toSeq)
      .getOrElse(Seq.empty)
    // The "Metrics Properties" field was added in Spark 3.4.0:
    val metricsProperties = jsonOption(json.get("Metrics Properties")).map(mapFromJson(_).toSeq)
      .getOrElse(Seq.empty)
    val environmentDetails = Map[String, Seq[(String, String)]](
      "JVM Information" -> mapFromJson(json.get("JVM Information")).toSeq,
      "Spark Properties" -> mapFromJson(json.get("Spark Properties")).toSeq,
      "Hadoop Properties" -> hadoopProperties,
      "System Properties" -> mapFromJson(json.get("System Properties")).toSeq,
      "Metrics Properties" -> metricsProperties,
      "Classpath Entries" -> mapFromJson(json.get("Classpath Entries")).toSeq)
    SparkListenerEnvironmentUpdate(environmentDetails)
  }

  def blockManagerAddedFromJson(json: JsonNode): SparkListenerBlockManagerAdded = {
    val blockManagerId = blockManagerIdFromJson(json.get("Block Manager ID"))
    val maxMem = json.get("Maximum Memory").extractLong
    val time = jsonOption(json.get("Timestamp")).map(_.extractLong).getOrElse(-1L)
    val maxOnHeapMem = jsonOption(json.get("Maximum Onheap Memory")).map(_.extractLong)
    val maxOffHeapMem = jsonOption(json.get("Maximum Offheap Memory")).map(_.extractLong)
    SparkListenerBlockManagerAdded(time, blockManagerId, maxMem, maxOnHeapMem, maxOffHeapMem)
  }

  def blockManagerRemovedFromJson(json: JsonNode): SparkListenerBlockManagerRemoved = {
    val blockManagerId = blockManagerIdFromJson(json.get("Block Manager ID"))
    val time = jsonOption(json.get("Timestamp")).map(_.extractLong).getOrElse(-1L)
    SparkListenerBlockManagerRemoved(time, blockManagerId)
  }

  def unpersistRDDFromJson(json: JsonNode): SparkListenerUnpersistRDD = {
    SparkListenerUnpersistRDD(json.get("RDD ID").extractInt)
  }

  def applicationStartFromJson(json: JsonNode): SparkListenerApplicationStart = {
    val appName = json.get("App Name").extractString
    val appId = jsonOption(json.get("App ID")).map(_.asText())
    val time = json.get("Timestamp").extractLong
    val sparkUser = json.get("User").extractString
    val appAttemptId = jsonOption(json.get("App Attempt ID")).map(_.asText())
    val driverLogs = jsonOption(json.get("Driver Logs")).map(mapFromJson)
    val driverAttributes = jsonOption(json.get("Driver Attributes")).map(mapFromJson)
    SparkListenerApplicationStart(appName, appId, time, sparkUser, appAttemptId, driverLogs,
      driverAttributes)
  }

  def applicationEndFromJson(json: JsonNode): SparkListenerApplicationEnd = {
    val exitCode = jsonOption(json.get("ExitCode")).map(_.extractInt)
    SparkListenerApplicationEnd(json.get("Timestamp").extractLong, exitCode)
  }

  def executorAddedFromJson(json: JsonNode): SparkListenerExecutorAdded = {
    val time = json.get("Timestamp").extractLong
    val executorId = json.get("Executor ID").extractString
    val executorInfo = executorInfoFromJson(json.get("Executor Info"))
    SparkListenerExecutorAdded(time, executorId, executorInfo)
  }

  def executorRemovedFromJson(json: JsonNode): SparkListenerExecutorRemoved = {
    val time = json.get("Timestamp").extractLong
    val executorId = json.get("Executor ID").extractString
    val reason = json.get("Removed Reason").extractString
    SparkListenerExecutorRemoved(time, executorId, reason)
  }

  def logStartFromJson(json: JsonNode): SparkListenerLogStart = {
    val sparkVersion = json.get("Spark Version").extractString
    SparkListenerLogStart(sparkVersion)
  }

  def executorMetricsUpdateFromJson(json: JsonNode): SparkListenerExecutorMetricsUpdate = {
    val execInfo = json.get("Executor ID").extractString
    val accumUpdates = json.get("Metrics Updated").extractElements.map { json =>
      val taskId = json.get("Task ID").extractLong
      val stageId = json.get("Stage ID").extractInt
      val stageAttemptId = json.get("Stage Attempt ID").extractInt
      val updates =
        json.get("Accumulator Updates").extractElements.map(accumulableInfoFromJson)
          .toArray.toImmutableArraySeq
      (taskId, stageId, stageAttemptId, updates)
    }.toArray.toImmutableArraySeq
    val executorUpdates = jsonOption(json.get("Executor Metrics Updated")).map { value =>
      value.extractElements.map { json =>
        val stageId = json.get("Stage ID").extractInt
        val stageAttemptId = json.get("Stage Attempt ID").extractInt
        val executorMetrics = executorMetricsFromJson(json.get("Executor Metrics"))
        ((stageId, stageAttemptId) -> executorMetrics)
      }.toMap
    }.getOrElse(Map.empty[(Int, Int), ExecutorMetrics])
    SparkListenerExecutorMetricsUpdate(execInfo, accumUpdates, executorUpdates)
  }

  def stageExecutorMetricsFromJson(json: JsonNode): SparkListenerStageExecutorMetrics = {
    val execId = json.get("Executor ID").extractString
    val stageId = json.get("Stage ID").extractInt
    val stageAttemptId = json.get("Stage Attempt ID").extractInt
    val executorMetrics = executorMetricsFromJson(json.get("Executor Metrics"))
    SparkListenerStageExecutorMetrics(execId, stageId, stageAttemptId, executorMetrics)
  }

  def blockUpdateFromJson(json: JsonNode): SparkListenerBlockUpdated = {
    val blockUpdatedInfo = blockUpdatedInfoFromJson(json.get("Block Updated Info"))
    SparkListenerBlockUpdated(blockUpdatedInfo)
  }

  /** --------------------------------------------------------------------- *
   * JSON deserialization methods for classes SparkListenerEvents depend on |
   * ---------------------------------------------------------------------- */

  def stageInfoFromJson(json: JsonNode): StageInfo = {
    val stageId = json.get("Stage ID").extractInt
    val attemptId = jsonOption(json.get("Stage Attempt ID")).map(_.extractInt).getOrElse(0)
    val stageName = json.get("Stage Name").extractString
    val numTasks = json.get("Number of Tasks").extractInt
    val rddInfos = json.get("RDD Info").extractElements.map(rddInfoFromJson).toArray
    val parentIds = jsonOption(json.get("Parent IDs"))
      .map { l => l.extractElements.map(_.extractInt).toArray.toImmutableArraySeq }
      .getOrElse(Seq.empty)
    val details = jsonOption(json.get("Details")).map(_.asText).getOrElse("")
    val submissionTime = jsonOption(json.get("Submission Time")).map(_.extractLong)
    val completionTime = jsonOption(json.get("Completion Time")).map(_.extractLong)
    val failureReason = jsonOption(json.get("Failure Reason")).map(_.asText)
    val accumulatedValues = {
      jsonOption(json.get("Accumulables")).map(_.extractElements) match {
        case Some(values) => values.map(accumulableInfoFromJson)
        case None => Seq.empty[AccumulableInfo]
      }
    }
    val isShufflePushEnabled =
      jsonOption(json.get("Shuffle Push Enabled")).map(_.extractBoolean).getOrElse(false)
    val shufflePushMergersCount =
      jsonOption(json.get("Shuffle Push Mergers Count")).map(_.extractInt).getOrElse(0)

    val rpId = jsonOption(json.get("Resource Profile Id")).map(_.extractInt)
    val stageProf = rpId.getOrElse(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val stageInfo = new StageInfo(stageId, attemptId, stageName, numTasks,
      rddInfos.toImmutableArraySeq, parentIds, details, resourceProfileId = stageProf,
      isShufflePushEnabled = isShufflePushEnabled,
      shuffleMergerCount = shufflePushMergersCount)
    stageInfo.submissionTime = submissionTime
    stageInfo.completionTime = completionTime
    stageInfo.failureReason = failureReason
    for (accInfo <- accumulatedValues) {
      stageInfo.accumulables(accInfo.id) = accInfo
    }
    stageInfo
  }

  def taskInfoFromJson(json: JsonNode): TaskInfo = {
    val taskId = json.get("Task ID").extractLong
    val index = json.get("Index").extractInt
    val attempt = jsonOption(json.get("Attempt")).map(_.extractInt).getOrElse(1)
    // The "Partition ID" field was added in Spark 3.3.0:
    val partitionId = jsonOption(json.get("Partition ID")).map(_.extractInt).getOrElse(-1)
    val launchTime = json.get("Launch Time").extractLong
    val executorId = weakIntern(json.get("Executor ID").extractString)
    val host = weakIntern(json.get("Host").extractString)
    val taskLocality = TaskLocality.withName(json.get("Locality").extractString)
    val speculative = jsonOption(json.get("Speculative")).exists(_.extractBoolean)
    val gettingResultTime = json.get("Getting Result Time").extractLong
    val finishTime = json.get("Finish Time").extractLong
    val failed = json.get("Failed").extractBoolean
    val killed = jsonOption(json.get("Killed")).exists(_.extractBoolean)
    val accumulables = jsonOption(json.get("Accumulables")).map(_.extractElements) match {
      case Some(values) => values.map(accumulableInfoFromJson).toArray.toImmutableArraySeq
      case None => Seq.empty[AccumulableInfo]
    }

    val taskInfo = new TaskInfo(
      taskId, index, attempt, partitionId, launchTime,
      executorId, host, taskLocality, speculative)
    taskInfo.gettingResultTime = gettingResultTime
    taskInfo.finishTime = finishTime
    taskInfo.failed = failed
    taskInfo.killed = killed
    taskInfo.setAccumulables(accumulables)
    taskInfo
  }

  def accumulableInfoFromJson(json: JsonNode): AccumulableInfo = {
    val id = json.get("ID").extractLong
    val name = jsonOption(json.get("Name")).map(_.asText)
    val update = jsonOption(json.get("Update")).map { v => accumValueFromJson(name, v) }
    val value = jsonOption(json.get("Value")).map { v => accumValueFromJson(name, v) }
    val internal = jsonOption(json.get("Internal")).exists(_.extractBoolean)
    val countFailedValues =
      jsonOption(json.get("Count Failed Values")).exists(_.extractBoolean)
    val metadata = jsonOption(json.get("Metadata")).map(_.asText)
    new AccumulableInfo(id, name, update, value, internal, countFailedValues, metadata)
  }

  /**
   * Deserialize the value of an accumulator from JSON.
   *
   * For accumulators representing internal task metrics, this looks up the relevant
   * [[AccumulatorParam]] to deserialize the value accordingly. For all other
   * accumulators, this will simply deserialize the value as a string.
   *
   * The behavior here must match that of [[accumValueToJson]]. Exposed for testing.
   */
  private[util] def accumValueFromJson(name: Option[String], value: JsonNode): Any = {
    if (name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))) {
      if (value.isIntegralNumber) {
        value.extractLong
      } else if (value.isArray) {
        value.extractElements.map { blockJson =>
          val id = BlockId(blockJson.get("Block ID").extractString)
          val status = blockStatusFromJson(blockJson.get("Status"))
          (id, status)
        }.toArray.toImmutableArraySeq.asJava
      } else {
        throw new IllegalArgumentException(s"unexpected json value $value for " +
          "accumulator " + name.get)
      }
    } else {
      value.asText
    }
  }

  def taskMetricsFromJson(json: JsonNode): TaskMetrics = {
    val metrics = TaskMetrics.empty
    if (json == null || json.isNull) {
      return metrics
    }
    metrics.setExecutorDeserializeTime(json.get("Executor Deserialize Time").extractLong)
    // The "Executor Deserialize CPU Time" field was added in Spark 2.1.0:
    metrics.setExecutorDeserializeCpuTime(
      jsonOption(json.get("Executor Deserialize CPU Time")).map(_.extractLong).getOrElse(0))
    metrics.setExecutorRunTime(json.get("Executor Run Time").extractLong)
    // The "Executor CPU Time" field was added in Spark 2.1.0:
    metrics.setExecutorCpuTime(
      jsonOption(json.get("Executor CPU Time")).map(_.extractLong).getOrElse(0))
    // The "Peak Execution Memory" field was added in Spark 3.0.0:
    metrics.setPeakExecutionMemory(
      jsonOption(json.get("Peak Execution Memory")).map(_.extractLong).getOrElse(0))
    metrics.setPeakOnHeapExecutionMemory(
      jsonOption(json.get("Peak On Heap Execution Memory")).map(_.extractLong).getOrElse(0))
    metrics.setPeakOffHeapExecutionMemory(
      jsonOption(json.get("Peak Off Heap Execution Memory")).map(_.extractLong).getOrElse(0))
    metrics.setResultSize(json.get("Result Size").extractLong)
    metrics.setJvmGCTime(json.get("JVM GC Time").extractLong)
    metrics.setResultSerializationTime(json.get("Result Serialization Time").extractLong)
    metrics.incMemoryBytesSpilled(json.get("Memory Bytes Spilled").extractLong)
    metrics.incDiskBytesSpilled(json.get("Disk Bytes Spilled").extractLong)

    // Shuffle read metrics
    jsonOption(json.get("Shuffle Read Metrics")).foreach { readJson =>
      val readMetrics = metrics.createTempShuffleReadMetrics()
      readMetrics.incRemoteBlocksFetched(readJson.get("Remote Blocks Fetched").extractInt)
      readMetrics.incLocalBlocksFetched(readJson.get("Local Blocks Fetched").extractInt)
      readMetrics.incRemoteBytesRead(readJson.get("Remote Bytes Read").extractLong)
      jsonOption(readJson.get("Remote Bytes Read To Disk"))
        .foreach { v => readMetrics.incRemoteBytesReadToDisk(v.extractLong)}
      readMetrics.incLocalBytesRead(
        jsonOption(readJson.get("Local Bytes Read")).map(_.extractLong).getOrElse(0L))
      readMetrics.incFetchWaitTime(readJson.get("Fetch Wait Time").extractLong)
      readMetrics.incRecordsRead(
        jsonOption(readJson.get("Total Records Read")).map(_.extractLong).getOrElse(0L))
      readMetrics.incRemoteReqsDuration(jsonOption(readJson.get("Remote Requests Duration"))
        .map(_.extractLong).getOrElse(0L))
      jsonOption(readJson.get("Shuffle Push Read Metrics")).foreach { shufflePushReadJson =>
        readMetrics.incCorruptMergedBlockChunks(jsonOption(
          shufflePushReadJson.get("Corrupt Merged Block Chunks"))
            .map(_.extractLong).getOrElse(0L))
        readMetrics.incMergedFetchFallbackCount(jsonOption(shufflePushReadJson
          .get("Merged Fetch Fallback Count")).map(_.extractLong).getOrElse(0L))
        readMetrics.incRemoteMergedBlocksFetched(jsonOption(shufflePushReadJson
          .get("Merged Remote Blocks Fetched")).map(_.extractLong).getOrElse(0L))
        readMetrics.incLocalMergedBlocksFetched(jsonOption(shufflePushReadJson
          .get("Merged Local Blocks Fetched")).map(_.extractLong).getOrElse(0L))
        readMetrics.incRemoteMergedChunksFetched(jsonOption(shufflePushReadJson
          .get("Merged Remote Chunks Fetched")).map(_.extractLong).getOrElse(0L))
        readMetrics.incLocalMergedChunksFetched(jsonOption(shufflePushReadJson
          .get("Merged Local Chunks Fetched")).map(_.extractLong).getOrElse(0L))
        readMetrics.incRemoteMergedBytesRead(jsonOption(shufflePushReadJson
          .get("Merged Remote Bytes Read")).map(_.extractLong).getOrElse(0L))
        readMetrics.incLocalMergedBytesRead(jsonOption(shufflePushReadJson
          .get("Merged Local Bytes Read")).map(_.extractLong).getOrElse(0L))
        readMetrics.incRemoteMergedReqsDuration(jsonOption(shufflePushReadJson
          .get("Merged Remote Requests Duration")).map(_.extractLong).getOrElse(0L))
      }
      metrics.mergeShuffleReadMetrics()
    }

    // Shuffle write metrics
    // TODO: Drop the redundant "Shuffle" since it's inconsistent with related classes.
    jsonOption(json.get("Shuffle Write Metrics")).foreach { writeJson =>
      val writeMetrics = metrics.shuffleWriteMetrics
      writeMetrics.incBytesWritten(writeJson.get("Shuffle Bytes Written").extractLong)
      writeMetrics.incRecordsWritten(
        jsonOption(writeJson.get("Shuffle Records Written")).map(_.extractLong).getOrElse(0L))
      writeMetrics.incWriteTime(writeJson.get("Shuffle Write Time").extractLong)
    }

    // Output metrics
    jsonOption(json.get("Output Metrics")).foreach { outJson =>
      val outputMetrics = metrics.outputMetrics
      outputMetrics.setBytesWritten(outJson.get("Bytes Written").extractLong)
      outputMetrics.setRecordsWritten(
        jsonOption(outJson.get("Records Written")).map(_.extractLong).getOrElse(0L))
    }

    // Input metrics
    jsonOption(json.get("Input Metrics")).foreach { inJson =>
      val inputMetrics = metrics.inputMetrics
      inputMetrics.incBytesRead(inJson.get("Bytes Read").extractLong)
      inputMetrics.incRecordsRead(
        jsonOption(inJson.get("Records Read")).map(_.extractLong).getOrElse(0L))
    }

    // Updated blocks
    jsonOption(json.get("Updated Blocks")).foreach { blocksJson =>
      metrics.setUpdatedBlockStatuses(blocksJson.extractElements.map { blockJson =>
        val id = BlockId(blockJson.get("Block ID").extractString)
        val status = blockStatusFromJson(blockJson.get("Status"))
        (id, status)
      }.toArray.toImmutableArraySeq)
    }

    metrics
  }

  private object TASK_END_REASON_FORMATTED_CLASS_NAMES {
    val success = Utils.getFormattedClassName(Success)
    val resubmitted = Utils.getFormattedClassName(Resubmitted)
    val fetchFailed = Utils.getFormattedClassName(FetchFailed)
    val exceptionFailure = Utils.getFormattedClassName(ExceptionFailure)
    val taskResultLost = Utils.getFormattedClassName(TaskResultLost)
    val taskKilled = Utils.getFormattedClassName(TaskKilled)
    val taskCommitDenied = Utils.getFormattedClassName(TaskCommitDenied)
    val executorLostFailure = Utils.getFormattedClassName(ExecutorLostFailure)
    val unknownReason = Utils.getFormattedClassName(UnknownReason)
  }

  def taskEndReasonFromJson(json: JsonNode): TaskEndReason = {
    import TASK_END_REASON_FORMATTED_CLASS_NAMES._

    json.get("Reason").extractString match {
      case `success` => Success
      case `resubmitted` => Resubmitted
      case `fetchFailed` =>
        val blockManagerAddress = blockManagerIdFromJson(json.get("Block Manager Address"))
        val shuffleId = json.get("Shuffle ID").extractInt
        val mapId = json.get("Map ID").extractLong
        val mapIndex = jsonOption(json.get("Map Index")).map(_.extractInt).getOrElse {
          // Note, we use the invalid value Int.MinValue here to fill the map index for backward
          // compatibility. Otherwise, the fetch failed event will be dropped when the history
          // server loads the event log written by the Spark version before 3.0.
          Int.MinValue
        }
        val reduceId = json.get("Reduce ID").extractInt
        val message = jsonOption(json.get("Message")).map(_.asText)
        new FetchFailed(blockManagerAddress, shuffleId, mapId, mapIndex, reduceId,
          message.getOrElse("Unknown reason"))
      case `exceptionFailure` =>
        val className = json.get("Class Name").extractString
        val description = json.get("Description").extractString
        val stackTrace = stackTraceFromJson(json.get("Stack Trace"))
        val fullStackTrace =
          jsonOption(json.get("Full Stack Trace")).map(_.asText).orNull
        // Fallback on getting accumulator updates from TaskMetrics, which was logged in Spark 1.x
        val accumUpdates = jsonOption(json.get("Accumulator Updates"))
          .map(_.extractElements.map(accumulableInfoFromJson).toArray.toImmutableArraySeq)
          .getOrElse(taskMetricsFromJson(json.get("Metrics")).accumulators().map(acc => {
            acc.toInfoUpdate
          }).toArray.toImmutableArraySeq)
        ExceptionFailure(className, description, stackTrace, fullStackTrace, None, accumUpdates)
      case `taskResultLost` => TaskResultLost
      case `taskKilled` =>
      // The "Kill Reason" field was added in Spark 2.2.0:
        val killReason = jsonOption(json.get("Kill Reason"))
          .map(_.asText).getOrElse("unknown reason")
        // The "Accumulator Updates" field was added in Spark 2.4.0:
        val accumUpdates = jsonOption(json.get("Accumulator Updates"))
          .map(_.extractElements.map(accumulableInfoFromJson).toArray.toImmutableArraySeq)
          .getOrElse(Seq[AccumulableInfo]())
        TaskKilled(killReason, accumUpdates)
      case `taskCommitDenied` =>
        // Unfortunately, the `TaskCommitDenied` message was introduced in 1.3.0 but the JSON
        // de/serialization logic was not added until 1.5.1. To provide backward compatibility
        // for reading those logs, we need to provide default values for all the fields.
        val jobId = jsonOption(json.get("Job ID")).map(_.extractInt).getOrElse(-1)
        val partitionId = jsonOption(json.get("Partition ID")).map(_.extractInt).getOrElse(-1)
        val attemptNo = jsonOption(json.get("Attempt Number")).map(_.extractInt).getOrElse(-1)
        TaskCommitDenied(jobId, partitionId, attemptNo)
      case `executorLostFailure` =>
        val exitCausedByApp = jsonOption(json.get("Exit Caused By App")).map(_.extractBoolean)
        val executorId = jsonOption(json.get("Executor ID")).map(_.asText)
        val reason = jsonOption(json.get("Loss Reason")).map(_.asText)
        ExecutorLostFailure(
          executorId.getOrElse("Unknown"),
          exitCausedByApp.getOrElse(true),
          reason)
      case `unknownReason` => UnknownReason
    }
  }

  def blockManagerIdFromJson(json: JsonNode): BlockManagerId = {
    // On metadata fetch fail, block manager ID can be null (SPARK-4471)
    if (json == null || json.isNull) {
      return null
    }
    val executorId = weakIntern(json.get("Executor ID").extractString)
    val host = weakIntern(json.get("Host").extractString)
    val port = json.get("Port").extractInt
    BlockManagerId(executorId, host, port)
  }

  private object JOB_RESULT_FORMATTED_CLASS_NAMES {
    val jobSucceeded = Utils.getFormattedClassName(JobSucceeded)
    val jobFailed = Utils.getFormattedClassName(JobFailed)
  }

  def jobResultFromJson(json: JsonNode): JobResult = {
    import JOB_RESULT_FORMATTED_CLASS_NAMES._

    json.get("Result").extractString match {
      case `jobSucceeded` => JobSucceeded
      case `jobFailed` =>
        val exception = exceptionFromJson(json.get("Exception"))
        new JobFailed(exception)
    }
  }

  def rddInfoFromJson(json: JsonNode): RDDInfo = {
    val rddId = json.get("RDD ID").extractInt
    val name = json.get("Name").extractString
    val scope = jsonOption(json.get("Scope"))
      .map(_.asText)
      .map(RDDOperationScope.fromJson)
    val callsite = jsonOption(json.get("Callsite")).map(_.asText).getOrElse("")
    val parentIds = jsonOption(json.get("Parent IDs"))
      .map { l => l.extractElements.map(_.extractInt).toArray.toImmutableArraySeq }
      .getOrElse(Seq.empty)
    val storageLevel = storageLevelFromJson(json.get("Storage Level"))
    // The "Barrier" field was added in Spark 3.0.0:
    val isBarrier = jsonOption(json.get("Barrier")).map(_.extractBoolean).getOrElse(false)
    val numPartitions = json.get("Number of Partitions").extractInt
    val numCachedPartitions = json.get("Number of Cached Partitions").extractInt
    val memSize = json.get("Memory Size").extractLong
    val diskSize = json.get("Disk Size").extractLong

    val outputDeterministicLevel = DeterministicLevel.withName(
      jsonOption(json.get("DeterministicLevel")).map(_.asText).getOrElse("DETERMINATE"))

    val rddInfo =
      new RDDInfo(rddId, name, numPartitions, storageLevel, isBarrier, parentIds, callsite, scope,
        outputDeterministicLevel)
    rddInfo.numCachedPartitions = numCachedPartitions
    rddInfo.memSize = memSize
    rddInfo.diskSize = diskSize
    rddInfo
  }

  def storageLevelFromJson(json: JsonNode): StorageLevel = {
    val useDisk = json.get("Use Disk").extractBoolean
    val useMemory = json.get("Use Memory").extractBoolean
    // The "Use Off Heap" field was added in Spark 3.4.0
    val useOffHeap = jsonOption(json.get("Use Off Heap")) match {
      case Some(value) => value.extractBoolean
      case None => false
    }
    val deserialized = json.get("Deserialized").extractBoolean
    val replication = json.get("Replication").extractInt
    StorageLevel(
      useDisk = useDisk,
      useMemory = useMemory,
      useOffHeap = useOffHeap,
      deserialized = deserialized,
      replication = replication)
  }

  def blockStatusFromJson(json: JsonNode): BlockStatus = {
    val storageLevel = storageLevelFromJson(json.get("Storage Level"))
    val memorySize = json.get("Memory Size").extractLong
    val diskSize = json.get("Disk Size").extractLong
    BlockStatus(storageLevel, memorySize, diskSize)
  }

  def executorInfoFromJson(json: JsonNode): ExecutorInfo = {
    val executorHost = json.get("Host").extractString
    val totalCores = json.get("Total Cores").extractInt
    val logUrls = mapFromJson(json.get("Log Urls")).toMap
    // The "Attributes" field was added in Spark 3.0.0:
    val attributes = jsonOption(json.get("Attributes")) match {
      case Some(attr) => mapFromJson(attr).toMap
      case None => Map.empty[String, String]
    }
    // The "Resources" field was added in Spark 3.0.0:
    val resources = jsonOption(json.get("Resources")) match {
      case Some(resources) => resourcesMapFromJson(resources).toMap
      case None => Map.empty[String, ResourceInformation]
    }
    // The "Resource Profile Id" field was added in Spark 3.4.0
    val resourceProfileId = jsonOption(json.get("Resource Profile Id")) match {
      case Some(id) => id.extractInt
      case None => ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
    }
    // The "Registration Time" field was added in Spark 3.4.0
    val registrationTs = jsonOption(json.get("Registration Time")) map { ts =>
      ts.extractLong
    }
    // The "Request Time" field was added in Spark 3.4.0
    val requestTs = jsonOption(json.get("Request Time")) map { ts =>
      ts.extractLong
    }

    new ExecutorInfo(executorHost, totalCores, logUrls, attributes.toMap, resources.toMap,
      resourceProfileId, registrationTs, requestTs)
  }

  def blockUpdatedInfoFromJson(json: JsonNode): BlockUpdatedInfo = {
    val blockManagerId = blockManagerIdFromJson(json.get("Block Manager ID"))
    val blockId = BlockId(json.get("Block ID").extractString)
    val storageLevel = storageLevelFromJson(json.get("Storage Level"))
    val memorySize = json.get("Memory Size").extractLong
    val diskSize = json.get("Disk Size").extractLong
    BlockUpdatedInfo(blockManagerId, blockId, storageLevel, memorySize, diskSize)
  }

  def resourcesMapFromJson(json: JsonNode): Map[String, ResourceInformation] = {
    assert(json.isObject, s"expected object, got ${json.getNodeType}")
    json.fields.asScala.map { field =>
      val resourceInfo = ResourceInformation.parseJson(field.getValue.toString)
      (field.getKey, resourceInfo)
    }.toMap
  }

  /** -------------------------------- *
   * Util JSON deserialization methods |
   * --------------------------------- */

  def mapFromJson(json: JsonNode): Map[String, String] = {
    assert(json.isObject, s"expected object, got ${json.getNodeType}")
    json.fields.asScala.map { field =>
      (field.getKey, field.getValue.extractString)
    }.toMap
  }

  def propertiesFromJson(json: JsonNode): Properties = {
    jsonOption(json).map { value =>
      val properties = new Properties
      mapFromJson(value).foreach { case (k, v) => properties.setProperty(k, v) }
      properties
    }.orNull
  }

  def UUIDFromJson(json: JsonNode): UUID = {
    val leastSignificantBits = json.get("Least Significant Bits").extractLong
    val mostSignificantBits = json.get("Most Significant Bits").extractLong
    new UUID(leastSignificantBits, mostSignificantBits)
  }

  def stackTraceFromJson(json: JsonNode): Array[StackTraceElement] = {
    jsonOption(json).map(_.extractElements.map { line =>
      val declaringClass = line.get("Declaring Class").extractString
      val methodName = line.get("Method Name").extractString
      val fileName = jsonOption(line.get("File Name")).map(_.extractString).orNull
      val lineNumber = line.get("Line Number").extractInt
      new StackTraceElement(declaringClass, methodName, fileName, lineNumber)
    }.toArray).getOrElse(Array[StackTraceElement]())
  }

  def exceptionFromJson(json: JsonNode): Exception = {
    val message = jsonOption(json.get("Message")).map(_.extractString).orNull
    val e = new Exception(message)
    e.setStackTrace(stackTraceFromJson(json.get("Stack Trace")))
    e
  }

  /** Return an option that translates NullNode to None */
  private def jsonOption(json: JsonNode): Option[JsonNode] = {
    if (json == null || json.isNull) {
      None
    } else {
      Some(json)
    }
  }

  /**
   * Implicit conversions to add methods to JsonNode that perform type-checking when
   * reading fields. This ensures that JSON parsing will fail if we process JSON with
   * unexpected input types (instead of silently falling back to default values).
   */
  private implicit class JsonNodeImplicits(json: JsonNode) {
    def extractElements: Iterator[JsonNode] = {
      require(json.isContainerNode, s"Expected container, got ${json.getNodeType}")
      json.elements.asScala
    }

    def extractBoolean: Boolean = {
      require(json.isBoolean, s"Expected boolean, got ${json.getNodeType}")
      json.booleanValue
    }

    def extractInt: Int = {
      require(json.isNumber, s"Expected number, got ${json.getNodeType}")
      json.intValue
    }

    def extractLong: Long = {
      require(json.isNumber, s"Expected number, got ${json.getNodeType}")
      json.longValue
    }

    def extractDouble: Double = {
      require(json.isNumber, s"Expected number, got ${json.getNodeType}")
      json.doubleValue
    }

    def extractString: String = {
      require(json.isTextual || json.isNull, s"Expected string or NULL, got ${json.getNodeType}")
      json.textValue
    }
  }
}
