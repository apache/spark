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

import scala.collection.JavaConverters._
import scala.collection.Map

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.rdd.{DeterministicLevel, RDDOperationScope}
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceProfile, TaskResourceRequest}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage._
import org.apache.spark.util.Utils.weakIntern

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
private[spark] object JsonProtocol {
  // TODO: Remove this file and put JSON serialization into each individual class.

  private implicit val format = DefaultFormats

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  /** ------------------------------------------------- *
   * JSON serialization methods for SparkListenerEvents |
   * -------------------------------------------------- */

  def sparkEventToJson(event: SparkListenerEvent): JValue = {
    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
        stageSubmittedToJson(stageSubmitted)
      case stageCompleted: SparkListenerStageCompleted =>
        stageCompletedToJson(stageCompleted)
      case taskStart: SparkListenerTaskStart =>
        taskStartToJson(taskStart)
      case taskGettingResult: SparkListenerTaskGettingResult =>
        taskGettingResultToJson(taskGettingResult)
      case taskEnd: SparkListenerTaskEnd =>
        taskEndToJson(taskEnd)
      case jobStart: SparkListenerJobStart =>
        jobStartToJson(jobStart)
      case jobEnd: SparkListenerJobEnd =>
        jobEndToJson(jobEnd)
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        environmentUpdateToJson(environmentUpdate)
      case blockManagerAdded: SparkListenerBlockManagerAdded =>
        blockManagerAddedToJson(blockManagerAdded)
      case blockManagerRemoved: SparkListenerBlockManagerRemoved =>
        blockManagerRemovedToJson(blockManagerRemoved)
      case unpersistRDD: SparkListenerUnpersistRDD =>
        unpersistRDDToJson(unpersistRDD)
      case applicationStart: SparkListenerApplicationStart =>
        applicationStartToJson(applicationStart)
      case applicationEnd: SparkListenerApplicationEnd =>
        applicationEndToJson(applicationEnd)
      case executorAdded: SparkListenerExecutorAdded =>
        executorAddedToJson(executorAdded)
      case executorRemoved: SparkListenerExecutorRemoved =>
        executorRemovedToJson(executorRemoved)
      case logStart: SparkListenerLogStart =>
        logStartToJson(logStart)
      case metricsUpdate: SparkListenerExecutorMetricsUpdate =>
        executorMetricsUpdateToJson(metricsUpdate)
      case stageExecutorMetrics: SparkListenerStageExecutorMetrics =>
        stageExecutorMetricsToJson(stageExecutorMetrics)
      case blockUpdate: SparkListenerBlockUpdated =>
        blockUpdateToJson(blockUpdate)
      case resourceProfileAdded: SparkListenerResourceProfileAdded =>
        resourceProfileAddedToJson(resourceProfileAdded)
      case _ => parse(mapper.writeValueAsString(event))
    }
  }

  def stageSubmittedToJson(stageSubmitted: SparkListenerStageSubmitted): JValue = {
    val stageInfo = stageInfoToJson(stageSubmitted.stageInfo)
    val properties = propertiesToJson(stageSubmitted.properties)
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageSubmitted) ~
    ("Stage Info" -> stageInfo) ~
    ("Properties" -> properties)
  }

  def stageCompletedToJson(stageCompleted: SparkListenerStageCompleted): JValue = {
    val stageInfo = stageInfoToJson(stageCompleted.stageInfo)
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageCompleted) ~
    ("Stage Info" -> stageInfo)
  }

  def taskStartToJson(taskStart: SparkListenerTaskStart): JValue = {
    val taskInfo = taskStart.taskInfo
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskStart) ~
    ("Stage ID" -> taskStart.stageId) ~
    ("Stage Attempt ID" -> taskStart.stageAttemptId) ~
    ("Task Info" -> taskInfoToJson(taskInfo))
  }

  def taskGettingResultToJson(taskGettingResult: SparkListenerTaskGettingResult): JValue = {
    val taskInfo = taskGettingResult.taskInfo
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskGettingResult) ~
    ("Task Info" -> taskInfoToJson(taskInfo))
  }

  def taskEndToJson(taskEnd: SparkListenerTaskEnd): JValue = {
    val taskEndReason = taskEndReasonToJson(taskEnd.reason)
    val taskInfo = taskEnd.taskInfo
    val executorMetrics = taskEnd.taskExecutorMetrics
    val taskMetrics = taskEnd.taskMetrics
    val taskMetricsJson = if (taskMetrics != null) taskMetricsToJson(taskMetrics) else JNothing
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskEnd) ~
    ("Stage ID" -> taskEnd.stageId) ~
    ("Stage Attempt ID" -> taskEnd.stageAttemptId) ~
    ("Task Type" -> taskEnd.taskType) ~
    ("Task End Reason" -> taskEndReason) ~
    ("Task Info" -> taskInfoToJson(taskInfo)) ~
    ("Task Executor Metrics" -> executorMetricsToJson(executorMetrics)) ~
    ("Task Metrics" -> taskMetricsJson)
  }

  def jobStartToJson(jobStart: SparkListenerJobStart): JValue = {
    val properties = propertiesToJson(jobStart.properties)
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.jobStart) ~
    ("Job ID" -> jobStart.jobId) ~
    ("Submission Time" -> jobStart.time) ~
    ("Stage Infos" -> jobStart.stageInfos.map(stageInfoToJson)) ~  // Added in Spark 1.2.0
    ("Stage IDs" -> jobStart.stageIds) ~
    ("Properties" -> properties)
  }

  def jobEndToJson(jobEnd: SparkListenerJobEnd): JValue = {
    val jobResult = jobResultToJson(jobEnd.jobResult)
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.jobEnd) ~
    ("Job ID" -> jobEnd.jobId) ~
    ("Completion Time" -> jobEnd.time) ~
    ("Job Result" -> jobResult)
  }

  def environmentUpdateToJson(environmentUpdate: SparkListenerEnvironmentUpdate): JValue = {
    val environmentDetails = environmentUpdate.environmentDetails
    val jvmInformation = mapToJson(environmentDetails("JVM Information").toMap)
    val sparkProperties = mapToJson(environmentDetails("Spark Properties").toMap)
    val hadoopProperties = mapToJson(environmentDetails("Hadoop Properties").toMap)
    val systemProperties = mapToJson(environmentDetails("System Properties").toMap)
    val classpathEntries = mapToJson(environmentDetails("Classpath Entries").toMap)
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.environmentUpdate) ~
    ("JVM Information" -> jvmInformation) ~
    ("Spark Properties" -> sparkProperties) ~
    ("Hadoop Properties" -> hadoopProperties) ~
    ("System Properties" -> systemProperties) ~
    ("Classpath Entries" -> classpathEntries)
  }

  def blockManagerAddedToJson(blockManagerAdded: SparkListenerBlockManagerAdded): JValue = {
    val blockManagerId = blockManagerIdToJson(blockManagerAdded.blockManagerId)
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockManagerAdded) ~
    ("Block Manager ID" -> blockManagerId) ~
    ("Maximum Memory" -> blockManagerAdded.maxMem) ~
    ("Timestamp" -> blockManagerAdded.time) ~
    ("Maximum Onheap Memory" -> blockManagerAdded.maxOnHeapMem) ~
    ("Maximum Offheap Memory" -> blockManagerAdded.maxOffHeapMem)
  }

  def blockManagerRemovedToJson(blockManagerRemoved: SparkListenerBlockManagerRemoved): JValue = {
    val blockManagerId = blockManagerIdToJson(blockManagerRemoved.blockManagerId)
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockManagerRemoved) ~
    ("Block Manager ID" -> blockManagerId) ~
    ("Timestamp" -> blockManagerRemoved.time)
  }

  def unpersistRDDToJson(unpersistRDD: SparkListenerUnpersistRDD): JValue = {
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.unpersistRDD) ~
    ("RDD ID" -> unpersistRDD.rddId)
  }

  def applicationStartToJson(applicationStart: SparkListenerApplicationStart): JValue = {
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.applicationStart) ~
    ("App Name" -> applicationStart.appName) ~
    ("App ID" -> applicationStart.appId.map(JString(_)).getOrElse(JNothing)) ~
    ("Timestamp" -> applicationStart.time) ~
    ("User" -> applicationStart.sparkUser) ~
    ("App Attempt ID" -> applicationStart.appAttemptId.map(JString(_)).getOrElse(JNothing)) ~
    ("Driver Logs" -> applicationStart.driverLogs.map(mapToJson).getOrElse(JNothing)) ~
    ("Driver Attributes" -> applicationStart.driverAttributes.map(mapToJson).getOrElse(JNothing))
  }

  def applicationEndToJson(applicationEnd: SparkListenerApplicationEnd): JValue = {
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.applicationEnd) ~
    ("Timestamp" -> applicationEnd.time)
  }

  def resourceProfileAddedToJson(profileAdded: SparkListenerResourceProfileAdded): JValue = {
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.resourceProfileAdded) ~
      ("Resource Profile Id" -> profileAdded.resourceProfile.id) ~
      ("Executor Resource Requests" ->
        executorResourceRequestMapToJson(profileAdded.resourceProfile.executorResources)) ~
      ("Task Resource Requests" ->
        taskResourceRequestMapToJson(profileAdded.resourceProfile.taskResources))
  }

  def executorAddedToJson(executorAdded: SparkListenerExecutorAdded): JValue = {
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.executorAdded) ~
    ("Timestamp" -> executorAdded.time) ~
    ("Executor ID" -> executorAdded.executorId) ~
    ("Executor Info" -> executorInfoToJson(executorAdded.executorInfo))
  }

  def executorRemovedToJson(executorRemoved: SparkListenerExecutorRemoved): JValue = {
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.executorRemoved) ~
    ("Timestamp" -> executorRemoved.time) ~
    ("Executor ID" -> executorRemoved.executorId) ~
    ("Removed Reason" -> executorRemoved.reason)
  }

  def logStartToJson(logStart: SparkListenerLogStart): JValue = {
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.logStart) ~
    ("Spark Version" -> SPARK_VERSION)
  }

  def executorMetricsUpdateToJson(metricsUpdate: SparkListenerExecutorMetricsUpdate): JValue = {
    val execId = metricsUpdate.execId
    val accumUpdates = metricsUpdate.accumUpdates
    val executorUpdates = metricsUpdate.executorUpdates
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.metricsUpdate) ~
    ("Executor ID" -> execId) ~
    ("Metrics Updated" -> accumUpdates.map { case (taskId, stageId, stageAttemptId, updates) =>
      ("Task ID" -> taskId) ~
      ("Stage ID" -> stageId) ~
      ("Stage Attempt ID" -> stageAttemptId) ~
      ("Accumulator Updates" -> JArray(updates.map(accumulableInfoToJson).toList))
    }) ~
    ("Executor Metrics Updated" -> executorUpdates.map {
      case ((stageId, stageAttemptId), metrics) =>
        ("Stage ID" -> stageId) ~
        ("Stage Attempt ID" -> stageAttemptId) ~
        ("Executor Metrics" -> executorMetricsToJson(metrics))
    })
  }

  def stageExecutorMetricsToJson(metrics: SparkListenerStageExecutorMetrics): JValue = {
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageExecutorMetrics) ~
    ("Executor ID" -> metrics.execId) ~
    ("Stage ID" -> metrics.stageId) ~
    ("Stage Attempt ID" -> metrics.stageAttemptId) ~
    ("Executor Metrics" -> executorMetricsToJson(metrics.executorMetrics))
  }

  def blockUpdateToJson(blockUpdate: SparkListenerBlockUpdated): JValue = {
    val blockUpdatedInfo = blockUpdatedInfoToJson(blockUpdate.blockUpdatedInfo)
    ("Event" -> SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockUpdate) ~
    ("Block Updated Info" -> blockUpdatedInfo)
  }

  /** ------------------------------------------------------------------- *
   * JSON serialization methods for classes SparkListenerEvents depend on |
   * -------------------------------------------------------------------- */

  def stageInfoToJson(stageInfo: StageInfo): JValue = {
    val rddInfo = JArray(stageInfo.rddInfos.map(rddInfoToJson).toList)
    val parentIds = JArray(stageInfo.parentIds.map(JInt(_)).toList)
    val submissionTime = stageInfo.submissionTime.map(JInt(_)).getOrElse(JNothing)
    val completionTime = stageInfo.completionTime.map(JInt(_)).getOrElse(JNothing)
    val failureReason = stageInfo.failureReason.map(JString(_)).getOrElse(JNothing)
    ("Stage ID" -> stageInfo.stageId) ~
    ("Stage Attempt ID" -> stageInfo.attemptNumber) ~
    ("Stage Name" -> stageInfo.name) ~
    ("Number of Tasks" -> stageInfo.numTasks) ~
    ("RDD Info" -> rddInfo) ~
    ("Parent IDs" -> parentIds) ~
    ("Details" -> stageInfo.details) ~
    ("Submission Time" -> submissionTime) ~
    ("Completion Time" -> completionTime) ~
    ("Failure Reason" -> failureReason) ~
    ("Accumulables" -> accumulablesToJson(stageInfo.accumulables.values)) ~
    ("Resource Profile Id" -> stageInfo.resourceProfileId)
  }

  def taskInfoToJson(taskInfo: TaskInfo): JValue = {
    ("Task ID" -> taskInfo.taskId) ~
    ("Index" -> taskInfo.index) ~
    ("Attempt" -> taskInfo.attemptNumber) ~
    ("Partition ID" -> taskInfo.partitionId) ~
    ("Launch Time" -> taskInfo.launchTime) ~
    ("Executor ID" -> taskInfo.executorId) ~
    ("Host" -> taskInfo.host) ~
    ("Locality" -> taskInfo.taskLocality.toString) ~
    ("Speculative" -> taskInfo.speculative) ~
    ("Getting Result Time" -> taskInfo.gettingResultTime) ~
    ("Finish Time" -> taskInfo.finishTime) ~
    ("Failed" -> taskInfo.failed) ~
    ("Killed" -> taskInfo.killed) ~
    ("Accumulables" -> accumulablesToJson(taskInfo.accumulables))
  }

  private lazy val accumulableExcludeList = Set("internal.metrics.updatedBlockStatuses")

  def accumulablesToJson(accumulables: Iterable[AccumulableInfo]): JArray = {
    JArray(accumulables
        .filterNot(_.name.exists(accumulableExcludeList.contains))
        .toList.sortBy(_.id).map(accumulableInfoToJson))
  }

  def accumulableInfoToJson(accumulableInfo: AccumulableInfo): JValue = {
    val name = accumulableInfo.name
    ("ID" -> accumulableInfo.id) ~
    ("Name" -> name) ~
    ("Update" -> accumulableInfo.update.map { v => accumValueToJson(name, v) }) ~
    ("Value" -> accumulableInfo.value.map { v => accumValueToJson(name, v) }) ~
    ("Internal" -> accumulableInfo.internal) ~
    ("Count Failed Values" -> accumulableInfo.countFailedValues) ~
    ("Metadata" -> accumulableInfo.metadata)
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
  private[util] def accumValueToJson(name: Option[String], value: Any): JValue = {
    if (name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))) {
      value match {
        case v: Int => JInt(v)
        case v: Long => JInt(v)
        // We only have 3 kind of internal accumulator types, so if it's not int or long, it must be
        // the blocks accumulator, whose type is `java.util.List[(BlockId, BlockStatus)]`
        case v: java.util.List[_] =>
          JArray(v.asScala.toList.flatMap {
            case (id: BlockId, status: BlockStatus) =>
              Some(
                ("Block ID" -> id.toString) ~
                ("Status" -> blockStatusToJson(status))
              )
            case _ =>
              // Ignore unsupported types. A user may put `METRICS_PREFIX` in the name. We should
              // not crash.
              None
          })
        case _ =>
          // Ignore unsupported types. A user may put `METRICS_PREFIX` in the name. We should not
          // crash.
          JNothing
      }
    } else {
      // For all external accumulators, just use strings
      JString(value.toString)
    }
  }

  def taskMetricsToJson(taskMetrics: TaskMetrics): JValue = {
    val shuffleReadMetrics: JValue =
      ("Remote Blocks Fetched" -> taskMetrics.shuffleReadMetrics.remoteBlocksFetched) ~
        ("Local Blocks Fetched" -> taskMetrics.shuffleReadMetrics.localBlocksFetched) ~
        ("Fetch Wait Time" -> taskMetrics.shuffleReadMetrics.fetchWaitTime) ~
        ("Remote Bytes Read" -> taskMetrics.shuffleReadMetrics.remoteBytesRead) ~
        ("Remote Bytes Read To Disk" -> taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk) ~
        ("Local Bytes Read" -> taskMetrics.shuffleReadMetrics.localBytesRead) ~
        ("Total Records Read" -> taskMetrics.shuffleReadMetrics.recordsRead)
    val shuffleWriteMetrics: JValue =
      ("Shuffle Bytes Written" -> taskMetrics.shuffleWriteMetrics.bytesWritten) ~
        ("Shuffle Write Time" -> taskMetrics.shuffleWriteMetrics.writeTime) ~
        ("Shuffle Records Written" -> taskMetrics.shuffleWriteMetrics.recordsWritten)
    val inputMetrics: JValue =
      ("Bytes Read" -> taskMetrics.inputMetrics.bytesRead) ~
        ("Records Read" -> taskMetrics.inputMetrics.recordsRead)
    val outputMetrics: JValue =
      ("Bytes Written" -> taskMetrics.outputMetrics.bytesWritten) ~
        ("Records Written" -> taskMetrics.outputMetrics.recordsWritten)
    val updatedBlocks =
      JArray(taskMetrics.updatedBlockStatuses.toList.map { case (id, status) =>
        ("Block ID" -> id.toString) ~
          ("Status" -> blockStatusToJson(status))
      })
    ("Executor Deserialize Time" -> taskMetrics.executorDeserializeTime) ~
    ("Executor Deserialize CPU Time" -> taskMetrics.executorDeserializeCpuTime) ~
    ("Executor Run Time" -> taskMetrics.executorRunTime) ~
    ("Executor CPU Time" -> taskMetrics.executorCpuTime) ~
    ("Peak Execution Memory" -> taskMetrics.peakExecutionMemory) ~
    ("Result Size" -> taskMetrics.resultSize) ~
    ("JVM GC Time" -> taskMetrics.jvmGCTime) ~
    ("Result Serialization Time" -> taskMetrics.resultSerializationTime) ~
    ("Memory Bytes Spilled" -> taskMetrics.memoryBytesSpilled) ~
    ("Disk Bytes Spilled" -> taskMetrics.diskBytesSpilled) ~
    ("Shuffle Read Metrics" -> shuffleReadMetrics) ~
    ("Shuffle Write Metrics" -> shuffleWriteMetrics) ~
    ("Input Metrics" -> inputMetrics) ~
    ("Output Metrics" -> outputMetrics) ~
    ("Updated Blocks" -> updatedBlocks)
  }

  /** Convert executor metrics to JSON. */
  def executorMetricsToJson(executorMetrics: ExecutorMetrics): JValue = {
    val metrics = ExecutorMetricType.metricToOffset.map { case (m, _) =>
      JField(m, executorMetrics.getMetricValue(m))
    }
    JObject(metrics.toSeq: _*)
  }

  def taskEndReasonToJson(taskEndReason: TaskEndReason): JValue = {
    val reason = Utils.getFormattedClassName(taskEndReason)
    val json: JObject = taskEndReason match {
      case fetchFailed: FetchFailed =>
        val blockManagerAddress = Option(fetchFailed.bmAddress).
          map(blockManagerIdToJson).getOrElse(JNothing)
        ("Block Manager Address" -> blockManagerAddress) ~
        ("Shuffle ID" -> fetchFailed.shuffleId) ~
        ("Map ID" -> fetchFailed.mapId) ~
        ("Map Index" -> fetchFailed.mapIndex) ~
        ("Reduce ID" -> fetchFailed.reduceId) ~
        ("Message" -> fetchFailed.message)
      case exceptionFailure: ExceptionFailure =>
        val stackTrace = stackTraceToJson(exceptionFailure.stackTrace)
        val accumUpdates = accumulablesToJson(exceptionFailure.accumUpdates)
        ("Class Name" -> exceptionFailure.className) ~
        ("Description" -> exceptionFailure.description) ~
        ("Stack Trace" -> stackTrace) ~
        ("Full Stack Trace" -> exceptionFailure.fullStackTrace) ~
        ("Accumulator Updates" -> accumUpdates)
      case taskCommitDenied: TaskCommitDenied =>
        ("Job ID" -> taskCommitDenied.jobID) ~
        ("Partition ID" -> taskCommitDenied.partitionID) ~
        ("Attempt Number" -> taskCommitDenied.attemptNumber)
      case ExecutorLostFailure(executorId, exitCausedByApp, reason) =>
        ("Executor ID" -> executorId) ~
        ("Exit Caused By App" -> exitCausedByApp) ~
        ("Loss Reason" -> reason)
      case taskKilled: TaskKilled =>
        val accumUpdates = JArray(taskKilled.accumUpdates.map(accumulableInfoToJson).toList)
        ("Kill Reason" -> taskKilled.reason) ~
        ("Accumulator Updates" -> accumUpdates)
      case _ => emptyJson
    }
    ("Reason" -> reason) ~ json
  }

  def blockManagerIdToJson(blockManagerId: BlockManagerId): JValue = {
    ("Executor ID" -> blockManagerId.executorId) ~
    ("Host" -> blockManagerId.host) ~
    ("Port" -> blockManagerId.port)
  }

  def jobResultToJson(jobResult: JobResult): JValue = {
    val result = Utils.getFormattedClassName(jobResult)
    val json = jobResult match {
      case JobSucceeded => emptyJson
      case jobFailed: JobFailed =>
        JObject("Exception" -> exceptionToJson(jobFailed.exception))
    }
    ("Result" -> result) ~ json
  }

  def rddInfoToJson(rddInfo: RDDInfo): JValue = {
    val storageLevel = storageLevelToJson(rddInfo.storageLevel)
    val parentIds = JArray(rddInfo.parentIds.map(JInt(_)).toList)
    ("RDD ID" -> rddInfo.id) ~
    ("Name" -> rddInfo.name) ~
    ("Scope" -> rddInfo.scope.map(_.toJson)) ~
    ("Callsite" -> rddInfo.callSite) ~
    ("Parent IDs" -> parentIds) ~
    ("Storage Level" -> storageLevel) ~
    ("Barrier" -> rddInfo.isBarrier) ~
    ("DeterministicLevel" -> rddInfo.outputDeterministicLevel.toString) ~
    ("Number of Partitions" -> rddInfo.numPartitions) ~
    ("Number of Cached Partitions" -> rddInfo.numCachedPartitions) ~
    ("Memory Size" -> rddInfo.memSize) ~
    ("Disk Size" -> rddInfo.diskSize)
  }

  def storageLevelToJson(storageLevel: StorageLevel): JValue = {
    ("Use Disk" -> storageLevel.useDisk) ~
    ("Use Memory" -> storageLevel.useMemory) ~
    ("Deserialized" -> storageLevel.deserialized) ~
    ("Replication" -> storageLevel.replication)
  }

  def blockStatusToJson(blockStatus: BlockStatus): JValue = {
    val storageLevel = storageLevelToJson(blockStatus.storageLevel)
    ("Storage Level" -> storageLevel) ~
    ("Memory Size" -> blockStatus.memSize) ~
    ("Disk Size" -> blockStatus.diskSize)
  }

  def executorInfoToJson(executorInfo: ExecutorInfo): JValue = {
    ("Host" -> executorInfo.executorHost) ~
    ("Total Cores" -> executorInfo.totalCores) ~
    ("Log Urls" -> mapToJson(executorInfo.logUrlMap)) ~
    ("Attributes" -> mapToJson(executorInfo.attributes)) ~
    ("Resources" -> resourcesMapToJson(executorInfo.resourcesInfo)) ~
    ("Resource Profile Id" -> executorInfo.resourceProfileId)
  }

  def resourcesMapToJson(m: Map[String, ResourceInformation]): JValue = {
    val jsonFields = m.map {
      case (k, v) => JField(k, v.toJson)
    }
    JObject(jsonFields.toList)
  }

  def blockUpdatedInfoToJson(blockUpdatedInfo: BlockUpdatedInfo): JValue = {
    ("Block Manager ID" -> blockManagerIdToJson(blockUpdatedInfo.blockManagerId)) ~
    ("Block ID" -> blockUpdatedInfo.blockId.toString) ~
    ("Storage Level" -> storageLevelToJson(blockUpdatedInfo.storageLevel)) ~
    ("Memory Size" -> blockUpdatedInfo.memSize) ~
    ("Disk Size" -> blockUpdatedInfo.diskSize)
  }

  def executorResourceRequestToJson(execReq: ExecutorResourceRequest): JValue = {
    ("Resource Name" -> execReq.resourceName) ~
    ("Amount" -> execReq.amount) ~
    ("Discovery Script" -> execReq.discoveryScript) ~
    ("Vendor" -> execReq.vendor)
  }

  def executorResourceRequestMapToJson(m: Map[String, ExecutorResourceRequest]): JValue = {
    val jsonFields = m.map {
      case (k, execReq) =>
        JField(k, executorResourceRequestToJson(execReq))
    }
    JObject(jsonFields.toList)
  }

  def taskResourceRequestToJson(taskReq: TaskResourceRequest): JValue = {
    ("Resource Name" -> taskReq.resourceName) ~
    ("Amount" -> taskReq.amount)
  }

  def taskResourceRequestMapToJson(m: Map[String, TaskResourceRequest]): JValue = {
    val jsonFields = m.map {
      case (k, taskReq) =>
        JField(k, taskResourceRequestToJson(taskReq))
    }
    JObject(jsonFields.toList)
  }

  /** ------------------------------ *
   * Util JSON serialization methods |
   * ------------------------------- */

  def mapToJson(m: Map[String, String]): JValue = {
    val jsonFields = m.map { case (k, v) => JField(k, JString(v)) }
    JObject(jsonFields.toList)
  }

  def propertiesToJson(properties: Properties): JValue = {
    Option(properties).map { p =>
      mapToJson(p.asScala)
    }.getOrElse(JNothing)
  }

  def UUIDToJson(id: UUID): JValue = {
    ("Least Significant Bits" -> id.getLeastSignificantBits) ~
    ("Most Significant Bits" -> id.getMostSignificantBits)
  }

  def stackTraceToJson(stackTrace: Array[StackTraceElement]): JValue = {
    JArray(stackTrace.map { case line =>
      ("Declaring Class" -> line.getClassName) ~
      ("Method Name" -> line.getMethodName) ~
      ("File Name" -> line.getFileName) ~
      ("Line Number" -> line.getLineNumber)
    }.toList)
  }

  def exceptionToJson(exception: Exception): JValue = {
    ("Message" -> exception.getMessage) ~
    ("Stack Trace" -> stackTraceToJson(exception.getStackTrace))
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

  def sparkEventFromJson(json: JValue): SparkListenerEvent = {
    import SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES._

    (json \ "Event").extract[String] match {
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
      case other => mapper.readValue(compact(render(json)), Utils.classForName(other))
        .asInstanceOf[SparkListenerEvent]
    }
  }

  def stageSubmittedFromJson(json: JValue): SparkListenerStageSubmitted = {
    val stageInfo = stageInfoFromJson(json \ "Stage Info")
    val properties = propertiesFromJson(json \ "Properties")
    SparkListenerStageSubmitted(stageInfo, properties)
  }

  def stageCompletedFromJson(json: JValue): SparkListenerStageCompleted = {
    val stageInfo = stageInfoFromJson(json \ "Stage Info")
    SparkListenerStageCompleted(stageInfo)
  }

  def taskStartFromJson(json: JValue): SparkListenerTaskStart = {
    val stageId = (json \ "Stage ID").extract[Int]
    val stageAttemptId =
      jsonOption(json \ "Stage Attempt ID").map(_.extract[Int]).getOrElse(0)
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    SparkListenerTaskStart(stageId, stageAttemptId, taskInfo)
  }

  def taskGettingResultFromJson(json: JValue): SparkListenerTaskGettingResult = {
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    SparkListenerTaskGettingResult(taskInfo)
  }

  /** Extract the executor metrics from JSON. */
  def executorMetricsFromJson(json: JValue): ExecutorMetrics = {
    val metrics =
      ExecutorMetricType.metricToOffset.map { case (metric, _) =>
        metric -> jsonOption(json \ metric).map(_.extract[Long]).getOrElse(0L)
      }
    new ExecutorMetrics(metrics.toMap)
  }

  def taskEndFromJson(json: JValue): SparkListenerTaskEnd = {
    val stageId = (json \ "Stage ID").extract[Int]
    val stageAttemptId =
      jsonOption(json \ "Stage Attempt ID").map(_.extract[Int]).getOrElse(0)
    val taskType = (json \ "Task Type").extract[String]
    val taskEndReason = taskEndReasonFromJson(json \ "Task End Reason")
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    val executorMetrics = executorMetricsFromJson(json \ "Task Executor Metrics")
    val taskMetrics = taskMetricsFromJson(json \ "Task Metrics")
    SparkListenerTaskEnd(stageId, stageAttemptId, taskType, taskEndReason, taskInfo,
      executorMetrics, taskMetrics)
  }

  def jobStartFromJson(json: JValue): SparkListenerJobStart = {
    val jobId = (json \ "Job ID").extract[Int]
    val submissionTime =
      jsonOption(json \ "Submission Time").map(_.extract[Long]).getOrElse(-1L)
    val stageIds = (json \ "Stage IDs").extract[List[JValue]].map(_.extract[Int])
    val properties = propertiesFromJson(json \ "Properties")
    // The "Stage Infos" field was added in Spark 1.2.0
    val stageInfos = jsonOption(json \ "Stage Infos")
      .map(_.extract[Seq[JValue]].map(stageInfoFromJson)).getOrElse {
        stageIds.map { id =>
          new StageInfo(id, 0, "unknown", 0, Seq.empty, Seq.empty, "unknown",
            resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
        }
      }
    SparkListenerJobStart(jobId, submissionTime, stageInfos, properties)
  }

  def jobEndFromJson(json: JValue): SparkListenerJobEnd = {
    val jobId = (json \ "Job ID").extract[Int]
    val completionTime =
      jsonOption(json \ "Completion Time").map(_.extract[Long]).getOrElse(-1L)
    val jobResult = jobResultFromJson(json \ "Job Result")
    SparkListenerJobEnd(jobId, completionTime, jobResult)
  }

  def resourceProfileAddedFromJson(json: JValue): SparkListenerResourceProfileAdded = {
    val profId = (json \ "Resource Profile Id").extract[Int]
    val executorReqs = executorResourceRequestMapFromJson(json \ "Executor Resource Requests")
    val taskReqs = taskResourceRequestMapFromJson(json \ "Task Resource Requests")
    val rp = new ResourceProfile(executorReqs.toMap, taskReqs.toMap)
    rp.setResourceProfileId(profId)
    SparkListenerResourceProfileAdded(rp)
  }

  def executorResourceRequestFromJson(json: JValue): ExecutorResourceRequest = {
    val rName = (json \ "Resource Name").extract[String]
    val amount = (json \ "Amount").extract[Int]
    val discoveryScript = (json \ "Discovery Script").extract[String]
    val vendor = (json \ "Vendor").extract[String]
    new ExecutorResourceRequest(rName, amount, discoveryScript, vendor)
  }

  def taskResourceRequestFromJson(json: JValue): TaskResourceRequest = {
    val rName = (json \ "Resource Name").extract[String]
    val amount = (json \ "Amount").extract[Int]
    new TaskResourceRequest(rName, amount)
  }

  def taskResourceRequestMapFromJson(json: JValue): Map[String, TaskResourceRequest] = {
    val jsonFields = json.asInstanceOf[JObject].obj
    jsonFields.collect { case JField(k, v) =>
      val req = taskResourceRequestFromJson(v)
      (k, req)
    }.toMap
  }

  def executorResourceRequestMapFromJson(json: JValue): Map[String, ExecutorResourceRequest] = {
    val jsonFields = json.asInstanceOf[JObject].obj
    jsonFields.collect { case JField(k, v) =>
      val req = executorResourceRequestFromJson(v)
      (k, req)
    }.toMap
  }

  def environmentUpdateFromJson(json: JValue): SparkListenerEnvironmentUpdate = {
    // For compatible with previous event logs
    val hadoopProperties = jsonOption(json \ "Hadoop Properties").map(mapFromJson(_).toSeq)
      .getOrElse(Seq.empty)
    val environmentDetails = Map[String, Seq[(String, String)]](
      "JVM Information" -> mapFromJson(json \ "JVM Information").toSeq,
      "Spark Properties" -> mapFromJson(json \ "Spark Properties").toSeq,
      "Hadoop Properties" -> hadoopProperties,
      "System Properties" -> mapFromJson(json \ "System Properties").toSeq,
      "Classpath Entries" -> mapFromJson(json \ "Classpath Entries").toSeq)
    SparkListenerEnvironmentUpdate(environmentDetails)
  }

  def blockManagerAddedFromJson(json: JValue): SparkListenerBlockManagerAdded = {
    val blockManagerId = blockManagerIdFromJson(json \ "Block Manager ID")
    val maxMem = (json \ "Maximum Memory").extract[Long]
    val time = jsonOption(json \ "Timestamp").map(_.extract[Long]).getOrElse(-1L)
    val maxOnHeapMem = jsonOption(json \ "Maximum Onheap Memory").map(_.extract[Long])
    val maxOffHeapMem = jsonOption(json \ "Maximum Offheap Memory").map(_.extract[Long])
    SparkListenerBlockManagerAdded(time, blockManagerId, maxMem, maxOnHeapMem, maxOffHeapMem)
  }

  def blockManagerRemovedFromJson(json: JValue): SparkListenerBlockManagerRemoved = {
    val blockManagerId = blockManagerIdFromJson(json \ "Block Manager ID")
    val time = jsonOption(json \ "Timestamp").map(_.extract[Long]).getOrElse(-1L)
    SparkListenerBlockManagerRemoved(time, blockManagerId)
  }

  def unpersistRDDFromJson(json: JValue): SparkListenerUnpersistRDD = {
    SparkListenerUnpersistRDD((json \ "RDD ID").extract[Int])
  }

  def applicationStartFromJson(json: JValue): SparkListenerApplicationStart = {
    val appName = (json \ "App Name").extract[String]
    val appId = jsonOption(json \ "App ID").map(_.extract[String])
    val time = (json \ "Timestamp").extract[Long]
    val sparkUser = (json \ "User").extract[String]
    val appAttemptId = jsonOption(json \ "App Attempt ID").map(_.extract[String])
    val driverLogs = jsonOption(json \ "Driver Logs").map(mapFromJson)
    val driverAttributes = jsonOption(json \ "Driver Attributes").map(mapFromJson)
    SparkListenerApplicationStart(appName, appId, time, sparkUser, appAttemptId, driverLogs,
      driverAttributes)
  }

  def applicationEndFromJson(json: JValue): SparkListenerApplicationEnd = {
    SparkListenerApplicationEnd((json \ "Timestamp").extract[Long])
  }

  def executorAddedFromJson(json: JValue): SparkListenerExecutorAdded = {
    val time = (json \ "Timestamp").extract[Long]
    val executorId = (json \ "Executor ID").extract[String]
    val executorInfo = executorInfoFromJson(json \ "Executor Info")
    SparkListenerExecutorAdded(time, executorId, executorInfo)
  }

  def executorRemovedFromJson(json: JValue): SparkListenerExecutorRemoved = {
    val time = (json \ "Timestamp").extract[Long]
    val executorId = (json \ "Executor ID").extract[String]
    val reason = (json \ "Removed Reason").extract[String]
    SparkListenerExecutorRemoved(time, executorId, reason)
  }

  def logStartFromJson(json: JValue): SparkListenerLogStart = {
    val sparkVersion = (json \ "Spark Version").extract[String]
    SparkListenerLogStart(sparkVersion)
  }

  def executorMetricsUpdateFromJson(json: JValue): SparkListenerExecutorMetricsUpdate = {
    val execInfo = (json \ "Executor ID").extract[String]
    val accumUpdates = (json \ "Metrics Updated").extract[List[JValue]].map { json =>
      val taskId = (json \ "Task ID").extract[Long]
      val stageId = (json \ "Stage ID").extract[Int]
      val stageAttemptId = (json \ "Stage Attempt ID").extract[Int]
      val updates =
        (json \ "Accumulator Updates").extract[List[JValue]].map(accumulableInfoFromJson)
      (taskId, stageId, stageAttemptId, updates)
    }
    val executorUpdates = (json \ "Executor Metrics Updated") match {
      case JNothing => Map.empty[(Int, Int), ExecutorMetrics]
      case value: JValue => value.extract[List[JValue]].map { json =>
        val stageId = (json \ "Stage ID").extract[Int]
        val stageAttemptId = (json \ "Stage Attempt ID").extract[Int]
        val executorMetrics = executorMetricsFromJson(json \ "Executor Metrics")
        ((stageId, stageAttemptId) -> executorMetrics)
      }.toMap
    }
    SparkListenerExecutorMetricsUpdate(execInfo, accumUpdates, executorUpdates)
  }

  def stageExecutorMetricsFromJson(json: JValue): SparkListenerStageExecutorMetrics = {
    val execId = (json \ "Executor ID").extract[String]
    val stageId = (json \ "Stage ID").extract[Int]
    val stageAttemptId = (json \ "Stage Attempt ID").extract[Int]
    val executorMetrics = executorMetricsFromJson(json \ "Executor Metrics")
    SparkListenerStageExecutorMetrics(execId, stageId, stageAttemptId, executorMetrics)
  }

  def blockUpdateFromJson(json: JValue): SparkListenerBlockUpdated = {
    val blockUpdatedInfo = blockUpdatedInfoFromJson(json \ "Block Updated Info")
    SparkListenerBlockUpdated(blockUpdatedInfo)
  }

  /** --------------------------------------------------------------------- *
   * JSON deserialization methods for classes SparkListenerEvents depend on |
   * ---------------------------------------------------------------------- */

  def stageInfoFromJson(json: JValue): StageInfo = {
    val stageId = (json \ "Stage ID").extract[Int]
    val attemptId = jsonOption(json \ "Stage Attempt ID").map(_.extract[Int]).getOrElse(0)
    val stageName = (json \ "Stage Name").extract[String]
    val numTasks = (json \ "Number of Tasks").extract[Int]
    val rddInfos = (json \ "RDD Info").extract[List[JValue]].map(rddInfoFromJson)
    val parentIds = jsonOption(json \ "Parent IDs")
      .map { l => l.extract[List[JValue]].map(_.extract[Int]) }
      .getOrElse(Seq.empty)
    val details = jsonOption(json \ "Details").map(_.extract[String]).getOrElse("")
    val submissionTime = jsonOption(json \ "Submission Time").map(_.extract[Long])
    val completionTime = jsonOption(json \ "Completion Time").map(_.extract[Long])
    val failureReason = jsonOption(json \ "Failure Reason").map(_.extract[String])
    val accumulatedValues = {
      jsonOption(json \ "Accumulables").map(_.extract[List[JValue]]) match {
        case Some(values) => values.map(accumulableInfoFromJson)
        case None => Seq.empty[AccumulableInfo]
      }
    }

    val rpId = jsonOption(json \ "Resource Profile Id").map(_.extract[Int])
    val stageProf = rpId.getOrElse(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val stageInfo = new StageInfo(stageId, attemptId, stageName, numTasks, rddInfos,
      parentIds, details, resourceProfileId = stageProf)
    stageInfo.submissionTime = submissionTime
    stageInfo.completionTime = completionTime
    stageInfo.failureReason = failureReason
    for (accInfo <- accumulatedValues) {
      stageInfo.accumulables(accInfo.id) = accInfo
    }
    stageInfo
  }

  def taskInfoFromJson(json: JValue): TaskInfo = {
    val taskId = (json \ "Task ID").extract[Long]
    val index = (json \ "Index").extract[Int]
    val attempt = jsonOption(json \ "Attempt").map(_.extract[Int]).getOrElse(1)
    val partitionId = jsonOption(json \ "Partition ID").map(_.extract[Int]).getOrElse(-1)
    val launchTime = (json \ "Launch Time").extract[Long]
    val executorId = weakIntern((json \ "Executor ID").extract[String])
    val host = weakIntern((json \ "Host").extract[String])
    val taskLocality = TaskLocality.withName((json \ "Locality").extract[String])
    val speculative = jsonOption(json \ "Speculative").exists(_.extract[Boolean])
    val gettingResultTime = (json \ "Getting Result Time").extract[Long]
    val finishTime = (json \ "Finish Time").extract[Long]
    val failed = (json \ "Failed").extract[Boolean]
    val killed = jsonOption(json \ "Killed").exists(_.extract[Boolean])
    val accumulables = jsonOption(json \ "Accumulables").map(_.extract[Seq[JValue]]) match {
      case Some(values) => values.map(accumulableInfoFromJson)
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

  def accumulableInfoFromJson(json: JValue): AccumulableInfo = {
    val id = (json \ "ID").extract[Long]
    val name = jsonOption(json \ "Name").map(_.extract[String])
    val update = jsonOption(json \ "Update").map { v => accumValueFromJson(name, v) }
    val value = jsonOption(json \ "Value").map { v => accumValueFromJson(name, v) }
    val internal = jsonOption(json \ "Internal").exists(_.extract[Boolean])
    val countFailedValues =
      jsonOption(json \ "Count Failed Values").exists(_.extract[Boolean])
    val metadata = jsonOption(json \ "Metadata").map(_.extract[String])
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
  private[util] def accumValueFromJson(name: Option[String], value: JValue): Any = {
    if (name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))) {
      value match {
        case JInt(v) => v.toLong
        case JArray(v) =>
          v.map { blockJson =>
            val id = BlockId((blockJson \ "Block ID").extract[String])
            val status = blockStatusFromJson(blockJson \ "Status")
            (id, status)
          }.asJava
        case _ => throw new IllegalArgumentException(s"unexpected json value $value for " +
          "accumulator " + name.get)
      }
    } else {
      value.extract[String]
    }
  }

  def taskMetricsFromJson(json: JValue): TaskMetrics = {
    val metrics = TaskMetrics.empty
    if (json == JNothing) {
      return metrics
    }
    metrics.setExecutorDeserializeTime((json \ "Executor Deserialize Time").extract[Long])
    metrics.setExecutorDeserializeCpuTime((json \ "Executor Deserialize CPU Time") match {
      case JNothing => 0
      case x => x.extract[Long]
    })
    metrics.setExecutorRunTime((json \ "Executor Run Time").extract[Long])
    metrics.setExecutorCpuTime((json \ "Executor CPU Time") match {
      case JNothing => 0
      case x => x.extract[Long]
    })
    metrics.setPeakExecutionMemory((json \ "Peak Execution Memory") match {
      case JNothing => 0
      case x => x.extract[Long]
    })
    metrics.setResultSize((json \ "Result Size").extract[Long])
    metrics.setJvmGCTime((json \ "JVM GC Time").extract[Long])
    metrics.setResultSerializationTime((json \ "Result Serialization Time").extract[Long])
    metrics.incMemoryBytesSpilled((json \ "Memory Bytes Spilled").extract[Long])
    metrics.incDiskBytesSpilled((json \ "Disk Bytes Spilled").extract[Long])

    // Shuffle read metrics
    jsonOption(json \ "Shuffle Read Metrics").foreach { readJson =>
      val readMetrics = metrics.createTempShuffleReadMetrics()
      readMetrics.incRemoteBlocksFetched((readJson \ "Remote Blocks Fetched").extract[Int])
      readMetrics.incLocalBlocksFetched((readJson \ "Local Blocks Fetched").extract[Int])
      readMetrics.incRemoteBytesRead((readJson \ "Remote Bytes Read").extract[Long])
      jsonOption(readJson \ "Remote Bytes Read To Disk")
        .foreach { v => readMetrics.incRemoteBytesReadToDisk(v.extract[Long])}
      readMetrics.incLocalBytesRead(
        jsonOption(readJson \ "Local Bytes Read").map(_.extract[Long]).getOrElse(0L))
      readMetrics.incFetchWaitTime((readJson \ "Fetch Wait Time").extract[Long])
      readMetrics.incRecordsRead(
        jsonOption(readJson \ "Total Records Read").map(_.extract[Long]).getOrElse(0L))
      metrics.mergeShuffleReadMetrics()
    }

    // Shuffle write metrics
    // TODO: Drop the redundant "Shuffle" since it's inconsistent with related classes.
    jsonOption(json \ "Shuffle Write Metrics").foreach { writeJson =>
      val writeMetrics = metrics.shuffleWriteMetrics
      writeMetrics.incBytesWritten((writeJson \ "Shuffle Bytes Written").extract[Long])
      writeMetrics.incRecordsWritten(
        jsonOption(writeJson \ "Shuffle Records Written").map(_.extract[Long]).getOrElse(0L))
      writeMetrics.incWriteTime((writeJson \ "Shuffle Write Time").extract[Long])
    }

    // Output metrics
    jsonOption(json \ "Output Metrics").foreach { outJson =>
      val outputMetrics = metrics.outputMetrics
      outputMetrics.setBytesWritten((outJson \ "Bytes Written").extract[Long])
      outputMetrics.setRecordsWritten(
        jsonOption(outJson \ "Records Written").map(_.extract[Long]).getOrElse(0L))
    }

    // Input metrics
    jsonOption(json \ "Input Metrics").foreach { inJson =>
      val inputMetrics = metrics.inputMetrics
      inputMetrics.incBytesRead((inJson \ "Bytes Read").extract[Long])
      inputMetrics.incRecordsRead(
        jsonOption(inJson \ "Records Read").map(_.extract[Long]).getOrElse(0L))
    }

    // Updated blocks
    jsonOption(json \ "Updated Blocks").foreach { blocksJson =>
      metrics.setUpdatedBlockStatuses(blocksJson.extract[List[JValue]].map { blockJson =>
        val id = BlockId((blockJson \ "Block ID").extract[String])
        val status = blockStatusFromJson(blockJson \ "Status")
        (id, status)
      })
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

  def taskEndReasonFromJson(json: JValue): TaskEndReason = {
    import TASK_END_REASON_FORMATTED_CLASS_NAMES._

    (json \ "Reason").extract[String] match {
      case `success` => Success
      case `resubmitted` => Resubmitted
      case `fetchFailed` =>
        val blockManagerAddress = blockManagerIdFromJson(json \ "Block Manager Address")
        val shuffleId = (json \ "Shuffle ID").extract[Int]
        val mapId = (json \ "Map ID").extract[Long]
        val mapIndex = json \ "Map Index" match {
          case JNothing =>
            // Note, we use the invalid value Int.MinValue here to fill the map index for backward
            // compatibility. Otherwise, the fetch failed event will be dropped when the history
            // server loads the event log written by the Spark version before 3.0.
            Int.MinValue
          case x => x.extract[Int]
        }
        val reduceId = (json \ "Reduce ID").extract[Int]
        val message = jsonOption(json \ "Message").map(_.extract[String])
        new FetchFailed(blockManagerAddress, shuffleId, mapId, mapIndex, reduceId,
          message.getOrElse("Unknown reason"))
      case `exceptionFailure` =>
        val className = (json \ "Class Name").extract[String]
        val description = (json \ "Description").extract[String]
        val stackTrace = stackTraceFromJson(json \ "Stack Trace")
        val fullStackTrace =
          jsonOption(json \ "Full Stack Trace").map(_.extract[String]).orNull
        // Fallback on getting accumulator updates from TaskMetrics, which was logged in Spark 1.x
        val accumUpdates = jsonOption(json \ "Accumulator Updates")
          .map(_.extract[List[JValue]].map(accumulableInfoFromJson))
          .getOrElse(taskMetricsFromJson(json \ "Metrics").accumulators().map(acc => {
            acc.toInfo(Some(acc.value), None)
          }))
        ExceptionFailure(className, description, stackTrace, fullStackTrace, None, accumUpdates)
      case `taskResultLost` => TaskResultLost
      case `taskKilled` =>
        val killReason = jsonOption(json \ "Kill Reason")
          .map(_.extract[String]).getOrElse("unknown reason")
        val accumUpdates = jsonOption(json \ "Accumulator Updates")
          .map(_.extract[List[JValue]].map(accumulableInfoFromJson))
          .getOrElse(Seq[AccumulableInfo]())
        TaskKilled(killReason, accumUpdates)
      case `taskCommitDenied` =>
        // Unfortunately, the `TaskCommitDenied` message was introduced in 1.3.0 but the JSON
        // de/serialization logic was not added until 1.5.1. To provide backward compatibility
        // for reading those logs, we need to provide default values for all the fields.
        val jobId = jsonOption(json \ "Job ID").map(_.extract[Int]).getOrElse(-1)
        val partitionId = jsonOption(json \ "Partition ID").map(_.extract[Int]).getOrElse(-1)
        val attemptNo = jsonOption(json \ "Attempt Number").map(_.extract[Int]).getOrElse(-1)
        TaskCommitDenied(jobId, partitionId, attemptNo)
      case `executorLostFailure` =>
        val exitCausedByApp = jsonOption(json \ "Exit Caused By App").map(_.extract[Boolean])
        val executorId = jsonOption(json \ "Executor ID").map(_.extract[String])
        val reason = jsonOption(json \ "Loss Reason").map(_.extract[String])
        ExecutorLostFailure(
          executorId.getOrElse("Unknown"),
          exitCausedByApp.getOrElse(true),
          reason)
      case `unknownReason` => UnknownReason
    }
  }

  def blockManagerIdFromJson(json: JValue): BlockManagerId = {
    // On metadata fetch fail, block manager ID can be null (SPARK-4471)
    if (json == JNothing) {
      return null
    }
    val executorId = weakIntern((json \ "Executor ID").extract[String])
    val host = weakIntern((json \ "Host").extract[String])
    val port = (json \ "Port").extract[Int]
    BlockManagerId(executorId, host, port)
  }

  private object JOB_RESULT_FORMATTED_CLASS_NAMES {
    val jobSucceeded = Utils.getFormattedClassName(JobSucceeded)
    val jobFailed = Utils.getFormattedClassName(JobFailed)
  }

  def jobResultFromJson(json: JValue): JobResult = {
    import JOB_RESULT_FORMATTED_CLASS_NAMES._

    (json \ "Result").extract[String] match {
      case `jobSucceeded` => JobSucceeded
      case `jobFailed` =>
        val exception = exceptionFromJson(json \ "Exception")
        new JobFailed(exception)
    }
  }

  def rddInfoFromJson(json: JValue): RDDInfo = {
    val rddId = (json \ "RDD ID").extract[Int]
    val name = (json \ "Name").extract[String]
    val scope = jsonOption(json \ "Scope")
      .map(_.extract[String])
      .map(RDDOperationScope.fromJson)
    val callsite = jsonOption(json \ "Callsite").map(_.extract[String]).getOrElse("")
    val parentIds = jsonOption(json \ "Parent IDs")
      .map { l => l.extract[List[JValue]].map(_.extract[Int]) }
      .getOrElse(Seq.empty)
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val isBarrier = jsonOption(json \ "Barrier").map(_.extract[Boolean]).getOrElse(false)
    val numPartitions = (json \ "Number of Partitions").extract[Int]
    val numCachedPartitions = (json \ "Number of Cached Partitions").extract[Int]
    val memSize = (json \ "Memory Size").extract[Long]
    val diskSize = (json \ "Disk Size").extract[Long]

    val outputDeterministicLevel = DeterministicLevel.withName(
      jsonOption(json \ "DeterministicLevel").map(_.extract[String]).getOrElse("DETERMINATE"))

    val rddInfo =
      new RDDInfo(rddId, name, numPartitions, storageLevel, isBarrier, parentIds, callsite, scope,
        outputDeterministicLevel)
    rddInfo.numCachedPartitions = numCachedPartitions
    rddInfo.memSize = memSize
    rddInfo.diskSize = diskSize
    rddInfo
  }

  def storageLevelFromJson(json: JValue): StorageLevel = {
    val useDisk = (json \ "Use Disk").extract[Boolean]
    val useMemory = (json \ "Use Memory").extract[Boolean]
    val deserialized = (json \ "Deserialized").extract[Boolean]
    val replication = (json \ "Replication").extract[Int]
    StorageLevel(useDisk, useMemory, deserialized, replication)
  }

  def blockStatusFromJson(json: JValue): BlockStatus = {
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val memorySize = (json \ "Memory Size").extract[Long]
    val diskSize = (json \ "Disk Size").extract[Long]
    BlockStatus(storageLevel, memorySize, diskSize)
  }

  def executorInfoFromJson(json: JValue): ExecutorInfo = {
    val executorHost = (json \ "Host").extract[String]
    val totalCores = (json \ "Total Cores").extract[Int]
    val logUrls = mapFromJson(json \ "Log Urls").toMap
    val attributes = jsonOption(json \ "Attributes") match {
      case Some(attr) => mapFromJson(attr).toMap
      case None => Map.empty[String, String]
    }
    val resources = jsonOption(json \ "Resources") match {
      case Some(resources) => resourcesMapFromJson(resources).toMap
      case None => Map.empty[String, ResourceInformation]
    }
    val resourceProfileId = jsonOption(json \ "Resource Profile Id") match {
      case Some(id) => id.extract[Int]
      case None => ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
    }
    new ExecutorInfo(executorHost, totalCores, logUrls, attributes.toMap, resources.toMap,
      resourceProfileId)
  }

  def blockUpdatedInfoFromJson(json: JValue): BlockUpdatedInfo = {
    val blockManagerId = blockManagerIdFromJson(json \ "Block Manager ID")
    val blockId = BlockId((json \ "Block ID").extract[String])
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val memorySize = (json \ "Memory Size").extract[Long]
    val diskSize = (json \ "Disk Size").extract[Long]
    BlockUpdatedInfo(blockManagerId, blockId, storageLevel, memorySize, diskSize)
  }

  def resourcesMapFromJson(json: JValue): Map[String, ResourceInformation] = {
    val jsonFields = json.asInstanceOf[JObject].obj
    jsonFields.collect { case JField(k, v) =>
      val resourceInfo = ResourceInformation.parseJson(v)
      (k, resourceInfo)
    }.toMap
  }

  /** -------------------------------- *
   * Util JSON deserialization methods |
   * --------------------------------- */

  def mapFromJson(json: JValue): Map[String, String] = {
    val jsonFields = json.asInstanceOf[JObject].obj
    jsonFields.collect { case JField(k, JString(v)) => (k, v) }.toMap
  }

  def propertiesFromJson(json: JValue): Properties = {
    jsonOption(json).map { value =>
      val properties = new Properties
      mapFromJson(json).foreach { case (k, v) => properties.setProperty(k, v) }
      properties
    }.getOrElse(null)
  }

  def UUIDFromJson(json: JValue): UUID = {
    val leastSignificantBits = (json \ "Least Significant Bits").extract[Long]
    val mostSignificantBits = (json \ "Most Significant Bits").extract[Long]
    new UUID(leastSignificantBits, mostSignificantBits)
  }

  def stackTraceFromJson(json: JValue): Array[StackTraceElement] = {
    json.extract[List[JValue]].map { line =>
      val declaringClass = (line \ "Declaring Class").extract[String]
      val methodName = (line \ "Method Name").extract[String]
      val fileName = (line \ "File Name").extract[String]
      val lineNumber = (line \ "Line Number").extract[Int]
      new StackTraceElement(declaringClass, methodName, fileName, lineNumber)
    }.toArray
  }

  def exceptionFromJson(json: JValue): Exception = {
    val e = new Exception((json \ "Message").extract[String])
    e.setStackTrace(stackTraceFromJson(json \ "Stack Trace"))
    e
  }

  /** Return an option that translates JNothing to None */
  private def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }

  private def emptyJson: JObject = JObject(List[JField]())

}
