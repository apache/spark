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

import java.util.{UUID, Properties}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable

import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import net.liftweb.json.DefaultFormats


import org.apache.spark.executor.{ShuffleWriteMetrics, ShuffleReadMetrics, TaskMetrics}
import org.apache.spark.scheduler._
import org.apache.spark.storage._
import org.apache.spark._

private[spark] object JsonProtocol {
  private implicit val format = DefaultFormats

  /**
   * JSON serialization methods for SparkListenerEvent's
   */

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
      case applicationStart: SparkListenerApplicationStart =>
        applicationStartToJson(applicationStart)
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        environmentUpdateToJson(environmentUpdate)
      case executorsStateChange: SparkListenerExecutorsStateChange =>
        executorsStateChangeToJson(executorsStateChange)
      case SparkListenerShutdown =>
        shutdownToJson()
    }
  }

  def stageSubmittedToJson(stageSubmitted: SparkListenerStageSubmitted): JValue = {
    val stageInfo = stageInfoToJson(stageSubmitted.stageInfo)
    val properties = propertiesToJson(stageSubmitted.properties)
    ("Event" -> Utils.getFormattedClassName(stageSubmitted)) ~
    ("Stage Info" -> stageInfo) ~
    ("Properties" -> properties)
  }

  def stageCompletedToJson(stageCompleted: SparkListenerStageCompleted): JValue = {
    val stageInfo = stageInfoToJson(stageCompleted.stageInfo)
    ("Event" -> Utils.getFormattedClassName(stageCompleted)) ~
    ("Stage Info" -> stageInfo)
  }

  def taskStartToJson(taskStart: SparkListenerTaskStart): JValue = {
    val taskInfo = taskInfoToJson(taskStart.taskInfo)
    ("Event" -> Utils.getFormattedClassName(taskStart)) ~
    ("Stage ID" -> taskStart.stageId) ~
    ("Task Info" -> taskInfo)
  }

  def taskGettingResultToJson(taskGettingResult: SparkListenerTaskGettingResult): JValue = {
    val taskInfo = taskInfoToJson(taskGettingResult.taskInfo)
    ("Event" -> Utils.getFormattedClassName(taskGettingResult)) ~
    ("Task Info" -> taskInfo)
  }

  def taskEndToJson(taskEnd: SparkListenerTaskEnd): JValue = {
    val taskEndReason = taskEndReasonToJson(taskEnd.reason)
    val taskInfo = taskInfoToJson(taskEnd.taskInfo)
    val taskMetrics = taskMetricsToJson(taskEnd.taskMetrics)
    ("Event" -> Utils.getFormattedClassName(taskEnd)) ~
    ("Stage ID" -> taskEnd.stageId) ~
    ("Task Type" -> taskEnd.taskType) ~
    ("Task End Reason" -> taskEndReason) ~
    ("Task Info" -> taskInfo) ~
    ("Task Metrics" -> taskMetrics)
  }

  def jobStartToJson(jobStart: SparkListenerJobStart): JValue = {
    val properties = propertiesToJson(jobStart.properties)
    ("Event" -> Utils.getFormattedClassName(jobStart)) ~
    ("Job ID" -> jobStart.jobId) ~
    ("Stage IDs" -> jobStart.stageIds) ~
    ("Properties" -> properties)
  }

  def jobEndToJson(jobEnd: SparkListenerJobEnd): JValue = {
    val jobResult = jobResultToJson(jobEnd.jobResult)
    ("Event" -> Utils.getFormattedClassName(jobEnd)) ~
    ("Job ID" -> jobEnd.jobId) ~
    ("Job Result" -> jobResult)
  }

  def applicationStartToJson(applicationStart: SparkListenerApplicationStart): JValue = {
    ("Event" -> Utils.getFormattedClassName(applicationStart)) ~
    ("App Name" -> applicationStart.appName)
  }

  def environmentUpdateToJson(environmentUpdate: SparkListenerEnvironmentUpdate): JValue = {
    val environmentDetails = environmentUpdate.environmentDetails
    val jvmInformation = mapToJson(environmentDetails("JVM Information").toMap)
    val sparkProperties = mapToJson(environmentDetails("Spark Properties").toMap)
    val systemProperties = mapToJson(environmentDetails("System Properties").toMap)
    val classpathEntries = mapToJson(environmentDetails("Classpath Entries").toMap)
    ("Event" -> Utils.getFormattedClassName(environmentUpdate)) ~
    ("JVM Information" -> jvmInformation) ~
    ("Spark Properties" -> sparkProperties) ~
    ("System Properties" -> systemProperties) ~
    ("Classpath Entries" -> classpathEntries)
  }

  def executorsStateChangeToJson(executorsStateChange: SparkListenerExecutorsStateChange)
      : JValue = {
    val storageStatusList =
      JArray(executorsStateChange.storageStatusList.map(storageStatusToJson).toList)
    ("Event" -> Utils.getFormattedClassName(executorsStateChange)) ~
    ("Storage Status List" -> storageStatusList)
  }

  def shutdownToJson(): JValue = {
    "Event" -> Utils.getFormattedClassName(SparkListenerShutdown)
  }

  /**
   * JSON serialization methods for classes SparkListenerEvent's depend on
   */

  def stageInfoToJson(stageInfo: StageInfo): JValue = {
    val rddInfo = rddInfoToJson(stageInfo.rddInfo)
    val taskInfos = JArray(stageInfo.taskInfos.map { case (info, metrics) =>
      ("Task Info" -> taskInfoToJson(info)) ~
      ("Task Metrics" -> taskMetricsToJson(metrics))
    }.toList)
    val submissionTime = stageInfo.submissionTime.map(JInt(_)).getOrElse(JNothing)
    val completionTime = stageInfo.completionTime.map(JInt(_)).getOrElse(JNothing)
    ("Stage ID" -> stageInfo.stageId) ~
    ("Stage Name" -> stageInfo.name) ~
    ("Number of Tasks" -> stageInfo.numTasks) ~
    ("RDD Info" -> rddInfo) ~
    ("Task Infos" -> taskInfos) ~
    ("Submission Time" -> submissionTime) ~
    ("Completion Time" -> completionTime) ~
    ("Emitted Task Size Warning" -> stageInfo.emittedTaskSizeWarning)
  }

  def taskInfoToJson(taskInfo: TaskInfo): JValue = {
    ("Task ID" -> taskInfo.taskId) ~
    ("Index" -> taskInfo.index) ~
    ("Launch Time" -> taskInfo.launchTime) ~
    ("Executor ID" -> taskInfo.executorId) ~
    ("Host" -> taskInfo.host) ~
    ("Locality" -> taskInfo.taskLocality.toString) ~
    ("Getting Result Time" -> taskInfo.gettingResultTime) ~
    ("Finish Time" -> taskInfo.finishTime) ~
    ("Failed" -> taskInfo.failed) ~
    ("Serialized Size" -> taskInfo.serializedSize)
  }

  def taskMetricsToJson(taskMetrics: TaskMetrics): JValue = {
    val shuffleReadMetrics =
      taskMetrics.shuffleReadMetrics.map(shuffleReadMetricsToJson).getOrElse(JNothing)
    val shuffleWriteMetrics =
      taskMetrics.shuffleWriteMetrics.map(shuffleWriteMetricsToJson).getOrElse(JNothing)
    val updatedBlocks = taskMetrics.updatedBlocks.map { blocks =>
        JArray(blocks.toList.map { case (id, status) =>
          ("Block ID" -> blockIdToJson(id)) ~
          ("Status" -> blockStatusToJson(status))
        })
      }.getOrElse(JNothing)
    ("Host Name" -> taskMetrics.hostname) ~
    ("Executor Deserialize Time" -> taskMetrics.executorDeserializeTime) ~
    ("Executor Run Time" -> taskMetrics.executorRunTime) ~
    ("Result Size" -> taskMetrics.resultSize) ~
    ("JVM GC Time" -> taskMetrics.jvmGCTime) ~
    ("Result Serialization Time" -> taskMetrics.resultSerializationTime) ~
    ("Memory Bytes Spilled" -> taskMetrics.memoryBytesSpilled) ~
    ("Disk Bytes Spilled" -> taskMetrics.diskBytesSpilled) ~
    ("Shuffle Read Metrics" -> shuffleReadMetrics) ~
    ("Shuffle Write Metrics" -> shuffleWriteMetrics) ~
    ("Updated Blocks" -> updatedBlocks)
  }

  def shuffleReadMetricsToJson(shuffleReadMetrics: ShuffleReadMetrics): JValue = {
    ("Shuffle Finish Time" -> shuffleReadMetrics.shuffleFinishTime) ~
    ("Total Blocks Fetched" -> shuffleReadMetrics.totalBlocksFetched) ~
    ("Remote Blocks Fetched" -> shuffleReadMetrics.remoteBlocksFetched) ~
    ("Local Blocks Fetched" -> shuffleReadMetrics.localBlocksFetched) ~
    ("Fetch Wait Time" -> shuffleReadMetrics.fetchWaitTime) ~
    ("Remote Fetch Time" -> shuffleReadMetrics.remoteFetchTime) ~
    ("Remote Bytes Read" -> shuffleReadMetrics.remoteBytesRead)
  }

  def shuffleWriteMetricsToJson(shuffleWriteMetrics: ShuffleWriteMetrics): JValue = {
    ("Shuffle Bytes Written" -> shuffleWriteMetrics.shuffleBytesWritten) ~
    ("Shuffle Write Time" -> shuffleWriteMetrics.shuffleWriteTime)
  }

  def taskEndReasonToJson(taskEndReason: TaskEndReason): JValue = {
    val reason = Utils.getFormattedClassName(taskEndReason)
    val json = taskEndReason match {
      case fetchFailed: FetchFailed =>
        val blockManagerAddress = blockManagerIdToJson(fetchFailed.bmAddress)
        ("Block Manager Address" -> blockManagerAddress) ~
        ("Shuffle ID" -> fetchFailed.shuffleId) ~
        ("Map ID" -> fetchFailed.mapId) ~
        ("Reduce ID" -> fetchFailed.reduceId)
      case exceptionFailure: ExceptionFailure =>
        val stackTrace = stackTraceToJson(exceptionFailure.stackTrace)
        val metrics = exceptionFailure.metrics.map(taskMetricsToJson).getOrElse(JNothing)
        ("Class Name" -> exceptionFailure.className) ~
        ("Description" -> exceptionFailure.description) ~
        ("Stack Trace" -> stackTrace) ~
        ("Metrics" -> metrics)
      case _ => Utils.emptyJson
    }
    ("Reason" -> reason) ~ json
  }

  def blockManagerIdToJson(blockManagerId: BlockManagerId): JValue = {
    ("Executor ID" -> blockManagerId.executorId) ~
    ("Host" -> blockManagerId.host) ~
    ("Port" -> blockManagerId.port) ~
    ("Netty Port" -> blockManagerId.nettyPort)
  }

  def jobResultToJson(jobResult: JobResult): JValue = {
    val result = Utils.getFormattedClassName(jobResult)
    val json = jobResult match {
      case JobSucceeded => Utils.emptyJson
      case jobFailed: JobFailed =>
        val exception = exceptionToJson(jobFailed.exception)
        ("Exception" -> exception) ~
        ("Failed Stage ID" -> jobFailed.failedStageId)
    }
    ("Result" -> result) ~ json
  }

  def storageStatusToJson(storageStatus: StorageStatus): JValue = {
    val blockManagerId = blockManagerIdToJson(storageStatus.blockManagerId)
    val blocks = JArray(
      storageStatus.blocks.toList.map { case (id, status) =>
        ("Block ID" -> blockIdToJson(id)) ~
        ("Status" -> blockStatusToJson(status))
      })
    ("Block Manager ID" -> blockManagerId) ~
    ("Maximum Memory" -> storageStatus.maxMem) ~
    ("Blocks" -> blocks)
  }

  def rddInfoToJson(rddInfo: RDDInfo): JValue = {
    val storageLevel = storageLevelToJson(rddInfo.storageLevel)
    ("RDD ID" -> rddInfo.id) ~
    ("Name" -> rddInfo.name) ~
    ("Storage Level" -> storageLevel) ~
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

  def blockIdToJson(blockId: BlockId): JValue = {
    val blockType = Utils.getFormattedClassName(blockId)
    val json: JObject = blockId match {
      case rddBlockId: RDDBlockId =>
        ("RDD ID" -> rddBlockId.rddId) ~
        ("Split Index" -> rddBlockId.splitIndex)
      case shuffleBlockId: ShuffleBlockId =>
        ("Shuffle ID" -> shuffleBlockId.shuffleId) ~
        ("Map ID" -> shuffleBlockId.mapId) ~
        ("Reduce ID" -> shuffleBlockId.reduceId)
      case broadcastBlockId: BroadcastBlockId =>
        "Broadcast ID" -> broadcastBlockId.broadcastId
      case broadcastHelperBlockId: BroadcastHelperBlockId =>
        ("Broadcast Block ID" -> blockIdToJson(broadcastHelperBlockId.broadcastId)) ~
        ("Helper Type" -> broadcastHelperBlockId.hType)
      case taskResultBlockId: TaskResultBlockId =>
        "Task ID" -> taskResultBlockId.taskId
      case streamBlockId: StreamBlockId =>
        ("Stream ID" -> streamBlockId.streamId) ~
        ("Unique ID" -> streamBlockId.uniqueId)
      case tempBlockId: TempBlockId =>
        val uuid = UUIDToJson(tempBlockId.id)
        "Temp ID" -> uuid
      case testBlockId: TestBlockId =>
        "Test ID" -> testBlockId.id
    }
    ("Type" -> blockType) ~ json
  }

  def blockStatusToJson(blockStatus: BlockStatus): JValue = {
    val storageLevel = storageLevelToJson(blockStatus.storageLevel)
    ("Storage Level" -> storageLevel) ~
    ("Memory Size" -> blockStatus.memSize) ~
    ("Disk Size" -> blockStatus.diskSize)
  }

  /**
   * Util JSON serialization methods
   */

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
    ("Message" -> exception.toString) ~
    ("Stack Trace" -> stackTraceToJson(exception.getStackTrace))
  }

  /**
   * JSON deserialization methods for SparkListenerEvent's
   */

  def sparkEventFromJson(json: JValue): SparkListenerEvent = {
    val stageSubmitted = Utils.getFormattedClassName(SparkListenerStageSubmitted)
    val stageCompleted = Utils.getFormattedClassName(SparkListenerStageCompleted)
    val taskStart = Utils.getFormattedClassName(SparkListenerTaskStart)
    val taskGettingResult = Utils.getFormattedClassName(SparkListenerTaskGettingResult)
    val taskEnd = Utils.getFormattedClassName(SparkListenerTaskEnd)
    val jobStart = Utils.getFormattedClassName(SparkListenerJobStart)
    val jobEnd = Utils.getFormattedClassName(SparkListenerJobEnd)
    val applicationStart = Utils.getFormattedClassName(SparkListenerApplicationStart)
    val environmentUpdate = Utils.getFormattedClassName(SparkListenerEnvironmentUpdate)
    val executorsStateChanged = Utils.getFormattedClassName(SparkListenerExecutorsStateChange)
    val shutdown = Utils.getFormattedClassName(SparkListenerShutdown)

    (json \ "Event").extract[String] match {
      case `stageSubmitted` => stageSubmittedFromJson(json)
      case `stageCompleted` => stageCompletedFromJson(json)
      case `taskStart` => taskStartFromJson(json)
      case `taskGettingResult` => taskGettingResultFromJson(json)
      case `taskEnd` => taskEndFromJson(json)
      case `jobStart` => jobStartFromJson(json)
      case `jobEnd` => jobEndFromJson(json)
      case `applicationStart` => applicationStartFromJson(json)
      case `environmentUpdate` => environmentUpdateFromJson(json)
      case `executorsStateChanged` => executorsStateChangeFromJson(json)
      case `shutdown` => SparkListenerShutdown
    }
  }

  def stageSubmittedFromJson(json: JValue): SparkListenerStageSubmitted = {
    val stageInfo = stageInfoFromJson(json \ "Stage Info")
    val properties = propertiesFromJson(json \ "Properties")
    new SparkListenerStageSubmitted(stageInfo, properties)
  }

  def stageCompletedFromJson(json: JValue): SparkListenerStageCompleted = {
    val stageInfo = stageInfoFromJson(json \ "Stage Info")
    new SparkListenerStageCompleted(stageInfo)
  }

  def taskStartFromJson(json: JValue): SparkListenerTaskStart = {
    val stageId = (json \ "Stage ID").extract[Int]
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    new SparkListenerTaskStart(stageId, taskInfo)
  }

  def taskGettingResultFromJson(json: JValue): SparkListenerTaskGettingResult = {
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    new SparkListenerTaskGettingResult(taskInfo)
  }

  def taskEndFromJson(json: JValue): SparkListenerTaskEnd = {
    val stageId = (json \ "Stage ID").extract[Int]
    val taskType = (json \ "Task Type").extract[String]
    val taskEndReason = taskEndReasonFromJson(json \ "Task End Reason")
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    val taskMetrics = taskMetricsFromJson(json \ "Task Metrics")
    new SparkListenerTaskEnd(stageId, taskType, taskEndReason, taskInfo, taskMetrics)
  }

  def jobStartFromJson(json: JValue): SparkListenerJobStart = {
    val jobId = (json \ "Job ID").extract[Int]
    val stageIds = (json \ "Stage IDs").extract[List[JValue]].map(_.extract[Int])
    val properties = propertiesFromJson(json \ "Properties")
    new SparkListenerJobStart(jobId, stageIds, properties)
  }

  def jobEndFromJson(json: JValue): SparkListenerJobEnd = {
    val jobId = (json \ "Job ID").extract[Int]
    val jobResult = jobResultFromJson(json \ "Job Result")
    new SparkListenerJobEnd(jobId, jobResult)
  }

  def applicationStartFromJson(json: JValue): SparkListenerApplicationStart = {
    new SparkListenerApplicationStart((json \ "App Name").extract[String])
  }

  def environmentUpdateFromJson(json: JValue): SparkListenerEnvironmentUpdate = {
    val environmentDetails = Map[String, Seq[(String, String)]](
      "JVM Information" -> mapFromJson(json \ "JVM Information").toSeq,
      "Spark Properties" -> mapFromJson(json \ "Spark Properties").toSeq,
      "System Properties" -> mapFromJson(json \ "System Properties").toSeq,
      "Classpath Entries" -> mapFromJson(json \ "Classpath Entries").toSeq)
    new SparkListenerEnvironmentUpdate(environmentDetails)
  }

  def executorsStateChangeFromJson(json: JValue): SparkListenerExecutorsStateChange = {
    val storageStatusList =
      (json \ "Storage Status List").extract[List[JValue]].map(storageStatusFromJson)
    new SparkListenerExecutorsStateChange(storageStatusList)
  }

  /**
   * JSON deserialization methods for classes SparkListenerEvent's depend on
   */

  def stageInfoFromJson(json: JValue): StageInfo = {
    val stageId = (json \ "Stage ID").extract[Int]
    val stageName = (json \ "Stage Name").extract[String]
    val numTasks = (json \ "Number of Tasks").extract[Int]
    val rddInfo = rddInfoFromJson(json \ "RDD Info")
    val taskInfos = (json \ "Task Infos").extract[List[JValue]].map { value =>
      (taskInfoFromJson(value \ "Task Info"), taskMetricsFromJson(value \ "Task Metrics"))
    }.toBuffer
    val submissionTime = Utils.jsonOption(json \ "Submission Time").map(_.extract[Long])
    val completionTime = Utils.jsonOption(json \ "Completion Time").map(_.extract[Long])
    val emittedTaskSizeWarning = (json \ "Emitted Task Size Warning").extract[Boolean]

    val stageInfo = new StageInfo(stageId, stageName, numTasks, rddInfo, taskInfos)
    stageInfo.submissionTime = submissionTime
    stageInfo.completionTime = completionTime
    stageInfo.emittedTaskSizeWarning = emittedTaskSizeWarning
    stageInfo
  }

  def taskInfoFromJson(json: JValue): TaskInfo = {
    val taskId = (json \ "Task ID").extract[Long]
    val index = (json \ "Index").extract[Int]
    val launchTime = (json \ "Launch Time").extract[Long]
    val executorId = (json \ "Executor ID").extract[String]
    val host = (json \ "Host").extract[String]
    val taskLocality = TaskLocality.withName((json \ "Locality").extract[String])
    val gettingResultTime = (json \ "Getting Result Time").extract[Long]
    val finishTime = (json \ "Finish Time").extract[Long]
    val failed = (json \ "Failed").extract[Boolean]
    val serializedSize = (json \ "Serialized Size").extract[Int]

    val taskInfo = new TaskInfo(taskId, index, launchTime, executorId, host, taskLocality)
    taskInfo.gettingResultTime = gettingResultTime
    taskInfo.finishTime = finishTime
    taskInfo.failed = failed
    taskInfo.serializedSize = serializedSize
    taskInfo
  }

  def taskMetricsFromJson(json: JValue): TaskMetrics = {
    val metrics = new TaskMetrics
    metrics.hostname = (json \ "Host Name").extract[String]
    metrics.executorDeserializeTime = (json \ "Executor Deserialize Time").extract[Long]
    metrics.executorRunTime = (json \ "Executor Run Time").extract[Long]
    metrics.jvmGCTime = (json \ "JVM GC Time").extract[Long]
    metrics.resultSerializationTime = (json \ "Result Serialization Time").extract[Long]
    metrics.memoryBytesSpilled = (json \ "Memory Bytes Spilled").extract[Long]
    metrics.diskBytesSpilled = (json \ "Disk Bytes Spilled").extract[Long]
    metrics.shuffleReadMetrics =
      Utils.jsonOption(json \ "Shuffle Read Metrics").map(shuffleReadMetricsFromJson)
    metrics.shuffleWriteMetrics =
      Utils.jsonOption(json \ "Shuffle Write Metrics").map(shuffleWriteMetricsFromJson)
    metrics.updatedBlocks = Utils.jsonOption(json \ "Updated Blocks").map { value =>
      value.extract[List[JValue]].map { block =>
        val id = blockIdFromJson(block \ "Block ID")
        val status = blockStatusFromJson(block \ "Status")
        (id, status)
      }
    }
    metrics
  }

  def shuffleReadMetricsFromJson(json: JValue): ShuffleReadMetrics = {
    val metrics = new ShuffleReadMetrics
    metrics.shuffleFinishTime = (json \ "Shuffle Finish Time").extract[Long]
    metrics.totalBlocksFetched = (json \ "Total Blocks Fetched").extract[Int]
    metrics.remoteBlocksFetched = (json \ "Remote Blocks Fetched").extract[Int]
    metrics.localBlocksFetched = (json \ "Local Blocks Fetched").extract[Int]
    metrics.fetchWaitTime = (json \ "Fetch Wait Time").extract[Long]
    metrics.remoteFetchTime = (json \ "Remote Fetch Time").extract[Long]
    metrics.remoteBytesRead = (json \ "Remote Bytes Read").extract[Long]
    metrics
  }

  def shuffleWriteMetricsFromJson(json: JValue): ShuffleWriteMetrics = {
    val metrics = new ShuffleWriteMetrics
    metrics.shuffleBytesWritten = (json \ "Shuffle Bytes Written").extract[Long]
    metrics.shuffleWriteTime = (json \ "Shuffle Write Time").extract[Long]
    metrics
  }

  def taskEndReasonFromJson(json: JValue): TaskEndReason = {
    val success = Utils.getFormattedClassName(Success)
    val resubmitted = Utils.getFormattedClassName(Resubmitted)
    val fetchFailed = Utils.getFormattedClassName(FetchFailed)
    val exceptionFailure = Utils.getFormattedClassName(ExceptionFailure)
    val taskResultLost = Utils.getFormattedClassName(TaskResultLost)
    val taskKilled = Utils.getFormattedClassName(TaskKilled)
    val executorLostFailure = Utils.getFormattedClassName(ExecutorLostFailure)
    val unknownReason = Utils.getFormattedClassName(UnknownReason)

    (json \ "Reason").extract[String] match {
      case `success` => Success
      case `resubmitted` => Resubmitted
      case `fetchFailed` =>
        val blockManagerAddress = blockManagerIdFromJson(json \ "Block Manager Address")
        val shuffleId = (json \ "Shuffle ID").extract[Int]
        val mapId = (json \ "Map ID").extract[Int]
        val reduceId = (json \ "Reduce ID").extract[Int]
        new FetchFailed(blockManagerAddress, shuffleId, mapId, reduceId)
      case `exceptionFailure` =>
        val className = (json \ "Class Name").extract[String]
        val description = (json \ "Description").extract[String]
        val stackTrace = stackTraceFromJson(json \ "Stack Trace")
        val metrics = Utils.jsonOption(json \ "Metrics").map(taskMetricsFromJson)
        new ExceptionFailure(className, description, stackTrace, metrics)
      case `taskResultLost` => TaskResultLost
      case `taskKilled` => TaskKilled
      case `executorLostFailure` => ExecutorLostFailure
      case `unknownReason` => UnknownReason
    }
  }

  def blockManagerIdFromJson(json: JValue): BlockManagerId = {
    val executorId = (json \ "Executor ID").extract[String]
    val host = (json \ "Host").extract[String]
    val port = (json \ "Port").extract[Int]
    val nettyPort = (json \ "Netty Port").extract[Int]
    BlockManagerId(executorId, host, port, nettyPort)
  }

  def jobResultFromJson(json: JValue): JobResult = {
    val jobSucceeded = Utils.getFormattedClassName(JobSucceeded)
    val jobFailed = Utils.getFormattedClassName(JobFailed)

    (json \ "Result").extract[String] match {
      case `jobSucceeded` => JobSucceeded
      case `jobFailed` =>
        val exception = exceptionFromJson(json \ "Exception")
        val failedStageId = (json \ "Failed Stage ID").extract[Int]
        new JobFailed(exception, failedStageId)
    }
  }

  def storageStatusFromJson(json: JValue): StorageStatus = {
    val blockManagerId = blockManagerIdFromJson(json \ "Block Manager ID")
    val maxMem = (json \ "Maximum Memory").extract[Long]
    val blocks = (json \ "Blocks").extract[List[JValue]].map { block =>
      val id = blockIdFromJson(block \ "Block ID")
      val status = blockStatusFromJson(block \ "Status")
      (id, status)
    }
    val blockMap = mutable.Map[BlockId, BlockStatus](blocks: _*)
    new StorageStatus(blockManagerId, maxMem, blockMap)
  }

  def rddInfoFromJson(json: JValue): RDDInfo = {
    val rddId = (json \ "RDD ID").extract[Int]
    val name = (json \ "Name").extract[String]
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val numPartitions = (json \ "Number of Partitions").extract[Int]
    val numCachedPartitions = (json \ "Number of Cached Partitions").extract[Int]
    val memSize = (json \ "Memory Size").extract[Long]
    val diskSize = (json \ "Disk Size").extract[Long]

    val rddInfo = new RDDInfo(rddId, name, numPartitions, storageLevel)
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

  def blockIdFromJson(json: JValue): BlockId = {
    val rddBlockId = Utils.getFormattedClassName(RDDBlockId)
    val shuffleBlockId = Utils.getFormattedClassName(ShuffleBlockId)
    val broadcastBlockId = Utils.getFormattedClassName(BroadcastBlockId)
    val broadcastHelperBlockId = Utils.getFormattedClassName(BroadcastHelperBlockId)
    val taskResultBlockId = Utils.getFormattedClassName(TaskResultBlockId)
    val streamBlockId = Utils.getFormattedClassName(StreamBlockId)
    val tempBlockId = Utils.getFormattedClassName(TempBlockId)
    val testBlockId = Utils.getFormattedClassName(TestBlockId)

    (json \ "Type").extract[String] match {
      case `rddBlockId` =>
        val rddId = (json \ "RDD ID").extract[Int]
        val splitIndex = (json \ "Split Index").extract[Int]
        new RDDBlockId(rddId, splitIndex)
      case `shuffleBlockId` =>
        val shuffleId = (json \ "Shuffle ID").extract[Int]
        val mapId = (json \ "Map ID").extract[Int]
        val reduceId = (json \ "Reduce ID").extract[Int]
        new ShuffleBlockId(shuffleId, mapId, reduceId)
      case `broadcastBlockId` =>
        val broadcastId = (json \ "Broadcast ID").extract[Long]
        new BroadcastBlockId(broadcastId)
      case `broadcastHelperBlockId` =>
        val broadcastBlockId =
          blockIdFromJson(json \ "Broadcast Block ID").asInstanceOf[BroadcastBlockId]
        val hType = (json \ "Helper Type").extract[String]
        new BroadcastHelperBlockId(broadcastBlockId, hType)
      case `taskResultBlockId` =>
        val taskId = (json \ "Task ID").extract[Long]
        new TaskResultBlockId(taskId)
      case `streamBlockId` =>
        val streamId = (json \ "Stream ID").extract[Int]
        val uniqueId = (json \ "Unique ID").extract[Long]
        new StreamBlockId(streamId, uniqueId)
      case `tempBlockId` =>
        val tempId = UUIDFromJson(json \ "Temp ID")
        new TempBlockId(tempId)
      case `testBlockId` =>
        val testId = (json \ "Test ID").extract[String]
        new TestBlockId(testId)
    }
  }

  def blockStatusFromJson(json: JValue): BlockStatus = {
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val memorySize = (json \ "Memory Size").extract[Long]
    val diskSize = (json \ "Disk Size").extract[Long]
    BlockStatus(storageLevel, memorySize, diskSize)
  }

  /**
   * Util JSON deserialization methods
   */

  def mapFromJson(json: JValue): Map[String, String] = {
    val jsonFields = json.asInstanceOf[JObject].obj
    jsonFields.map { case JField(k, JString(v)) => (k, v) }.toMap
  }

  def propertiesFromJson(json: JValue): Properties = {
    val properties = new Properties()
    if (json != JNothing) {
      mapFromJson(json).map { case (k, v) => properties.setProperty(k, v) }
    }
    properties
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

}
