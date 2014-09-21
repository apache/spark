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

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._


import org.apache.spark.executor.{DataReadMethod, InputMetrics, ShuffleReadMetrics,
  ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.scheduler._
import org.apache.spark.storage._
import org.apache.spark._

private[spark] object JsonProtocol {
  // TODO: Remove this file and put JSON serialization into each individual class.

  private implicit val format = DefaultFormats

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

      // These aren't used, but keeps compiler happy
      case SparkListenerShutdown => JNothing
      case SparkListenerExecutorMetricsUpdate(_, _) => JNothing
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
    val taskInfo = taskStart.taskInfo
    ("Event" -> Utils.getFormattedClassName(taskStart)) ~
    ("Stage ID" -> taskStart.stageId) ~
    ("Stage Attempt ID" -> taskStart.stageAttemptId) ~
    ("Task Info" -> taskInfoToJson(taskInfo))
  }

  def taskGettingResultToJson(taskGettingResult: SparkListenerTaskGettingResult): JValue = {
    val taskInfo = taskGettingResult.taskInfo
    ("Event" -> Utils.getFormattedClassName(taskGettingResult)) ~
    ("Task Info" -> taskInfoToJson(taskInfo))
  }

  def taskEndToJson(taskEnd: SparkListenerTaskEnd): JValue = {
    val taskEndReason = taskEndReasonToJson(taskEnd.reason)
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    val taskMetricsJson = if (taskMetrics != null) taskMetricsToJson(taskMetrics) else JNothing
    ("Event" -> Utils.getFormattedClassName(taskEnd)) ~
    ("Stage ID" -> taskEnd.stageId) ~
    ("Stage Attempt ID" -> taskEnd.stageAttemptId) ~
    ("Task Type" -> taskEnd.taskType) ~
    ("Task End Reason" -> taskEndReason) ~
    ("Task Info" -> taskInfoToJson(taskInfo)) ~
    ("Task Metrics" -> taskMetricsJson)
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

  def blockManagerAddedToJson(blockManagerAdded: SparkListenerBlockManagerAdded): JValue = {
    val blockManagerId = blockManagerIdToJson(blockManagerAdded.blockManagerId)
    ("Event" -> Utils.getFormattedClassName(blockManagerAdded)) ~
    ("Block Manager ID" -> blockManagerId) ~
    ("Maximum Memory" -> blockManagerAdded.maxMem) ~
    ("Timestamp" -> blockManagerAdded.time)
  }

  def blockManagerRemovedToJson(blockManagerRemoved: SparkListenerBlockManagerRemoved): JValue = {
    val blockManagerId = blockManagerIdToJson(blockManagerRemoved.blockManagerId)
    ("Event" -> Utils.getFormattedClassName(blockManagerRemoved)) ~
    ("Block Manager ID" -> blockManagerId) ~
    ("Timestamp" -> blockManagerRemoved.time)
  }

  def unpersistRDDToJson(unpersistRDD: SparkListenerUnpersistRDD): JValue = {
    ("Event" -> Utils.getFormattedClassName(unpersistRDD)) ~
    ("RDD ID" -> unpersistRDD.rddId)
  }

  def applicationStartToJson(applicationStart: SparkListenerApplicationStart): JValue = {
    ("Event" -> Utils.getFormattedClassName(applicationStart)) ~
    ("App Name" -> applicationStart.appName) ~
    ("App ID" -> applicationStart.appId.map(JString(_)).getOrElse(JNothing)) ~
    ("Timestamp" -> applicationStart.time) ~
    ("User" -> applicationStart.sparkUser)
  }

  def applicationEndToJson(applicationEnd: SparkListenerApplicationEnd): JValue = {
    ("Event" -> Utils.getFormattedClassName(applicationEnd)) ~
    ("Timestamp" -> applicationEnd.time)
  }


  /** ------------------------------------------------------------------- *
   * JSON serialization methods for classes SparkListenerEvents depend on |
   * -------------------------------------------------------------------- */

  def stageInfoToJson(stageInfo: StageInfo): JValue = {
    val rddInfo = JArray(stageInfo.rddInfos.map(rddInfoToJson).toList)
    val submissionTime = stageInfo.submissionTime.map(JInt(_)).getOrElse(JNothing)
    val completionTime = stageInfo.completionTime.map(JInt(_)).getOrElse(JNothing)
    val failureReason = stageInfo.failureReason.map(JString(_)).getOrElse(JNothing)
    ("Stage ID" -> stageInfo.stageId) ~
    ("Stage Attempt ID" -> stageInfo.attemptId) ~
    ("Stage Name" -> stageInfo.name) ~
    ("Number of Tasks" -> stageInfo.numTasks) ~
    ("RDD Info" -> rddInfo) ~
    ("Details" -> stageInfo.details) ~
    ("Submission Time" -> submissionTime) ~
    ("Completion Time" -> completionTime) ~
    ("Failure Reason" -> failureReason) ~
    ("Accumulables" -> JArray(
        stageInfo.accumulables.values.map(accumulableInfoToJson).toList))
  }

  def taskInfoToJson(taskInfo: TaskInfo): JValue = {
    ("Task ID" -> taskInfo.taskId) ~
    ("Index" -> taskInfo.index) ~
    ("Attempt" -> taskInfo.attempt) ~
    ("Launch Time" -> taskInfo.launchTime) ~
    ("Executor ID" -> taskInfo.executorId) ~
    ("Host" -> taskInfo.host) ~
    ("Locality" -> taskInfo.taskLocality.toString) ~
    ("Speculative" -> taskInfo.speculative) ~
    ("Getting Result Time" -> taskInfo.gettingResultTime) ~
    ("Finish Time" -> taskInfo.finishTime) ~
    ("Failed" -> taskInfo.failed) ~
    ("Accumulables" -> JArray(taskInfo.accumulables.map(accumulableInfoToJson).toList))
  }

  def accumulableInfoToJson(accumulableInfo: AccumulableInfo): JValue = {
    ("ID" -> accumulableInfo.id) ~
    ("Name" -> accumulableInfo.name) ~
    ("Update" -> accumulableInfo.update.map(new JString(_)).getOrElse(JNothing)) ~
    ("Value" -> accumulableInfo.value)
  }

  def taskMetricsToJson(taskMetrics: TaskMetrics): JValue = {
    val shuffleReadMetrics =
      taskMetrics.shuffleReadMetrics.map(shuffleReadMetricsToJson).getOrElse(JNothing)
    val shuffleWriteMetrics =
      taskMetrics.shuffleWriteMetrics.map(shuffleWriteMetricsToJson).getOrElse(JNothing)
    val inputMetrics =
      taskMetrics.inputMetrics.map(inputMetricsToJson).getOrElse(JNothing)
    val updatedBlocks =
      taskMetrics.updatedBlocks.map { blocks =>
        JArray(blocks.toList.map { case (id, status) =>
          ("Block ID" -> id.toString) ~
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
    ("Input Metrics" -> inputMetrics) ~
    ("Updated Blocks" -> updatedBlocks)
  }

  def shuffleReadMetricsToJson(shuffleReadMetrics: ShuffleReadMetrics): JValue = {
    ("Remote Blocks Fetched" -> shuffleReadMetrics.remoteBlocksFetched) ~
    ("Local Blocks Fetched" -> shuffleReadMetrics.localBlocksFetched) ~
    ("Fetch Wait Time" -> shuffleReadMetrics.fetchWaitTime) ~
    ("Remote Bytes Read" -> shuffleReadMetrics.remoteBytesRead)
  }

  def shuffleWriteMetricsToJson(shuffleWriteMetrics: ShuffleWriteMetrics): JValue = {
    ("Shuffle Bytes Written" -> shuffleWriteMetrics.shuffleBytesWritten) ~
    ("Shuffle Write Time" -> shuffleWriteMetrics.shuffleWriteTime)
  }

  def inputMetricsToJson(inputMetrics: InputMetrics): JValue = {
    ("Data Read Method" -> inputMetrics.readMethod.toString) ~
    ("Bytes Read" -> inputMetrics.bytesRead)
  }

  def taskEndReasonToJson(taskEndReason: TaskEndReason): JValue = {
    val reason = Utils.getFormattedClassName(taskEndReason)
    val json = taskEndReason match {
      case fetchFailed: FetchFailed =>
        val blockManagerAddress = Option(fetchFailed.bmAddress).
          map(blockManagerIdToJson).getOrElse(JNothing)
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
    ("Port" -> blockManagerId.port)
  }

  def jobResultToJson(jobResult: JobResult): JValue = {
    val result = Utils.getFormattedClassName(jobResult)
    val json = jobResult match {
      case JobSucceeded => Utils.emptyJson
      case jobFailed: JobFailed =>
        JObject("Exception" -> exceptionToJson(jobFailed.exception))
    }
    ("Result" -> result) ~ json
  }

  def rddInfoToJson(rddInfo: RDDInfo): JValue = {
    val storageLevel = storageLevelToJson(rddInfo.storageLevel)
    ("RDD ID" -> rddInfo.id) ~
    ("Name" -> rddInfo.name) ~
    ("Storage Level" -> storageLevel) ~
    ("Number of Partitions" -> rddInfo.numPartitions) ~
    ("Number of Cached Partitions" -> rddInfo.numCachedPartitions) ~
    ("Memory Size" -> rddInfo.memSize) ~
    ("Tachyon Size" -> rddInfo.tachyonSize) ~
    ("Disk Size" -> rddInfo.diskSize)
  }

  def storageLevelToJson(storageLevel: StorageLevel): JValue = {
    ("Use Disk" -> storageLevel.useDisk) ~
    ("Use Memory" -> storageLevel.useMemory) ~
    ("Use Tachyon" -> storageLevel.useOffHeap) ~
    ("Deserialized" -> storageLevel.deserialized) ~
    ("Replication" -> storageLevel.replication)
  }

  def blockStatusToJson(blockStatus: BlockStatus): JValue = {
    val storageLevel = storageLevelToJson(blockStatus.storageLevel)
    ("Storage Level" -> storageLevel) ~
    ("Memory Size" -> blockStatus.memSize) ~
    ("Tachyon Size" -> blockStatus.tachyonSize) ~
    ("Disk Size" -> blockStatus.diskSize)
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

  def sparkEventFromJson(json: JValue): SparkListenerEvent = {
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
    val stageAttemptId = (json \ "Stage Attempt ID").extractOpt[Int].getOrElse(0)
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    SparkListenerTaskStart(stageId, stageAttemptId, taskInfo)
  }

  def taskGettingResultFromJson(json: JValue): SparkListenerTaskGettingResult = {
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    SparkListenerTaskGettingResult(taskInfo)
  }

  def taskEndFromJson(json: JValue): SparkListenerTaskEnd = {
    val stageId = (json \ "Stage ID").extract[Int]
    val stageAttemptId = (json \ "Stage Attempt ID").extractOpt[Int].getOrElse(0)
    val taskType = (json \ "Task Type").extract[String]
    val taskEndReason = taskEndReasonFromJson(json \ "Task End Reason")
    val taskInfo = taskInfoFromJson(json \ "Task Info")
    val taskMetrics = taskMetricsFromJson(json \ "Task Metrics")
    SparkListenerTaskEnd(stageId, stageAttemptId, taskType, taskEndReason, taskInfo, taskMetrics)
  }

  def jobStartFromJson(json: JValue): SparkListenerJobStart = {
    val jobId = (json \ "Job ID").extract[Int]
    val stageIds = (json \ "Stage IDs").extract[List[JValue]].map(_.extract[Int])
    val properties = propertiesFromJson(json \ "Properties")
    SparkListenerJobStart(jobId, stageIds, properties)
  }

  def jobEndFromJson(json: JValue): SparkListenerJobEnd = {
    val jobId = (json \ "Job ID").extract[Int]
    val jobResult = jobResultFromJson(json \ "Job Result")
    SparkListenerJobEnd(jobId, jobResult)
  }

  def environmentUpdateFromJson(json: JValue): SparkListenerEnvironmentUpdate = {
    val environmentDetails = Map[String, Seq[(String, String)]](
      "JVM Information" -> mapFromJson(json \ "JVM Information").toSeq,
      "Spark Properties" -> mapFromJson(json \ "Spark Properties").toSeq,
      "System Properties" -> mapFromJson(json \ "System Properties").toSeq,
      "Classpath Entries" -> mapFromJson(json \ "Classpath Entries").toSeq)
    SparkListenerEnvironmentUpdate(environmentDetails)
  }

  def blockManagerAddedFromJson(json: JValue): SparkListenerBlockManagerAdded = {
    val blockManagerId = blockManagerIdFromJson(json \ "Block Manager ID")
    val maxMem = (json \ "Maximum Memory").extract[Long]
    val time = Utils.jsonOption(json \ "Timestamp").map(_.extract[Long]).getOrElse(-1L)
    SparkListenerBlockManagerAdded(time, blockManagerId, maxMem)
  }

  def blockManagerRemovedFromJson(json: JValue): SparkListenerBlockManagerRemoved = {
    val blockManagerId = blockManagerIdFromJson(json \ "Block Manager ID")
    val time = Utils.jsonOption(json \ "Timestamp").map(_.extract[Long]).getOrElse(-1L)
    SparkListenerBlockManagerRemoved(time, blockManagerId)
  }

  def unpersistRDDFromJson(json: JValue): SparkListenerUnpersistRDD = {
    SparkListenerUnpersistRDD((json \ "RDD ID").extract[Int])
  }

  def applicationStartFromJson(json: JValue): SparkListenerApplicationStart = {
    val appName = (json \ "App Name").extract[String]
    val appId = Utils.jsonOption(json \ "App ID").map(_.extract[String])
    val time = (json \ "Timestamp").extract[Long]
    val sparkUser = (json \ "User").extract[String]
    SparkListenerApplicationStart(appName, appId, time, sparkUser)
  }

  def applicationEndFromJson(json: JValue): SparkListenerApplicationEnd = {
    SparkListenerApplicationEnd((json \ "Timestamp").extract[Long])
  }


  /** --------------------------------------------------------------------- *
   * JSON deserialization methods for classes SparkListenerEvents depend on |
   * ---------------------------------------------------------------------- */

  def stageInfoFromJson(json: JValue): StageInfo = {
    val stageId = (json \ "Stage ID").extract[Int]
    val attemptId = (json \ "Attempt ID").extractOpt[Int].getOrElse(0)
    val stageName = (json \ "Stage Name").extract[String]
    val numTasks = (json \ "Number of Tasks").extract[Int]
    val rddInfos = (json \ "RDD Info").extract[List[JValue]].map(rddInfoFromJson(_))
    val details = (json \ "Details").extractOpt[String].getOrElse("")
    val submissionTime = Utils.jsonOption(json \ "Submission Time").map(_.extract[Long])
    val completionTime = Utils.jsonOption(json \ "Completion Time").map(_.extract[Long])
    val failureReason = Utils.jsonOption(json \ "Failure Reason").map(_.extract[String])
    val accumulatedValues = (json \ "Accumulables").extractOpt[List[JValue]] match {
      case Some(values) => values.map(accumulableInfoFromJson(_))
      case None => Seq[AccumulableInfo]()
    }

    val stageInfo = new StageInfo(stageId, attemptId, stageName, numTasks, rddInfos, details)
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
    val attempt = (json \ "Attempt").extractOpt[Int].getOrElse(1)
    val launchTime = (json \ "Launch Time").extract[Long]
    val executorId = (json \ "Executor ID").extract[String]
    val host = (json \ "Host").extract[String]
    val taskLocality = TaskLocality.withName((json \ "Locality").extract[String])
    val speculative = (json \ "Speculative").extractOpt[Boolean].getOrElse(false)
    val gettingResultTime = (json \ "Getting Result Time").extract[Long]
    val finishTime = (json \ "Finish Time").extract[Long]
    val failed = (json \ "Failed").extract[Boolean]
    val accumulables = (json \ "Accumulables").extractOpt[Seq[JValue]] match {
      case Some(values) => values.map(accumulableInfoFromJson(_))
      case None => Seq[AccumulableInfo]()
    }

    val taskInfo =
      new TaskInfo(taskId, index, attempt, launchTime, executorId, host, taskLocality, speculative)
    taskInfo.gettingResultTime = gettingResultTime
    taskInfo.finishTime = finishTime
    taskInfo.failed = failed
    accumulables.foreach { taskInfo.accumulables += _ }
    taskInfo
  }

  def accumulableInfoFromJson(json: JValue): AccumulableInfo = {
    val id = (json \ "ID").extract[Long]
    val name = (json \ "Name").extract[String]
    val update = Utils.jsonOption(json \ "Update").map(_.extract[String])
    val value = (json \ "Value").extract[String]
    AccumulableInfo(id, name, update, value)
  }

  def taskMetricsFromJson(json: JValue): TaskMetrics = {
    if (json == JNothing) {
      return TaskMetrics.empty
    }
    val metrics = new TaskMetrics
    metrics.hostname = (json \ "Host Name").extract[String]
    metrics.executorDeserializeTime = (json \ "Executor Deserialize Time").extract[Long]
    metrics.executorRunTime = (json \ "Executor Run Time").extract[Long]
    metrics.resultSize = (json \ "Result Size").extract[Long]
    metrics.jvmGCTime = (json \ "JVM GC Time").extract[Long]
    metrics.resultSerializationTime = (json \ "Result Serialization Time").extract[Long]
    metrics.memoryBytesSpilled = (json \ "Memory Bytes Spilled").extract[Long]
    metrics.diskBytesSpilled = (json \ "Disk Bytes Spilled").extract[Long]
    metrics.setShuffleReadMetrics(
      Utils.jsonOption(json \ "Shuffle Read Metrics").map(shuffleReadMetricsFromJson))
    metrics.shuffleWriteMetrics =
      Utils.jsonOption(json \ "Shuffle Write Metrics").map(shuffleWriteMetricsFromJson)
    metrics.inputMetrics =
      Utils.jsonOption(json \ "Input Metrics").map(inputMetricsFromJson)
    metrics.updatedBlocks =
      Utils.jsonOption(json \ "Updated Blocks").map { value =>
        value.extract[List[JValue]].map { block =>
          val id = BlockId((block \ "Block ID").extract[String])
          val status = blockStatusFromJson(block \ "Status")
          (id, status)
        }
      }
    metrics
  }

  def shuffleReadMetricsFromJson(json: JValue): ShuffleReadMetrics = {
    val metrics = new ShuffleReadMetrics
    metrics.remoteBlocksFetched = (json \ "Remote Blocks Fetched").extract[Int]
    metrics.localBlocksFetched = (json \ "Local Blocks Fetched").extract[Int]
    metrics.fetchWaitTime = (json \ "Fetch Wait Time").extract[Long]
    metrics.remoteBytesRead = (json \ "Remote Bytes Read").extract[Long]
    metrics
  }

  def shuffleWriteMetricsFromJson(json: JValue): ShuffleWriteMetrics = {
    val metrics = new ShuffleWriteMetrics
    metrics.shuffleBytesWritten = (json \ "Shuffle Bytes Written").extract[Long]
    metrics.shuffleWriteTime = (json \ "Shuffle Write Time").extract[Long]
    metrics
  }

  def inputMetricsFromJson(json: JValue): InputMetrics = {
    val metrics = new InputMetrics(
      DataReadMethod.withName((json \ "Data Read Method").extract[String]))
    metrics.bytesRead = (json \ "Bytes Read").extract[Long]
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
    BlockManagerId(executorId, host, port)
  }

  def jobResultFromJson(json: JValue): JobResult = {
    val jobSucceeded = Utils.getFormattedClassName(JobSucceeded)
    val jobFailed = Utils.getFormattedClassName(JobFailed)

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
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val numPartitions = (json \ "Number of Partitions").extract[Int]
    val numCachedPartitions = (json \ "Number of Cached Partitions").extract[Int]
    val memSize = (json \ "Memory Size").extract[Long]
    val tachyonSize = (json \ "Tachyon Size").extract[Long]
    val diskSize = (json \ "Disk Size").extract[Long]

    val rddInfo = new RDDInfo(rddId, name, numPartitions, storageLevel)
    rddInfo.numCachedPartitions = numCachedPartitions
    rddInfo.memSize = memSize
    rddInfo.tachyonSize = tachyonSize
    rddInfo.diskSize = diskSize
    rddInfo
  }

  def storageLevelFromJson(json: JValue): StorageLevel = {
    val useDisk = (json \ "Use Disk").extract[Boolean]
    val useMemory = (json \ "Use Memory").extract[Boolean]
    val useTachyon = (json \ "Use Tachyon").extract[Boolean]
    val deserialized = (json \ "Deserialized").extract[Boolean]
    val replication = (json \ "Replication").extract[Int]
    StorageLevel(useDisk, useMemory, useTachyon, deserialized, replication)
  }

  def blockStatusFromJson(json: JValue): BlockStatus = {
    val storageLevel = storageLevelFromJson(json \ "Storage Level")
    val memorySize = (json \ "Memory Size").extract[Long]
    val diskSize = (json \ "Disk Size").extract[Long]
    val tachyonSize = (json \ "Tachyon Size").extract[Long]
    BlockStatus(storageLevel, memorySize, diskSize, tachyonSize)
  }


  /** -------------------------------- *
   * Util JSON deserialization methods |
   * --------------------------------- */

  def mapFromJson(json: JValue): Map[String, String] = {
    val jsonFields = json.asInstanceOf[JObject].obj
    jsonFields.map { case JField(k, JString(v)) => (k, v) }.toMap
  }

  def propertiesFromJson(json: JValue): Properties = {
    Utils.jsonOption(json).map { value =>
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

}
