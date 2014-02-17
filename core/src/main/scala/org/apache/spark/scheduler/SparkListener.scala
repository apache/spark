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

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.util.{Utils, Distribution}
import org.apache.spark.{Logging, TaskEndReason}
import org.apache.spark.executor.TaskMetrics

import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import org.apache.spark.storage.StorageStatus
import net.liftweb.json.DefaultFormats

trait JsonSerializable { def toJson: JValue }

sealed trait SparkListenerEvent extends JsonSerializable {
  override def toJson = "Event" -> Utils.getFormattedClassName(this)
}

case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties)
  extends SparkListenerEvent {
  override def toJson = {
    val propertiesJson = Utils.propertiesToJson(properties)
    super.toJson ~
    ("Stage Info" -> stageInfo.toJson) ~
    ("Properties" -> propertiesJson)
  }
}

case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent {
  override def toJson = {
    super.toJson ~
    ("Stage Info" -> stageInfo.toJson)
  }
}

case class SparkListenerTaskStart(stageId: Int, taskInfo: TaskInfo) extends SparkListenerEvent {
  override def toJson = {
    super.toJson ~
    ("Stage ID" -> stageId) ~
    ("Task Info" -> taskInfo.toJson)
  }
}

case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent {
  override def toJson = {
    super.toJson ~
    ("Task Info" -> taskInfo.toJson)
  }
}

case class SparkListenerTaskEnd(
    stageId: Int,
    taskType: String,
    reason: TaskEndReason,
    taskInfo: TaskInfo,
    taskMetrics: TaskMetrics)
  extends SparkListenerEvent {
  override def toJson = {
    super.toJson ~
    ("Stage ID" -> stageId) ~
    ("Task Type" -> taskType) ~
    ("Task End Reason" -> reason.toJson) ~
    ("Task Info" -> taskInfo.toJson) ~
    ("Task Metrics" -> taskMetrics.toJson)
  }
}

case class SparkListenerJobStart(jobId: Int, stageIds: Seq[Int], properties: Properties = null)
  extends SparkListenerEvent {
  override def toJson = {
    val stageIdsJson = JArray(stageIds.map(JInt(_)).toList)
    val propertiesJson = Utils.propertiesToJson(properties)
    super.toJson ~
    ("Job ID" -> jobId) ~
    ("Stage IDs" -> stageIdsJson) ~
    ("Properties" -> propertiesJson)
  }
}

case class SparkListenerJobEnd(jobId: Int, jobResult: JobResult) extends SparkListenerEvent {
  override def toJson = {
    super.toJson ~
    ("Job ID" -> jobId) ~
    ("Job Result" -> jobResult.toJson)
  }
}

/** An event used in the listener to shutdown the listener daemon thread. */
private[scheduler] case object SparkListenerShutdown extends SparkListenerEvent

/** An event used in the EnvironmentUI */
private[spark] case class SparkListenerLoadEnvironment(
    jvmInformation: Seq[(String, String)],
    sparkProperties: Seq[(String, String)],
    systemProperties: Seq[(String, String)],
    classpathEntries: Seq[(String, String)])
  extends SparkListenerEvent {

  override def toJson = {
    val jvmInformationJson = Utils.mapToJson(jvmInformation.toMap)
    val sparkPropertiesJson = Utils.mapToJson(sparkProperties.toMap)
    val systemPropertiesJson = Utils.mapToJson(systemProperties.toMap)
    val classpathEntriesJson = Utils.mapToJson(classpathEntries.toMap)
    super.toJson ~
    ("JVM Information" -> jvmInformationJson) ~
    ("Spark Properties" -> sparkPropertiesJson) ~
    ("System Properties" -> systemPropertiesJson) ~
    ("Classpath Entries" -> classpathEntriesJson)
  }
}

/** An event used in the ExecutorUI to fetch storage status from SparkEnv */
private[spark] case class SparkListenerStorageStatusFetch(storageStatusList: Seq[StorageStatus])
  extends SparkListenerEvent {
  override def toJson = {
    val storageStatusListJson = JArray(storageStatusList.map(_.toJson).toList)
    super.toJson ~
    ("Storage Status List" -> storageStatusListJson)
  }
}

object SparkListenerEvent {
  /**
   * Deserialize a SparkListenerEvent from JSON
   */
  def fromJson(json: JValue): SparkListenerEvent = {
    implicit val format = DefaultFormats
    val stageSubmitted =  Utils.getFormattedClassName(SparkListenerStageSubmitted)
    val stageCompleted =  Utils.getFormattedClassName(SparkListenerStageCompleted)
    val taskStart =  Utils.getFormattedClassName(SparkListenerTaskStart)
    val taskGettingResult =  Utils.getFormattedClassName(SparkListenerTaskGettingResult)
    val taskEnd =  Utils.getFormattedClassName(SparkListenerTaskEnd)
    val jobStart =  Utils.getFormattedClassName(SparkListenerJobStart)
    val jobEnd =  Utils.getFormattedClassName(SparkListenerJobEnd)
    val shutdown =  Utils.getFormattedClassName(SparkListenerShutdown)
    val loadEnvironment = Utils.getFormattedClassName(SparkListenerLoadEnvironment)
    val storageStatusFetch =  Utils.getFormattedClassName(SparkListenerStorageStatusFetch)

    (json \ "Event").extract[String] match {
      case `stageSubmitted` => stageSubmittedFromJson(json)
      case `stageCompleted` => stageCompletedFromJson(json)
      case `taskStart` => taskStartFromJson(json)
      case `taskGettingResult` => taskGettingResultFromJson(json)
      case `taskEnd` => taskEndFromJson(json)
      case `jobStart` => jobStartFromJson(json)
      case `jobEnd` => jobEndFromJson(json)
      case `shutdown` => SparkListenerShutdown
      case `loadEnvironment` => loadEnvironmentFromJson(json)
      case `storageStatusFetch` => storageStatusFetchFromJson(json)
    }
  }

  private def stageSubmittedFromJson(json: JValue) = {
    new SparkListenerStageSubmitted(
      StageInfo.fromJson(json \ "Stage Info"),
      Utils.propertiesFromJson(json \ "Properties"))
  }

  private def stageCompletedFromJson(json: JValue) = {
    new SparkListenerStageCompleted(StageInfo.fromJson(json \ "Stage Info"))
  }

  private def taskStartFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new SparkListenerTaskStart(
      (json \ "Stage ID").extract[Int],
      TaskInfo.fromJson(json \ "Task Info"))
  }

  private def taskGettingResultFromJson(json: JValue) = {
    new SparkListenerTaskGettingResult(TaskInfo.fromJson(json \ "Task Info"))
  }

  private def taskEndFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new SparkListenerTaskEnd(
      (json \ "Stage ID").extract[Int],
      (json \ "Task Type").extract[String],
      TaskEndReason.fromJson(json \ "Task End Reason"),
      TaskInfo.fromJson(json \ "Task Info"),
      TaskMetrics.fromJson(json \ "Task Metrics"))
  }

  private def jobStartFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    val stageIds = (json \ "Stage IDs").extract[List[JValue]].map(_.extract[Int])
    new SparkListenerJobStart(
      (json \ "Job ID").extract[Int],
      stageIds,
      Utils.propertiesFromJson(json \ "Properties")
    )
  }

  private def jobEndFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new SparkListenerJobEnd(
      (json \ "Job ID").extract[Int],
      JobResult.fromJson(json \ "Job Result"))
  }

  private def loadEnvironmentFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new SparkListenerLoadEnvironment(
      Utils.mapFromJson(json \ "JVM Information").toSeq,
      Utils.mapFromJson(json \ "Spark Properties").toSeq,
      Utils.mapFromJson(json \ "System Properties").toSeq,
      Utils.mapFromJson(json \ "Classpath Entries").toSeq)
  }

  private def storageStatusFetchFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    val storageStatusList =
      (json \ "Storage Status List").extract[List[JValue]].map(StorageStatus.fromJson)
    new SparkListenerStorageStatusFetch(storageStatusList)
  }
}


/**
 * Interface for listening to events from the Spark scheduler.
 */
trait SparkListener {
  /**
   * Called when a stage is completed, with information on the completed stage
   */
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted) { }

  /**
   * Called when a stage is submitted
   */
  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) { }

  /**
   * Called when a task starts
   */
  def onTaskStart(taskStart: SparkListenerTaskStart) { }

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) { }

  /**
   * Called when a task ends
   */
  def onTaskEnd(taskEnd: SparkListenerTaskEnd) { }

  /**
   * Called when a job starts
   */
  def onJobStart(jobStart: SparkListenerJobStart) { }

  /**
   * Called when a job ends
   */
  def onJobEnd(jobEnd: SparkListenerJobEnd) { }

}

/**
 * Simple SparkListener that logs a few summary statistics when each stage completes
 */
class StatsReportListener extends SparkListener with Logging {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    import org.apache.spark.scheduler.StatsReportListener._
    implicit val sc = stageCompleted
    this.logInfo("Finished stage: " + stageCompleted.stageInfo)
    showMillisDistribution("task runtime:", (info, _) => Some(info.duration))

    // Shuffle write
    showBytesDistribution("shuffle bytes written:",
      (_,metric) => metric.shuffleWriteMetrics.map(_.shuffleBytesWritten))

    // Fetch & I/O
    showMillisDistribution("fetch wait time:",
      (_, metric) => metric.shuffleReadMetrics.map(_.fetchWaitTime))
    showBytesDistribution("remote bytes read:",
      (_, metric) => metric.shuffleReadMetrics.map(_.remoteBytesRead))
    showBytesDistribution("task result size:", (_, metric) => Some(metric.resultSize))

    // Runtime breakdown
    val runtimePcts = stageCompleted.stageInfo.taskInfos.map{ case (info, metrics) =>
      RuntimePercentage(info.duration, metrics)
    }
    showDistribution("executor (non-fetch) time pct: ",
      Distribution(runtimePcts.map{_.executorPct * 100}), "%2.0f %%")
    showDistribution("fetch wait time pct: ",
      Distribution(runtimePcts.flatMap{_.fetchPct.map{_ * 100}}), "%2.0f %%")
    showDistribution("other time pct: ", Distribution(runtimePcts.map{_.other * 100}), "%2.0f %%")
  }

}

private[spark] object StatsReportListener extends Logging {

  // For profiling, the extremes are more interesting
  val percentiles = Array[Int](0,5,10,25,50,75,90,95,100)
  val probabilities = percentiles.map{_ / 100.0}
  val percentilesHeader = "\t" + percentiles.mkString("%\t") + "%"

  def extractDoubleDistribution(stage: SparkListenerStageCompleted,
      getMetric: (TaskInfo, TaskMetrics) => Option[Double])
    : Option[Distribution] = {
    Distribution(stage.stageInfo.taskInfos.flatMap {
      case ((info,metric)) => getMetric(info, metric)})
  }

  // Is there some way to setup the types that I can get rid of this completely?
  def extractLongDistribution(stage: SparkListenerStageCompleted,
      getMetric: (TaskInfo, TaskMetrics) => Option[Long])
    : Option[Distribution] = {
    extractDoubleDistribution(stage, (info, metric) => getMetric(info,metric).map{_.toDouble})
  }

  def showDistribution(heading: String, d: Distribution, formatNumber: Double => String) {
    val stats = d.statCounter
    val quantiles = d.getQuantiles(probabilities).map(formatNumber)
    logInfo(heading + stats)
    logInfo(percentilesHeader)
    logInfo("\t" + quantiles.mkString("\t"))
  }

  def showDistribution(heading: String, dOpt: Option[Distribution], formatNumber: Double => String)
  {
    dOpt.foreach { d => showDistribution(heading, d, formatNumber)}
  }

  def showDistribution(heading: String, dOpt: Option[Distribution], format:String) {
    def f(d:Double) = format.format(d)
    showDistribution(heading, dOpt, f _)
  }

  def showDistribution(
      heading: String,
      format: String,
      getMetric: (TaskInfo, TaskMetrics) => Option[Double])
      (implicit stage: SparkListenerStageCompleted) {
    showDistribution(heading, extractDoubleDistribution(stage, getMetric), format)
  }

  def showBytesDistribution(heading:String, getMetric: (TaskInfo, TaskMetrics) => Option[Long])
    (implicit stage: SparkListenerStageCompleted) {
    showBytesDistribution(heading, extractLongDistribution(stage, getMetric))
  }

  def showBytesDistribution(heading: String, dOpt: Option[Distribution]) {
    dOpt.foreach{dist => showBytesDistribution(heading, dist)}
  }

  def showBytesDistribution(heading: String, dist: Distribution) {
    showDistribution(heading, dist, (d => Utils.bytesToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(heading: String, dOpt: Option[Distribution]) {
    showDistribution(heading, dOpt,
      (d => StatsReportListener.millisToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(heading: String, getMetric: (TaskInfo, TaskMetrics) => Option[Long])
    (implicit stage: SparkListenerStageCompleted) {
    showMillisDistribution(heading, extractLongDistribution(stage, getMetric))
  }

  val seconds = 1000L
  val minutes = seconds * 60
  val hours = minutes * 60

  /**
   * Reformat a time interval in milliseconds to a prettier format for output
   */
  def millisToString(ms: Long) = {
    val (size, units) =
      if (ms > hours) {
        (ms.toDouble / hours, "hours")
      } else if (ms > minutes) {
        (ms.toDouble / minutes, "min")
      } else if (ms > seconds) {
        (ms.toDouble / seconds, "s")
      } else {
        (ms.toDouble, "ms")
      }
    "%.1f %s".format(size, units)
  }
}

private case class RuntimePercentage(executorPct: Double, fetchPct: Option[Double], other: Double)

private object RuntimePercentage {
  def apply(totalTime: Long, metrics: TaskMetrics): RuntimePercentage = {
    val denom = totalTime.toDouble
    val fetchTime = metrics.shuffleReadMetrics.map{_.fetchWaitTime}
    val fetch = fetchTime.map{_ / denom}
    val exec = (metrics.executorRunTime - fetchTime.getOrElse(0L)) / denom
    val other = 1.0 - (exec + fetch.getOrElse(0d))
    RuntimePercentage(exec, fetch, other)
  }
}
