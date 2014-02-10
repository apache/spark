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

sealed trait SparkListenerEvents

case class SparkListenerStageSubmitted(stage: StageInfo, properties: Properties)
     extends SparkListenerEvents

case class SparkListenerStageCompleted(stage: StageInfo) extends SparkListenerEvents

case class SparkListenerTaskStart(task: Task[_], taskInfo: TaskInfo) extends SparkListenerEvents

case class SparkListenerTaskGettingResult(
  task: Task[_], taskInfo: TaskInfo) extends SparkListenerEvents

case class SparkListenerTaskEnd(task: Task[_], reason: TaskEndReason, taskInfo: TaskInfo,
     taskMetrics: TaskMetrics) extends SparkListenerEvents

case class SparkListenerJobStart(job: ActiveJob, stageIds: Array[Int],
    properties: Properties = null) extends SparkListenerEvents

case class SparkListenerJobEnd(job: ActiveJob, jobResult: JobResult)
     extends SparkListenerEvents

/** An event used in the listener to shutdown the listener daemon thread. */
private[scheduler] case object SparkListenerShutdown extends SparkListenerEvents

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
    this.logInfo("Finished stage: " + stageCompleted.stage)
    showMillisDistribution("task runtime:", (info, _) => Some(info.duration))

    //shuffle write
    showBytesDistribution("shuffle bytes written:",
      (_,metric) => metric.shuffleWriteMetrics.map(_.shuffleBytesWritten))

    //fetch & io
    showMillisDistribution("fetch wait time:",
      (_, metric) => metric.shuffleReadMetrics.map(_.fetchWaitTime))
    showBytesDistribution("remote bytes read:",
      (_, metric) => metric.shuffleReadMetrics.map(_.remoteBytesRead))
    showBytesDistribution("task result size:", (_, metric) => Some(metric.resultSize))

    //runtime breakdown

    val runtimePcts = stageCompleted.stage.taskInfos.map{
      case (info, metrics) => RuntimePercentage(info.duration, metrics)
    }
    showDistribution("executor (non-fetch) time pct: ",
      Distribution(runtimePcts.map{_.executorPct * 100}), "%2.0f %%")
    showDistribution("fetch wait time pct: ",
      Distribution(runtimePcts.flatMap{_.fetchPct.map{_ * 100}}), "%2.0f %%")
    showDistribution("other time pct: ", Distribution(runtimePcts.map{_.other * 100}), "%2.0f %%")
  }

}

private[spark] object StatsReportListener extends Logging {

  //for profiling, the extremes are more interesting
  val percentiles = Array[Int](0,5,10,25,50,75,90,95,100)
  val probabilities = percentiles.map{_ / 100.0}
  val percentilesHeader = "\t" + percentiles.mkString("%\t") + "%"

  def extractDoubleDistribution(stage: SparkListenerStageCompleted,
      getMetric: (TaskInfo,TaskMetrics) => Option[Double])
    : Option[Distribution] = {
    Distribution(stage.stage.taskInfos.flatMap {
      case ((info,metric)) => getMetric(info, metric)})
  }

  //is there some way to setup the types that I can get rid of this completely?
  def extractLongDistribution(stage: SparkListenerStageCompleted,
      getMetric: (TaskInfo,TaskMetrics) => Option[Long])
    : Option[Distribution] = {
    extractDoubleDistribution(stage, (info, metric) => getMetric(info,metric).map{_.toDouble})
  }

  def showDistribution(heading: String, d: Distribution, formatNumber: Double => String) {
    val stats = d.statCounter
    val quantiles = d.getQuantiles(probabilities).map{formatNumber}
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

  def showBytesDistribution(heading:String, getMetric: (TaskInfo,TaskMetrics) => Option[Long])
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
   * reformat a time interval in milliseconds to a prettier format for output
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
