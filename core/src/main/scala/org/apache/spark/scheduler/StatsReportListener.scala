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

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.util.{Distribution, Utils}


/**
 * :: DeveloperApi ::
 * Simple SparkListener that logs a few summary statistics when each stage completes.
 */
@DeveloperApi
class StatsReportListener extends SparkListener with Logging {

  import org.apache.spark.scheduler.StatsReportListener._

  private val taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      taskInfoMetrics += ((info, metrics))
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    implicit val sc = stageCompleted
    this.logInfo(s"Finished stage: ${getStatusDetail(stageCompleted.stageInfo)}")
    showMillisDistribution("task runtime:", (info, _) => info.duration, taskInfoMetrics.toSeq)

    // Shuffle write
    showBytesDistribution("shuffle bytes written:",
      (_, metric) => metric.shuffleWriteMetrics.bytesWritten, taskInfoMetrics.toSeq)

    // Fetch & I/O
    showMillisDistribution("fetch wait time:",
      (_, metric) => metric.shuffleReadMetrics.fetchWaitTime, taskInfoMetrics.toSeq)
    showBytesDistribution("remote bytes read:",
      (_, metric) => metric.shuffleReadMetrics.remoteBytesRead, taskInfoMetrics.toSeq)
    showBytesDistribution("task result size:",
      (_, metric) => metric.resultSize, taskInfoMetrics.toSeq)

    // Runtime breakdown
    val runtimePcts = taskInfoMetrics.map { case (info, metrics) =>
      RuntimePercentage(info.duration, metrics)
    }
    showDistribution("executor (non-fetch) time pct: ",
      Distribution(runtimePcts.map(_.executorPct * 100)), "%2.0f %%")
    showDistribution("fetch wait time pct: ",
      Distribution(runtimePcts.flatMap(_.fetchPct.map(_ * 100))), "%2.0f %%")
    showDistribution("other time pct: ", Distribution(runtimePcts.map(_.other * 100)), "%2.0f %%")
    taskInfoMetrics.clear()
  }

  private def getStatusDetail(info: StageInfo): String = {
    val failureReason = info.failureReason.map("(" + _ + ")").getOrElse("")
    val timeTaken = info.submissionTime.map(
      x => info.completionTime.getOrElse(System.currentTimeMillis()) - x
    ).getOrElse("-")

    s"Stage(${info.stageId}, ${info.attemptNumber}); Name: '${info.name}'; " +
      s"Status: ${info.getStatusString}$failureReason; numTasks: ${info.numTasks}; " +
      s"Took: $timeTaken msec"
  }

}

private[spark] object StatsReportListener extends Logging {

  // For profiling, the extremes are more interesting
  val percentiles = Array[Int](0, 5, 10, 25, 50, 75, 90, 95, 100)
  val probabilities = percentiles.map(_ / 100.0)
  val percentilesHeader = "\t" + percentiles.mkString("%\t") + "%"

  def extractDoubleDistribution(
    taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
    getMetric: (TaskInfo, TaskMetrics) => Double): Option[Distribution] = {
    Distribution(taskInfoMetrics.map { case (info, metric) => getMetric(info, metric) })
  }

  // Is there some way to setup the types that I can get rid of this completely?
  def extractLongDistribution(
    taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
    getMetric: (TaskInfo, TaskMetrics) => Long): Option[Distribution] = {
    extractDoubleDistribution(
      taskInfoMetrics,
      (info, metric) => { getMetric(info, metric).toDouble })
  }

  def showDistribution(heading: String, d: Distribution, formatNumber: Double => String): Unit = {
    val stats = d.statCounter
    val quantiles = d.getQuantiles(probabilities).map(formatNumber)
    logInfo(heading + stats)
    logInfo(percentilesHeader)
    logInfo("\t" + quantiles.mkString("\t"))
  }

  def showDistribution(
      heading: String,
      dOpt: Option[Distribution],
      formatNumber: Double => String): Unit = {
    dOpt.foreach { d => showDistribution(heading, d, formatNumber)}
  }

  def showDistribution(heading: String, dOpt: Option[Distribution], format: String): Unit = {
    def f(d: Double): String = format.format(d)
    showDistribution(heading, dOpt, f _)
  }

  def showDistribution(
      heading: String,
      format: String,
      getMetric: (TaskInfo, TaskMetrics) => Double,
      taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]): Unit = {
    showDistribution(heading, extractDoubleDistribution(taskInfoMetrics, getMetric), format)
  }

  def showBytesDistribution(
      heading: String,
      getMetric: (TaskInfo, TaskMetrics) => Long,
      taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]): Unit = {
    showBytesDistribution(heading, extractLongDistribution(taskInfoMetrics, getMetric))
  }

  def showBytesDistribution(heading: String, dOpt: Option[Distribution]): Unit = {
    dOpt.foreach { dist => showBytesDistribution(heading, dist) }
  }

  def showBytesDistribution(heading: String, dist: Distribution): Unit = {
    showDistribution(heading, dist, (d => Utils.bytesToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(heading: String, dOpt: Option[Distribution]): Unit = {
    showDistribution(heading, dOpt,
      (d => StatsReportListener.millisToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(
      heading: String,
      getMetric: (TaskInfo, TaskMetrics) => Long,
      taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]): Unit = {
    showMillisDistribution(heading, extractLongDistribution(taskInfoMetrics, getMetric))
  }

  val seconds = 1000L
  val minutes = seconds * 60
  val hours = minutes * 60

  /**
   * Reformat a time interval in milliseconds to a prettier format for output
   */
  def millisToString(ms: Long): String = {
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
    val fetchTime = Some(metrics.shuffleReadMetrics.fetchWaitTime)
    val fetch = fetchTime.map(_ / denom)
    val exec = (metrics.executorRunTime - fetchTime.getOrElse(0L)) / denom
    val other = 1.0 - (exec + fetch.getOrElse(0d))
    RuntimePercentage(exec, fetch, other)
  }
}
