package spark.scheduler

import spark.scheduler.cluster.TaskInfo
import spark.util.Distribution
import spark.{Utils, Logging}
import spark.executor.TaskMetrics

trait SparkListener {
  /**
   * called when a stage is completed, with information on the completed stage
   */
  def onStageCompleted(stageCompleted: StageCompleted)
}

sealed trait SparkListenerEvents

case class StageCompleted(val stageInfo: StageInfo) extends SparkListenerEvents


/**
 * Simple SparkListener that logs a few summary statistics when each stage completes
 */
class StatsReportListener extends SparkListener with Logging {
  def onStageCompleted(stageCompleted: StageCompleted) {
    import spark.scheduler.StatsReportListener._
    implicit val sc = stageCompleted
    this.logInfo("Finished stage: " + stageCompleted.stageInfo)
    showMillisDistribution("task runtime:", (info, _) => Some(info.duration))

    //shuffle write
    showBytesDistribution("shuffle bytes written:",(_,metric) => metric.shuffleWriteMetrics.map{_.shuffleBytesWritten})

    //fetch & io
    showMillisDistribution("fetch wait time:",(_, metric) => metric.shuffleReadMetrics.map{_.fetchWaitTime})
    showBytesDistribution("remote bytes read:", (_, metric) => metric.shuffleReadMetrics.map{_.remoteBytesRead})
    showBytesDistribution("task result size:", (_, metric) => Some(metric.resultSize))

    //runtime breakdown
    val runtimePcts = stageCompleted.stageInfo.taskInfos.map{
      case (info, metrics) => RuntimePercentage(info.duration, metrics)
    }
    showDistribution("executor (non-fetch) time pct: ", Distribution(runtimePcts.map{_.executorPct * 100}), "%2.0f %%")
    showDistribution("fetch wait time pct: ", Distribution(runtimePcts.flatMap{_.fetchPct.map{_ * 100}}), "%2.0f %%")
    showDistribution("other time pct: ", Distribution(runtimePcts.map{_.other * 100}), "%2.0f %%")
  }

}

object StatsReportListener extends Logging {

  //for profiling, the extremes are more interesting
  val percentiles = Array[Int](0,5,10,25,50,75,90,95,100)
  val probabilities = percentiles.map{_ / 100.0}
  val percentilesHeader = "\t" + percentiles.mkString("%\t") + "%"

  def extractDoubleDistribution(stage:StageCompleted, getMetric: (TaskInfo,TaskMetrics) => Option[Double]): Option[Distribution] = {
    Distribution(stage.stageInfo.taskInfos.flatMap{
      case ((info,metric)) => getMetric(info, metric)})
  }

  //is there some way to setup the types that I can get rid of this completely?
  def extractLongDistribution(stage:StageCompleted, getMetric: (TaskInfo,TaskMetrics) => Option[Long]): Option[Distribution] = {
    extractDoubleDistribution(stage, (info, metric) => getMetric(info,metric).map{_.toDouble})
  }

  def showDistribution(heading: String, d: Distribution, formatNumber: Double => String) {
    val stats = d.statCounter
    logInfo(heading + stats)
    val quantiles = d.getQuantiles(probabilities).map{formatNumber}
    logInfo(percentilesHeader)
    logInfo("\t" + quantiles.mkString("\t"))
  }

  def showDistribution(heading: String, dOpt: Option[Distribution], formatNumber: Double => String) {
    dOpt.foreach { d => showDistribution(heading, d, formatNumber)}
  }

  def showDistribution(heading: String, dOpt: Option[Distribution], format:String) {
    def f(d:Double) = format.format(d)
    showDistribution(heading, dOpt, f _)
  }

  def showDistribution(heading:String, format: String, getMetric: (TaskInfo,TaskMetrics) => Option[Double])
    (implicit stage: StageCompleted) {
    showDistribution(heading, extractDoubleDistribution(stage, getMetric), format)
  }

  def showBytesDistribution(heading:String, getMetric: (TaskInfo,TaskMetrics) => Option[Long])
    (implicit stage: StageCompleted) {
    showBytesDistribution(heading, extractLongDistribution(stage, getMetric))
  }

  def showBytesDistribution(heading: String, dOpt: Option[Distribution]) {
    dOpt.foreach{dist => showBytesDistribution(heading, dist)}
  }

  def showBytesDistribution(heading: String, dist: Distribution) {
    showDistribution(heading, dist, (d => Utils.memoryBytesToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(heading: String, dOpt: Option[Distribution]) {
    showDistribution(heading, dOpt, (d => StatsReportListener.millisToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(heading: String, getMetric: (TaskInfo, TaskMetrics) => Option[Long])
    (implicit stage: StageCompleted) {
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



case class RuntimePercentage(executorPct: Double, fetchPct: Option[Double], other: Double)
object RuntimePercentage {
  def apply(totalTime: Long, metrics: TaskMetrics): RuntimePercentage = {
    val denom = totalTime.toDouble
    val fetchTime = metrics.shuffleReadMetrics.map{_.fetchWaitTime}
    val fetch = fetchTime.map{_ / denom}
    val exec = (metrics.executorRunTime - fetchTime.getOrElse(0l)) / denom
    val other = 1.0 - (exec + fetch.getOrElse(0d))
    RuntimePercentage(exec, fetch, other)
  }
}
