package spark.scheduler

import spark.util.Distribution
import spark.{Utils, Logging}

trait SparkListener {
  def onStageCompleted(stageCompleted: StageCompleted)
}

sealed trait SparkListenerEvents

case class StageCompleted(val stageInfo: StageInfo) extends SparkListenerEvents



class StatsReportListener extends SparkListener with Logging {
  def onStageCompleted(stageCompleted: StageCompleted) {
    import spark.scheduler.StatsReportListener._
    this.logInfo("Finished stage: " + stageCompleted.stageInfo)
    showMillisDistribution("task runtime:", stageCompleted.stageInfo.getTaskRuntimeDistribution)
    showBytesDistribution("shuffle bytes written:", stageCompleted.stageInfo.getShuffleBytesWrittenDistribution)

    //fetch & some io info
    showMillisDistribution("fetch wait time:",stageCompleted.stageInfo.getRemoteFetchWaitTimeDistribution)
    showBytesDistribution("remote bytes read:", stageCompleted.stageInfo.getRemoteBytesReadDistribution)
    showBytesDistribution("task result size:", stageCompleted.stageInfo.getTaskResultSizeDistribution)

    //runtime breakdown
    showDistribution("executor (non-fetch) time pct: ", stageCompleted.stageInfo.getExectuorRuntimePercentage, "%2.0f \\%")
    showDistribution("fetch wait time pct: ", stageCompleted.stageInfo.getFetchRuntimePercentage, "%2.0f \\%")
    showDistribution("other time pct: ", stageCompleted.stageInfo.getOtherRuntimePercentage, "%2.0f \\%")
  }

}

object StatsReportListener extends Logging {

  //for profiling, the extremes are more interesting
  val percentiles = Array[Int](0,5,10,25,50,75,90,95,100)
  val probabilities = percentiles.map{_ / 100.0}
  val percentilesHeader = "\t" + percentiles.mkString("%\t") + "%"

  def showDistribution(heading: String, dOpt: Option[Distribution], format:String) {
    def f(d:Double) = format.format(d)
    showDistribution(heading, dOpt, f _)
  }

  def showBytesDistribution(heading: String, dOpt: Option[Distribution]) {
    showDistribution(heading, dOpt, d => Utils.memoryBytesToString(d.toLong))
  }

  def showMillisDistribution(heading: String, dOpt: Option[Distribution]) {
    showDistribution(heading, dOpt, d => StatsReportListener.millisToString(d.toLong))
  }

  def showDistribution(heading: String, dOpt: Option[Distribution], formatNumber: Double => String) {
    dOpt.foreach { d =>
      val stats = d.statCounter
      logInfo(heading + stats)
      val quantiles = d.getQuantiles(probabilities).map{formatNumber}
      logInfo(percentilesHeader)
      logInfo("\t" + quantiles.mkString("\t"))
    }
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