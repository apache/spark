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
    println("Finished stage: " + stageCompleted.stageInfo)
    showDistribution("task runtime:", stageCompleted.stageInfo.getTaskRuntimeDistribution, "%4.0f")
    showDistribution("shuffle bytes written:", stageCompleted.stageInfo.getShuffleBytesWrittenDistribution, d => Utils.memoryBytesToString(d.toLong))
    showDistribution("fetch wait time:",stageCompleted.stageInfo.getRemoteFetchWaitTimeDistribution, "%4.0f")
    showDistribution("remote bytes read:", stageCompleted.stageInfo.getRemoteBytesReadDistribution, d => Utils.memoryBytesToString(d.toLong))
  }

  //for profiling, the extremes are more interesting
  val percentiles = Array[Int](0,5,10,25,50,75,90,95,100)
  val probabilities = percentiles.map{_ / 100.0}
  val percentilesHeader = "\t" + percentiles.mkString("%\t") + "%"

  def showDistribution(heading: String, dOpt: Option[Distribution], format:String) {
    def f(d:Double) = format.format(d)
    showDistribution(heading, dOpt, f _)
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
}