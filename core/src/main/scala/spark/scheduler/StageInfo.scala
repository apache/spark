package spark.scheduler

import cluster.TaskInfo
import collection._
import spark.util.Distribution
import spark.executor.TaskMetrics

case class StageInfo(
    val stage: Stage,
    val taskInfos: mutable.Buffer[TaskInfo] = mutable.Buffer[TaskInfo](),
    val taskMetrics: mutable.Buffer[TaskMetrics] = mutable.Buffer[TaskMetrics]()
) {

  override def toString = stage.rdd.toString

  def getTaskRuntimeDistribution = {
    Distribution(taskInfos.map{_.duration.toDouble})
  }

  def getShuffleBytesWrittenDistribution = {
    Distribution(taskMetrics.flatMap{_.shuffleBytesWritten.map{_.toDouble}})
  }

  def getRemoteFetchWaitTimeDistribution = {
    Distribution(taskMetrics.flatMap{_.remoteFetchWaitTime.map{_.toDouble}})
  }

  def getRemoteBytesReadDistribution = {
    Distribution(taskMetrics.flatMap{_.remoteBytesRead.map{_.toDouble}})
  }
}
