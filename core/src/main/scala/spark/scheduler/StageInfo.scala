package spark.scheduler

import spark.scheduler.cluster.TaskInfo
import scala.collection._
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

  def getTaskResultSizeDistribution = {
    Distribution(taskMetrics.map{_.resultSize.toDouble})
  }

  lazy val runtimePercentages = taskMetrics.zip(taskInfos).map{
    case (metrics, info) => RuntimePercentage(info.duration, metrics)
  }

  /**
   * distribution of the percentage of task runtime of the executor itself, excluding time spent waiting on a fetch
   */
  def getExectuorRuntimePercentage = {
    Distribution(runtimePercentages.map{_.executorPct})
  }

  /**
   * distribution of the percentage of task runtime spent waiting on a fetch
   */
  def getFetchRuntimePercentage = {
    Distribution(runtimePercentages.flatMap{_.fetchPct})
  }

  /**
   * distribution of the percentage of task runtime spent not waiting on fetch, and not actively executing on
   * a remote machine (eg., serializing task, sending it over network, sending results back over network)
   */
  def getOtherRuntimePercentage = {
    Distribution(runtimePercentages.map{_.other})
  }
}

private[spark] case class RuntimePercentage(executorPct: Double, fetchPct: Option[Double], other: Double)
private[spark] object RuntimePercentage {
  def apply(totalTime: Long, metrics: TaskMetrics): RuntimePercentage = {
    val denom = totalTime.toDouble
    val fetch = metrics.remoteFetchWaitTime.map{_ / denom}
    val exec = (metrics.executorRunTime - metrics.remoteFetchWaitTime.getOrElse(0l)) / denom
    val other = 1.0 - (exec + fetch.getOrElse(0d))
    RuntimePercentage(exec, fetch, other)
  }
}
