package spark.scheduler

import cluster.TaskInfo
import collection._
import spark.util.Distribution

case class StageInfo(val stage: Stage, val taskInfos: mutable.Buffer[TaskInfo] = mutable.Buffer[TaskInfo]()) {
  def getTaskRuntimeDistribution = {
    new Distribution(taskInfos.map{_.duration.toDouble})
  }
}
