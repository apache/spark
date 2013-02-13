package spark.scheduler

import spark.scheduler.cluster.TaskInfo
import scala.collection._
import spark.executor.TaskMetrics

case class StageInfo(
    val stage: Stage,
    val taskInfos: mutable.Buffer[(TaskInfo, TaskMetrics)] = mutable.Buffer[(TaskInfo, TaskMetrics)]()
) {
  override def toString = stage.rdd.toString
}