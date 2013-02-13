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
}