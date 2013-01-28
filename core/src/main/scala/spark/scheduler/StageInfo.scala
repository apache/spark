package spark.scheduler

import cluster.TaskInfo
import collection._

case class StageInfo(val stage: Stage, val taskInfos: mutable.Buffer[TaskInfo] = mutable.Buffer[TaskInfo]())
