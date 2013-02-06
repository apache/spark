package spark.executor

import spark.scheduler.Task

case class TaskMetrics(
  val totalBlocksFetched : Option[Int],
  val remoteBlocksFetched: Option[Int],
  val localBlocksFetched: Option[Int],
  val remoteFetchWaitTime: Option[Long],
  val remoteBytesRead: Option[Long]
)

object TaskMetrics {
  private[spark] def apply(task: Task[_]) : TaskMetrics =
    TaskMetrics(None, None, None, task.remoteFetchWaitTime, task.remoteReadBytes)
}