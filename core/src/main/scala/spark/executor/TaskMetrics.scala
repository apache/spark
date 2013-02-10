package spark.executor

import spark.scheduler.Task

case class TaskMetrics(
  val totalBlocksFetched : Option[Int],
  val remoteBlocksFetched: Option[Int],
  val localBlocksFetched: Option[Int],
  val remoteFetchWaitTime: Option[Long],
  val remoteBytesRead: Option[Long],
  val shuffleBytesWritten: Option[Long]
)

object TaskMetrics {
  private[spark] def apply(task: Task[_]) : TaskMetrics = {
    TaskMetrics(task.totalBlocksFetched, task.remoteBlocksFetched, task.localBlocksFetched,
      task.remoteFetchWaitTime, task.remoteReadBytes, task.shuffleBytesWritten)
  }
}