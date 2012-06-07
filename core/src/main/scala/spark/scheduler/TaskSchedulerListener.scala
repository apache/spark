package spark.scheduler

import scala.collection.mutable.Map

import spark.TaskEndReason

/**
 * Interface for getting events back from the TaskScheduler.
 */
trait TaskSchedulerListener {
  // A task has finished or failed.
  def taskEnded(task: Task[_], reason: TaskEndReason, result: Any, accumUpdates: Map[Long, Any]): Unit

  // A node was lost from the cluster.
  def hostLost(host: String): Unit
}
