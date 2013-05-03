package spark.scheduler

import spark.scheduler.cluster.TaskInfo
import scala.collection.mutable.Map

import spark.TaskEndReason
import spark.executor.TaskMetrics

/**
 * Interface for getting events back from the TaskScheduler.
 */
private[spark] trait TaskSchedulerListener {
  // A task has finished or failed.
  def taskEnded(task: Task[_], reason: TaskEndReason, result: Any, accumUpdates: Map[Long, Any],
                taskInfo: TaskInfo, taskMetrics: TaskMetrics): Unit

  // A node was added to the cluster.
  def executorGained(execId: String, hostPort: String): Unit

  // A node was lost from the cluster.
  def executorLost(execId: String): Unit

  // The TaskScheduler wants to abort an entire task set.
  def taskSetFailed(taskSet: TaskSet, reason: String): Unit
}
