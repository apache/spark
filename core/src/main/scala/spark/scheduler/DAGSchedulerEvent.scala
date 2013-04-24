package spark.scheduler

import spark.scheduler.cluster.TaskInfo
import scala.collection.mutable.Map

import spark._
import spark.executor.TaskMetrics

/**
 * Types of events that can be handled by the DAGScheduler. The DAGScheduler uses an event queue
 * architecture where any thread can post an event (e.g. a task finishing or a new job being
 * submitted) but there is a single "logic" thread that reads these events and takes decisions.
 * This greatly simplifies synchronization.
 */
private[spark] sealed trait DAGSchedulerEvent

private[spark] case class JobSubmitted(
    finalRDD: RDD[_],
    func: (TaskContext, Iterator[_]) => _,
    partitions: Array[Int],
    allowLocal: Boolean,
    callSite: String,
    listener: JobListener)
  extends DAGSchedulerEvent

private[spark] case class CompletionEvent(
    task: Task[_],
    reason: TaskEndReason,
    result: Any,
    accumUpdates: Map[Long, Any],
    taskInfo: TaskInfo,
    taskMetrics: TaskMetrics)
  extends DAGSchedulerEvent

private[spark] case class ExecutorGained(execId: String, hostPort: String) extends DAGSchedulerEvent {
  Utils.checkHostPort(hostPort, "Required hostport")
}

private[spark] case class ExecutorLost(execId: String) extends DAGSchedulerEvent

private[spark] case class TaskSetFailed(taskSet: TaskSet, reason: String) extends DAGSchedulerEvent

private[spark] case object StopDAGScheduler extends DAGSchedulerEvent
