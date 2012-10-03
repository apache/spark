package spark.scheduler

import scala.collection.mutable.Map

import spark._

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
    accumUpdates: Map[Long, Any])
  extends DAGSchedulerEvent

private[spark] case class HostLost(host: String) extends DAGSchedulerEvent

private[spark] case class TaskSetFailed(taskSet: TaskSet, reason: String) extends DAGSchedulerEvent

private[spark] case object StopDAGScheduler extends DAGSchedulerEvent
