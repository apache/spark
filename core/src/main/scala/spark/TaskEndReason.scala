package spark

import spark.storage.BlockManagerId

/**
 * Various possible reasons why a task ended. The low-level TaskScheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
 */
private[spark] sealed trait TaskEndReason

private[spark] case object Success extends TaskEndReason

private[spark] 
case object Resubmitted extends TaskEndReason // Task was finished earlier but we've now lost it

private[spark] case class FetchFailed(
    bmAddress: BlockManagerId,
    shuffleId: Int,
    mapId: Int,
    reduceId: Int)
  extends TaskEndReason

private[spark] case class ExceptionFailure(
    className: String,
    description: String,
    stackTrace: Array[StackTraceElement])
  extends TaskEndReason

private[spark] case class OtherFailure(message: String) extends TaskEndReason

private[spark] case class TaskResultTooBigFailure() extends TaskEndReason
