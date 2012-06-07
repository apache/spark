package spark

import spark.storage.BlockManagerId

/**
 * Various possible reasons why a task ended. The low-level TaskScheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
 */
sealed trait TaskEndReason

case object Success extends TaskEndReason
case object Resubmitted extends TaskEndReason // Task was finished earlier but we've now lost it
case class FetchFailed(bmAddress: BlockManagerId, shuffleId: Int, mapId: Int, reduceId: Int) extends TaskEndReason
case class ExceptionFailure(exception: Throwable) extends TaskEndReason
case class OtherFailure(message: String) extends TaskEndReason
