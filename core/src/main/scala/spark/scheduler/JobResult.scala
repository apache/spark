package spark.scheduler

/**
 * A result of a job in the DAGScheduler.
 */
sealed trait JobResult

case class JobSucceeded(results: Seq[_]) extends JobResult
case class JobFailed(exception: Exception) extends JobResult
