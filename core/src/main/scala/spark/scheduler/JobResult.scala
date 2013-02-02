package spark.scheduler

/**
 * A result of a job in the DAGScheduler.
 */
private[spark] sealed trait JobResult

private[spark] case object JobSucceeded extends JobResult
private[spark] case class JobFailed(exception: Exception) extends JobResult
