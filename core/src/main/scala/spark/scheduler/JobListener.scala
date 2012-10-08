package spark.scheduler

/**
 * Interface used to listen for job completion or failure events after submitting a job to the
 * DAGScheduler. The listener is notified each time a task succeeds, as well as if the whole
 * job fails (and no further taskSucceeded events will happen).
 */
private[spark] trait JobListener {
  def taskSucceeded(index: Int, result: Any)
  def jobFailed(exception: Exception)
}
