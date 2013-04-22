package spark.partial

import spark._
import spark.scheduler.JobListener

/**
 * A JobListener for an approximate single-result action, such as count() or non-parallel reduce().
 * This listener waits up to timeout milliseconds and will return a partial answer even if the
 * complete answer is not available by then.
 *
 * This class assumes that the action is performed on an entire RDD[T] via a function that computes
 * a result of type U for each partition, and that the action returns a partial or complete result
 * of type R. Note that the type R must *include* any error bars on it (e.g. see BoundedInt).
 */
private[spark] class ApproximateActionListener[T, U, R](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    evaluator: ApproximateEvaluator[U, R],
    timeout: Long)
  extends JobListener {

  val startTime = System.currentTimeMillis()
  val totalTasks = rdd.partitions.size
  var finishedTasks = 0
  var failure: Option[Exception] = None             // Set if the job has failed (permanently)
  var resultObject: Option[PartialResult[R]] = None // Set if we've already returned a PartialResult

  override def taskSucceeded(index: Int, result: Any) {
    synchronized {
      evaluator.merge(index, result.asInstanceOf[U])
      finishedTasks += 1
      if (finishedTasks == totalTasks) {
        // If we had already returned a PartialResult, set its final value
        resultObject.foreach(r => r.setFinalValue(evaluator.currentResult()))
        // Notify any waiting thread that may have called awaitResult
        this.notifyAll()
      }
    }
  }

  override def jobFailed(exception: Exception) {
    synchronized {
      failure = Some(exception)
      this.notifyAll()
    }
  }

  /**
   * Waits for up to timeout milliseconds since the listener was created and then returns a
   * PartialResult with the result so far. This may be complete if the whole job is done.
   */
  def awaitResult(): PartialResult[R] = synchronized {
    val finishTime = startTime + timeout
    while (true) {
      val time = System.currentTimeMillis()
      if (failure != None) {
        throw failure.get
      } else if (finishedTasks == totalTasks) {
        return new PartialResult(evaluator.currentResult(), true)
      } else if (time >= finishTime) {
        resultObject = Some(new PartialResult(evaluator.currentResult(), false))
        return resultObject.get
      } else {
        this.wait(finishTime - time)
      }
    }
    // Should never be reached, but required to keep the compiler happy
    return null
  }
}
