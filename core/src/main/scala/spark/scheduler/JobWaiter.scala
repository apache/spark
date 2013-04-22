package spark.scheduler

import scala.collection.mutable.ArrayBuffer

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 */
private[spark] class JobWaiter[T](totalTasks: Int, resultHandler: (Int, T) => Unit)
  extends JobListener {

  private var finishedTasks = 0

  private var jobFinished = false          // Is the job as a whole finished (succeeded or failed)?
  private var jobResult: JobResult = null  // If the job is finished, this will be its result

  override def taskSucceeded(index: Int, result: Any) {
    synchronized {
      if (jobFinished) {
        throw new UnsupportedOperationException("taskSucceeded() called on a finished JobWaiter")
      }
      resultHandler(index, result.asInstanceOf[T])
      finishedTasks += 1
      if (finishedTasks == totalTasks) {
        jobFinished = true
        jobResult = JobSucceeded
        this.notifyAll()
      }
    }
  }

  override def jobFailed(exception: Exception) {
    synchronized {
      if (jobFinished) {
        throw new UnsupportedOperationException("jobFailed() called on a finished JobWaiter")
      }
      jobFinished = true
      jobResult = JobFailed(exception)
      this.notifyAll()
    }
  }

  def awaitResult(): JobResult = synchronized {
    while (!jobFinished) {
      this.wait()
    }
    return jobResult
  }
}
