package spark.scheduler

import scala.collection.mutable.ArrayBuffer

/**
 * An object that waits for a DAGScheduler job to complete.
 */
private[spark] class JobWaiter(totalTasks: Int) extends JobListener {
  private val taskResults = ArrayBuffer.fill[Any](totalTasks)(null)
  private var finishedTasks = 0

  private var jobFinished = false          // Is the job as a whole finished (succeeded or failed)?
  private var jobResult: JobResult = null  // If the job is finished, this will be its result

  override def taskSucceeded(index: Int, result: Any) {
    synchronized {
      if (jobFinished) {
        throw new UnsupportedOperationException("taskSucceeded() called on a finished JobWaiter")
      }
      taskResults(index) = result
      finishedTasks += 1
      if (finishedTasks == totalTasks) {
        jobFinished = true
        jobResult = JobSucceeded(taskResults)
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

  def getResult(): JobResult = synchronized {
    while (!jobFinished) {
      this.wait()
    }
    return jobResult
  }
}
