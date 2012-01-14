package spark

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * A simple Scheduler implementation that runs tasks locally in a thread pool.
 * Optionally the scheduler also allows each task to fail up to maxFailures times,
 * which is useful for testing fault recovery.
 */
private class LocalScheduler(threads: Int, maxFailures: Int) extends DAGScheduler with Logging {
  var attemptId = new AtomicInteger(0)
  var threadPool = Executors.newFixedThreadPool(threads, DaemonThreadFactory)

  val env = SparkEnv.get
  
  override def start() {}
  
  override def waitForRegister() {}

  override def submitTasks(tasks: Seq[Task[_]]) {
    val failCount = new Array[Int](tasks.size)

    def submitTask(task: Task[_], idInJob: Int) {
      val myAttemptId = attemptId.getAndIncrement()
      threadPool.submit(new Runnable {
        def run() {
          runTask(task, idInJob, myAttemptId)
        }
      })
    }

    def runTask(task: Task[_], idInJob: Int, attemptId: Int) {
      logInfo("Running task " + idInJob)
      // Set the Spark execution environment for the worker thread
      SparkEnv.set(env)
      try {
        // Serialize and deserialize the task so that accumulators are
        // changed to thread-local ones; this adds a bit of unnecessary
        // overhead but matches how the Mesos Executor works
        Accumulators.clear
        val bytes = Utils.serialize(task)
        logInfo("Size of task " + idInJob + " is " + bytes.size + " bytes")
        val deserializedTask = Utils.deserialize[Task[_]](
          bytes, Thread.currentThread.getContextClassLoader)
        val result: Any = deserializedTask.run(attemptId)
        val accumUpdates = Accumulators.values
        logInfo("Finished task " + idInJob)
        taskEnded(task, Success, result, accumUpdates)
      } catch {
        case t: Throwable => {
          logError("Exception in task " + idInJob, t)
          failCount.synchronized {
            failCount(idInJob) += 1
            if (failCount(idInJob) <= maxFailures) {
              submitTask(task, idInJob)
            } else {
              // TODO: Do something nicer here to return all the way to the user
              System.exit(1)
            }
          }
        }
      }
    }

    for ((task, i) <- tasks.zipWithIndex) {
      submitTask(task, i)
    }
  }
  
  override def stop() {}

  override def defaultParallelism() = threads
}
