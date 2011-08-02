package spark

import java.util.concurrent._

/**
 * A simple Scheduler implementation that runs tasks locally in a thread pool.
 */
private class LocalScheduler(threads: Int) extends DAGScheduler with Logging {
  var attemptId = 0
  var threadPool: ExecutorService =
    Executors.newFixedThreadPool(threads, DaemonThreadFactory)

  val env = SparkEnv.get
  
  override def start() {}
  
  override def waitForRegister() {}

  override def submitTasks(tasks: Seq[Task[_]]) {
    tasks.zipWithIndex.foreach { case (task, i) =>      
      val myAttemptId = attemptId
      attemptId = attemptId + 1
      threadPool.submit(new Runnable {
        def run() {
          logInfo("Running task " + i)
          // Set the Spark execution environment for the worker thread
          SparkEnv.set(env)
          try {
            // Serialize and deserialize the task so that accumulators are
            // changed to thread-local ones; this adds a bit of unnecessary
            // overhead but matches how the Mesos Executor works
            Accumulators.clear
            val bytes = Utils.serialize(tasks(i))
            logInfo("Size of task " + i + " is " + bytes.size + " bytes")
            val deserializedTask = Utils.deserialize[Task[_]](
              bytes, Thread.currentThread.getContextClassLoader)
            val result: Any = deserializedTask.run(myAttemptId)
            val accumUpdates = Accumulators.values
            logInfo("Finished task " + i)
            taskEnded(tasks(i), Success, result, accumUpdates)
          } catch {
            case t: Throwable => {
              // TODO: Do something nicer here
              logError("Exception in task " + i, t)
              System.exit(1)
              null
            }
          }
        }
      })
    }
  }
  
  override def stop() {}

  override def defaultParallelism() = threads
}
