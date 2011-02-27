package spark

import java.util.concurrent._

/**
 * A simple Scheduler implementation that runs tasks locally in a thread pool.
 */
private class LocalScheduler(threads: Int) extends DAGScheduler with Logging {
  var threadPool: ExecutorService =
    Executors.newFixedThreadPool(threads, DaemonThreadFactory)
  
  override def start() {}
  
  override def waitForRegister() {}

  override def submitTasks(tasks: Seq[Task[_]]) {
    tasks.zipWithIndex.foreach { case (task, i) =>
      threadPool.submit(new Runnable {
        def run() {
          logInfo("Running task " + i)
          try {
            // Serialize and deserialize the task so that accumulators are
            // changed to thread-local ones; this adds a bit of unnecessary
            // overhead but matches how the Mesos Executor works
            Accumulators.clear
            val bytes = Utils.serialize(tasks(i))
            logInfo("Size of task " + i + " is " + bytes.size + " bytes")
            val task = Utils.deserialize[Task[_]](
              bytes, currentThread.getContextClassLoader)
            val result: Any = task.run
            val accumUpdates = Accumulators.values
            logInfo("Finished task " + i)
            taskEnded(task, true, result, accumUpdates)
          } catch {
            case e: Exception => {
              // TODO: Do something nicer here
              logError("Exception in task " + i, e)
              System.exit(1)
              null
            }
          }
        }
      })
    }
  }
  
  override def stop() {}

  override def numCores() = threads
}