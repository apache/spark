package spark

import java.util.concurrent._

import scala.collection.mutable.Map

// A simple Scheduler implementation that runs tasks locally in a thread pool.
private class LocalScheduler(threads: Int) extends Scheduler {
  var threadPool: ExecutorService =
    Executors.newFixedThreadPool(threads, DaemonThreadFactory)
  
  override def start() {}
  
  override def waitForRegister() {}
  
  override def runTasks[T](tasks: Array[Task[T]])(implicit m: ClassManifest[T])
      : Array[T] = {
    val futures = new Array[Future[TaskResult[T]]](tasks.length)
    
    for (i <- 0 until tasks.length) {
      futures(i) = threadPool.submit(new Callable[TaskResult[T]]() {
        def call(): TaskResult[T] = {
          println("Running task " + i)
          try {
            // Serialize and deserialize the task so that accumulators are
            // changed to thread-local ones; this adds a bit of unnecessary
            // overhead but matches how the Nexus Executor works
            Accumulators.clear
            val bytes = Utils.serialize(tasks(i))
            println("Size of task " + i + " is " + bytes.size + " bytes")
            val task = Utils.deserialize[Task[T]](
              bytes, currentThread.getContextClassLoader)
            val value = task.run
            val accumUpdates = Accumulators.values
            println("Finished task " + i)
            new TaskResult[T](value, accumUpdates)
          } catch {
            case e: Exception => {
              // TODO: Do something nicer here
              System.err.println("Exception in task " + i + ":")
              e.printStackTrace()
              System.exit(1)
              null
            }
          }
        }
      })
    }
    
    val taskResults = futures.map(_.get)
    for (result <- taskResults)
      Accumulators.add(currentThread, result.accumUpdates)
    return taskResults.map(_.value).toArray(m)
  }
  
  override def stop() {}

  override def numCores() = threads
}

// A ThreadFactory that creates daemon threads
private object DaemonThreadFactory extends ThreadFactory {
  override def newThread(r: Runnable): Thread = {
    val t = new Thread(r);
    t.setDaemon(true)
    return t
  }
}
