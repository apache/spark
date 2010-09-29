package spark

import java.util.concurrent.{Executors, ExecutorService}

import mesos.{ExecutorArgs, ExecutorDriver, MesosExecutorDriver}
import mesos.{TaskDescription, TaskState, TaskStatus}

object Executor {
  def main(args: Array[String]) {
    System.loadLibrary("mesos")

    val exec = new mesos.Executor() {
      var classLoader: ClassLoader = null
      var threadPool: ExecutorService = null

      override def init(d: ExecutorDriver, args: ExecutorArgs) {
        // Read spark.* system properties
        val props = Utils.deserialize[Array[(String, String)]](args.getData)
        for ((key, value) <- props)
          System.setProperty(key, value)
        
        // Initialize broadcast system (uses some properties read above)
        Broadcast.initialize(false)
        
        // If the REPL is in use, create a ClassLoader that will be able to
        // read new classes defined by the REPL as the user types code
        classLoader = this.getClass.getClassLoader
        val classUri = System.getProperty("spark.repl.class.uri")
        if (classUri != null) {
          println("Using REPL class URI: " + classUri)
          classLoader = new repl.ExecutorClassLoader(classUri, classLoader)
        }
        Thread.currentThread.setContextClassLoader(classLoader)
        
        // Start worker thread pool (they will inherit our context ClassLoader)
        threadPool = Executors.newCachedThreadPool()
      }
      
      override def launchTask(d: ExecutorDriver, desc: TaskDescription) {
        // Pull taskId and arg out of TaskDescription because it won't be a
        // valid pointer after this method call (TODO: fix this in C++/SWIG)
        val taskId = desc.getTaskId
        val arg = desc.getArg
        threadPool.execute(new Runnable() {
          def run() = {
            println("Running task ID " + taskId)
            try {
              Accumulators.clear
              val task = Utils.deserialize[Task[Any]](arg, classLoader)
              val value = task.run
              val accumUpdates = Accumulators.values
              val result = new TaskResult(value, accumUpdates)
              d.sendStatusUpdate(new TaskStatus(
                taskId, TaskState.TASK_FINISHED, Utils.serialize(result)))
              println("Finished task ID " + taskId)
            } catch {
              case e: Exception => {
                // TODO: Handle errors in tasks less dramatically
                System.err.println("Exception in task ID " + taskId + ":")
                e.printStackTrace
                System.exit(1)
              }
            }
          }
        })
      }
    }

    new MesosExecutorDriver(exec).run()
  }
}
