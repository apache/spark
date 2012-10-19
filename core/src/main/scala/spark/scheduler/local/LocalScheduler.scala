package spark.scheduler.local

import java.io.File
import java.net.URLClassLoader
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.HashMap

import spark._
import executor.ExecutorURLClassLoader
import spark.scheduler._

/**
 * A simple TaskScheduler implementation that runs tasks locally in a thread pool. Optionally
 * the scheduler also allows each task to fail up to maxFailures times, which is useful for
 * testing fault recovery.
 */
private[spark] class LocalScheduler(threads: Int, maxFailures: Int, sc: SparkContext)
  extends TaskScheduler
  with Logging {

  var attemptId = new AtomicInteger(0)
  var threadPool = Executors.newFixedThreadPool(threads, DaemonThreadFactory)
  val env = SparkEnv.get
  var listener: TaskSchedulerListener = null

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  val classLoader = new ExecutorURLClassLoader(Array(), Thread.currentThread.getContextClassLoader)
  
  // TODO: Need to take into account stage priority in scheduling

  override def start() { }

  override def setListener(listener: TaskSchedulerListener) { 
    this.listener = listener
  }

  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
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
        Accumulators.clear()
        Thread.currentThread().setContextClassLoader(classLoader)

        // Serialize and deserialize the task so that accumulators are changed to thread-local ones;
        // this adds a bit of unnecessary overhead but matches how the Mesos Executor works.
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val bytes = Task.serializeWithDependencies(task, sc.addedFiles, sc.addedJars, ser)
        logInfo("Size of task " + idInJob + " is " + bytes.limit + " bytes")
        val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(bytes)
        updateDependencies(taskFiles, taskJars)   // Download any files added with addFile
        val deserializedTask = ser.deserialize[Task[_]](
            taskBytes, Thread.currentThread.getContextClassLoader)

        // Run it
        val result: Any = deserializedTask.run(attemptId)

        // Serialize and deserialize the result to emulate what the Mesos
        // executor does. This is useful to catch serialization errors early
        // on in development (so when users move their local Spark programs
        // to the cluster, they don't get surprised by serialization errors).
        val resultToReturn = ser.deserialize[Any](ser.serialize(result))
        val accumUpdates = Accumulators.values
        logInfo("Finished task " + idInJob)
        listener.taskEnded(task, Success, resultToReturn, accumUpdates)
      } catch {
        case t: Throwable => {
          logError("Exception in task " + idInJob, t)
          failCount.synchronized {
            failCount(idInJob) += 1
            if (failCount(idInJob) <= maxFailures) {
              submitTask(task, idInJob)
            } else {
              // TODO: Do something nicer here to return all the way to the user
              listener.taskEnded(task, new ExceptionFailure(t), null, null)
            }
          }
        }
      }
    }

    for ((task, i) <- tasks.zipWithIndex) {
      submitTask(task, i)
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    // Fetch missing dependencies
    for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
      logInfo("Fetching " + name + " with timestamp " + timestamp)
      Utils.fetchFile(name, new File("."))
      currentFiles(name) = timestamp
    }
    for ((name, timestamp) <- newJars if currentJars.getOrElse(name, -1L) < timestamp) {
      logInfo("Fetching " + name + " with timestamp " + timestamp)
      Utils.fetchFile(name, new File("."))
      currentJars(name) = timestamp
      // Add it to our class loader
      val localName = name.split("/").last
      val url = new File(".", localName).toURI.toURL
      if (!classLoader.getURLs.contains(url)) {
        logInfo("Adding " + url + " to class loader")
        classLoader.addURL(url)
      }
    }
  }
  
  override def stop() {
    threadPool.shutdownNow()
  }

  override def defaultParallelism() = threads
}
