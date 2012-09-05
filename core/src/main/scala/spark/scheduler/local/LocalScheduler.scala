package spark.scheduler.local

import java.io.File
import java.net.URLClassLoader
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.HashMap

import spark._
import spark.scheduler._

/**
 * A simple TaskScheduler implementation that runs tasks locally in a thread pool. Optionally
 * the scheduler also allows each task to fail up to maxFailures times, which is useful for
 * testing fault recovery.
 */
class LocalScheduler(threads: Int, maxFailures: Int, sc: SparkContext) extends TaskScheduler with Logging {
  var attemptId = new AtomicInteger(0)
  var threadPool = Executors.newFixedThreadPool(threads, DaemonThreadFactory)
  val env = SparkEnv.get
  var listener: TaskSchedulerListener = null
  val fileSet: HashMap[String, Long] = new HashMap[String, Long]()
  val jarSet: HashMap[String, Long] = new HashMap[String, Long]()
  
  // TODO: Need to take into account stage priority in scheduling

  override def start() { }

  override def setListener(listener: TaskSchedulerListener) { 
    this.listener = listener
  }

  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    val failCount = new Array[Int](tasks.size)

    def submitTask(task: Task[_], idInJob: Int) {
      task.fileSet ++= sc.addedFiles
      task.jarSet ++= sc.addedJars
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
      task.downloadDependencies(fileSet, jarSet)
      // Create a new classLaoder for the downloaded JARs
      Thread.currentThread.setContextClassLoader(createClassLoader())
      try {
        // Serialize and deserialize the task so that accumulators are changed to thread-local ones;
        // this adds a bit of unnecessary overhead but matches how the Mesos Executor works.
        Accumulators.clear
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val bytes = ser.serialize(task)
        logInfo("Size of task " + idInJob + " is " + bytes.limit + " bytes")
        val deserializedTask = ser.deserialize[Task[_]](
            bytes, Thread.currentThread.getContextClassLoader)
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
  
  
  override def stop() {
    threadPool.shutdownNow()
  }

  private def createClassLoader() : ClassLoader = {
    val currentLoader = Thread.currentThread.getContextClassLoader()
    val urls = jarSet.keySet.map { uri => 
      new File(uri.split("/").last).toURI.toURL
    }.toArray
    logInfo("Creating ClassLoader with jars: " + urls.mkString)
    return new URLClassLoader(urls, currentLoader)
  }

  override def defaultParallelism() = threads
}
