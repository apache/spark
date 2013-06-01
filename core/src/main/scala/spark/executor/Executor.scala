package spark.executor

import java.io.{File, FileOutputStream}
import java.net.{URI, URL, URLClassLoader}
import java.util.concurrent._

import org.apache.hadoop.fs.FileUtil

import scala.collection.mutable.{ArrayBuffer, Map, HashMap}

import spark.broadcast._
import spark.scheduler._
import spark._
import java.nio.ByteBuffer

/**
 * The Mesos executor for Spark.
 */
private[spark] class Executor(executorId: String, slaveHostname: String, properties: Seq[(String, String)]) extends Logging {

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  initLogging()

  // No ip or host:port - just hostname
  Utils.checkHost(slaveHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(slaveHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(slaveHostname)

  // Set spark.* system properties from executor arg
  for ((key, value) <- properties) {
    System.setProperty(key, value)
  }

  // Create our ClassLoader and set it on this thread
  private val urlClassLoader = createClassLoader()
  Thread.currentThread.setContextClassLoader(urlClassLoader)

  // Make any thread terminations due to uncaught exceptions kill the entire
  // executor process to avoid surprising stalls.
  Thread.setDefaultUncaughtExceptionHandler(
    new Thread.UncaughtExceptionHandler {
      override def uncaughtException(thread: Thread, exception: Throwable) {
        try {
          logError("Uncaught exception in thread " + thread, exception)
          
          // We may have been called from a shutdown hook. If so, we must not call System.exit().
          // (If we do, we will deadlock.)
          if (!Utils.inShutdown()) {
            if (exception.isInstanceOf[OutOfMemoryError]) {
              System.exit(ExecutorExitCode.OOM)
            } else {
              System.exit(ExecutorExitCode.UNCAUGHT_EXCEPTION)
            }
          }
        } catch {
          case oom: OutOfMemoryError => Runtime.getRuntime.halt(ExecutorExitCode.OOM)
          case t: Throwable => Runtime.getRuntime.halt(ExecutorExitCode.UNCAUGHT_EXCEPTION_TWICE)
        }
      }
    }
  )

  // Initialize Spark environment (using system properties read above)
  val env = SparkEnv.createFromSystemProperties(executorId, slaveHostname, 0, false, false)
  SparkEnv.set(env)
  private val akkaFrameSize = env.actorSystem.settings.config.getBytes("akka.remote.netty.message-frame-size")

  // Start worker thread pool
  val threadPool = new ThreadPoolExecutor(
    1, 128, 600, TimeUnit.SECONDS, new SynchronousQueue[Runnable])

  def launchTask(context: ExecutorBackend, taskId: Long, serializedTask: ByteBuffer) {
    threadPool.execute(new TaskRunner(context, taskId, serializedTask))
  }

  class TaskRunner(context: ExecutorBackend, taskId: Long, serializedTask: ByteBuffer)
    extends Runnable {

    override def run() {
      val startTime = System.currentTimeMillis()
      SparkEnv.set(env)
      Thread.currentThread.setContextClassLoader(urlClassLoader)
      val ser = SparkEnv.get.closureSerializer.newInstance()
      logInfo("Running task ID " + taskId)
      context.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      try {
        SparkEnv.set(env)
        Accumulators.clear()
        val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
        updateDependencies(taskFiles, taskJars)
        val task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
        logInfo("Its generation is " + task.generation)
        env.mapOutputTracker.updateGeneration(task.generation)
        val taskStart = System.currentTimeMillis()
        val value = task.run(taskId.toInt)
        val taskFinish = System.currentTimeMillis()
        task.metrics.foreach{ m =>
          m.executorDeserializeTime = (taskStart - startTime).toInt
          m.executorRunTime = (taskFinish - taskStart).toInt
        }
        //TODO I'd also like to track the time it takes to serialize the task results, but that is huge headache, b/c
        // we need to serialize the task metrics first.  If TaskMetrics had a custom serialized format, we could
        // just change the relevants bytes in the byte buffer
        val accumUpdates = Accumulators.values
        val result = new TaskResult(value, accumUpdates, task.metrics.getOrElse(null))
        val serializedResult = ser.serialize(result)
        logInfo("Serialized size of result for " + taskId + " is " + serializedResult.limit)
        if (serializedResult.limit >= (akkaFrameSize - 1024)) {
          context.statusUpdate(taskId, TaskState.FAILED, ser.serialize(TaskResultTooBigFailure()))
          return
        }
        context.statusUpdate(taskId, TaskState.FINISHED, serializedResult)
        logInfo("Finished task ID " + taskId)
      } catch {
        case ffe: FetchFailedException => {
          val reason = ffe.toTaskEndReason
          context.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))
        }

        case t: Throwable => {
          val reason = ExceptionFailure(t.getClass.getName, t.toString, t.getStackTrace)
          context.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

          // TODO: Should we exit the whole executor here? On the one hand, the failed task may
          // have left some weird state around depending on when the exception was thrown, but on
          // the other hand, maybe we could detect that when future tasks fail and exit then.
          logError("Exception in task ID " + taskId, t)
          //System.exit(1)
        }
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): ExecutorURLClassLoader = {
    var loader = this.getClass.getClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }.toArray
    loader = new URLClassLoader(urls, loader)

    // If the REPL is in use, add another ClassLoader that will read
    // new classes defined by the REPL as the user types code
    val classUri = System.getProperty("spark.repl.class.uri")
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      loader = {
        try {
          val klass = Class.forName("spark.repl.ExecutorClassLoader")
            .asInstanceOf[Class[_ <: ClassLoader]]
          val constructor = klass.getConstructor(classOf[String], classOf[ClassLoader])
          constructor.newInstance(classUri, loader)
        } catch {
          case _: ClassNotFoundException => loader
        }
      }
    }

    return new ExecutorURLClassLoader(Array(), loader)
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    synchronized {
      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory))
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newJars if currentJars.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory))
        currentJars(name) = timestamp
        // Add it to our class loader
        val localName = name.split("/").last
        val url = new File(SparkFiles.getRootDirectory, localName).toURI.toURL
        if (!urlClassLoader.getURLs.contains(url)) {
          logInfo("Adding " + url + " to class loader")
          urlClassLoader.addURL(url)
        }
      }
    }
  }
}
