package spark.executor

import java.io.{File, FileOutputStream}
import java.net.{URL, URLClassLoader}
import java.util.concurrent._

import scala.collection.mutable.ArrayBuffer

import spark.broadcast._
import spark.scheduler._
import spark._
import java.nio.ByteBuffer

/**
 * The Mesos executor for Spark.
 */
class Executor extends Logging {
  var classLoader: ClassLoader = null
  var threadPool: ExecutorService = null
  var env: SparkEnv = null

  val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  initLogging()

  def initialize(slaveHostname: String, properties: Seq[(String, String)]) {
    // Make sure the local hostname we report matches the cluster scheduler's name for this host
    Utils.setCustomHostname(slaveHostname)

    // Set spark.* system properties from executor arg
    for ((key, value) <- properties) {
      System.setProperty(key, value)
    }

    // Initialize Spark environment (using system properties read above)
    env = SparkEnv.createFromSystemProperties(slaveHostname, 0, false, false)
    SparkEnv.set(env)
    // Old stuff that isn't yet using env
    Broadcast.initialize(false)

    // Create our ClassLoader (using spark properties) and set it on this thread
    classLoader = createClassLoader()
    Thread.currentThread.setContextClassLoader(classLoader)

    // Start worker thread pool
    threadPool = new ThreadPoolExecutor(
      1, 128, 600, TimeUnit.SECONDS, new SynchronousQueue[Runnable])
  }

  def launchTask(context: ExecutorContext, taskId: Long, serializedTask: ByteBuffer) {
    threadPool.execute(new TaskRunner(context, taskId, serializedTask))
  }

  class TaskRunner(context: ExecutorContext, taskId: Long, serializedTask: ByteBuffer)
    extends Runnable {

    override def run() {
      SparkEnv.set(env)
      Thread.currentThread.setContextClassLoader(classLoader)
      val ser = SparkEnv.get.closureSerializer.newInstance()
      logInfo("Running task ID " + taskId)
      context.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      try {
        SparkEnv.set(env)
        Thread.currentThread.setContextClassLoader(classLoader)
        Accumulators.clear()
        val task = ser.deserialize[Task[Any]](serializedTask, classLoader)
        env.mapOutputTracker.updateGeneration(task.generation)
        val value = task.run(taskId.toInt)
        val accumUpdates = Accumulators.values
        val result = new TaskResult(value, accumUpdates)
        context.statusUpdate(taskId, TaskState.FINISHED, ser.serialize(result))
        logInfo("Finished task ID " + taskId)
      } catch {
        case ffe: FetchFailedException => {
          val reason = ffe.toTaskEndReason
          context.statusUpdate(taskId, TaskState.FINISHED, ser.serialize(reason))
        }

        case t: Throwable => {
          val reason = ExceptionFailure(t)
          context.statusUpdate(taskId, TaskState.FINISHED, ser.serialize(reason))

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
  private def createClassLoader(): ClassLoader = {
    var loader = this.getClass.getClassLoader

    // If any JAR URIs are given through spark.jar.uris, fetch them to the
    // current directory and put them all on the classpath. We assume that
    // each URL has a unique file name so that no local filenames will clash
    // in this process. This is guaranteed by ClusterScheduler.
    val uris = System.getProperty("spark.jar.uris", "")
    val localFiles = ArrayBuffer[String]()
    for (uri <- uris.split(",").filter(_.size > 0)) {
      val url = new URL(uri)
      val filename = url.getPath.split("/").last
      downloadFile(url, filename)
      localFiles += filename
    }
    if (localFiles.size > 0) {
      val urls = localFiles.map(f => new File(f).toURI.toURL).toArray
      loader = new URLClassLoader(urls, loader)
    }

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

    return loader
  }

  // Download a file from a given URL to the local filesystem
  private def downloadFile(url: URL, localPath: String) {
    val in = url.openStream()
    val out = new FileOutputStream(localPath)
    Utils.copyStream(in, out, true)
  }
}
