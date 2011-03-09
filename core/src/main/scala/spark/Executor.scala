package spark

import java.io.{File, FileOutputStream}
import java.net.{URI, URL, URLClassLoader}
import java.util.concurrent._

import scala.collection.mutable.ArrayBuffer

import mesos.{ExecutorArgs, ExecutorDriver, MesosExecutorDriver}
import mesos.{TaskDescription, TaskState, TaskStatus}

/**
 * The Mesos executor for Spark.
 */
class Executor extends mesos.Executor with Logging {
  var classLoader: ClassLoader = null
  var threadPool: ExecutorService = null

  override def init(d: ExecutorDriver, args: ExecutorArgs) {
    // Read spark.* system properties from executor arg
    val props = Utils.deserialize[Array[(String, String)]](args.getData)
    for ((key, value) <- props)
      System.setProperty(key, value)

    // Initialize cache and broadcast system (uses some properties read above)
    Cache.initialize()
    Serializer.initialize()
    Broadcast.initialize(false)
    
    // Create our ClassLoader (using spark properties) and set it on this thread
    classLoader = createClassLoader()
    Thread.currentThread.setContextClassLoader(classLoader)
    
    // Start worker thread pool (they will inherit our context ClassLoader)
    threadPool = new ThreadPoolExecutor(1, 128, 600, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable])
  }
  
  override def launchTask(d: ExecutorDriver, desc: TaskDescription) {
    // Pull taskId and arg out of TaskDescription because it won't be a
    // valid pointer after this method call (TODO: fix this in C++/SWIG)
    val taskId = desc.getTaskId
    val arg = desc.getArg
    threadPool.execute(new Runnable() {
      def run() = {
        logInfo("Running task ID " + taskId)
        try {
          Accumulators.clear
          val task = Utils.deserialize[Task[Any]](arg, classLoader)
          val value = task.run
          val accumUpdates = Accumulators.values
          val result = new TaskResult(value, accumUpdates)
          d.sendStatusUpdate(new TaskStatus(
            taskId, TaskState.TASK_FINISHED, Utils.serialize(result)))
          logInfo("Finished task ID " + taskId)
        } catch {
          case e: Exception => {
            // TODO: Handle errors in tasks less dramatically
            logError("Exception in task ID " + taskId, e)
            System.exit(1)
          }
        }
      }
    })
  }

  // Create a ClassLoader for use in tasks, adding any JARs specified by the
  // user or any classes created by the interpreter to the search path
  private def createClassLoader(): ClassLoader = {
    var loader = this.getClass.getClassLoader

    // If any JAR URIs are given through spark.jar.uris, fetch them to the
    // current directory and put them all on the classpath. We assume that
    // each URL has a unique file name so that no local filenames will clash
    // in this process. This is guaranteed by MesosScheduler.
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
      loader = new repl.ExecutorClassLoader(classUri, loader)
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

/**
 * Executor entry point.
 */
object Executor extends Logging {
  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    // Create a new Executor and start it running
    val exec = new Executor
    new MesosExecutorDriver(exec).run()
  }
}
