package spark

import java.io.{File, FileOutputStream}
import java.net.{URI, URL, URLClassLoader}
import java.util.concurrent._

import scala.collection.mutable.ArrayBuffer

import com.google.protobuf.ByteString

import org.apache.mesos._
import org.apache.mesos.Protos._

/**
 * The Mesos executor for Spark.
 */
class Executor extends org.apache.mesos.Executor with Logging {
  var classLoader: ClassLoader = null
  var threadPool: ExecutorService = null
  var env: SparkEnv = null

  override def init(d: ExecutorDriver, args: ExecutorArgs) {
    // Read spark.* system properties from executor arg
    val props = Utils.deserialize[Array[(String, String)]](args.getData.toByteArray)
    for ((key, value) <- props)
      System.setProperty(key, value)

    // Initialize Spark environment (using system properties read above)
    env = SparkEnv.createFromSystemProperties(false)
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
  
  override def launchTask(d: ExecutorDriver, task: TaskDescription) {
    threadPool.execute(new TaskRunner(task, d))
  }

  class TaskRunner(desc: TaskDescription, d: ExecutorDriver)
  extends Runnable {
    override def run() = {
      logInfo("Running task ID " + desc.getTaskId)
      try {
        SparkEnv.set(env)
        Thread.currentThread.setContextClassLoader(classLoader)
        Accumulators.clear
        val task = Utils.deserialize[Task[Any]](desc.getData.toByteArray, classLoader)
        for (gen <- task.generation) // Update generation if any is set
          env.mapOutputTracker.updateGeneration(gen)
        val value = task.run
        val accumUpdates = Accumulators.values
        val result = new TaskResult(value, accumUpdates)
        val status = TaskStatus.newBuilder()
                       .setTaskId(desc.getTaskId)
                       .setSlaveId(desc.getSlaveId)
                       .setState(TaskState.TASK_FINISHED)
                       .setData(ByteString.copyFrom(Utils.serialize(result)))
                       .build()
        d.sendStatusUpdate(status)
        logInfo("Finished task ID " + desc.getTaskId)
      } catch {
        case ffe: FetchFailedException => {
          val reason = ffe.toTaskEndReason
          val status = TaskStatus.newBuilder()
                         .setTaskId(desc.getTaskId)
                         .setSlaveId(desc.getSlaveId)
                         .setState(TaskState.TASK_FAILED)
                         .setData(ByteString.copyFrom(Utils.serialize(reason)))
                         .build()
          d.sendStatusUpdate(status)
        }
        case e: Exception => {
          // TODO: Handle errors in tasks less dramatically
          logError("Exception in task ID " + desc.getTaskId, e)
          System.exit(1)
        }
      }
    }
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
      loader = {
        try {
          val klass = Class.forName("spark.repl.ExecutorClassLoader").asInstanceOf[Class[_ <: ClassLoader]]
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

  override def error(d: ExecutorDriver, code: Int, message: String) {
    logError("Error from Mesos: %s (code %d)".format(message, code))
  }

  override def killTask(d: ExecutorDriver, tid: TaskID) {
    logWarning("Mesos asked us to kill task " + tid + "; ignoring (not yet implemented)")
  }

  override def shutdown(d: ExecutorDriver) {}

  override def frameworkMessage(d: ExecutorDriver, data: Array[Byte]) {}
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
