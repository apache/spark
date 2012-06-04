package spark

import java.io.{File, FileOutputStream}
import java.net.{URI, URL, URLClassLoader}
import java.util.concurrent._

import scala.actors.remote.RemoteActor
import scala.collection.mutable.ArrayBuffer

import com.google.protobuf.ByteString

import org.apache.mesos._
import org.apache.mesos.Protos._

import spark.broadcast._

/**
 * The Mesos executor for Spark.
 */
class Executor extends org.apache.mesos.Executor with Logging {
  var classLoader: ClassLoader = null
  var threadPool: ExecutorService = null
  var env: SparkEnv = null

  initLogging()

  override def registered(
      driver: ExecutorDriver,
      executorInfo: ExecutorInfo,
      frameworkInfo: FrameworkInfo,
      slaveInfo: SlaveInfo) {
    // Read spark.* system properties from executor arg
    val props = Utils.deserialize[Array[(String, String)]](executorInfo.getData.toByteArray)
    for ((key, value) <- props) {
      System.setProperty(key, value)
    }

    // Make sure an appropriate class loader is set for remote actors
    RemoteActor.classLoader = getClass.getClassLoader

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

  override def disconnected(d: ExecutorDriver) {}

  override def reregistered(d: ExecutorDriver, s: SlaveInfo) {}
  
  override def launchTask(d: ExecutorDriver, task: TaskInfo) {
    threadPool.execute(new TaskRunner(task, d))
  }

  class TaskRunner(info: TaskInfo, d: ExecutorDriver)
  extends Runnable {
    override def run() = {
      val tid = info.getTaskId.getValue
      SparkEnv.set(env)
      Thread.currentThread.setContextClassLoader(classLoader)
      val ser = SparkEnv.get.closureSerializer.newInstance()
      logInfo("Running task ID " + tid)
      d.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(info.getTaskId)
          .setState(TaskState.TASK_RUNNING)
          .build())
      try {
        Accumulators.clear
        val task = ser.deserialize[Task[Any]](info.getData.toByteArray, classLoader)
        for (gen <- task.generation) {// Update generation if any is set
          env.mapOutputTracker.updateGeneration(gen)
        }
        val value = task.run(tid.toInt)
        val accumUpdates = Accumulators.values
        val result = new TaskResult(value, accumUpdates)
        d.sendStatusUpdate(TaskStatus.newBuilder()
            .setTaskId(info.getTaskId)
            .setState(TaskState.TASK_FINISHED)
            .setData(ByteString.copyFrom(ser.serialize(result)))
            .build())
        logInfo("Finished task ID " + tid)
      } catch {
        case ffe: FetchFailedException => {
          val reason = ffe.toTaskEndReason
          d.sendStatusUpdate(TaskStatus.newBuilder()
              .setTaskId(info.getTaskId)
              .setState(TaskState.TASK_FAILED)
              .setData(ByteString.copyFrom(ser.serialize(reason)))
              .build())
        }
        case t: Throwable => {
          val reason = ExceptionFailure(t)
          d.sendStatusUpdate(TaskStatus.newBuilder()
              .setTaskId(info.getTaskId)
              .setState(TaskState.TASK_FAILED)
              .setData(ByteString.copyFrom(ser.serialize(reason)))
              .build())

          // TODO: Handle errors in tasks less dramatically
          logError("Exception in task ID " + tid, t)
          System.exit(1)
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

  override def error(d: ExecutorDriver, message: String) {
    logError("Error from Mesos: " + message)
  }

  override def killTask(d: ExecutorDriver, t: TaskID) {
    logWarning("Mesos asked us to kill task " + t.getValue + "; ignoring (not yet implemented)")
  }

  override def shutdown(d: ExecutorDriver) {}

  override def frameworkMessage(d: ExecutorDriver, data: Array[Byte]) {}
}

/**
 * Executor entry point.
 */
object Executor extends Logging {
  def main(args: Array[String]) {
    MesosNativeLibrary.load()
    // Create a new Executor and start it running
    val exec = new Executor
    new MesosExecutorDriver(exec).run()
  }
}
