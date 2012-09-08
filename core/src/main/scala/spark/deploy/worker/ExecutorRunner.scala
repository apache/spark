package spark.deploy.worker

import java.io._
import spark.deploy.{ExecutorState, ExecutorStateChanged, JobDescription}
import akka.actor.ActorRef
import spark.{Utils, Logging}
import java.net.{URI, URL}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import scala.Some
import spark.deploy.ExecutorStateChanged

/**
 * Manages the execution of one executor process.
 */
class ExecutorRunner(
    val jobId: String,
    val execId: Int,
    val jobDesc: JobDescription,
    val cores: Int,
    val memory: Int,
    val worker: ActorRef,
    val workerId: String,
    val hostname: String,
    val sparkHome: File,
    val workDir: File)
  extends Logging {

  val fullId = jobId + "/" + execId
  var workerThread: Thread = null
  var process: Process = null
  var shutdownHook: Thread = null

  def start() {
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      override def run() { fetchAndRunExecutor() }
    }
    workerThread.start()

    // Shutdown hook that kills actors on shutdown.
    shutdownHook = new Thread() { 
      override def run() {
        if (process != null) {
          logInfo("Shutdown hook killing child process.")
          process.destroy()
          process.waitFor()
        }
      }
    }
    Runtime.getRuntime.addShutdownHook(shutdownHook)
  }

  /** Stop this executor runner, including killing the process it launched */
  def kill() {
    if (workerThread != null) {
      workerThread.interrupt()
      workerThread = null
      if (process != null) {
        logInfo("Killing process!")
        process.destroy()
        process.waitFor()
      }
      worker ! ExecutorStateChanged(jobId, execId, ExecutorState.KILLED, None)
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
    }
  }

  /**
   * Download a file requested by the executor. Supports fetching the file in a variety of ways,
   * including HTTP, HDFS and files on a standard filesystem, based on the URL parameter.
   */
  def fetchFile(url: String, targetDir: File) {
    val filename = url.split("/").last
    val targetFile = new File(targetDir, filename)
    if (url.startsWith("http://") || url.startsWith("https://") || url.startsWith("ftp://")) {
      // Use the java.net library to fetch it
      logInfo("Fetching " + url + " to " + targetFile)
      val in = new URL(url).openStream()
      val out = new FileOutputStream(targetFile)
      Utils.copyStream(in, out, true)
    } else {
      // Use the Hadoop filesystem library, which supports file://, hdfs://, s3://, and others
      val uri = new URI(url)
      val conf = new Configuration()
      val fs = FileSystem.get(uri, conf)
      val in = fs.open(new Path(uri))
      val out = new FileOutputStream(targetFile)
      Utils.copyStream(in, out, true)
    }
    // Decompress the file if it's a .tar or .tar.gz
    if (filename.endsWith(".tar.gz") || filename.endsWith(".tgz")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xzf", filename), targetDir)
    } else if (filename.endsWith(".tar")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xf", filename), targetDir)
    }
  }

  /** Replace variables such as {{SLAVEID}} and {{CORES}} in a command argument passed to us */
  def substituteVariables(argument: String): String = argument match {
    case "{{SLAVEID}}" => workerId
    case "{{HOSTNAME}}" => hostname
    case "{{CORES}}" => cores.toString
    case other => other
  }

  def buildCommandSeq(): Seq[String] = {
    val command = jobDesc.command
    val runScript = new File(sparkHome, "run").getCanonicalPath
    Seq(runScript, command.mainClass) ++ command.arguments.map(substituteVariables)
  }

  /** Spawn a thread that will redirect a given stream to a file */
  def redirectStream(in: InputStream, file: File) {
    val out = new FileOutputStream(file)
    new Thread("redirect output to " + file) {
      override def run() {
        try {
          Utils.copyStream(in, out, true)
        } catch {
          case e: IOException =>
            logInfo("Redirection to " + file + " closed: " + e.getMessage)
        }
      }
    }.start()
  }

  /**
   * Download and run the executor described in our JobDescription
   */
  def fetchAndRunExecutor() {
    try {
      // Create the executor's working directory
      val executorDir = new File(workDir, jobId + "/" + execId)
      if (!executorDir.mkdirs()) {
        throw new IOException("Failed to create directory " + executorDir)
      }

      // Download the files it depends on into it (disabled for now)
      //for (url <- jobDesc.fileUrls) {
      //  fetchFile(url, executorDir)
      //}

      // Launch the process
      val command = buildCommandSeq()
      val builder = new ProcessBuilder(command: _*).directory(executorDir)
      val env = builder.environment()
      for ((key, value) <- jobDesc.command.environment) {
        env.put(key, value)
      }
      env.put("SPARK_CORES", cores.toString)
      env.put("SPARK_MEMORY", memory.toString)
      // In case we are running this from within the Spark Shell
      // so we are not creating a parent process.
      env.put("SPARK_LAUNCH_WITH_SCALA", "0")
      process = builder.start()

      // Redirect its stdout and stderr to files
      redirectStream(process.getInputStream, new File(executorDir, "stdout"))
      redirectStream(process.getErrorStream, new File(executorDir, "stderr"))

      // Wait for it to exit; this is actually a bad thing if it happens, because we expect to run
      // long-lived processes only. However, in the future, we might restart the executor a few
      // times on the same machine.
      val exitCode = process.waitFor()
      val message = "Command exited with code " + exitCode
      worker ! ExecutorStateChanged(jobId, execId, ExecutorState.FAILED, Some(message))
    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for executor " + fullId + " interrupted")

      case e: Exception => {
        logError("Error running executor", e)
        if (process != null) {
          process.destroy()
        }
        val message = e.getClass + ": " + e.getMessage
        worker ! ExecutorStateChanged(jobId, execId, ExecutorState.FAILED, Some(message))
      }
    }
  }
}
