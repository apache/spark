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
private[spark] class ExecutorRunner(
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

  /** Replace variables such as {{SLAVEID}} and {{CORES}} in a command argument passed to us */
  def substituteVariables(argument: String): String = argument match {
    case "{{SLAVEID}}" => workerId
    case "{{HOSTNAME}}" => hostname
    case "{{CORES}}" => cores.toString
    case other => other
  }

  def buildCommandSeq(): Seq[String] = {
    val command = jobDesc.command
    val script = if (System.getProperty("os.name").startsWith("Windows")) "run.cmd" else "run";
    val runScript = new File(sparkHome, script).getCanonicalPath
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
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
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
