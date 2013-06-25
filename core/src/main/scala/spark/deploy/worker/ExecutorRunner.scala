package spark.deploy.worker

import java.io._
import spark.deploy.{ExecutorState, ExecutorStateChanged, ApplicationDescription}
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
    val appId: String,
    val execId: Int,
    val appDesc: ApplicationDescription,
    val cores: Int,
    val memory: Int,
    val worker: ActorRef,
    val workerId: String,
    val hostPort: String,
    val sparkHome: File,
    val workDir: File)
  extends Logging {

  Utils.checkHostPort(hostPort, "Expected hostport")

  val fullId = appId + "/" + execId
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
      worker ! ExecutorStateChanged(appId, execId, ExecutorState.KILLED, None, None)
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  def substituteVariables(argument: String): String = argument match {
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => Utils.parseHostPort(hostPort)._1
    case "{{CORES}}" => cores.toString
    case other => other
  }

  def buildCommandSeq(): Seq[String] = {
    val command = appDesc.command
    val runner = if (System.getenv("JAVA_HOME") == null) {
      "java"
    } else {
      System.getenv("JAVA_HOME") + "/bin/java"
    }
    // SPARK-698: do not call the run.cmd script, as process.destroy()
    // fails to kill a process tree on Windows
    Seq(runner) ++ buildJavaOpts() ++ Seq(command.mainClass) ++
      command.arguments.map(substituteVariables)
  }

  /**
   * Attention: this must always be aligned with the environment variables in the run scripts and
   * the way the JAVA_OPTS are assembled there.
   */
  def buildJavaOpts(): Seq[String] = {
    val libraryOpts = if (System.getenv("SPARK_LIBRARY_PATH") == null) {
      Nil
    } else {
      List("-Djava.library.path=" + System.getenv("SPARK_LIBRARY_PATH"))
    }

    val userOpts = if (System.getenv("SPARK_JAVA_OPTS") == null) {
      Nil
    } else {
      Utils.splitCommandString(System.getenv("SPARK_JAVA_OPTS"))
    }

    val memoryOpts = Seq("-Xms" + memory + "M", "-Xmx" + memory + "M")

    // Figure out our classpath with the external compute-classpath script
    val ext = if (System.getProperty("os.name").startsWith("Windows")) ".cmd" else ".sh"
    val classPath = Utils.executeAndGetOutput(Seq(sparkHome + "/bin/compute-classpath" + ext))

    Seq("-cp", classPath) ++ libraryOpts ++ userOpts ++ memoryOpts
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
   * Download and run the executor described in our ApplicationDescription
   */
  def fetchAndRunExecutor() {
    try {
      // Create the executor's working directory
      val executorDir = new File(workDir, appId + "/" + execId)
      if (!executorDir.mkdirs()) {
        throw new IOException("Failed to create directory " + executorDir)
      }

      // Launch the process
      val command = buildCommandSeq()
      val builder = new ProcessBuilder(command: _*).directory(executorDir)
      val env = builder.environment()
      for ((key, value) <- appDesc.command.environment) {
        env.put(key, value)
      }
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
      worker ! ExecutorStateChanged(appId, execId, ExecutorState.FAILED, Some(message),
                                    Some(exitCode))
    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for executor " + fullId + " interrupted")

      case e: Exception => {
        logError("Error running executor", e)
        if (process != null) {
          process.destroy()
        }
        val message = e.getClass + ": " + e.getMessage
        worker ! ExecutorStateChanged(appId, execId, ExecutorState.FAILED, Some(message), None)
      }
    }
  }
}
