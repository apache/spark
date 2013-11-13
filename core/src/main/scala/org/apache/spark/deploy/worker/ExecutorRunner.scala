/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.worker

import java.io._
import java.lang.System.getenv

import akka.actor.ActorRef

import com.google.common.base.Charsets
import com.google.common.io.Files

import org.apache.spark.{Logging}
import org.apache.spark.deploy.{ExecutorState, ApplicationDescription}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.util.Utils

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
    val host: String,
    val sparkHome: File,
    val workDir: File,
    var state: ExecutorState.Value)
  extends Logging {

  val fullId = appId + "/" + execId
  var workerThread: Thread = null
  var process: Process = null
  var shutdownHook: Thread = null

  private def getAppEnv(key: String): Option[String] =
    appDesc.command.environment.get(key).orElse(Option(getenv(key)))

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
      state = ExecutorState.KILLED
      worker ! ExecutorStateChanged(appId, execId, state, None, None)
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  def substituteVariables(argument: String): String = argument match {
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case other => other
  }

  def buildCommandSeq(): Seq[String] = {
    val command = appDesc.command
    val runner = getAppEnv("JAVA_HOME").map(_ + "/bin/java").getOrElse("java")
    // SPARK-698: do not call the run.cmd script, as process.destroy()
    // fails to kill a process tree on Windows
    Seq(runner) ++ buildJavaOpts() ++ Seq(command.mainClass) ++
      (command.arguments ++ Seq(appId)).map(substituteVariables)
  }

  /**
   * Attention: this must always be aligned with the environment variables in the run scripts and
   * the way the JAVA_OPTS are assembled there.
   */
  def buildJavaOpts(): Seq[String] = {
    val libraryOpts = getAppEnv("SPARK_LIBRARY_PATH")
      .map(p => List("-Djava.library.path=" + p))
      .getOrElse(Nil)
    val workerLocalOpts = Option(getenv("SPARK_JAVA_OPTS")).map(Utils.splitCommandString).getOrElse(Nil)
    val userOpts = getAppEnv("SPARK_JAVA_OPTS").map(Utils.splitCommandString).getOrElse(Nil)
    val memoryOpts = Seq("-Xms" + memory + "M", "-Xmx" + memory + "M")

    // Figure out our classpath with the external compute-classpath script
    val ext = if (System.getProperty("os.name").startsWith("Windows")) ".cmd" else ".sh"
    val classPath = Utils.executeAndGetOutput(
        Seq(sparkHome + "/bin/compute-classpath" + ext),
        extraEnvironment=appDesc.command.environment)

    Seq("-cp", classPath) ++ libraryOpts ++ workerLocalOpts ++ userOpts ++ memoryOpts
  }

  /** Spawn a thread that will redirect a given stream to a file */
  def redirectStream(in: InputStream, file: File) {
    val out = new FileOutputStream(file, true)
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
      logInfo("Launch command: " + command.mkString("\"", "\" \"", "\""))
      val builder = new ProcessBuilder(command: _*).directory(executorDir)
      val env = builder.environment()
      for ((key, value) <- appDesc.command.environment) {
        env.put(key, value)
      }
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      env.put("SPARK_LAUNCH_WITH_SCALA", "0")
      process = builder.start()

      val header = "Spark Executor Command: %s\n%s\n\n".format(
        command.mkString("\"", "\" \"", "\""), "=" * 40)

      // Redirect its stdout and stderr to files
      val stdout = new File(executorDir, "stdout")
      redirectStream(process.getInputStream, stdout)

      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, Charsets.UTF_8)
      redirectStream(process.getErrorStream, stderr)

      // Wait for it to exit; this is actually a bad thing if it happens, because we expect to run
      // long-lived processes only. However, in the future, we might restart the executor a few
      // times on the same machine.
      val exitCode = process.waitFor()
      state = ExecutorState.FAILED
      val message = "Command exited with code " + exitCode
      worker ! ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode))
    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for executor " + fullId + " interrupted")

      case e: Exception => {
        logError("Error running executor", e)
        if (process != null) {
          process.destroy()
        }
        state = ExecutorState.FAILED
        val message = e.getClass + ": " + e.getMessage
        worker ! ExecutorStateChanged(appId, execId, state, Some(message), None)
      }
    }
  }
}
