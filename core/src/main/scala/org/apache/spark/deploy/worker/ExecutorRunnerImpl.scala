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

import scala.collection.JavaConverters._

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files

import org.apache.spark.Logging
import org.apache.spark.deploy.ExecutorState._
import org.apache.spark.deploy.{ApplicationDescription, ExecutorDescription, ExecutorState}
import org.apache.spark.util.ShutdownHookManager
import org.apache.spark.util.logging.FileAppender

/**
 * Manages the execution of one executor process.
 * This is currently only used in standalone mode.
 */
private[deploy] class ExecutorRunnerImpl(
    processSetup: ChildProcessCommonSetup[ApplicationDescription],
    workerSetup: WorkerSetup,
    stateChangedListener: StateChangeListener[ApplicationDescription, ExecutorRunnerInfo],
    override val appId: String,
    appLocalDirs: Seq[String],
    @volatile var state: ExecutorState.Value)
  extends ChildProcessRunner[ApplicationDescription, ExecutorRunnerInfo]
  with ExecutorRunnerInfo with Logging { self =>

  override def info: ExecutorRunnerImpl = this
  override def setup: ChildProcessCommonSetup[ApplicationDescription] = processSetup

  private val fullId = appId + "/" + processSetup.id
  private var workerThread: Thread = null
  private var process: Process = null
  private var stdoutAppender: FileAppender = null
  private var stderrAppender: FileAppender = null

  @volatile var exception: Option[Exception] = None

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  private var shutdownHook: AnyRef = null

  override def start() {
    exception = None
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      override def run() { fetchAndRunExecutor() }
    }
    workerThread.start()
    // Shutdown hook that kills actors on shutdown.
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      killProcess(Some("Worker shutting down")) }
  }

  /**
   * Kill executor process, wait for exit and notify worker to update resource status.
   *
   * @param message the exception message which caused the executor's death
   */
  private def killProcess(message: Option[String]) {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      process.destroy()
      exitCode = Some(process.waitFor())
    }
    stateChangedListener(this, message,
      exitCode.filter(_ != 0).map(new NonZeroExitCodeException(_)))
  }

  /** Stop this executor runner, including killing the process it launched */
  override def kill() {
    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  private[worker] def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerSetup.workerUri
    case "{{EXECUTOR_ID}}" => processSetup.id
    case "{{HOSTNAME}}" => processSetup.host
    case "{{CORES}}" => processSetup.cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  /**
   * Download and run the executor described in our ApplicationDescription
   */
  private def fetchAndRunExecutor() {
    try {
      // Launch the process
      val builder = CommandUtils.buildProcessBuilder(
        processSetup.description.command,
        workerSetup.securityManager,
        processSetup.memory,
        workerSetup.sparkHome.getAbsolutePath,
        substituteVariables)
      val command = builder.command()
      val formattedCommand = command.asScala.mkString("\"", "\" \"", "\"")
      logInfo(s"Launch command: $formattedCommand")

      builder.directory(processSetup.workDir)
      builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls
      val baseUrl =
        s"http://${processSetup.publicAddress}:${processSetup.webUIPort}" +
          s"/logPage/?appId=$appId&executorId=${processSetup.id}&logType="
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")

      process = builder.start()
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        formattedCommand, "=" * 40)

      // Redirect its stdout and stderr to files
      val stdout = new File(processSetup.workDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, workerSetup.conf)

      val stderr = new File(processSetup.workDir, "stderr")
      Files.write(header, stderr, UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, workerSetup.conf)

      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code
      val exitCode = process.waitFor()
      state = ExecutorState.EXITED
      val message = "Command exited with code " + exitCode
      stateChangedListener(self, Some(message),
        Some(exitCode).filter(_ != 0).map(new NonZeroExitCodeException(_)))
    } catch {
      case interrupted: InterruptedException => {
        logInfo("Runner thread for executor " + fullId + " interrupted")
        state = ExecutorState.KILLED
        killProcess(None)
      }
      case e: Exception => {
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        exception = Some(e)
        killProcess(Some(e.toString))
      }
    }
  }
}

private[deploy] trait ExecutorRunnerInfo extends ChildRunnerInfo[ApplicationDescription] {
  def appId: String
  def state: ExecutorState

  def createExecutorDescription(): ExecutorDescription = {
    new ExecutorDescription(appId, setup.id.toInt, setup.cores, state)
  }
}
