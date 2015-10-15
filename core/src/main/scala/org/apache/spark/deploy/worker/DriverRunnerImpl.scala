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
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging

import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 * This is currently only used in standalone cluster deploy mode.
 */
private[deploy] class DriverRunnerImpl(
    processSetup: ChildProcessCommonSetup[DriverDescription],
    workerSetup: WorkerSetup,
    stateChangeListener: StateChangeListener[DriverDescription, DriverRunnerInfo])
  extends ChildProcessRunner[DriverDescription, DriverRunnerInfo]
  with DriverRunnerInfo with Logging { self =>

  override def state: DriverState = finalState.getOrElse(DriverState.RUNNING)
  override def info: DriverRunnerImpl = this
  override def exception: Option[Exception] = finalException
  override def setup: ChildProcessCommonSetup[DriverDescription] = processSetup

  @volatile private var process: Option[Process] = None
  @volatile private var killed = false

  // Populated once finished
  private[worker] var finalState: Option[DriverState] = None
  private[worker] var finalException: Option[Exception] = None
  private var finalExitCode: Option[Int] = None

  // Decoupled for testing
  def setClock(_clock: Clock): Unit = {
    clock = _clock
  }

  def setSleeper(_sleeper: Sleeper): Unit = {
    sleeper = _sleeper
  }

  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile(f => {Thread.sleep(1000); !killed})
  }

  /** Starts a thread to run and manage the driver. */
  def start(): Unit = {
    new Thread("DriverRunner for " + processSetup.id) {
      override def run() {
        try {
          val localJarFilename = downloadUserJar(processSetup.workDir)

          def substituteVariables(argument: String): String = argument match {
            case "{{WORKER_URL}}" => workerSetup.workerUri
            case "{{USER_JAR}}" => localJarFilename
            case other => other
          }

          // TODO: If we add ability to submit multiple jars they should also be added here
          val builder = CommandUtils.buildProcessBuilder(
            processSetup.description.command,
            workerSetup.securityManager,
            processSetup.memory,
            workerSetup.sparkHome.getAbsolutePath,
            substituteVariables)
          launchDriver(builder, processSetup.workDir, processSetup.description.supervise)
        }
        catch {
          case e: Exception => finalException = Some(e)
        }

        val state =
          if (killed) {
            DriverState.KILLED
          } else if (finalException.isDefined) {
            DriverState.ERROR
          } else {
            finalExitCode match {
              case Some(0) => DriverState.FINISHED
              case _ => DriverState.FAILED
            }
          }

        finalState = Some(state)

        stateChangeListener(self, None,
          finalException orElse finalExitCode.filter(_ != 0).map(new NonZeroExitCodeException(_)))
      }
    }.start()
  }

  /** Terminate this driver (or prevent it from ever starting if not yet started) */
  def kill(): Unit = {
    synchronized {
      process.foreach(p => p.destroy())
      killed = true
    }
  }

  /**
   * Download the user jar into the supplied directory and return its local path.
   * Will throw an exception if there are errors downloading the jar.
   */
  private def downloadUserJar(driverDir: File): String = {
    val jarPath = new Path(processSetup.description.jarUrl)

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(workerSetup.conf)
    val destPath = new File(driverDir.getAbsolutePath, jarPath.getName)
    val jarFileName = jarPath.getName
    val localJarFile = new File(driverDir, jarFileName)
    val localJarFilename = localJarFile.getAbsolutePath

    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar $jarPath to $destPath")
      Utils.fetchFile(
        processSetup.description.jarUrl,
        driverDir,
        workerSetup.conf,
        workerSetup.securityManager,
        hadoopConf,
        System.currentTimeMillis(),
        useCache = false)
    }

    if (!localJarFile.exists()) { // Verify copy succeeded
      throw new Exception(s"Did not see expected jar $jarFileName in $driverDir")
    }

    localJarFilename
  }

  private def launchDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean) {
    builder.directory(baseDir)
    def initialize(process: Process): Unit = {
      // Redirect stdout and stderr to files
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val formattedCommand = builder.command.asScala.mkString("\"", "\" \"", "\"")
      val header = "Launch Command: %s\n%s\n\n".format(formattedCommand, "=" * 40)
      Files.append(header, stderr, UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }

  private[spark] def runCommandWithRetry(
      command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Unit = {
    // Time to wait between submission retries.
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    val successfulRunDuration = 5

    var keepTrying = !killed

    while (keepTrying) {
      logInfo("Launch Command: " + command.command.mkString("\"", "\" \"", "\""))

      synchronized {
        if (killed) { return }
        process = Some(command.start())
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      val exitCode = process.get.waitFor()
      if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000) {
        waitSeconds = 1
      }

      if (supervise && exitCode != 0 && !killed) {
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        sleeper.sleep(waitSeconds)
        waitSeconds = waitSeconds * 2 // exponential back-off
      }

      keepTrying = supervise && exitCode != 0 && !killed
      finalExitCode = Some(exitCode)
    }
  }
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int)
}

// Needed because ProcessBuilder is a final class and cannot be mocked
private[deploy] trait ProcessBuilderLike {
  def start(): Process
  def command: Seq[String]
}

private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder): ProcessBuilderLike = new ProcessBuilderLike {
    override def start(): Process = processBuilder.start()
    override def command: Seq[String] = processBuilder.command().asScala
  }
}

private[deploy] trait DriverRunnerInfo extends ChildRunnerInfo[DriverDescription] {
  def state: DriverState
}
