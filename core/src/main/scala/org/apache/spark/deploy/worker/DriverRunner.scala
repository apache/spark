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
import java.net.URI
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import com.google.common.io.{Files, FileWriteMode}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.StandaloneResourceUtils.prepareResourcesFile
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{DRIVER_RESOURCES_FILE, SPARK_DRIVER_PREFIX}
import org.apache.spark.internal.config.UI.UI_REVERSE_PROXY
import org.apache.spark.internal.config.Worker.WORKER_DRIVER_TERMINATE_TIMEOUT
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{Clock, ShutdownHookManager, SystemClock, Utils}

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 * This is currently only used in standalone cluster deploy mode.
 */
private[deploy] class DriverRunner(
    conf: SparkConf,
    val driverId: String,
    val workDir: File,
    val sparkHome: File,
    val driverDesc: DriverDescription,
    val worker: RpcEndpointRef,
    val workerUrl: String,
    val workerWebUiUrl: String,
    val securityManager: SecurityManager,
    val resources: Map[String, ResourceInformation] = Map.empty)
  extends Logging {

  @volatile private var process: Option[Process] = None
  @volatile private var killed = false

  // Populated once finished
  @volatile private[worker] var finalState: Option[DriverState] = None
  @volatile private[worker] var finalException: Option[Exception] = None

  // Timeout to wait for when trying to terminate a driver.
  private val driverTerminateTimeoutMs = conf.get(WORKER_DRIVER_TERMINATE_TIMEOUT)

  // Decoupled for testing
  def setClock(_clock: Clock): Unit = {
    clock = _clock
  }

  def setSleeper(_sleeper: Sleeper): Unit = {
    sleeper = _sleeper
  }

  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile { _ =>
      Thread.sleep(1000)
      !killed
    }
  }

  /** Starts a thread to run and manage the driver. */
  private[worker] def start() = {
    new Thread("DriverRunner for " + driverId) {
      override def run(): Unit = {
        var shutdownHook: AnyRef = null
        try {
          shutdownHook = ShutdownHookManager.addShutdownHook { () =>
            logInfo(log"Worker shutting down, killing driver ${MDC(DRIVER_ID, driverId)}")
            kill()
          }

          // prepare driver jars and run driver
          val exitCode = prepareAndRunDriver()

          // set final state depending on if forcibly killed and process exit code
          finalState = if (exitCode == 0) {
            Some(DriverState.FINISHED)
          } else if (killed) {
            Some(DriverState.KILLED)
          } else {
            Some(DriverState.FAILED)
          }
        } catch {
          case e: Exception =>
            kill()
            finalState = Some(DriverState.ERROR)
            finalException = Some(e)
        } finally {
          if (shutdownHook != null) {
            ShutdownHookManager.removeShutdownHook(shutdownHook)
          }
        }

        // notify worker of final driver state, possible exception
        worker.send(DriverStateChanged(driverId, finalState.get, finalException))
      }
    }.start()
  }

  /** Terminate this driver (or prevent it from ever starting if not yet started) */
  private[worker] def kill(): Unit = {
    logInfo("Killing driver process!")
    killed = true
    synchronized {
      process.foreach { p =>
        val exitCode = Utils.terminateProcess(p, driverTerminateTimeoutMs)
        if (exitCode.isEmpty) {
          logWarning(log"Failed to terminate driver process: ${MDC(PROCESS, p)} " +
              log". This process will likely be orphaned.")
        }
      }
    }
  }

  /**
   * Creates the working directory for this driver.
   * Will throw an exception if there are errors preparing the directory.
   */
  private def createWorkingDirectory(): File = {
    val driverDir = new File(workDir, driverId)
    if (!driverDir.exists() && !driverDir.mkdirs()) {
      throw new IOException("Failed to create directory " + driverDir)
    }
    driverDir
  }

  /**
   * Download the user jar into the supplied directory and return its local path.
   * Will throw an exception if there are errors downloading the jar.
   */
  private def downloadUserJar(driverDir: File): String = {
    val jarFileName = new URI(driverDesc.jarUrl).getPath.split("/").last
    val localJarFile = new File(driverDir, jarFileName)
    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(log"Copying user jar ${MDC(JAR_URL, driverDesc.jarUrl)}" +
        log" to ${MDC(FILE_NAME, localJarFile)}")
      Utils.fetchFile(
        driverDesc.jarUrl,
        driverDir,
        conf,
        SparkHadoopUtil.get.newConfiguration(conf),
        System.currentTimeMillis(),
        useCache = false)
      if (!localJarFile.exists()) { // Verify copy succeeded
        throw new IOException(
          s"Can not find expected jar $jarFileName which should have been loaded in $driverDir")
      }
    }
    localJarFile.getAbsolutePath
  }

  private[worker] def prepareAndRunDriver(): Int = {
    val driverDir = createWorkingDirectory()
    val localJarFilename = downloadUserJar(driverDir)
    val resourceFileOpt = prepareResourcesFile(SPARK_DRIVER_PREFIX, resources, driverDir)

    def substituteVariables(argument: String): String = argument match {
      case "{{WORKER_URL}}" => workerUrl
      case "{{USER_JAR}}" => localJarFilename
      case other => other
    }

    // config resource file for driver, which would be used to load resources when driver starts up
    val javaOpts = driverDesc.command.javaOpts ++ resourceFileOpt.map(f =>
      Seq(s"-D${DRIVER_RESOURCES_FILE.key}=${f.getAbsolutePath}")).getOrElse(Seq.empty)
    // TODO: If we add ability to submit multiple jars they should also be added here
    val builder = CommandUtils.buildProcessBuilder(driverDesc.command.copy(javaOpts = javaOpts),
      securityManager, driverDesc.mem, sparkHome.getAbsolutePath, substituteVariables)

    // add WebUI driver log url to environment
    val reverseProxy = conf.get(UI_REVERSE_PROXY)
    val workerUrlRef = UIUtils.makeHref(reverseProxy, driverId, workerWebUiUrl)
    builder.environment.put("SPARK_DRIVER_LOG_URL_STDOUT",
      s"$workerUrlRef/logPage/?driverId=$driverId&logType=stdout")
    builder.environment.put("SPARK_DRIVER_LOG_URL_STDERR",
      s"$workerUrlRef/logPage/?driverId=$driverId&logType=stderr")

    runDriver(builder, driverDir, driverDesc.supervise)
  }

  private def runDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean): Int = {
    builder.directory(baseDir)
    def initialize(process: Process): Unit = {
      // Redirect stdout and stderr to files
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val redactedCommand = Utils.redactCommandLineArgs(conf, builder.command.asScala.toSeq)
        .mkString("\"", "\" \"", "\"")
      val header = "Launch Command: %s\n%s\n\n".format(redactedCommand, "=" * 40)
      Files.asCharSink(stderr, StandardCharsets.UTF_8, FileWriteMode.APPEND).write(header)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }

  private[worker] def runCommandWithRetry(
      command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Int = {
    var exitCode = -1
    // Time to wait between submission retries.
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    val successfulRunDuration = 5
    var keepTrying = !killed

    val redactedCommand = Utils.redactCommandLineArgs(conf, command.command)
      .mkString("\"", "\" \"", "\"")
    while (keepTrying) {
      logInfo(log"Launch Command: ${MDC(COMMAND, redactedCommand)}")

      synchronized {
        if (killed) { return exitCode }
        process = Some(command.start())
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      exitCode = process.get.waitFor()

      // check if attempting another run
      keepTrying = supervise && exitCode != 0 && !killed
      if (keepTrying) {
        if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000L) {
          waitSeconds = 1
        }
        logInfo(log"Command exited with status ${MDC(EXIT_CODE, exitCode)}," +
          log" re-launching after ${MDC(TIME_UNITS, waitSeconds)} s.")
        sleeper.sleep(waitSeconds)
        waitSeconds = waitSeconds * 2 // exponential back-off
      }
    }

    exitCode
  }
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int): Unit
}

// Needed because ProcessBuilder is a final class and cannot be mocked
private[deploy] trait ProcessBuilderLike {
  def start(): Process
  def command: Seq[String]
}

private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder): ProcessBuilderLike = new ProcessBuilderLike {
    override def start(): Process = processBuilder.start()
    override def command: Seq[String] = processBuilder.command().asScala.toSeq
  }
}
