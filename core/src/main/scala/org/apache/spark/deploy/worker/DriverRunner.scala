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

import scala.collection.mutable.Map

import akka.actor.ActorRef
import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}

import org.apache.spark.Logging
import org.apache.spark.deploy.{Command, DriverDescription}
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.master.DriverState

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 */
private[spark] class DriverRunner(
    val driverId: String,
    val workDir: File,
    val sparkHome: File,
    val driverDesc: DriverDescription,
    val worker: ActorRef,
    val workerUrl: String)
  extends Logging {

  @volatile var process: Option[Process] = None
  @volatile var killed = false

  /** Starts a thread to run and manage the driver. */
  def start() = {
    new Thread("DriverRunner for " + driverId) {
      override def run() {
        var exn: Option[Exception] = None

        try {
          val driverDir = createWorkingDirectory()
          val localJarFilename = downloadUserJar(driverDir)

          // Make sure user application jar is on the classpath
          // TODO: If we add ability to submit multiple jars they should also be added here
          val env = Map(driverDesc.command.environment.toSeq: _*)
          env("SPARK_CLASSPATH") = env.getOrElse("SPARK_CLASSPATH", "") + s":$localJarFilename"
          val newCommand = Command(driverDesc.command.mainClass,
            driverDesc.command.arguments.map(substituteVariables), env)

          val command = CommandUtils.buildCommandSeq(newCommand, driverDesc.mem,
            sparkHome.getAbsolutePath)
          runCommand(command, env, driverDir, driverDesc.supervise)
        }
        catch {
          case e: Exception => exn = Some(e)
        }

        val finalState =
          if (killed) { DriverState.KILLED }
          else if (exn.isDefined) { DriverState.FAILED }
          else { DriverState.FINISHED }

        worker ! DriverStateChanged(driverId, finalState, exn)
      }
    }.start()
  }

  /** Terminate this driver (or prevent it from ever starting if not yet started) */
  def kill() {
    synchronized {
      process.foreach(p => p.destroy())
      killed = true
    }
  }

  /** Replace variables in a command argument passed to us */
  private def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case other => other
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

    val jarPath = new Path(driverDesc.jarUrl)

    val emptyConf = new Configuration() // TODO: In docs explain it needs to be full HDFS path
    val jarFileSystem = jarPath.getFileSystem(emptyConf)

    val destPath = new Path(driverDir.getAbsolutePath())
    val destFileSystem = destPath.getFileSystem(emptyConf)
    val jarFileName = jarPath.getName
    val localJarFile = new File(driverDir, jarFileName)
    val localJarFilename = localJarFile.getAbsolutePath

    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar $jarPath to $destPath")
      FileUtil.copy(jarFileSystem, jarPath, destFileSystem, destPath, false, false, emptyConf)
    }

    if (!localJarFile.exists()) { // Verify copy succeeded
      throw new Exception(s"Did not see expected jar $jarFileName in $driverDir")
    }

    localJarFilename
  }

  /** Launch the supplied command. */
  private def runCommand(command: Seq[String], envVars: Map[String, String], baseDir: File,
      supervise: Boolean) {
    // Time to wait between submission retries.
    var waitSeconds = 1
    var keepTrying = !killed

    while (keepTrying) {
      logInfo("Launch Command: " + command.mkString("\"", "\" \"", "\""))
      val builder = new ProcessBuilder(command: _*).directory(baseDir)
      envVars.map{ case(k,v) => builder.environment().put(k, v) }

      synchronized {
        if (killed) { return }

        process = Some(builder.start())

        // Redirect stdout and stderr to files
        val stdout = new File(baseDir, "stdout")
        CommandUtils.redirectStream(process.get.getInputStream, stdout)

        val stderr = new File(baseDir, "stderr")
        val header = "Launch Command: %s\n%s\n\n".format(
          command.mkString("\"", "\" \"", "\""), "=" * 40)
        Files.write(header, stderr, Charsets.UTF_8)
        CommandUtils.redirectStream(process.get.getErrorStream, stderr)
      }

      val exitCode = process.get.waitFor()

      if (supervise && exitCode != 0 && !killed) {
        waitSeconds = waitSeconds * 1 // exponential back-off
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        (0 until waitSeconds).takeWhile(f => {Thread.sleep(1000); !killed})
      }

      keepTrying = supervise && exitCode != 0 && !killed
    }
  }
}
