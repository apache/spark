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

import akka.actor.ActorRef
import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}

import org.apache.spark.Logging
import org.apache.spark.deploy.DriverDescription
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.util.Utils

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 */
private[spark] class DriverRunner(
    val driverId: String,
    val workDir: File,
    val driverDesc: DriverDescription,
    val worker: ActorRef)
  extends Logging {

  var process: Option[Process] = None
  @volatile var killed = false

  /** Starts a thread to run and manage the driver. */
  def start() = {
    new Thread("DriverRunner for " + driverId) {
      override def run() {
        var exn: Option[Exception] = None

        try {
          val driverDir = createWorkingDirectory()
          val localJarFilename = downloadUserJar(driverDir)
          val command = Seq("java") ++ driverDesc.javaOptions ++ Seq(s"-Xmx${driverDesc.mem}m") ++
            Seq("-cp", localJarFilename) ++ Seq(driverDesc.mainClass) ++ driverDesc.options
          runCommandWithRetry(command, driverDesc.envVars, driverDir)
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
    killed = true
    process.foreach(p => p.destroy())
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
   * Creates the working directory for this driver.
   * Will throw an exception if there are errors preparing the directory.
   */
  def createWorkingDirectory(): File = {
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
  def downloadUserJar(driverDir: File): String = {

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

  /** Continue launching the supplied command until it exits zero or is killed. */
  def runCommandWithRetry(command: Seq[String], envVars: Seq[(String, String)], baseDir: File) = {
    // Time to wait between submission retries.
    var waitSeconds = 1
    var cleanExit = false

    while (!cleanExit && !killed) {
      Thread.sleep(waitSeconds * 1000)

      logInfo("Launch Command: " + command.mkString("\"", "\" \"", "\""))
      val builder = new ProcessBuilder(command: _*).directory(baseDir)
      envVars.map{ case(k,v) => builder.environment().put(k, v) }

      process = Some(builder.start())

      // Redirect stdout and stderr to files
      val stdout = new File(baseDir, "stdout")
      redirectStream(process.get.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val header = "Launch Command: %s\n%s\n\n".format(
        command.mkString("\"", "\" \"", "\""), "=" * 40)
      Files.write(header, stderr, Charsets.UTF_8)
      redirectStream(process.get.getErrorStream, stderr)

      val exitCode =
        /* There is a race here I've elected to ignore for now because it's very unlikely and not
         * simple to fix. This could see `killed=false` then the main thread gets a kill request
         * and sets `killed=true` and destroys the not-yet-started process, then this thread
         * launches the process. For now, in that case the user can just re-submit the kill
         * request. */
        if (killed) -1
        else process.get.waitFor()

      cleanExit = exitCode == 0
      if (!cleanExit && !killed) {
        waitSeconds = waitSeconds * 2 // exponential back-off
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
      }
    }
  }
}
