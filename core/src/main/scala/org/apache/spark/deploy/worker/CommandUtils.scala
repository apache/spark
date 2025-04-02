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

import java.io.{File, FileOutputStream, InputStream, IOException}

import scala.collection.Map
import scala.jdk.CollectionConverters._

import org.apache.spark.{SecurityManager, SSLOptions}
import org.apache.spark.deploy.Command
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.launcher.WorkerCommandBuilder
import org.apache.spark.util.Utils

/**
 * Utilities for running commands with the spark classpath.
 */
private[deploy]
object CommandUtils extends Logging {

  /**
   * Build a ProcessBuilder based on the given parameters.
   * The `env` argument is exposed for testing.
   */
  def buildProcessBuilder(
      command: Command,
      securityMgr: SecurityManager,
      memory: Int,
      sparkHome: String,
      substituteArguments: String => String,
      classPaths: Seq[String] = Seq.empty,
      env: Map[String, String] = sys.env): ProcessBuilder = {
    val localCommand = buildLocalCommand(
      command, securityMgr, substituteArguments, classPaths, env)
    val commandSeq = buildCommandSeq(localCommand, memory, sparkHome)
    val builder = new ProcessBuilder(commandSeq: _*)
    val environment = builder.environment()
    for ((key, value) <- localCommand.environment) {
      environment.put(key, value)
    }
    builder
  }

  private def buildCommandSeq(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    // SPARK-698: do not call the run.cmd script, as process.destroy()
    // fails to kill a process tree on Windows
    val cmd = new WorkerCommandBuilder(sparkHome, memory, command).buildCommand()
    (cmd.asScala ++ Seq(command.mainClass) ++ command.arguments).toSeq
  }

  /**
   * Build a command based on the given one, taking into account the local environment
   * of where this command is expected to run, substitute any placeholders, and append
   * any extra class paths.
   */
  private def buildLocalCommand(
      command: Command,
      securityMgr: SecurityManager,
      substituteArguments: String => String,
      classPath: Seq[String] = Seq.empty,
      env: Map[String, String]): Command = {
    val libraryPathName = Utils.libraryPathEnvName
    val libraryPathEntries = command.libraryPathEntries
    val cmdLibraryPath = command.environment.get(libraryPathName)

    var newEnvironment = if (libraryPathEntries.nonEmpty && libraryPathName.nonEmpty) {
      val libraryPaths = libraryPathEntries ++ cmdLibraryPath ++ env.get(libraryPathName)
      command.environment ++ Map(libraryPathName -> libraryPaths.mkString(File.pathSeparator))
    } else {
      command.environment
    }

    // set auth secret to env variable if needed
    if (securityMgr.isAuthenticationEnabled()) {
      newEnvironment = newEnvironment ++
        Map(SecurityManager.ENV_AUTH_SECRET -> securityMgr.getSecretKey())
    }
    // set SSL env variables if needed
    newEnvironment ++= securityMgr.getEnvironmentForSslRpcPasswords

    Command(
      command.mainClass,
      command.arguments.map(substituteArguments),
      newEnvironment,
      command.classPathEntries ++ classPath,
      Seq.empty, // library path already captured in environment variable
      // filter out secrets from java options
      command.javaOpts.filterNot(opts =>
        opts.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF) ||
        SSLOptions.SPARK_RPC_SSL_PASSWORD_FIELDS.exists(
          field => opts.startsWith("-D" + field)
        )
      ))
  }

  /** Spawn a thread that will redirect a given stream to a file */
  def redirectStream(in: InputStream, file: File): Unit = {
    val out = new FileOutputStream(file, true)
    // TODO: It would be nice to add a shutdown hook here that explains why the output is
    //       terminating. Otherwise if the worker dies the executor logs will silently stop.
    new Thread("redirect output to " + file) {
      override def run(): Unit = {
        try {
          Utils.copyStream(in, out, true)
        } catch {
          case e: IOException =>
            logInfo(log"Redirection to ${MDC(LogKeys.FILE_NAME, file)} closed: " +
              log"${MDC(LogKeys.ERROR, e.getMessage)}")
        }
      }
    }.start()
  }
}
