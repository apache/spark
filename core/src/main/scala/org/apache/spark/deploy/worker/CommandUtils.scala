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

import java.io.{File, FileOutputStream, IOException, InputStream}
import java.lang.System._

import org.apache.spark.Logging
import org.apache.spark.deploy.Command
import org.apache.spark.util.Utils

/**
 ** Utilities for running commands with the spark classpath.
 */
private[spark]
object CommandUtils extends Logging {
  def buildCommandSeq(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    val runner = getEnv("JAVA_HOME", command).map(_ + "/bin/java").getOrElse("java")

    // SPARK-698: do not call the run.cmd script, as process.destroy()
    // fails to kill a process tree on Windows
    Seq(runner) ++ buildJavaOpts(command, memory, sparkHome) ++ Seq(command.mainClass) ++
      command.arguments
  }

  private def getEnv(key: String, command: Command): Option[String] =
    command.environment.get(key).orElse(Option(System.getenv(key)))

  /**
   * Attention: this must always be aligned with the environment variables in the run scripts and
   * the way the JAVA_OPTS are assembled there.
   */
  def buildJavaOpts(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    val libraryOpts = getEnv("SPARK_LIBRARY_PATH", command)
      .map(p => List("-Djava.library.path=" + p))
      .getOrElse(Nil)
    val workerLocalOpts = Option(getenv("SPARK_JAVA_OPTS"))
      .map(Utils.splitCommandString).getOrElse(Nil)
    val userOpts = getEnv("SPARK_JAVA_OPTS", command).map(Utils.splitCommandString).getOrElse(Nil)
    val memoryOpts = Seq(s"-Xms${memory}M", s"-Xmx${memory}M")

    // Figure out our classpath with the external compute-classpath script
    val ext = if (System.getProperty("os.name").startsWith("Windows")) ".cmd" else ".sh"
    val classPath = Utils.executeAndGetOutput(
      Seq(sparkHome + "/bin/compute-classpath" + ext),
      extraEnvironment=command.environment)

    Seq("-cp", classPath) ++ libraryOpts ++ workerLocalOpts ++ userOpts ++ memoryOpts
  }

  /** Spawn a thread that will redirect a given stream to a file */
  def redirectStream(in: InputStream, file: File) {
    val out = new FileOutputStream(file, true)
    // TODO: It would be nice to add a shutdown hook here that explains why the output is
    //       terminating. Otherwise if the worker dies the executor logs will silently stop.
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
}
