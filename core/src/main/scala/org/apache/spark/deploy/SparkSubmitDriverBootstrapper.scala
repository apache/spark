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

package org.apache.spark.deploy

import java.io.File

import scala.collection.JavaConversions._

import org.apache.spark.util.{RedirectThread, Utils}

/**
 * Launch an application through Spark submit in client mode with the appropriate classpath,
 * library paths, java options and memory. These properties of the JVM must be set before the
 * driver JVM is launched. The sole purpose of this class is to avoid handling the complexity
 * of parsing the properties file for such relevant configs in Bash.
 *
 * Usage: org.apache.spark.deploy.SparkSubmitDriverBootstrapper <submit args>
 */
private[spark] object SparkSubmitDriverBootstrapper {

  // Note: This class depends on the behavior of `bin/spark-class` and `bin/spark-submit`.
  // Any changes made there must be reflected in this file.

  def main(args: Array[String]): Unit = {

    // This should be called only from `bin/spark-class`
    if (!sys.env.contains("SPARK_CLASS")) {
      System.err.println("SparkSubmitDriverBootstrapper must be called from `bin/spark-class`!")
      System.exit(1)
    }

    val submitArgs = args
    val runner = sys.env("RUNNER")
    val classpath = sys.env("CLASSPATH")
    val javaOpts = sys.env("JAVA_OPTS")
    val defaultDriverMemory = sys.env("OUR_JAVA_MEM")

    // Spark submit specific environment variables
    val deployMode = sys.env("SPARK_SUBMIT_DEPLOY_MODE")
    val propertiesFile = sys.env("SPARK_SUBMIT_PROPERTIES_FILE")
    val bootstrapDriver = sys.env("SPARK_SUBMIT_BOOTSTRAP_DRIVER")
    val submitDriverMemory = sys.env.get("SPARK_SUBMIT_DRIVER_MEMORY")
    val submitLibraryPath = sys.env.get("SPARK_SUBMIT_LIBRARY_PATH")
    val submitClasspath = sys.env.get("SPARK_SUBMIT_CLASSPATH")
    val submitJavaOpts = sys.env.get("SPARK_SUBMIT_OPTS")

    assume(runner != null, "RUNNER must be set")
    assume(classpath != null, "CLASSPATH must be set")
    assume(javaOpts != null, "JAVA_OPTS must be set")
    assume(defaultDriverMemory != null, "OUR_JAVA_MEM must be set")
    assume(deployMode == "client", "SPARK_SUBMIT_DEPLOY_MODE must be \"client\"!")
    assume(propertiesFile != null, "SPARK_SUBMIT_PROPERTIES_FILE must be set")
    assume(bootstrapDriver != null, "SPARK_SUBMIT_BOOTSTRAP_DRIVER must be set")

    // Parse the properties file for the equivalent spark.driver.* configs
    val properties = Utils.getPropertiesFromFile(propertiesFile)
    val confDriverMemory = properties.get("spark.driver.memory")
    val confLibraryPath = properties.get("spark.driver.extraLibraryPath")
    val confClasspath = properties.get("spark.driver.extraClassPath")
    val confJavaOpts = properties.get("spark.driver.extraJavaOptions")

    // Favor Spark submit arguments over the equivalent configs in the properties file.
    // Note that we do not actually use the Spark submit values for library path, classpath,
    // and Java opts here, because we have already captured them in Bash.

    val newDriverMemory = submitDriverMemory
      .orElse(confDriverMemory)
      .getOrElse(defaultDriverMemory)

    val newClasspath =
      if (submitClasspath.isDefined) {
        classpath
      } else {
        classpath + confClasspath.map(sys.props("path.separator") + _).getOrElse("")
      }

    val newJavaOpts =
      if (submitJavaOpts.isDefined) {
        // SPARK_SUBMIT_OPTS is already captured in JAVA_OPTS
        javaOpts
      } else {
        javaOpts + confJavaOpts.map(" " + _).getOrElse("")
      }

    val filteredJavaOpts = Utils.splitCommandString(newJavaOpts)
      .filterNot(_.startsWith("-Xms"))
      .filterNot(_.startsWith("-Xmx"))

    // Build up command
    val command: Seq[String] =
      Seq(runner) ++
      Seq("-cp", newClasspath) ++
      filteredJavaOpts ++
      Seq(s"-Xms$newDriverMemory", s"-Xmx$newDriverMemory") ++
      Seq("org.apache.spark.deploy.SparkSubmit") ++
      submitArgs

    // Print the launch command. This follows closely the format used in `bin/spark-class`.
    if (sys.env.contains("SPARK_PRINT_LAUNCH_COMMAND")) {
      System.err.print("Spark Command: ")
      System.err.println(command.mkString(" "))
      System.err.println("========================================\n")
    }

    // Start the driver JVM
    val filteredCommand = command.filter(_.nonEmpty)
    val builder = new ProcessBuilder(filteredCommand)
    val env = builder.environment()

    if (submitLibraryPath.isEmpty && confLibraryPath.nonEmpty) {
      val libraryPaths = confLibraryPath ++ sys.env.get(Utils.libraryPathEnvName)
      env.put(Utils.libraryPathEnvName, libraryPaths.mkString(sys.props("path.separator")))
    }

    val process = builder.start()

    // If we kill an app while it's running, its sub-process should be killed too.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        if (process != null) {
          process.destroy()
          process.waitFor()
        }
      }
    })

    // Redirect stdout and stderr from the child JVM
    val stdoutThread = new RedirectThread(process.getInputStream, System.out, "redirect stdout")
    val stderrThread = new RedirectThread(process.getErrorStream, System.err, "redirect stderr")
    stdoutThread.start()
    stderrThread.start()

    // Redirect stdin to child JVM only if we're not running Windows. This is because the
    // subprocess there already reads directly from our stdin, so we should avoid spawning a
    // thread that contends with the subprocess in reading from System.in.
    val isWindows = Utils.isWindows
    val isSubprocess = sys.env.contains("IS_SUBPROCESS")
    if (!isWindows) {
      val stdinThread = new RedirectThread(System.in, process.getOutputStream, "redirect stdin",
        propagateEof = true)
      stdinThread.start()
      // Spark submit (JVM) may run as a subprocess, and so this JVM should terminate on
      // broken pipe, signaling that the parent process has exited. This is the case if the
      // application is launched directly from python, as in the PySpark shell. In Windows,
      // the termination logic is handled in java_gateway.py
      if (isSubprocess) {
        stdinThread.join()
        process.destroy()
      }
    }
    val returnCode = process.waitFor()
    sys.exit(returnCode)
  }

}
