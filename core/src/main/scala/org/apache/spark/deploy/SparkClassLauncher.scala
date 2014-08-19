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
 * Wrapper of `bin/spark-class` that prepares the launch environment of the child JVM properly.
 */
object SparkClassLauncher {

  /**
   * Launch a Spark class with the given class paths, library paths, java options and memory.
   * If we are launching an application through Spark submit in client mode, we must also
   * take into account special `spark.driver.*` properties needed to start the driver JVM.
   */
  def main(args: Array[String]): Unit = {
    if (args.size < 8) {
      System.err.println(
        """
          |Usage: org.apache.spark.deploy.SparkClassLauncher
          |
          |  [properties file]    - path to your Spark properties file
          |  [java runner]        - command to launch the child JVM
          |  [java class paths]   - class paths to pass to the child JVM
          |  [java library paths] - library paths to pass to the child JVM
          |  [java opts]          - java options to pass to the child JVM
          |  [java memory]        - memory used to launch the child JVM
          |  [client mode]        - whether the child JVM will run the Spark driver
          |  [main class]         - main class to run in the child JVM
          |  <main args>          - arguments passed to this main class
          |
          |Example:
          |  org.apache.spark.deploy.SparkClassLauncher.SparkClassLauncher
          |    conf/spark-defaults.conf java /classpath1:/classpath2 /librarypath1:/librarypath2
          |    "-XX:-UseParallelGC -Dsome=property" 5g true org.apache.spark.deploy.SparkSubmit
          |    --master local --class org.apache.spark.examples.SparkPi 10
        """.stripMargin)
      System.exit(1)
    }
    val propertiesFile = args(0)
    val javaRunner = args(1)
    val clClassPaths = args(2)
    val clLibraryPaths = args(3)
    val clJavaOpts = args(4)
    val clJavaMemory = args(5)
    val clientMode = args(6) == "true"
    val mainClass = args(7)

    // In client deploy mode, parse the properties file for certain `spark.driver.*` configs.
    // These configs encode java options, class paths, and library paths needed to launch the JVM.
    val properties =
      if (clientMode) {
        SparkSubmitArguments.getPropertiesFromFile(new File(propertiesFile)).toMap
      } else {
        Map[String, String]()
      }
    val confDriverMemory = properties.get("spark.driver.memory")
    val confClassPaths = properties.get("spark.driver.extraClassPath")
    val confLibraryPaths = properties.get("spark.driver.extraLibraryPath")
    val confJavaOpts = properties.get("spark.driver.extraJavaOptions")

    // Merge relevant command line values with the config equivalents, if any
    val javaMemory =
      if (clientMode) {
        confDriverMemory.getOrElse(clJavaMemory)
      } else {
        clJavaMemory
      }
    val pathSeparator = sys.props("path.separator")
    val classPaths = clClassPaths + confClassPaths.map(pathSeparator + _).getOrElse("")
    val libraryPaths = clLibraryPaths + confLibraryPaths.map(pathSeparator + _).getOrElse("")
    val javaOpts = Utils.splitCommandString(clJavaOpts) ++
      confJavaOpts.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val filteredJavaOpts = javaOpts.filterNot { opt =>
      opt.startsWith("-Djava.library.path") || opt.startsWith("-Xms") || opt.startsWith("-Xmx")
    }

    // Build up command
    val command: Seq[String] =
      Seq(javaRunner) ++
      { if (classPaths.nonEmpty) Seq("-cp", classPaths) else Seq.empty } ++
      { if (libraryPaths.nonEmpty) Seq(s"-Djava.library.path=$libraryPaths") else Seq.empty } ++
      filteredJavaOpts ++
      Seq(s"-Xms$javaMemory", s"-Xmx$javaMemory") ++
      Seq(mainClass) ++
      args.slice(8, args.size)

    command.foreach(println)

    val builder = new ProcessBuilder(command)
    val process = builder.start()
    new RedirectThread(System.in, process.getOutputStream, "redirect stdin").start()
    new RedirectThread(process.getInputStream, System.out, "redirect stdout").start()
    new RedirectThread(process.getErrorStream, System.err, "redirect stderr").start()
    System.exit(process.waitFor())
  }

}
