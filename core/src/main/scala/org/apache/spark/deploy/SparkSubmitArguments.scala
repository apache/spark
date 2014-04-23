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

import java.io.{File, FileInputStream, IOException}
import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ArrayBuffer}

import org.apache.spark.SparkException

/**
 * Parses and encapsulates arguments from the spark-submit script.
 */
private[spark] class SparkSubmitArguments(args: Array[String]) {
  var master: String = null
  var deployMode: String = null
  var executorMemory: String = null
  var executorCores: String = null
  var totalExecutorCores: String = null
  var propertiesFile: String = null
  var driverMemory: String = null
  var driverExtraClassPath: String = null
  var driverExtraLibraryPath: String = null
  var driverExtraJavaOptions: String = null
  var driverCores: String = null
  var supervise: Boolean = false
  var queue: String = null
  var numExecutors: String = null
  var files: String = null
  var archives: String = null
  var mainClass: String = null
  var primaryResource: String = null
  var name: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var jars: String = null
  var verbose: Boolean = false

  parseOpts(args.toList)
  loadDefaults()
  checkRequiredArguments()

  /** Return default present in the currently defined defaults file. */
  def getDefaultSparkProperties = {
    val defaultProperties = new HashMap[String, String]()
    if (verbose) SparkSubmit.printStream.println(s"Using properties file: $propertiesFile")
    Option(propertiesFile).foreach { filename =>
      val file = new File(filename)
      SparkSubmitArguments.getPropertiesFromFile(file).foreach { case (k, v) =>
        if (k.startsWith("spark")) {
          defaultProperties(k) = v
          if (verbose) SparkSubmit.printStream.println(s"Adding default property: $k=$v")
        }
        else {
          SparkSubmit.printWarning(s"Ignoring non-spark config property: $k=$v")
        }
      }
    }
    defaultProperties
  }

  /** Fill in any undefined values based on the current properties file or built-in defaults. */
  private def loadDefaults() = {

    // Use common defaults file, if not specified by user
    if (propertiesFile == null) {
      sys.env.get("SPARK_HOME").foreach { sparkHome =>
        val sep = File.separator
        val defaultPath = s"${sparkHome}${sep}conf${sep}spark-defaults.conf"
        val file = new File(defaultPath)
        if (file.exists()) {
          propertiesFile = file.getAbsolutePath
        }
      }
    }

    val defaultProperties = getDefaultSparkProperties
    // Use properties file as fallback for values which have a direct analog to
    // arguments in this script.
    master = Option(master).getOrElse(defaultProperties.get("spark.master").orNull)
    executorMemory = Option(executorMemory)
      .getOrElse(defaultProperties.get("spark.executor.memory").orNull)
    executorCores = Option(executorCores)
      .getOrElse(defaultProperties.get("spark.executor.cores").orNull)
    totalExecutorCores = Option(totalExecutorCores)
      .getOrElse(defaultProperties.get("spark.cores.max").orNull)
    name = Option(name).getOrElse(defaultProperties.get("spark.app.name").orNull)
    jars = Option(jars).getOrElse(defaultProperties.get("spark.jars").orNull)

    // This supports env vars in older versions of Spark
    master = Option(master).getOrElse(System.getenv("MASTER"))
    deployMode = Option(deployMode).getOrElse(System.getenv("DEPLOY_MODE"))

    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    master = Option(master).getOrElse("local")
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def checkRequiredArguments() = {
    if (args.length == 0) printUsageAndExit(-1)
    if (primaryResource == null) SparkSubmit.printErrorAndExit("Must specify a primary resource")
    if (mainClass == null) SparkSubmit.printErrorAndExit("Must specify a main class with --class")

    if (master.startsWith("yarn")) {
      val hasHadoopEnv = sys.env.contains("HADOOP_CONF_DIR") || sys.env.contains("YARN_CONF_DIR")
      val testing = sys.env.contains("SPARK_TESTING")
      if (!hasHadoopEnv && !testing) {
        throw new Exception(s"When running with master '$master' " +
          "either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.")
      }
    }
  }

  override def toString =  {
    s"""Parsed arguments:
    |  master                  $master
    |  deployMode              $deployMode
    |  executorMemory          $executorMemory
    |  executorCores           $executorCores
    |  totalExecutorCores      $totalExecutorCores
    |  propertiesFile          $propertiesFile
    |  driverMemory            $driverMemory
    |  driverCores             $driverCores
    |  driverExtraClassPath    $driverExtraClassPath
    |  driverExtraLibraryPath  $driverExtraLibraryPath
    |  driverExtraJavaOptions  $driverExtraJavaOptions
    |  supervise               $supervise
    |  queue                   $queue
    |  numExecutors            $numExecutors
    |  files                   $files
    |  archives                $archives
    |  mainClass               $mainClass
    |  primaryResource         $primaryResource
    |  name                    $name
    |  childArgs               [${childArgs.mkString(" ")}]
    |  jars                    $jars
    |  verbose                 $verbose
    |
    |Default properties from $propertiesFile:
    |${getDefaultSparkProperties.mkString("  ", "\n  ", "\n")}
    """.stripMargin
  }

  private def parseOpts(opts: List[String]): Unit = opts match {
    case ("--name") :: value :: tail =>
      name = value
      parseOpts(tail)

    case ("--master") :: value :: tail =>
      master = value
      parseOpts(tail)

    case ("--class") :: value :: tail =>
      mainClass = value
      parseOpts(tail)

    case ("--deploy-mode") :: value :: tail =>
      if (value != "client" && value != "cluster") {
        SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
      }
      deployMode = value
      parseOpts(tail)

    case ("--num-executors") :: value :: tail =>
      numExecutors = value
      parseOpts(tail)

    case ("--total-executor-cores") :: value :: tail =>
      totalExecutorCores = value
      parseOpts(tail)

    case ("--executor-cores") :: value :: tail =>
      executorCores = value
      parseOpts(tail)

    case ("--executor-memory") :: value :: tail =>
      executorMemory = value
      parseOpts(tail)

    case ("--driver-memory") :: value :: tail =>
      driverMemory = value
      parseOpts(tail)

    case ("--driver-cores") :: value :: tail =>
      driverCores = value
      parseOpts(tail)

    case ("--driver-class-path") :: value :: tail =>
      driverExtraClassPath = value
      parseOpts(tail)

    case ("--driver-java-options") :: value :: tail =>
      driverExtraJavaOptions = value
      parseOpts(tail)

    case ("--driver-library-path") :: value :: tail =>
      driverExtraLibraryPath = value
      parseOpts(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parseOpts(tail)

    case ("--supervise") :: tail =>
      supervise = true
      parseOpts(tail)

    case ("--queue") :: value :: tail =>
      queue = value
      parseOpts(tail)

    case ("--files") :: value :: tail =>
      files = value
      parseOpts(tail)

    case ("--archives") :: value :: tail =>
      archives = value
      parseOpts(tail)

    case ("--arg") :: value :: tail =>
      childArgs += value
      parseOpts(tail)

    case ("--jars") :: value :: tail =>
      jars = value
      parseOpts(tail)

    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case ("--verbose" | "-v") :: tail =>
      verbose = true
      parseOpts(tail)

    case value :: tail =>
      if (value.startsWith("-")) {
        val errMessage = s"Unrecognized option '$value'."
        val suggestion: Option[String] = value match {
          case v if v.startsWith("--") && v.contains("=") =>
            val parts = v.split("=")
            Some(s"Perhaps you want '${parts(0)} ${parts(1)}'?")
          case _ =>
            None
        }
        SparkSubmit.printErrorAndExit(errMessage + suggestion.map(" " + _).getOrElse(""))
      }

      if (primaryResource != null) {
        val error = s"Found two conflicting resources, $value and $primaryResource." +
          " Expecting only one resource."
        SparkSubmit.printErrorAndExit(error)
      }
      primaryResource = value
      parseOpts(tail)

    case Nil =>
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    val outStream = SparkSubmit.printStream
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    outStream.println(
      """Usage: spark-submit <app jar> [options]
        |Options:
        |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
        |  --deploy-mode DEPLOY_MODE   Mode to deploy the app in, either 'client' or 'cluster'.
        |  --class CLASS_NAME          Name of your app's main class (required for Java apps).
        |  --arg ARG                   Argument to be passed to your application's main class. This
        |                              option can be specified multiple times for multiple args.
        |  --name NAME                 The name of your application (Default: 'Spark').
        |  --jars JARS                 A comma-separated list of local jars to include on the
        |                              driver classpath and that SparkContext.addJar will work
        |                              with. Doesn't work on standalone with 'cluster' deploy mode.
        |  --files FILES               Comma separated list of files to be placed in the working dir
        |                              of each executor.
        |  --properties-file FILE      Path to a file from which to load extra properties. If not
        |                              specified, this will look for conf/spark-defaults.conf.
        |
        |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 512M).
        |  --driver-java-options       Extra Java options to pass to the driver
        |  --driver-library-path       Extra library path entries to pass to the driver
        |  --driver-class-path         Extra class path entries to pass to the driver
        |
        |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).
        |
        | Spark standalone with cluster deploy mode only:
        |  --driver-cores NUM          Cores for driver (Default: 1).
        |  --supervise                 If given, restarts the driver on failure.
        |
        | Spark standalone and Mesos only:
        |  --total-executor-cores NUM  Total cores for all executors.
        |
        | YARN-only:
        |  --executor-cores NUM        Number of cores per executor (Default: 1).
        |  --queue QUEUE_NAME          The YARN queue to submit to (Default: 'default').
        |  --num-executors NUM         Number of executors to (Default: 2).
        |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
        |                              working dir of each executor.""".stripMargin
    )
    SparkSubmit.exitFn()
  }
}

object SparkSubmitArguments {
  /** Load properties present in the given file. */
  def getPropertiesFromFile(file: File): Seq[(String, String)] = {
    require(file.exists(), s"Properties file ${file.getName} does not exist")
    val inputStream = new FileInputStream(file)
    val properties = new Properties()
    try {
      properties.load(inputStream)
    } catch {
      case e: IOException =>
        val message = s"Failed when loading Spark properties file ${file.getName}"
        throw new SparkException(message, e)
    }
    properties.stringPropertyNames().toSeq.map(k => (k, properties(k)))
  }
}
