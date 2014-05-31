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
import java.util.jar.JarFile

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.SparkException
import org.apache.spark.util.Utils

/**
 * Parses and encapsulates arguments from the spark-submit script.
 */
private[spark] class SparkSubmitArguments(args: Seq[String]) {
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
  var isPython: Boolean = false
  var pyFiles: String = null

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
        } else {
          SparkSubmit.printWarning(s"Ignoring non-spark config property: $k=$v")
        }
      }
    }
    defaultProperties
  }

  /** Fill in any undefined values based on the current properties file or built-in defaults. */
  private def loadDefaults(): Unit = {

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

    // Try to set main class from JAR if no --class argument is given
    if (mainClass == null && !isPython && primaryResource != null) {
      try {
        val jar = new JarFile(primaryResource)
        // Note that this might still return null if no main-class is set; we catch that later
        mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
      } catch {
        case e: Exception =>
          SparkSubmit.printErrorAndExit("Cannot load main class from JAR: " + primaryResource)
          return
      }
    }

    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    master = Option(master).getOrElse("local[*]")

    // Set name from main class if not given
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = Utils.stripDirectory(primaryResource)
    }
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def checkRequiredArguments() = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
    if (primaryResource == null) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python file)")
    }
    if (mainClass == null && !isPython) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (pyFiles != null && !isPython) {
      SparkSubmit.printErrorAndExit("--py-files given but primary resource is not a Python script")
    }

    // Require all python files to be local, so we can add them to the PYTHONPATH
    if (isPython) {
      if (Utils.nonLocalPaths(primaryResource).nonEmpty) {
        SparkSubmit.printErrorAndExit(s"Only local python files are supported: $primaryResource")
      }
      val nonLocalPyFiles = Utils.nonLocalPaths(pyFiles).mkString(",")
      if (nonLocalPyFiles.nonEmpty) {
        SparkSubmit.printErrorAndExit(
          s"Only local additional python files are supported: $nonLocalPyFiles")
      }
    }

    if (master.startsWith("yarn")) {
      val hasHadoopEnv = sys.env.contains("HADOOP_CONF_DIR") || sys.env.contains("YARN_CONF_DIR")
      if (!hasHadoopEnv && !Utils.isTesting) {
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
    |  pyFiles                 $pyFiles
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

  /** Fill in values by parsing user options. */
  private def parseOpts(opts: Seq[String]): Unit = {
    // Delineates parsing of Spark options from parsing of user options.
    var inSparkOpts = true
    parse(opts)

    def parse(opts: Seq[String]): Unit = opts match {
      case ("--name") :: value :: tail =>
        name = value
        parse(tail)

      case ("--master") :: value :: tail =>
        master = value
        parse(tail)

      case ("--class") :: value :: tail =>
        mainClass = value
        parse(tail)

      case ("--deploy-mode") :: value :: tail =>
        if (value != "client" && value != "cluster") {
          SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value
        parse(tail)

      case ("--num-executors") :: value :: tail =>
        numExecutors = value
        parse(tail)

      case ("--total-executor-cores") :: value :: tail =>
        totalExecutorCores = value
        parse(tail)

      case ("--executor-cores") :: value :: tail =>
        executorCores = value
        parse(tail)

      case ("--executor-memory") :: value :: tail =>
        executorMemory = value
        parse(tail)

      case ("--driver-memory") :: value :: tail =>
        driverMemory = value
        parse(tail)

      case ("--driver-cores") :: value :: tail =>
        driverCores = value
        parse(tail)

      case ("--driver-class-path") :: value :: tail =>
        driverExtraClassPath = value
        parse(tail)

      case ("--driver-java-options") :: value :: tail =>
        driverExtraJavaOptions = value
        parse(tail)

      case ("--driver-library-path") :: value :: tail =>
        driverExtraLibraryPath = value
        parse(tail)

      case ("--properties-file") :: value :: tail =>
        propertiesFile = value
        parse(tail)

      case ("--supervise") :: tail =>
        supervise = true
        parse(tail)

      case ("--queue") :: value :: tail =>
        queue = value
        parse(tail)

      case ("--files") :: value :: tail =>
        files = Utils.resolveURIs(value)
        parse(tail)

      case ("--py-files") :: value :: tail =>
        pyFiles = Utils.resolveURIs(value)
        parse(tail)

      case ("--archives") :: value :: tail =>
        archives = Utils.resolveURIs(value)
        parse(tail)

      case ("--jars") :: value :: tail =>
        jars = Utils.resolveURIs(value)
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case ("--verbose" | "-v") :: tail =>
        verbose = true
        parse(tail)

      case value :: tail =>
        if (inSparkOpts) {
          value match {
            // convert --foo=bar to --foo bar
            case v if v.startsWith("--") && v.contains("=") && v.split("=").size == 2 =>
              val parts = v.split("=")
              parse(Seq(parts(0), parts(1)) ++ tail)
            case v if v.startsWith("-") =>
              val errMessage = s"Unrecognized option '$value'."
              SparkSubmit.printErrorAndExit(errMessage)
            case v =>
              primaryResource =
                if (!SparkSubmit.isShell(v)) {
                  Utils.resolveURI(v).toString
                } else {
                  v
                }
              inSparkOpts = false
              isPython = SparkSubmit.isPython(v)
              parse(tail)
          }
        } else {
          if (!value.isEmpty) {
            childArgs += value
          }
          parse(tail)
        }

      case Nil =>
    }
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    val outStream = SparkSubmit.printStream
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    outStream.println(
      """Usage: spark-submit [options] <app jar | python file> [app options]
        |Options:
        |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
        |  --deploy-mode DEPLOY_MODE   Where to run the driver program: either "client" to run
        |                              on the local machine, or "cluster" to run inside cluster.
        |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
        |  --name NAME                 A name of your application.
        |  --jars JARS                 Comma-separated list of local jars to include on the driver
        |                              and executor classpaths.
        |  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
        |                              on the PYTHONPATH for Python apps.
        |  --files FILES               Comma-separated list of files to be placed in the working
        |                              directory of each executor.
        |  --properties-file FILE      Path to a file from which to load extra properties. If not
        |                              specified, this will look for conf/spark-defaults.conf.
        |
        |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 512M).
        |  --driver-java-options       Extra Java options to pass to the driver.
        |  --driver-library-path       Extra library path entries to pass to the driver.
        |  --driver-class-path         Extra class path entries to pass to the driver. Note that
        |                              jars added with --jars are automatically included in the
        |                              classpath.
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
        |  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
        |  --num-executors NUM         Number of executors to launch (Default: 2).
        |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
        |                              working directory of each executor.""".stripMargin
    )
    SparkSubmit.exitFn()
  }
}

object SparkSubmitArguments {
  /** Load properties present in the given file. */
  def getPropertiesFromFile(file: File): Seq[(String, String)] = {
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")
    val inputStream = new FileInputStream(file)
    try {
      val properties = new Properties()
      properties.load(inputStream)
      properties.stringPropertyNames().toSeq.map(k => (k, properties(k).trim))
    } catch {
      case e: IOException =>
        val message = s"Failed when loading Spark properties file $file"
        throw new SparkException(message, e)
    } finally {
      inputStream.close()
    }
  }
}
