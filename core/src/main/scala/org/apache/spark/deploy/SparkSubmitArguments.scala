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

import java.io._
import java.util.jar.JarFile
import java.util.Properties

import scala.collection._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.commons.lang3.CharEncoding

import org.apache.spark.deploy.ConfigConstants._
import org.apache.spark.util.Utils



/**
 * Pulls configuration information together in order of priority
 *
 * Entries in the conf Map will be filled in the following priority order
 * 1. entries specified on the command line (except from --conf entries)
 * 2. Entries specified on the command line with --conf
 * 3. Environment variables (including legacy variable mappings)
 * 4. System config properties (eg by using -Dspark.var.name)
 * 5  SPARK_DEFAULT_CONF/spark-defaults.conf or SPARK_HOME/conf/spark-defaults.conf if either exist
 * 6. hard coded defaults
 *
*/
private[spark] class SparkSubmitArguments(args: Seq[String]) {
  /**
   * Stores all configuration items except for child arguments,
   * referenced by the constants defined in ConfigConstants.scala.
   */
  val conf = new mutable.HashMap[String, String]()

  def master  = conf(SPARK_MASTER)
  def master_= (value: String):Unit = conf.put(SPARK_MASTER, value)

  def deployMode = conf(SPARK_DEPLOY_MODE)
  def deployMode_= (value: String):Unit = conf.put(SPARK_DEPLOY_MODE, value)

  def executorMemory = conf(SPARK_EXECUTOR_MEMORY)
  def executorMemory_= (value: String):Unit = conf.put(SPARK_EXECUTOR_MEMORY, value)

  def executorCores = conf(SPARK_EXECUTOR_CORES)
  def executorCores_= (value: String):Unit = conf.put(SPARK_EXECUTOR_CORES, value)

  def totalExecutorCores = conf.get(SPARK_CORES_MAX)
  def totalExecutorCores_= (value: String):Unit = conf.put(SPARK_CORES_MAX, value)

  def driverMemory = conf(SPARK_DRIVER_MEMORY)
  def driverMemory_= (value: String):Unit = conf.put(SPARK_DRIVER_MEMORY, value)

  def driverExtraClassPath = conf.get(SPARK_DRIVER_EXTRA_CLASSPATH)
  def driverExtraClassPath_= (value: String):Unit = conf.put(SPARK_DRIVER_EXTRA_CLASSPATH, value)

  def driverExtraLibraryPath = conf.get(SPARK_DRIVER_EXTRA_LIBRARY_PATH)
  def driverExtraLibraryPath_= (value: String):Unit = conf.put(SPARK_DRIVER_EXTRA_LIBRARY_PATH, value)

  def driverExtraJavaOptions = conf.get(SPARK_DRIVER_EXTRA_JAVA_OPTIONS)
  def driverExtraJavaOptions_= (value: String):Unit = conf.put(SPARK_DRIVER_EXTRA_JAVA_OPTIONS, value)

  def driverCores = conf(SPARK_DRIVER_CORES)
  def driverCores_= (value: String):Unit = conf.put(SPARK_DRIVER_CORES, value)

  def supervise = conf(SPARK_DRIVER_SUPERVISE) == true.toString
  def supervise_= (value: String):Unit = conf.put(SPARK_DRIVER_SUPERVISE, value)

  def queue = conf(SPARK_YARN_QUEUE)
  def queue_= (value: String):Unit = conf.put(SPARK_YARN_QUEUE, value)

  def numExecutors = conf(SPARK_EXECUTOR_INSTANCES)
  def numExecutors_= (value: String):Unit = conf.put(SPARK_EXECUTOR_INSTANCES, value)

  def files = conf.get(SPAKR_FILES)
  def files_= (value: String):Unit = conf.put(SPAKR_FILES, value)

  def archives = conf.get(SPARK_YARN_DIST_ARCHIVES)
  def archives_= (value: String):Unit = conf.put(SPARK_YARN_DIST_ARCHIVES, value)

  def mainClass = conf.get(SPARK_APP_CLASS)
  def mainClass_= (value: String):Unit = conf.put(SPARK_APP_CLASS, value)

  def primaryResource = conf(SPARK_APP_PRIMARY_RESOURCE)
  def primaryResource_= (value: String):Unit = conf.put(SPARK_APP_PRIMARY_RESOURCE, value)

  def name = conf(SPARK_APP_NAME)
  def name_= (value: String):Unit = conf.put(SPARK_APP_NAME, value)

  def jars = conf.get(SPARK_JARS)
  def jars_= (value: String):Unit = conf.put(SPARK_JARS, value)

  def pyFiles = conf.get(SPARK_SUBMIT_PYFILES)
  def pyFiles_= (value: String):Unit = conf.put(SPARK_SUBMIT_PYFILES, value)

  lazy val verbose = conf(SPARK_VERBOSE) == true.toString
  lazy val isPython = primaryResource != null && SparkSubmit.isPython(primaryResource)

  var childArgs = new mutable.ArrayBuffer[String]
  
  /**
   * Used to store parameters parsed from command line (except for --conf and child arguments)
   */
  private val cmdLineConfig = new mutable.HashMap[String, String]

  /**
   * arguments passed via --conf command line options
    */
  private val cmdLineConfConfig = new mutable.HashMap[String, String]

  /**
   * Values from a property file specified with --properties
   */
  private val cmdLinePropertyFileValues = new mutable.HashMap[String,String]

  try {
    // parse command line options
    parseOpts(args.toList)

    // if property file exists then update the command line arguments, but don't override
    // existing arguments
    cmdLinePropertyFileValues.foreach{ case(k,v) =>
      cmdLineConfig.getOrElseUpdate(k,v)
    }

    // See comments at start of class definition for the location and priority of configuration sources.
    conf ++= SparkSubmitArguments.mergeSparkProperties(Seq(cmdLineConfig, cmdLineConfConfig))

    // Some configuration items can be derived here if they are not yet present.
    deriveConfigurations()

    checkRequiredArguments()
  } catch {
    // ApplicationExitExceptions should only occur during debugging.
    case e: SparkSubmit.ApplicationExitException =>
    // IOException are possible when we are attempting to read property files.
    case e: IOException => SparkSubmit.printErrorAndExit(e.getLocalizedMessage)
  }

  private def deriveConfigurations() = {
    // These config items point to file paths, but may need to be converted to absolute file uris.
    val configFileUris = List(SPAKR_FILES, SPARK_SUBMIT_PYFILES, SPARK_YARN_DIST_ARCHIVES,
      SPARK_JARS, SPARK_APP_PRIMARY_RESOURCE)

    // Process configFileUris with resolvedURIs function if they are present.
    configFileUris
      .filter { key => conf.contains(key) &&
        ((key != SPARK_APP_PRIMARY_RESOURCE) || (!SparkSubmit.isInternalOrShell(conf(key))))}
      .foreach { key =>
        conf.put(key, Utils.resolveURIs(conf(key), testWindows=false))
      }

    // Try to set main class from JAR if no --class argument is given.
    if (mainClass == null && !isPython && primaryResource != null) {
      try {
        val jar = new JarFile(primaryResource)
        // Note that this might still return null if no main-class is set; we catch that later.
        val manifestMainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
        if (manifestMainClass != null && !manifestMainClass.isEmpty) {
          mainClass = manifestMainClass
        }
      } catch {
        case e: Exception =>
          SparkSubmit.printErrorAndExit("Cannot load main class from JAR: " + primaryResource)
      }
    }
    conf.get(SPARK_MASTER) match {
       case Some("yarn-standalone") =>
          SparkSubmit.printWarning("'yarn-standalone' is deprecated. Use 'yarn-cluster' instead.")
          master = "yarn-cluster"
       case _ =>
    }

    // Set name from main class if not given.
    // Todo: test main spark app name alternatives
    name = conf.get(SPARK_APP_NAME)
      .orElse( conf.get(SPARK_APP_CLASS))
      .orElse( conf.get(SPARK_APP_PRIMARY_RESOURCE).map(x => Utils.stripDirectory(x)) )
      .orNull
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def checkRequiredArguments() = {
    if (!conf.isDefinedAt(SPARK_APP_PRIMARY_RESOURCE)) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python file)")
    }
    if (!conf.isDefinedAt(SPARK_APP_CLASS)) {
      SparkSubmit.printErrorAndExit("No main class")
    }
    if (deployMode != "client" && deployMode != "cluster") {
      SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
    }
    if (!conf.isDefinedAt(SPARK_APP_CLASS) && !isPython) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (conf.isDefinedAt(SPARK_SUBMIT_PYFILES) && !isPython) {
      SparkSubmit.printErrorAndExit("--py-files given but primary resource is not a Python script")
    }

    // Require all python files to be local, so we can add them to the PYTHONPATH.
    if (isPython) {
      if (Utils.nonLocalPaths(primaryResource).nonEmpty) {
        SparkSubmit.printErrorAndExit(s"Only local python files are supported: $primaryResource")
      }
      val nonLocalPyFiles = Utils.nonLocalPaths(pyFiles.getOrElse("")).mkString(",")
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
    conf.mkString("\n")
  }

  /** Fill in values by parsing user options. */
  private def parseOpts(opts: Seq[String]): Unit = {
    val EQ_SEPARATED_OPT="""(--[^=]+)=(.+)""".r

    // Delineates parsing of Spark options from parsing of user options.
    parse(opts)

    /**
     * NOTE: If you add or remove spark-submit options,
     * modify NOT ONLY this file but also utils.sh
     */
    def parse(opts: Seq[String]): Unit = opts match {
      case ("--name") :: value :: tail =>
        cmdLineConfig.put(SPARK_APP_NAME, value)
        parse(tail)

      case ("--master") :: value :: tail =>
        cmdLineConfig.put(SPARK_MASTER, value)
        parse(tail)

      case ("--class") :: value :: tail =>
        cmdLineConfig.put(SPARK_APP_CLASS, value)
        parse(tail)

      case ("--deploy-mode") :: value :: tail =>
        cmdLineConfig.put(SPARK_DEPLOY_MODE, value)
        parse(tail)

      case ("--num-executors") :: value :: tail =>
        cmdLineConfig.put(SPARK_EXECUTOR_INSTANCES, value)
        parse(tail)

      case ("--total-executor-cores") :: value :: tail =>
        cmdLineConfig.put(SPARK_CORES_MAX, value)
        parse(tail)

      case ("--executor-cores") :: value :: tail =>
        cmdLineConfig.put(SPARK_EXECUTOR_CORES, value)
        parse(tail)

      case ("--executor-memory") :: value :: tail =>
        cmdLineConfig.put(SPARK_EXECUTOR_MEMORY, value)
        parse(tail)

      case ("--driver-memory") :: value :: tail =>
        cmdLineConfig.put(SPARK_DRIVER_MEMORY, value)
        parse(tail)

      case ("--driver-cores") :: value :: tail =>
        cmdLineConfig.put(SPARK_DRIVER_CORES, value)
        parse(tail)

      case ("--driver-class-path") :: value :: tail =>
        cmdLineConfig.put(SPARK_DRIVER_EXTRA_CLASSPATH, value)
        parse(tail)

      case ("--driver-java-options") :: value :: tail =>
        cmdLineConfig.put(SPARK_DRIVER_EXTRA_JAVA_OPTIONS, value)
        parse(tail)

      case ("--driver-library-path") :: value :: tail =>
        cmdLineConfig.put(SPARK_DRIVER_EXTRA_LIBRARY_PATH, value)
        parse(tail)

      case ("--properties-file") :: value :: tail =>
        /*  We merge the property file config options into the rest of the command lines options
         *  after we have finished the rest of the command line processing as property files
         *  cannot override explicit command line options .
         */
        cmdLinePropertyFileValues ++= SparkSubmitArguments.getPropertyValuesFromFile(value)
        parse(tail)

      case ("--supervise") :: tail =>
        cmdLineConfig.put(SPARK_DRIVER_SUPERVISE, true.toString)
        parse(tail)

      case ("--queue") :: value :: tail =>
        cmdLineConfig.put(SPARK_YARN_QUEUE, value)
        parse(tail)

      case ("--files") :: value :: tail =>
        cmdLineConfig.put(SPAKR_FILES, value)
        parse(tail)

      case ("--py-files") :: value :: tail =>
        cmdLineConfig.put(SPARK_SUBMIT_PYFILES, value)
        parse(tail)

      case ("--archives") :: value :: tail =>
        cmdLineConfig.put(SPARK_YARN_DIST_ARCHIVES, value)
        parse(tail)

      case ("--jars") :: value :: tail =>
        cmdLineConfig.put(SPARK_JARS, value)
        parse(tail)

      case ("--conf" | "-c") :: value :: tail =>
        value.split("=", 2).toSeq match {
          case Seq(k, v) => cmdLineConfConfig(k) = v
          case _ => SparkSubmit.printErrorAndExit(s"Spark config without '=': $value")
        }
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case ("--verbose" | "-v") :: tail =>
        cmdLineConfig.put(SPARK_VERBOSE, true.toString)
        parse(tail)

      case EQ_SEPARATED_OPT(opt, value) :: tail =>
        parse(opt :: value :: tail)

      case value :: tail if value.startsWith("-") =>
        SparkSubmit.printErrorAndExit(s"Unrecognized option '$value'.")

      case value :: tail =>
        cmdLineConfig.put(SPARK_APP_PRIMARY_RESOURCE, value)
        childArgs ++= tail

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
        |  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
        |                              on one of the worker machines inside the cluster ("cluster")
        |                              (Default: client).
        |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
        |  --name NAME                 A name of your application.
        |  --jars JARS                 Comma-separated list of local jars to include on the driver
        |                              and executor classpaths.
        |  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
        |                              on the PYTHONPATH for Python apps.
        |  --files FILES               Comma-separated list of files to be placed in the working
        |                              directory of each executor.
        |
        |  --conf PROP=VALUE           Arbitrary Spark configuration property.
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
        |  --help, -h                  Show this help message and exit
        |  --verbose, -v               Print additional debug output
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


private[spark] object SparkSubmitArguments {
  /**
   * Default property values - string literals are defined in ConfigConstants.scala
   */
  val DEFAULTS = Map(
    SPARK_MASTER -> "local[*]",
    SPARK_VERBOSE -> "false",
    SPARK_DEPLOY_MODE -> "client",
    SPARK_EXECUTOR_MEMORY -> "1G",
    SPARK_EXECUTOR_CORES -> "1" ,
    SPARK_EXECUTOR_INSTANCES -> "2",
    SPARK_DRIVER_MEMORY -> "512M",
    SPARK_DRIVER_CORES -> "1",
    SPARK_DRIVER_SUPERVISE -> "false",
    SPARK_YARN_QUEUE -> "default",
    SPARK_EXECUTOR_INSTANCES -> "2"
  )

  /**
   * Function returns the spark submit default config map (Map[configName->ConfigValue])
   * Function is over-writable to allow for easier debugging
   */
  private[spark] var getHardCodedDefaultValues: () => Map[String, String] = () => {
    DEFAULTS
  }

  /**
   * System environment variables.
   * Function is over-writable to allow for easier debugging
   */
  private[spark] var genEnvVars: () => Map[String, String] = () => sys.env

  /**
   * Gets configuration from reading SPARK_CONF_DIR/spark-defaults.conf if it exists
   * otherwise reads SPARK_HOME/conf/spark-defaults.conf if it exists
   * otherwise returns an empty config structure
   * Function is over-writable to allow for easier debugging
   * @return Map[PropName->PropValue] or empty map if file does not exist
   * @throws IOException if unable to access a specified spark-defaults.conf file
   */
  private[spark] var getSparkDefaultFileConfig: () => Map[String, String] = () => {
    val baseConfDir: Option[String] = sys.env.get(ENV_SPARK_HOME).map(_ + File.separator + DIR_NAME_SPARK_CONF)
    val altConfDir: Option[String] = sys.env.get(ENV_ALT_SPARK_CONF_PATH)
    val confDir: Option[String] = altConfDir.orElse(baseConfDir)
    val confPath =  confDir.map(path => path + File.separator + FILENAME_SPARK_DEFAULTS_CONF)
    try {
      confPath.flatMap { getFileIfExists }
        .map{ loadPropFile }
        .getOrElse(Map.empty)
    } catch {
      // If an IOException occurs, report which file we were trying to open.
      case e: IOException => throw new IOException("IOException reading spark-defaults.conf file at " +
        confPath.getOrElse("unknown address"), e)
    }

  }

  /**
   * Resolves Configuration sources in order of highest to lowest
   * 1. Each map passed in as additionalConfig from first to last
   * 2. Environment variables (including legacy variable mappings)
   * 3. System config properties (eg by using -Dspark.var.name)
   * 4  SPARK_DEFAULT_CONF/spark-defaults.conf or SPARK_HOME/conf/spark-defaults.conf
   * 5. hard coded defaults
   *
   * @param additionalConfigs Seq of additional Map[ConfigName->ConfigValue] in order of highest
   *                          priority to lowest this will have priority over internal sources.
   * @return Map[propName->propFile] containing values merged from all sources in order of priority.
   */
  def mergeSparkProperties(additionalConfigs: Seq [Map[String,String]]) = {
    val hardCodedDefaultConfig: Map[String,String] = getHardCodedDefaultValues()

    // Read in configuration from the spark defaults conf file if it exists.
    val sparkDefaultConfig = getSparkDefaultFileConfig()

    // Map legacy variables to their equivalent full name config variable
    val legacyEnvVars = Seq(
      "MASTER" -> SPARK_MASTER,
      "DEPLOY_MODE" -> SPARK_DEPLOY_MODE,
      "SPARK_DRIVER_MEMORY" -> SPARK_DRIVER_MEMORY,
      "SPARK_EXECUTOR_MEMORY" -> SPARK_EXECUTOR_MEMORY)

    var envVarConfig = genEnvVars() ++ legacyEnvVars
      .filter { case (k, _) => sys.env.contains(k) }
      .map { case (k, v) => (v, sys.env(k)) }

    Utils.mergePropertyMaps( additionalConfigs ++ Seq(
      envVarConfig,
      sys.props,
      sparkDefaultConfig,
      hardCodedDefaultConfig
    ))
  }

  /**
   * Returns an Optional File if it exists
   * @param filePath Path to create a file object on
   * @return Some(File) if file exists in file system None otherwise
   */
  def getFileIfExists(filePath: String): Option[File] = {
    val File = new File(filePath)
    if (File.exists) {
      Some(File)
    } else {
      None
    }
  }

  /**
   * Parses a property file using the java properties file parser
   * @param filePath Path to property file
   * @return Map of config values parsed from file
   * @throws FileNotFoundException if file does not exist
   * @throws IOException if file exists but is not accessible
   */
  def getPropertyValuesFromFile(filePath: String): Map[String, String] = {
    val propFile = new File(filePath)
    loadPropFile(propFile)
  }

  /**
   * returns a loaded property file
   * @param propFile File object pointing to properties file
   * @return java properties object
   * @throws FileNotFoundException if file does not exist
   * @throws IOException if file exists but is not accessible
   */
  def loadPropFile(propFile: File): Map[String, String] = {
    var isr: InputStreamReader = new InputStreamReader(new FileInputStream(propFile), CharEncoding.UTF_8)
    try {
      getPropertyValuesFromStream(isr)
    } finally {
      isr.close()
    }
  }

  /**
   * Loads property object from stream. the passed InputStreamReader is not closed
   * @param r Reader to load property file from
   * @return Map[PropName->PropValue]
   */
  def getPropertyValuesFromStream(r: Reader) = {
    val prop = new Properties()
    prop.load(r)
    prop.asInstanceOf[java.util.Map[String, String]]
  }
}
