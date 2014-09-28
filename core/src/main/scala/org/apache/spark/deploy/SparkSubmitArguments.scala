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

import java.io.{InputStreamReader, File, FileInputStream, InputStream}
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
 * 4. System config variables (eg by using -Dspark.var.name)
 * 5  SPARK_DEFAULT_CONF/spark-defaults.conf or SPARK_HOME/conf/spark-defaults.conf if either exist
 * 6. hard coded defaults in class path at spark-submit-defaults.prop
 *
 * A property file specified by one of the means listed above gets read in and the properties are
 * considered to be at the priority of the method that specified the files. A property specified in
 * a property file will not override an existing config value at that same level.
*/
private[spark] class SparkSubmitArguments(args: Seq[String]) {
  /**
   * Stores all configuration items except for child arguments,
   * referenced by the constants defined in ConfigConstants.scala.
   */
  val conf = new mutable.HashMap[String, String]()

  def master  = conf(SparkMaster)
  def master_= (value: String):Unit = conf.put(SparkMaster, value)

  def deployMode = conf(SparkDeployMode)
  def deployMode_= (value: String):Unit = conf.put(SparkDeployMode, value)

  def executorMemory = conf(SparkExecutorMemory)
  def executorMemory_= (value: String):Unit = conf.put(SparkExecutorMemory, value)

  def executorCores = conf(SparkExecutorCores)
  def executorCores_= (value: String):Unit = conf.put(SparkExecutorCores, value)

  def totalExecutorCores = conf.get(SparkCoresMax)
  def totalExecutorCores_= (value: String):Unit = conf.put(SparkCoresMax, value)

  def driverMemory = conf(SparkDriverMemory)
  def driverMemory_= (value: String):Unit = conf.put(SparkDriverMemory, value)

  def driverExtraClassPath = conf.get(SparkDriverExtraClassPath)
  def driverExtraClassPath_= (value: String):Unit = conf.put(SparkDriverExtraClassPath, value)

  def driverExtraLibraryPath = conf.get(SparkDriverExtraLibraryPath)
  def driverExtraLibraryPath_= (value: String):Unit = conf.put(SparkDriverExtraLibraryPath, value)

  def driverExtraJavaOptions = conf.get(SparkDriverExtraJavaOptions)
  def driverExtraJavaOptions_= (value: String):Unit = conf.put(SparkDriverExtraJavaOptions, value)

  def driverCores = conf(SparkDriverCores)
  def driverCores_= (value: String):Unit = conf.put(SparkDriverCores, value)

  def supervise = conf(SparkDriverSupervise) == true.toString
  def supervise_= (value: String):Unit = conf.put(SparkDriverSupervise, value)

  def queue = conf(SparkYarnQueue)
  def queue_= (value: String):Unit = conf.put(SparkYarnQueue, value)

  def numExecutors = conf(SparkExecutorInstances)
  def numExecutors_= (value: String):Unit = conf.put(SparkExecutorInstances, value)

  def files = conf.get(SparkFiles)
  def files_= (value: String):Unit = conf.put(SparkFiles, value)

  def archives = conf.get(SparkYarnDistArchives)
  def archives_= (value: String):Unit = conf.put(SparkYarnDistArchives, value)

  def mainClass = conf.get(SparkAppClass)
  def mainClass_= (value: String):Unit = conf.put(SparkAppClass, value)

  def primaryResource = conf.get(SparkAppPrimaryResource).get
  def primaryResource_= (value: String):Unit = conf.put(SparkAppPrimaryResource, value)

  def name = conf.get(SparkAppName)
  def name_= (value: String):Unit = conf.put(SparkAppName, value)

  def jars = conf.get(SparkJars)
  def jars_= (value: String):Unit = conf.put(SparkJars, value)

  def pyFiles = conf.get(SparkSubmitPyFiles)
  def pyFiles_= (value: String):Unit = conf.put(SparkSubmitPyFiles, value)

  lazy val verbose = conf(SparkVerbose) == true.toString
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
    case e: SparkSubmit.ApplicationExitException =>
      // We should only get here during test running.
  }

  private def deriveConfigurations() = {
    // These config items point to file paths, but may need to be converted to absolute file uris.
    val configFileUris = List(SparkFiles, SparkSubmitPyFiles, SparkYarnDistArchives,
      SparkJars, SparkAppPrimaryResource)

    // Process configFileUris with resolvedURIs function if they are present.
    configFileUris
      .filter { key => conf.contains(key) &&
        ((key != SparkAppPrimaryResource) || (!SparkSubmit.isInternalOrShell(conf(key))))}
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
    conf.get(SparkMaster) match {
       case Some("yarn-standalone") =>
          SparkSubmit.printWarning("'yarn-standalone' is deprecated. Use 'yarn-cluster' instead.")
          master = "yarn-cluster"
       case _ =>
    }
    // Set name from main class if not given.
    name = conf.get(SparkAppName)
      .orElse( conf.get(SparkAppClass))
      .getOrElse(if (conf.contains(SparkAppPrimaryResource))
      Utils.stripDirectory(primaryResource)
      else null)
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def checkRequiredArguments() = {
    conf.get(SparkPropertiesFile).foreach{ propFilePath =>
      val propFile = new File(propFilePath)
      if (!propFile.exists()) {
        SparkSubmit.printErrorAndExit(s"--property-file $propFilePath does not exists")
      }
    }
    if (!conf.isDefinedAt(SparkAppPrimaryResource)) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python file)")
    }
    if (!conf.isDefinedAt(SparkAppClass)) {
      SparkSubmit.printErrorAndExit("No main class")
    }
    if (deployMode != "client" && deployMode != "cluster") {
      SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
    }
    if (!conf.isDefinedAt(SparkAppClass) && !isPython) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (conf.isDefinedAt(SparkSubmitPyFiles) && !isPython) {
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
        cmdLineConfig.put(SparkAppName, value)
        parse(tail)

      case ("--master") :: value :: tail =>
        cmdLineConfig.put(SparkMaster, value)
        parse(tail)

      case ("--class") :: value :: tail =>
        cmdLineConfig.put(SparkAppClass, value)
        parse(tail)

      case ("--deploy-mode") :: value :: tail =>
        cmdLineConfig.put(SparkDeployMode, value)
        parse(tail)

      case ("--num-executors") :: value :: tail =>
        cmdLineConfig.put(SparkExecutorInstances, value)
        parse(tail)

      case ("--total-executor-cores") :: value :: tail =>
        cmdLineConfig.put(SparkCoresMax, value)
        parse(tail)

      case ("--executor-cores") :: value :: tail =>
        cmdLineConfig.put(SparkExecutorCores, value)
        parse(tail)

      case ("--executor-memory") :: value :: tail =>
        cmdLineConfig.put(SparkExecutorMemory, value)
        parse(tail)

      case ("--driver-memory") :: value :: tail =>
        cmdLineConfig.put(SparkDriverMemory, value)
        parse(tail)

      case ("--driver-cores") :: value :: tail =>
        cmdLineConfig.put(SparkDriverCores, value)
        parse(tail)

      case ("--driver-class-path") :: value :: tail =>
        cmdLineConfig.put(SparkDriverExtraClassPath, value)
        parse(tail)

      case ("--driver-java-options") :: value :: tail =>
        cmdLineConfig.put(SparkDriverExtraJavaOptions, value)
        parse(tail)

      case ("--driver-library-path") :: value :: tail =>
        cmdLineConfig.put(SparkDriverExtraLibraryPath, value)
        parse(tail)

      case ("--properties-file") :: value :: tail =>
        // Process the property file options after we have finished the rest of the command line
        cmdLinePropertyFileValues ++= SparkSubmitArguments.getPropertyValuesFromFile(value)
        parse(tail)

      case ("--supervise") :: tail =>
        cmdLineConfig.put(SparkDriverSupervise, true.toString)
        parse(tail)

      case ("--queue") :: value :: tail =>
        cmdLineConfig.put(SparkYarnQueue, value)
        parse(tail)

      case ("--files") :: value :: tail =>
        cmdLineConfig.put(SparkFiles, value)
        parse(tail)

      case ("--py-files") :: value :: tail =>
        cmdLineConfig.put(SparkSubmitPyFiles, value)
        parse(tail)

      case ("--archives") :: value :: tail =>
        cmdLineConfig.put(SparkYarnDistArchives, value)
        parse(tail)

      case ("--jars") :: value :: tail =>
        cmdLineConfig.put(SparkJars, value)
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
        cmdLineConfig.put(SparkVerbose, true.toString)
        parse(tail)

      case EQ_SEPARATED_OPT(opt, value) :: tail =>
        parse(opt :: value :: tail)

      case value :: tail if value.startsWith("-") =>
        SparkSubmit.printErrorAndExit(s"Unrecognized option '$value'.")

      case value :: tail =>
        cmdLineConfig.put(SparkAppPrimaryResource, value)
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
   * Resolves Configuration sources in order of highest to lowest
   * 1. Each map passed in as additionalConfig from first to last
   * 2. Environment variables (including legacy variable mappings)
   * 3. System config variables (eg by using -Dspark.var.name)
   * 4  SPARK_DEFAULT_CONF/spark-defaults.conf or SPARK_HOME/conf/spark-defaults.conf
   * 5. hard coded defaults in class path at spark-submit-defaults.prop
   *
   * A property file specified by one of the means listed above gets read in and the properties are
   * considered to be at the priority of the method that specified the files.
   * A property specified in a property file will not override an existing
   * config value at that same level.
   *
   * @param additionalConfigs Seq of additional Map[ConfigName->ConfigValue] in order of highest
   *                          priority to lowest this will have priority over internal sources.
   * @return Map[propName->propFile] containing values merged from all sources in order of priority.
   */
  def mergeSparkProperties(additionalConfigs: Seq [Map[String,String]]) = {
    // Configuration read in from spark-submit-defaults.prop file found on the classpath
    var hardCodedDefaultConfig: Option[Map[String,String]] = None


    var is: InputStream = null
    var isr: Option[InputStreamReader] = None
    try {
      is = Thread.currentThread().getContextClassLoader.getResourceAsStream(ClassPathSparkSubmitDefaults)

      // Only open InputStreamReader if the InputStream was successfully opened.
      isr = Option(is).map{is: InputStream =>
        new InputStreamReader(is, CharEncoding.UTF_8)
      }

      hardCodedDefaultConfig = isr.map( defaultValueStream =>
        SparkSubmitArguments.getPropertyValuesFromStream(defaultValueStream))
    } finally {
      Option(is).foreach(_.close)
      isr.foreach(_.close)
    }

    if (hardCodedDefaultConfig.isEmpty || (hardCodedDefaultConfig.get.size == 0)) {
      throw new IllegalStateException(s"Default values not found at classpath $ClassPathSparkSubmitDefaults")
    }

    // Read in configuration from the spark defaults conf file if it exists.
    var sparkDefaultConfig = SparkSubmitArguments.getSparkDefaultFileConfig

    // Read in configuration info from the java system properties.
    val systemPropertyConfig = sys.props

    // Read in Configuration variables from the environment to support legacy variables.
    val environmentConfig = System.getenv().asScala

    val legacyEnvVars = Seq(
      "MASTER" -> SparkMaster,
      "DEPLOY_MODE" -> SparkDeployMode,
      "SPARK_DRIVER_MEMORY" -> SparkDriverMemory,
      "SPARK_EXECUTOR_MEMORY" -> SparkExecutorMemory)

    // Legacy variables act at the priority of a system property.
    val propsWithEnvVars = new mutable.HashMap() ++ systemPropertyConfig ++
      legacyEnvVars.map( {case(varName, propName) => (environmentConfig.get(varName), propName) })
      .filter( {case(varVariable, _) => varVariable.isDefined && !varVariable.get.isEmpty} )
      .map{case(varVariable, propName) => (propName, varVariable.get)}

    val configSources  = additionalConfigs ++ Seq (
      environmentConfig,
      propsWithEnvVars,
      sparkDefaultConfig,
      hardCodedDefaultConfig.get
    )

    // Load properties file at priority level of source that specified the property file
    // loaded property file configs will not override existing configs at the priority
    // level the property file was specified at
    val processedConfigSource = configSources
      .map( configMap => getFileBasedPropertiesIfSpecified(configMap) ++ configMap)

    Utils.mergePropertyMaps(processedConfigSource)
  }

  /**
   * Returns a map of config values from a property file if
   * the passed configMap has a SparkPropertiesFile defined pointing to a file
   * @param configMap Map of config values to check for file path from
   * @return Map of config values if map holds a valid SparkPropertiesFile, Map.Empty otherwise
   */
  def getFileBasedPropertiesIfSpecified(configMap: Map[String, String]) = {
    if (configMap.contains(SparkPropertiesFile)) {
      SparkSubmitArguments.getPropertyValuesFromFile(configMap.get(SparkPropertiesFile).get)
    } else {
      Map.empty
    }
  }

  /**
   * Gets configuration from reading SPARK_CONF_DIR/spark-defaults.conf if it exists
   * otherwise reads SPARK_HOME/conf/spark-defaults.conf if it exists
   * otherwise returns an empty config structure
   * @return Map[PropName->PropValue] or empty map if file does not exist
   */
  def getSparkDefaultFileConfig: Map[String, String] = {
    val baseConfDir: Option[String] = sys.env.get(EnvSparkHome).map(_ + File.separator + DirNameSparkConf)
    val altConfDir: Option[String] = sys.env.get(EnvAltSparkConfPath)
    val confDir: Option[String] = altConfDir.orElse(baseConfDir)

    confDir.map(path => path + File.separator + FileNameSparkDefaultsConf)
      .flatMap{path: String =>
        val file = new File(path)
        if (file.exists) Some(file) else None
      }
      .map(confFile => loadPropFile(confFile))
      .getOrElse(Map.empty)
  }
    
  /**
   * Parses a property file using the java properties file parser
   * @param filePath Path to property file
   * @return Map of config values parsed from file, empty map if file does not exist
   */
  def getPropertyValuesFromFile(filePath: String): Map[String, String] = {
    val propFile = new File(filePath)
    loadPropFile(propFile)
  }

  /**
   * returns a loaded property file
   * @param propFile File object pointing to properties file
   * @return java properties object
   */
  def loadPropFile(propFile: File): Map[String, String] = {
    var fis: FileInputStream = null
    var isr: InputStreamReader = null
    var propValues: Map[String, String] = Map.empty

    try {
      fis = new FileInputStream(propFile)
      isr = Option(fis).map( fis => new InputStreamReader(fis, CharEncoding.UTF_8)).get

      propValues = getPropertyValuesFromStream(isr)
    } finally {
      Option(isr).foreach( _.close )
      Option(fis).foreach( _.close )
    }
    propValues
  }

  /**
   * Loads property object from stream. the passed InputStreamReader is not closed
   * @param isr Input stream reader to load property file from
   * @return Map[PropName->PropValue]
   */
  def getPropertyValuesFromStream(isr: InputStreamReader ) = {
    val prop = new Properties()
    prop.load(isr)
    prop.asInstanceOf[java.util.Map[String, String]]
  }
}
