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

import java.io.{InputStream, File, FileInputStream}
import java.util.jar.JarFile
import java.util.Properties
import java.util.{Map=>JavaMap}
import scala.collection.JavaConversions._

import org.apache.spark.deploy.ConfigConstants._
import org.apache.spark.util.Utils

import scala.collection._
import scala.collection.JavaConverters._
import scala.collection.{mutable=>m}

/**
 * Pulls configuration information together in order of priority
 *
 * Entries in the conf Map will be filled in the following priority order
 * 1. entries specified on the command line (except from --conf entries)
 * 3. Entries specified on the command line with --conf
 * 3. Environment variables (including legacy variable mappings)
 * 4. System config variables (eg by using -Dspark.var.name)
 * 5  SPARK_DEFAULT_CONF/spark-defaults.conf or SPARK_HOME/conf/spark-defaults.conf if either exist
 * 6. hard coded defaults in class path at spark-submit-defaults.prop
 *
 * A property file specified by one of the means listed above gets read in and the properties are
 * considered to be at the priority of the method that specified the files. A property specified in
 * a property file will not override an existing config value at that same level
*/
private[spark] class SparkSubmitArguments(args: Seq[String]) {
  /**
   * Stores all configuration items except for child arguments,
   * referenced by the constantsdefined in ConfigConstants.scala
   */
  val conf = new m.HashMap[String, String]()

  def master = conf.get(SparkMaster).get
  def master_= (value: String):Unit = conf.put(SparkMaster, value)

  def deployMode = conf.get(SparkDeployMode).get
  def deployMode_= (value: String):Unit = conf.put(SparkDeployMode, value)

  def executorMemory = conf.get(SparkExecutorMemory).get
  def executorMemory_= (value: String):Unit = conf.put(SparkExecutorMemory, value)

  def executorCores = conf.get(SparkExecutorCores).get
  def executorCores_= (value: String):Unit = conf.put(SparkExecutorCores, value)

  def totalExecutorCores = conf.get(SparkCoresMax)
  def totalExecutorCores_= (value: String):Unit = conf.put(SparkCoresMax, value)

  def driverMemory = conf.get(SparkDriverMemory).get
  def driverMemory_= (value: String):Unit = conf.put(SparkDriverMemory, value)

  def driverExtraClassPath = conf.get(SparkDriverExtraClassPath)
  def driverExtraClassPath_= (value: String):Unit = conf.put(SparkDriverExtraClassPath, value)

  def driverExtraLibraryPath = conf.get(SparkDriverExtraLibraryPath)
  def driverExtraLibraryPath_= (value: String):Unit = conf.put(SparkDriverExtraLibraryPath, value)

  def driverExtraJavaOptions = conf.get(SparkDriverExtraJavaOptions)
  def driverExtraJavaOptions_= (value: String):Unit = conf.put(SparkDriverExtraJavaOptions, value)

  def driverCores = conf.get(SparkDriverCores).get
  def driverCores_= (value: String):Unit = conf.put(SparkDriverCores, value)

  def supervise = conf.get(SparkDriverSupervise).get == true.toString
  def supervise_= (value: String):Unit = conf.put(SparkDriverSupervise, value)

  def queue = conf.get(SparkYarnQueue).get
  def queue_= (value: String):Unit = conf.put(SparkYarnQueue, value)

  def numExecutors = conf.get(SparkExecutorInstances).get
  def numExecutors_= (value: String):Unit = conf.put(SparkExecutorInstances, value)

  def files = conf.get(SparkFiles)
  def files_= (value: String):Unit = conf.put(SparkFiles, value)

  def archives = conf.get(SparkYarnDistArchives)
  def archives_= (value: String):Unit = conf.put(SparkYarnDistArchives, value)

  def mainClass = conf.get(SparkAppClass)
  def mainClass_= (value: String):Unit = conf.put(SparkAppClass, value)

  def primaryResource = conf.get(SparkAppPrimaryResource)
  def primaryResource_= (value: String):Unit = conf.put(SparkAppPrimaryResource, value)

  def name = conf.get(SparkAppName)
  def name_= (value: String):Unit = conf.put(SparkAppName, value)

  def jars = conf.get(SparkJars)
  def jars_= (value: String):Unit = conf.put(SparkJars, value)

  def pyFiles = conf.get(SparkSubmitPyFiles)
  def pyFiles_= (value: String):Unit = conf.put(SparkSubmitPyFiles, value)

  lazy val verbose: Boolean =  SparkVerbose == true.toString
  lazy val isPython: Boolean = primaryResource.isDefined && SparkSubmit.isPython(primaryResource.get)
  var childArgs: m.ArrayBuffer[String] = new m.ArrayBuffer[String]()

  // any child argument detected on command line are stored here.
  private var cmdLineChildArgs = new m.ArrayBuffer[String]()
  
  /**
   * Used to store parameters parsed from command line (except for --conf and child arguments)
   */
  private val cmdLineOptionConfig = new m.HashMap[String, String]()

  /**
   * arguments passed via --conf command line options
    */
  private val cmdLineConfOptionConfig = new m.HashMap[String, String]()


  parseOpts(args.toList)

  // see comments at start of file detailing the location and priority of configuration sources
  conf ++= SparkSubmitArguments.mergeSparkProperties(Vector(cmdLineOptionConfig, cmdLineConfOptionConfig))

  // some configuration items can be derived if there are not present
  deriveConfigurations()

  checkRequiredArguments()

  private def deriveConfigurations() = {

    // These config items point to file paths, but may need to be converted to absolute file uris
    val configFileUris = List(SparkFiles, SparkSubmitPyFiles, SparkYarnDistArchives,
      SparkJars, SparkAppPrimaryResource)

    val resolvedFileUris = for{
      id <- configFileUris
      if conf.isDefinedAt(id) &&
        ((id != SparkAppPrimaryResource) || (!SparkSubmit.isInternalOrShell(conf.get(id).get)))
    } yield (id -> Utils.resolveURIs(conf.get(id).get, testWindows=false))

    conf ++= resolvedFileUris

    // Try to set main class from JAR if no --class argument is given
    if (mainClass == null && !isPython && primaryResource != null) {
      try {
        val jar = new JarFile(primaryResource.get)
        // Note that this might still return null if no main-class is set; we catch that later
        val manifestMainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
        if (manifestMainClass != null && !manifestMainClass.isEmpty) {
          mainClass = manifestMainClass
        }
      } catch {
        case e: Exception =>
          SparkSubmit.printErrorAndExit("Cannot load main class from JAR: " + primaryResource)
      }

      if (master == "yarn-standalone") {
        SparkSubmit.printWarning("\"yarn-standalone\" is deprecated. Use \"yarn-cluster\" instead.")
        master = "yarn-cluster"
      }
    }

    if (name.isEmpty && primaryResource.isDefined) {
      name = Utils.stripDirectory(primaryResource.get)
    }
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def checkRequiredArguments() = {
    conf.get(SparkPropertiesFile).foreach(propFilePath => {
      val propFile = new File(propFilePath)
      if (!propFile.exists()) {
        SparkSubmit.printErrorAndExit(s"--property-file $propFilePath does not exists")
      }
    })

    if (primaryResource.isDefined && mainClass.isEmpty) {
      SparkSubmit.printErrorAndExit("No main class")
    }

    if (deployMode != "client" && deployMode != "cluster") {
      SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
    }
    if (!conf.isDefinedAt(SparkAppPrimaryResource)) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python file)")
    }
    if (!conf.isDefinedAt(SparkAppClass) && !isPython) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (conf.isDefinedAt(SparkSubmitPyFiles) && !isPython) {
      SparkSubmit.printErrorAndExit("--py-files given but primary resource is not a Python script")
    }

    // Require all python files to be local, so we can add them to the PYTHONPATH
    if (isPython) {
      if (Utils.nonLocalPaths(primaryResource.getOrElse("")).nonEmpty) {
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
        cmdLineOptionConfig.put(SparkAppName, value)
        parse(tail)

      case ("--master") :: value :: tail =>
        cmdLineOptionConfig.put(SparkMaster, value)
        parse(tail)

      case ("--class") :: value :: tail =>
        cmdLineOptionConfig.put(SparkAppClass, value)
        parse(tail)

      case ("--deploy-mode") :: value :: tail =>
        cmdLineOptionConfig.put(SparkDeployMode, value)
        parse(tail)

      case ("--num-executors") :: value :: tail =>
        cmdLineOptionConfig.put(SparkExecutorInstances, value)
        parse(tail)

      case ("--total-executor-cores") :: value :: tail =>
        cmdLineOptionConfig.put(SparkCoresMax, value)
        parse(tail)

      case ("--executor-cores") :: value :: tail =>
        cmdLineOptionConfig.put(SparkExecutorCores, value)
        parse(tail)

      case ("--executor-memory") :: value :: tail =>
        cmdLineOptionConfig.put(SparkExecutorMemory, value)
        parse(tail)

      case ("--driver-memory") :: value :: tail =>
        cmdLineOptionConfig.put(SparkDriverMemory, value)
        parse(tail)

      case ("--driver-cores") :: value :: tail =>
        cmdLineOptionConfig.put(SparkDriverCores, value)
        parse(tail)

      case ("--driver-class-path") :: value :: tail =>
        cmdLineOptionConfig.put(SparkDriverExtraClassPath, value)
        parse(tail)

      case ("--driver-java-options") :: value :: tail =>
        cmdLineOptionConfig.put(SparkDriverExtraJavaOptions, value)
        parse(tail)

      case ("--driver-library-path") :: value :: tail =>
        cmdLineOptionConfig.put(SparkDriverExtraLibraryPath, value)
        parse(tail)

      case ("--properties-file") :: value :: tail =>
        cmdLineOptionConfig.put(SparkPropertiesFile, value)
        parse(tail)

      case ("--supervise") :: tail =>
        cmdLineOptionConfig.put(SparkDriverSupervise, true.toString)
        parse(tail)

      case ("--queue") :: value :: tail =>
        cmdLineOptionConfig.put(SparkYarnQueue, value)
        parse(tail)

      case ("--files") :: value :: tail =>
        cmdLineOptionConfig.put(SparkFiles, value)
        parse(tail)

      case ("--py-files") :: value :: tail =>
        cmdLineOptionConfig.put(SparkSubmitPyFiles, value)
        parse(tail)

      case ("--archives") :: value :: tail =>
        cmdLineOptionConfig.put(SparkYarnDistArchives, value)
        parse(tail)

      case ("--jars") :: value :: tail =>
        cmdLineOptionConfig.put(SparkJars, value)
        parse(tail)

      case ("--conf" | "-c") :: value :: tail =>
        value.split("=", 2).toSeq match {
          case Seq(k, v) => cmdLineConfOptionConfig(k) = v
          case _ => SparkSubmit.printErrorAndExit(s"Spark config without '=': $value")
        }
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case ("--verbose" | "-v") :: tail =>
        cmdLineOptionConfig.put(SparkVerbose, true.toString)
        parse(tail)

      case EQ_SEPARATED_OPT(opt, value) :: tail =>
        parse(opt :: value :: tail)

      case value :: tail if value.startsWith("-") =>
        SparkSubmit.printErrorAndExit(s"Unrecognized option '$value'.")

      case value :: tail =>
        cmdLineOptionConfig.put(SparkAppPrimaryResource, value)
        cmdLineChildArgs ++= tail

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


object SparkSubmitArguments {
  /**
   * Resolves Configuration sources in order of highest to lowest
   * 1. Each map passed in as additionalConfig from first to last
   * 2. Environment variables (including legacy variable mappings)
   * 3. System config variables (eg by using -Dspark.var.name)
   * 4  SPARK_DEFAULT_CONF/spark-defaults.conf or SPARK_HOME/conf/spark-defaults.conf if either exist
   * 5. hard coded defaults in class path at spark-submit-defaults.prop
   *
   * A property file specified by one of the means listed above gets read in and the properties are
   * considered to be at the priority of the method that specified the files. A property specified in
   * a property file will not override an existing config value at that same level
   *
   * @param additionalConfigs additional Map[ConfigName->ConfigValue] in order of highest priority to lowest
   * @return Map[propName->propFile] containing values merged from all locations in order of priority
   */
  def mergeSparkProperties(additionalConfigs: Vector[Map[String,String]]): Map[String, String] = {

    // Configuration read in from spark-submit-defaults.prop file found on the classpath
    val is = Option(Thread.currentThread().getContextClassLoader()
      .getResourceAsStream(SparkSubmitDefaults))

    val hardCodedDefaultConfig = is.flatMap(x => {Some(SparkSubmitArguments.getPropertyValuesFromStream(x))})

    if (hardCodedDefaultConfig.isEmpty || (hardCodedDefaultConfig.get.size == 0)) {
      throw new IllegalStateException(s"Default values not found at classpath $SparkSubmitDefaults")
    }

    // Configuration read in from defaults file if it exists
    var sparkDefaultConfig = SparkSubmitArguments.getSparkDefaultFileConfig

    if (sparkDefaultConfig.isDefinedAt(SparkPropertiesFile))
    {
        SparkSubmitArguments.getPropertyValuesFromFile(sparkDefaultConfig.get(SparkPropertiesFile).get)
    } else {
      Map.empty
    }

    // Configuration from java system properties
    val systemPropertyConfig = SparkSubmitArguments.getPropertyMap(System.getProperties)

    // Configuration variables from the environment
    // support legacy variables
    val environmentConfig = System.getenv().asScala

    val legacyEnvVars = List("MASTER"->SparkMaster, "DEPLOY_MODE"->SparkDeployMode)

    // legacy variables act at the priority of a system property
    systemPropertyConfig ++ legacyEnvVars.map( {case(varName, propName) => (environmentConfig.get(varName), propName) })
      .filter( {case(varVariable, _) => varVariable.isDefined} )
      .map{case(varVariable, propName) => (propName, varVariable.get)}

    val ConfigSources  = additionalConfigs ++ Vector (
      environmentConfig,
      systemPropertyConfig,
      sparkDefaultConfig,
      hardCodedDefaultConfig.get
    )

    // Load properties file at priority level of source that specified the property file
    // loaded property file configs will not override existing configs at the priority
    // level the property file was specified at
    val processedConfigSource = ConfigSources
      .map( configMap => getFileBasedPropertiesIfSpecified(configMap) ++ configMap)

    MergedPropertyMap.mergePropertyMaps(processedConfigSource)
  }

  /**
   * Returns a map of config values from a property file if
   * the passed configMap has a SparkPropertiesFile defined pointing to a file
   * @param configMap Map of config values to check for file path from
   * @return Map of config values if map holds a valid SparkPropertiesFile, Map.Empty elsewise
   */
  def getFileBasedPropertiesIfSpecified(configMap: Map[String, String]): Map[String,String] = {
    if (configMap.isDefinedAt(SparkPropertiesFile))
    {
      SparkSubmitArguments.getPropertyValuesFromFile(configMap.get(SparkPropertiesFile).get)
    } else {
      Map.empty
    }
  }

  /**
   *   Returns a loaded property file
   */
  def loadPropFile(propFile: File): Properties = {
    val prop = new Properties()
    prop.load(new FileInputStream(propFile))
    prop
  }

  /**
   * Gets configuration from reading SPARK_CONF_DIR/spark-defaults.conf if it exists
   * otherwise reads SPARK_HOME/conf/spark-defaults.conf if it exists
   * otherwise returns an empty config structure
   * @return config object
   */
  def getSparkDefaultFileConfig: Map[String, String] = {
    val baseConfDir: Option[String] = sys.env.get(SparkHome).map(_ + File.separator + SparkConfDir)
    val altConfDir: Option[String] = sys.env.get(AltSparkConfPath)
    val confDir: Option[String] = altConfDir.orElse(baseConfDir)

    confDir.flatMap(path => Some(path + File.separator + SparkDefaultsConfFile))
      .flatMap(path => Some(new File(path)))
      .filter(confFile => confFile.exists())
      .flatMap(confFile => Some(loadPropFile(confFile)))
      .flatMap(prop => Some(getPropertyMap(prop)))
      .getOrElse(Map.empty)
  }

  /**
   * Converts the passed java property object into a scale Map[String, String]
   * @param prop Java properties object
   * @return Map[propName->propValue]
   */
  def getPropertyMap(prop: Properties): m.Map[String,String] = {
    new m.HashMap() ++ prop.entrySet
      .map(entry => (entry.getKey.toString -> entry.getValue.toString))
  }
    
  /**
   * Parses a property file using the typesafe conf property file parser
   * Typesafe conf is used rather then native java property code for consistency purposed
   * @param filePath Path to property file
   * @return Map of config values parsed from file, empty map if file does not exist
   */
  def getPropertyValuesFromFile(filePath: String): Map[String, String] =
  {
    val propFile = new File(filePath)
    if (propFile.exists) {
      getPropertyValuesFromStream(new FileInputStream(propFile))
    } else {
      Map.empty
    }
  }

  /**
   * Loads property object from stream.
   * @param is Input stream to load property file from
   * @return Map[PropName->PropValue]
   */
  def getPropertyValuesFromStream(is: InputStream ): Map[String, String] = {
    val prop = new Properties()
    prop.load(is)
    val propVals = for {
      entry <- prop.entrySet.asScala
    } yield (entry.getKey.toString -> entry.getValue.toString)

    propVals.toMap
  }

}
