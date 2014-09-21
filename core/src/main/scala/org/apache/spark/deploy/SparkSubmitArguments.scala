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
import java.util.jar.JarFile
import scala.collection.JavaConversions._

import com.typesafe.config._
import org.apache.spark.deploy.ConfigConstants._
import org.apache.spark.util.Utils

import scala.collection.{Set, Map}
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.parallel.mutable
import scala.sys.SystemProperties

/**
 * Configuration structure formed by merging all possible sources of configuration
 * information in order of priority (from lowest priority to highest)
 * 1. hard coded defaults in class path at spark-submit-defaults.conf
 * 2. SPARK_DEFAULT_CONF/spark-defaults.conf or SPARK_HOME/conf/spark-defaults.conf if either exist
 * 3. System config variables (eg by using -Dspark.var.name)
 * 4. Environment variables
 * 5. properties file specified on the command line with --properties-file
 * 6. command line option or --conf override
 *
 * Those items with defaults return a string, those without a default returns Option[String]
 */
private[spark] class SparkSubmitArguments(args: Seq[String]) {
  /**
   * Stores all configuration items except for child arguments, reference by the constants
   * defined in ConfigConstants.scala

   */
  val conf = new HashMap[String, String]()

  def master = getStringConfig(SparkMaster).get
  def master_= (value: String):Unit = conf.put(SparkMaster, value)

  def deployMode = getStringConfig(SparkDeployMode).get
  def deployMode_= (value: String):Unit = conf.put(SparkDeployMode, value)

  def executorMemory = getStringConfig(SparkExecutorMemory).get
  def executorMemory_= (value: String):Unit = conf.put(SparkExecutorMemory, value)

  def executorCores = getStringConfig(SparkExecutorCores).get
  def executorCores_= (value: String):Unit = conf.put(SparkExecutorCores, value)

  def totalExecutorCores = getStringConfig(SparkCoresMax)
  def totalExecutorCores_= (value: String):Unit = conf.put(SparkCoresMax, value)

  def driverMemory = getStringConfig(SparkDriverMemory).get
  def driverMemory_= (value: String):Unit = conf.put(SparkDriverMemory, value)

  def driverExtraClassPath = getStringConfig(SparkDriverExtraClassPath)
  def driverExtraClassPath_= (value: String):Unit = conf.put(SparkDriverExtraClassPath, value)

  def driverExtraLibraryPath = getStringConfig(SparkDriverExtraLibraryPath)
  def driverExtraLibraryPath_= (value: String):Unit = conf.put(SparkDriverExtraLibraryPath, value)

  def driverExtraJavaOptions = getStringConfig(SparkDriverExtraJavaOptions)
  def driverExtraJavaOptions_= (value: String):Unit = conf.put(SparkDriverExtraJavaOptions, value)

  def driverCores = getStringConfig(SparkDriverCores).get
  def driverCores_= (value: String):Unit = conf.put(SparkDriverCores, value)

  def supervise = getStringConfig(SparkDriverSupervise).get == true.toString
  def supervise_= (value: String):Unit = conf.put(SparkDriverSupervise, value)

  def queue = getStringConfig(SparkYarnQueue).get
  def queue_= (value: String):Unit = conf.put(SparkYarnQueue, value)

  def numExecutors = getStringConfig(SparkExecutorInstances).get
  def numExecutors_= (value: String):Unit = conf.put(SparkExecutorInstances, value)

  def files = getStringConfig(SparkFiles)
  def files_= (value: String):Unit = conf.put(SparkFiles, value)

  def archives = getStringConfig(SparkYarnDistArchives)
  def archives_= (value: String):Unit = conf.put(SparkYarnDistArchives, value)

  def mainClass = getStringConfig(SparkAppClass)
  def mainClass_= (value: String):Unit = conf.put(SparkAppClass, value)

  def primaryResource = getStringConfig(SparkAppPrimaryResource)
  def primaryResource_= (value: String):Unit = conf.put(SparkAppPrimaryResource, value)

  def name = getStringConfig(SparkAppName)
  def name_= (value: String):Unit = conf.put(SparkAppName, value)

  def jars = getStringConfig(SparkJars)
  def jars_= (value: String):Unit = conf.put(SparkJars, value)

  def pyFiles = getStringConfig(SparkSubmitPyFiles)
  def pyFiles_= (value: String):Unit = conf.put(SparkSubmitPyFiles, value)

  val sparkProperties = new HashMap[String, String]()

  lazy val verbose: Boolean =  SparkVerbose == true.toString
  lazy val isPython: Boolean = primaryResource.isDefined && SparkSubmit.isPython(primaryResource.get)
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()

  // any child argument detected on command line are stored here.
  private var childArgsCommandLine = new ArrayBuffer[String]()

  /**
   * Configuration read in from property file if specified on command line
   */
  private var propertiesFileConfig: Config = ConfigFactory.empty
  /**
   * Used to store parameters parsed from command line before constructing master config object
   */
  private val parsedParameters = new HashMap[String, String]()

  parseOpts(args.toList)
  // parameters delivered by conf command do not have priority over explicit
  // command line parameter
  for((k,v) <- sparkProperties) {
    parsedParameters.getOrElseUpdate(k,v)
  }
  var resolvedConfig: Config = mergeSparkProperties
  printResolvedConfig()

  // The only config item that doesn't fit nicely into a Map[String, String]
  // is the child arguments config item
  if (resolvedConfig.hasPath(SparkAppArguments))
  {
    childArgs ++= resolvedConfig.getStringList(SparkAppArguments)
  }

  // now add the rest of the config items to the config map
  conf ++= resolvedConfig.entrySet.map(entry => entry.getKey -> entry.getValue.unwrapped.toString)

  // some configuration items can be derived if there are not present
  deriveConfigurations()

  checkRequiredArguments()

  private def configExists(path: String) = conf.get(path).isDefined

  /**
   * Returns an option containing config item
   * @param path property path of config item
   * @return Option of config value
   */
  def getStringConfig(path: String): Option[String] = {
    conf.get(path)
  }


  /**
   * Resolves Configuration sources in order of lowest priority to highest
   * 1. hard coded defaults in class path at spark-submit-defaults.conf
   * 2. SPARK_DEFAULT_CONF/spark-defaults.conf or SPARK_HOME/conf/spark-defaults.conf if either exist
   * 3. System config variables (eg by using -Dspark.var.name)
   * 4. Environment variables
   * 5. properties file specified on the command line with --properties-file
   * 6. command line option or --conf override
   */
  private def mergeSparkProperties = {
    val CommandLineOrigin: String = "Command line"

    val defaultParseOptions = ConfigParseOptions.defaults()
      .setSyntax(ConfigSyntax.CONF)
      .setOriginDescription("Default values")
      .setClassLoader(Thread.currentThread.getContextClassLoader)
      .setAllowMissing(true)

    new SystemProperties().put("config.trace","loads")

    // Configuration read in from spark-submit-defaults.conf file found on the classpath
    val hardCodedDefaultConfig: Config = ConfigFactory.parseResources(
      SparkSubmitDefaults, defaultParseOptions)
    if (hardCodedDefaultConfig.entrySet().size == 0)
    {
      throw new IllegalStateException(s"Default values not found at classpath $SparkSubmitDefaults")
    }

    // Configuration read in from defaults file if it exists
    val sparkDefaultConfig = SparkSubmitArguments.getSparkDefaultFileConfig

    // Configuration from java system properties
    val systemPropertyConfig = ConfigFactory.systemProperties()

    // Configuration variables from the environment
    val environmentConfig: Config = ConfigFactory.systemEnvironment()

    // Configuration read in from the command line
    val parsedSparkCommandLineConfig = if (childArgsCommandLine.isEmpty) {
      ConfigFactory.parseMap(parsedParameters, CommandLineOrigin)
    } else {
      val childArgs: java.lang.Iterable[String] = childArgsCommandLine

      ConfigFactory.parseMap(parsedParameters, CommandLineOrigin)
        .withValue(SparkAppArguments, ConfigValueFactory.fromAnyRef(childArgs))
    }

    ConfigFactory.empty()
      .withFallback(parsedSparkCommandLineConfig)
      .withFallback(propertiesFileConfig)
      .withFallback(environmentConfig)
      .withFallback(systemPropertyConfig)
      .withFallback(sparkDefaultConfig)
      .withFallback(hardCodedDefaultConfig)
      .resolve()
  }

  private def deriveConfigurations() = {
    // TODO: test legacy variables
    // This supports env vars in older versions of Spark
    //master = Option(master).getOrElse(System.getenv("MASTER"))
    //deployMode = Option(deployMode).getOrElse(System.getenv("DEPLOY_MODE"))

    // These config items point to file paths, but may need to be converted to absolute file uris
    val configFileUris = List(SparkFiles, SparkSubmitPyFiles, SparkYarnDistArchives,
      SparkJars, SparkAppPrimaryResource)

    val resolvedFileUris = for{
      id <- configFileUris
      if configExists(id) &&
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

    // TODO: test defaults
    // now in spark-submit-default.conf file in classpath
    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    //master = Option(master).getOrElse("local[*]")

    if (name.isEmpty && primaryResource.isDefined) {
      name = Utils.stripDirectory(primaryResource.get)
    }

  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def checkRequiredArguments() = {
    if (primaryResource.isDefined && mainClass.isEmpty) {
      SparkSubmit.printErrorAndExit("No main class")
    }

    if (deployMode != "client" && deployMode != "cluster") {
      SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
    }
    if (!configExists(SparkAppPrimaryResource)) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python file)")
    }
    if (!configExists(SparkAppClass) && !isPython) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (configExists(SparkSubmitPyFiles) && !isPython) {
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

  def printResolvedConfig() = {
    val renderOptions = ConfigRenderOptions.concise().setOriginComments(false).setFormatted(true)
    SparkSubmit.printStream.println(resolvedConfig.root().get("spark").render(renderOptions))
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
        //name = value
        parsedParameters.put(SparkAppName, value)
        parse(tail)

      case ("--master") :: value :: tail =>
        //master = value
        parsedParameters.put(SparkMaster, value)
        parse(tail)

      case ("--class") :: value :: tail =>
        //mainClass = value
        parsedParameters.put(SparkAppClass, value)
        parse(tail)

      case ("--deploy-mode") :: value :: tail =>
        //deployMode = value
        parsedParameters.put(SparkDeployMode, value)
        parse(tail)

      case ("--num-executors") :: value :: tail =>
        //numExecutors = value
        parsedParameters.put(SparkExecutorInstances, value)
        parse(tail)

      case ("--total-executor-cores") :: value :: tail =>
        //totalExecutorCores = value
        parsedParameters.put(SparkCoresMax, value)
        parse(tail)

      case ("--executor-cores") :: value :: tail =>
        //executorCores = value
        parsedParameters.put(SparkExecutorCores, value)
        parse(tail)

      case ("--executor-memory") :: value :: tail =>
        //executorMemory = value
        parsedParameters.put(SparkExecutorMemory, value)
        parse(tail)

      case ("--driver-memory") :: value :: tail =>
        //driverMemory = value
        parsedParameters.put(SparkDriverMemory, value)
        parse(tail)

      case ("--driver-cores") :: value :: tail =>
        //driverCores = value
        parsedParameters.put(SparkDriverCores, value)
        parse(tail)

      case ("--driver-class-path") :: value :: tail =>
        //driverExtraClassPath = value
        parsedParameters.put(SparkDriverExtraClassPath, value)
        parse(tail)

      case ("--driver-java-options") :: value :: tail =>
        //driverExtraJavaOptions = value
        parsedParameters.put(SparkDriverExtraJavaOptions, value)
        parse(tail)

      case ("--driver-library-path") :: value :: tail =>
        //driverExtraLibraryPath = value
        parsedParameters.put(SparkDriverExtraLibraryPath, value)
        parse(tail)

      case ("--properties-file") :: value :: tail =>
        //propertiesFile = value
        propertiesFileConfig = SparkSubmitArguments.getConfigValuesFromFile(value, value)
        parse(tail)

      case ("--supervise") :: tail =>
        //supervise = true
        parsedParameters.put(SparkDriverSupervise, true.toString)
        parse(tail)

      case ("--queue") :: value :: tail =>
        //queue = value
        parsedParameters.put(SparkYarnQueue, value)
        parse(tail)

      case ("--files") :: value :: tail =>
        //files = Utils.resolveURIs(value)
        // TODO: resolveURIs on all files
        parsedParameters.put(SparkFiles, value)
        parse(tail)

      case ("--py-files") :: value :: tail =>
        //pyFiles = Utils.resolveURIs(value)
        // TODO: resolveURIs on all pyFiles
        //parsedParameters.put(SparkSubmitPyFiles, value)
        parse(tail)

      case ("--archives") :: value :: tail =>
        //archives = Utils.resolveURIs(value)
        // TODO: resolveURIs on all archives
        parsedParameters.put(SparkYarnDistArchives, value)
        parse(tail)

      case ("--jars") :: value :: tail =>
        //jars = Utils.resolveURIs(value)
        // TODO: resolveURIs on all jars
        parsedParameters.put(SparkJars, value)
        parse(tail)

      case ("--conf" | "-c") :: value :: tail =>
        value.split("=", 2).toSeq match {
          case Seq(k, v) => sparkProperties(k) = v
          case _ => SparkSubmit.printErrorAndExit(s"Spark config without '=': $value")
        }
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case ("--verbose" | "-v") :: tail =>
        //verbose = true
        parsedParameters.put(SparkVerbose, true.toString)
        parse(tail)

      case EQ_SEPARATED_OPT(opt, value) :: tail =>
        parse(opt :: value :: tail)

      case value :: tail if value.startsWith("-") =>
        SparkSubmit.printErrorAndExit(s"Unrecognized option '$value'.")

      case value :: tail =>
        /*
        primaryResource =
          if (!SparkSubmit.isShell(value) && !SparkSubmit.isInternal(value)) {
            Utils.resolveURI(value).toString
          } else {
            value
          }
          */
        // TODO: Run resolveURI on returned SparkAppPrimaryResource
        parsedParameters.put(SparkAppPrimaryResource, value)
        //isPython = SparkSubmit.isPython(value)
        childArgsCommandLine ++= tail
        //parsedParameters.put(SparkAppArguments, tail.mkString)

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
   * Gets configuration from reading SPARK_CONF_DIR/spark-defaults.conf if it exists
   * otherwise reads SPARK_HOME/conf/spark-defaults.conf if it exists
   * otherwise returns an empty config structure
   * @return config object
   */
  def getSparkDefaultFileConfig: Config = {
    val baseConfDir = sys.env.get(SparkHome).map( _ + File.separator + SparkConfDir)
    val altConfDir = sys.env.get(AltSparkConfPath)

    val defaultFile = for {
      confDir <- altConfDir.orElse(baseConfDir)
      confPath = confDir + File.separator + SparkDefaultsConfFile
      if new File(confPath).exists
    } yield getConfigValuesFromFile(confPath, originDescription = confPath)

    defaultFile.getOrElse(ConfigFactory.empty())
  }


  /**
   * Parses a property file using the typesafe conf property file parser
   * Typesafe conf is used rather then native java property code for consistency purposed
   * @param filePath Path to property file
   * @return Map of config values parsed from file
   */
  def getPropertyValuesFromFile(filePath: String): Map[String, String] =
  {
    val propFileParserOptions = ConfigParseOptions
      .defaults()
      .setSyntax(ConfigSyntax.PROPERTIES)

    def propValues = getConfigValuesFromFileWithOptions(filePath, propFileParserOptions)
      .entrySet().map( entry => entry.getKey -> entry.getValue.unwrapped().toString)

    Map( propValues.toSeq: _* )
  }

  def getConfigValuesFromFile(filePath: String, originDescription: String): Config =
  {
    val propFileParserOptions = ConfigParseOptions
      .defaults()
      .setOriginDescription(originDescription)

    getConfigValuesFromFileWithOptions(filePath, propFileParserOptions)
  }

  /** Return default present in the currently defined defaults file. */
  def getConfigValuesFromFileWithOptions(filePath: String, propFileParserOptions: ConfigParseOptions): Config = {
    var propConfig = ConfigFactory.empty()

    val propFile = new File(filePath)
    try {
      require(propFile.exists(), s"Properties file $filePath does not exist")
      require(propFile.isFile, s"Properties file $filePath is not a normal file")

      propConfig = ConfigFactory.parseFile(propFile, propFileParserOptions)
    } catch {
      case e: Exception => {
        val eMsg = e.toString
        SparkSubmit.printErrorAndExit(s"Error attempting to parse property file '$filePath'. $eMsg")
      }
    }
    propConfig
  }
}
