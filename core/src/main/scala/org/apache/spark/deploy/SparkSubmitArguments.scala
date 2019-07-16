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

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.lang.reflect.InvocationTargetException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.{List => JList}
import java.util.jar.JarFile

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source
import scala.util.Try

import org.apache.spark.{SparkConf, SparkException, SparkUserAppException}
import org.apache.spark.deploy.SparkSubmitAction._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.DYN_ALLOCATION_ENABLED
import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils

/**
 * Parses and encapsulates arguments from the spark-submit script.
 * The env argument is used for testing.
 */
private[deploy] class SparkSubmitArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends SparkSubmitArgumentsParser with Logging {
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
  var queue: String = null
  var numExecutors: String = null
  var files: String = null
  var archives: String = null
  var mainClass: String = null
  var primaryResource: String = null
  var name: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var jars: String = null
  var packages: String = null
  var repositories: String = null
  var ivyRepoPath: String = null
  var ivySettingsPath: Option[String] = None
  var packagesExclusions: String = null
  var verbose: Boolean = false
  var isPython: Boolean = false
  var pyFiles: String = null
  var isR: Boolean = false
  var action: SparkSubmitAction = null
  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()
  var proxyUser: String = null
  var principal: String = null
  var keytab: String = null
  private var dynamicAllocationEnabled: Boolean = false

  // Standalone cluster mode only
  var supervise: Boolean = false
  var driverCores: String = null
  var submissionToKill: String = null
  var submissionToRequestStatusFor: String = null
  var useRest: Boolean = false // used internally

  /** Default properties present in the currently defined defaults file. */
  lazy val defaultSparkProperties: HashMap[String, String] = {
    val defaultProperties = new HashMap[String, String]()
    if (verbose) {
      logInfo(s"Using properties file: $propertiesFile")
    }
    Option(propertiesFile).foreach { filename =>
      val properties = Utils.getPropertiesFromFile(filename)
      properties.foreach { case (k, v) =>
        defaultProperties(k) = v
      }
      // Property files may contain sensitive information, so redact before printing
      if (verbose) {
        Utils.redact(properties).foreach { case (k, v) =>
          logInfo(s"Adding default property: $k=$v")
        }
      }
    }
    defaultProperties
  }

  // Set parameters from command line arguments
  parse(args.asJava)

  // Populate `sparkProperties` map from properties file
  mergeDefaultSparkProperties()
  // Remove keys that don't start with "spark." from `sparkProperties`.
  ignoreNonSparkProperties()
  // Use `sparkProperties` map along with env vars to fill in any missing parameters
  loadEnvironmentArguments()

  useRest = sparkProperties.getOrElse("spark.master.rest.enabled", "false").toBoolean

  validateArguments()

  /**
   * Merge values from the default properties file with those specified through --conf.
   * When this is called, `sparkProperties` is already filled with configs from the latter.
   */
  private def mergeDefaultSparkProperties(): Unit = {
    // Use common defaults file, if not specified by user
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    // Honor --conf before the defaults file
    defaultSparkProperties.foreach { case (k, v) =>
      if (!sparkProperties.contains(k)) {
        sparkProperties(k) = v
      }
    }
  }

  /**
   * Remove keys that don't start with "spark." from `sparkProperties`.
   */
  private def ignoreNonSparkProperties(): Unit = {
    sparkProperties.foreach { case (k, v) =>
      if (!k.startsWith("spark.")) {
        sparkProperties -= k
        logWarning(s"Ignoring non-spark config property: $k=$v")
      }
    }
  }

  /**
   * Load arguments from environment variables, Spark properties etc.
   */
  private def loadEnvironmentArguments(): Unit = {
    master = Option(master)
      .orElse(sparkProperties.get("spark.master"))
      .orElse(env.get("MASTER"))
      .orNull
    driverExtraClassPath = Option(driverExtraClassPath)
      .orElse(sparkProperties.get(config.DRIVER_CLASS_PATH.key))
      .orNull
    driverExtraJavaOptions = Option(driverExtraJavaOptions)
      .orElse(sparkProperties.get(config.DRIVER_JAVA_OPTIONS.key))
      .orNull
    driverExtraLibraryPath = Option(driverExtraLibraryPath)
      .orElse(sparkProperties.get(config.DRIVER_LIBRARY_PATH.key))
      .orNull
    driverMemory = Option(driverMemory)
      .orElse(sparkProperties.get(config.DRIVER_MEMORY.key))
      .orElse(env.get("SPARK_DRIVER_MEMORY"))
      .orNull
    driverCores = Option(driverCores)
      .orElse(sparkProperties.get(config.DRIVER_CORES.key))
      .orNull
    executorMemory = Option(executorMemory)
      .orElse(sparkProperties.get(config.EXECUTOR_MEMORY.key))
      .orElse(env.get("SPARK_EXECUTOR_MEMORY"))
      .orNull
    executorCores = Option(executorCores)
      .orElse(sparkProperties.get(config.EXECUTOR_CORES.key))
      .orElse(env.get("SPARK_EXECUTOR_CORES"))
      .orNull
    totalExecutorCores = Option(totalExecutorCores)
      .orElse(sparkProperties.get(config.CORES_MAX.key))
      .orNull
    name = Option(name).orElse(sparkProperties.get("spark.app.name")).orNull
    jars = Option(jars).orElse(sparkProperties.get(config.JARS.key)).orNull
    files = Option(files).orElse(sparkProperties.get(config.FILES.key)).orNull
    pyFiles = Option(pyFiles).orElse(sparkProperties.get(config.SUBMIT_PYTHON_FILES.key)).orNull
    ivyRepoPath = sparkProperties.get("spark.jars.ivy").orNull
    ivySettingsPath = sparkProperties.get("spark.jars.ivySettings")
    packages = Option(packages).orElse(sparkProperties.get("spark.jars.packages")).orNull
    packagesExclusions = Option(packagesExclusions)
      .orElse(sparkProperties.get("spark.jars.excludes")).orNull
    repositories = Option(repositories)
      .orElse(sparkProperties.get("spark.jars.repositories")).orNull
    deployMode = Option(deployMode)
      .orElse(sparkProperties.get(config.SUBMIT_DEPLOY_MODE.key))
      .orElse(env.get("DEPLOY_MODE"))
      .orNull
    numExecutors = Option(numExecutors)
      .getOrElse(sparkProperties.get(config.EXECUTOR_INSTANCES.key).orNull)
    queue = Option(queue).orElse(sparkProperties.get("spark.yarn.queue")).orNull
    keytab = Option(keytab)
      .orElse(sparkProperties.get("spark.kerberos.keytab"))
      .orElse(sparkProperties.get("spark.yarn.keytab"))
      .orNull
    principal = Option(principal)
      .orElse(sparkProperties.get("spark.kerberos.principal"))
      .orElse(sparkProperties.get("spark.yarn.principal"))
      .orNull
    dynamicAllocationEnabled =
      sparkProperties.get(DYN_ALLOCATION_ENABLED.key).exists("true".equalsIgnoreCase)

    // Try to set main class from JAR if no --class argument is given
    if (mainClass == null && !isPython && !isR && primaryResource != null) {
      val uri = new URI(primaryResource)
      val uriScheme = uri.getScheme()

      uriScheme match {
        case "file" =>
          try {
            Utils.tryWithResource(new JarFile(uri.getPath)) { jar =>
              // Note that this might still return null if no main-class is set; we catch that later
              mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
            }
          } catch {
            case _: Exception =>
              error(s"Cannot load main class from JAR $primaryResource")
          }
        case _ =>
          error(
            s"Cannot load main class from JAR $primaryResource with URI $uriScheme. " +
            "Please specify a class through --class.")
      }
    }

    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    master = Option(master).getOrElse("local[*]")

    // In YARN mode, app name can be set via SPARK_YARN_APP_NAME (see SPARK-5222)
    if (master.startsWith("yarn")) {
      name = Option(name).orElse(env.get("SPARK_YARN_APP_NAME")).orNull
    }

    // Set name from main class if not given
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = new File(primaryResource).getName()
    }

    // Action should be SUBMIT unless otherwise specified
    action = Option(action).getOrElse(SUBMIT)
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def validateArguments(): Unit = {
    action match {
      case SUBMIT => validateSubmitArguments()
      case KILL => validateKillArguments()
      case REQUEST_STATUS => validateStatusRequestArguments()
      case PRINT_VERSION =>
    }
  }

  private def validateSubmitArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
    if (primaryResource == null) {
      error("Must specify a primary resource (JAR or Python or R file)")
    }
    if (mainClass == null && SparkSubmit.isUserJar(primaryResource)) {
      error("No main class set in JAR; please specify one with --class")
    }
    if (driverMemory != null
        && Try(JavaUtils.byteStringAsBytes(driverMemory)).getOrElse(-1L) <= 0) {
      error("Driver memory must be a positive number")
    }
    if (executorMemory != null
        && Try(JavaUtils.byteStringAsBytes(executorMemory)).getOrElse(-1L) <= 0) {
      error("Executor memory must be a positive number")
    }
    if (executorCores != null && Try(executorCores.toInt).getOrElse(-1) <= 0) {
      error("Executor cores must be a positive number")
    }
    if (totalExecutorCores != null && Try(totalExecutorCores.toInt).getOrElse(-1) <= 0) {
      error("Total executor cores must be a positive number")
    }
    if (!dynamicAllocationEnabled &&
      numExecutors != null && Try(numExecutors.toInt).getOrElse(-1) <= 0) {
      error("Number of executors must be a positive number")
    }

    if (master.startsWith("yarn")) {
      val hasHadoopEnv = env.contains("HADOOP_CONF_DIR") || env.contains("YARN_CONF_DIR")
      if (!hasHadoopEnv && !Utils.isTesting) {
        error(s"When running with master '$master' " +
          "either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.")
      }
    }

    if (proxyUser != null && principal != null) {
      error("Only one of --proxy-user or --principal can be provided.")
    }
  }

  private def validateKillArguments(): Unit = {
    if (submissionToKill == null) {
      error("Please specify a submission to kill.")
    }
  }

  private def validateStatusRequestArguments(): Unit = {
    if (submissionToRequestStatusFor == null) {
      error("Please specify a submission to request status for.")
    }
  }

  def isStandaloneCluster: Boolean = {
    master.startsWith("spark://") && deployMode == "cluster"
  }

  override def toString: String = {
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
    |  packages                $packages
    |  packagesExclusions      $packagesExclusions
    |  repositories            $repositories
    |  verbose                 $verbose
    |
    |Spark properties used, including those specified through
    | --conf and those from the properties file $propertiesFile:
    |${Utils.redact(sparkProperties).mkString("  ", "\n  ", "\n")}
    """.stripMargin
  }

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case NAME =>
        name = value

      case MASTER =>
        master = value

      case CLASS =>
        mainClass = value

      case DEPLOY_MODE =>
        if (value != "client" && value != "cluster") {
          error("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value

      case NUM_EXECUTORS =>
        numExecutors = value

      case TOTAL_EXECUTOR_CORES =>
        totalExecutorCores = value

      case EXECUTOR_CORES =>
        executorCores = value

      case EXECUTOR_MEMORY =>
        executorMemory = value

      case DRIVER_MEMORY =>
        driverMemory = value

      case DRIVER_CORES =>
        driverCores = value

      case DRIVER_CLASS_PATH =>
        driverExtraClassPath = value

      case DRIVER_JAVA_OPTIONS =>
        driverExtraJavaOptions = value

      case DRIVER_LIBRARY_PATH =>
        driverExtraLibraryPath = value

      case PROPERTIES_FILE =>
        propertiesFile = value

      case KILL_SUBMISSION =>
        submissionToKill = value
        if (action != null) {
          error(s"Action cannot be both $action and $KILL.")
        }
        action = KILL

      case STATUS =>
        submissionToRequestStatusFor = value
        if (action != null) {
          error(s"Action cannot be both $action and $REQUEST_STATUS.")
        }
        action = REQUEST_STATUS

      case SUPERVISE =>
        supervise = true

      case QUEUE =>
        queue = value

      case FILES =>
        files = Utils.resolveURIs(value)

      case PY_FILES =>
        pyFiles = Utils.resolveURIs(value)

      case ARCHIVES =>
        archives = Utils.resolveURIs(value)

      case JARS =>
        jars = Utils.resolveURIs(value)

      case PACKAGES =>
        packages = value

      case PACKAGES_EXCLUDE =>
        packagesExclusions = value

      case REPOSITORIES =>
        repositories = value

      case CONF =>
        val (confName, confValue) = SparkSubmitUtils.parseSparkConfProperty(value)
        sparkProperties(confName) = confValue

      case PROXY_USER =>
        proxyUser = value

      case PRINCIPAL =>
        principal = value

      case KEYTAB =>
        keytab = value

      case HELP =>
        printUsageAndExit(0)

      case VERBOSE =>
        verbose = true

      case VERSION =>
        action = SparkSubmitAction.PRINT_VERSION

      case USAGE_ERROR =>
        printUsageAndExit(1)

      case _ =>
        error(s"Unexpected argument '$opt'.")
    }
    action != SparkSubmitAction.PRINT_VERSION
  }

  /**
   * Handle unrecognized command line options.
   *
   * The first unrecognized option is treated as the "primary resource". Everything else is
   * treated as application arguments.
   */
  override protected def handleUnknown(opt: String): Boolean = {
    if (opt.startsWith("-")) {
      error(s"Unrecognized option '$opt'.")
    }

    primaryResource =
      if (!SparkSubmit.isShell(opt) && !SparkSubmit.isInternal(opt)) {
        Utils.resolveURI(opt).toString
      } else {
        opt
      }
    isPython = SparkSubmit.isPython(opt)
    isR = SparkSubmit.isR(opt)
    false
  }

  override protected def handleExtraArgs(extra: JList[String]): Unit = {
    childArgs ++= extra.asScala
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    if (unknownParam != null) {
      logInfo("Unknown/unsupported param " + unknownParam)
    }
    val command = sys.env.getOrElse("_SPARK_CMD_USAGE",
      """Usage: spark-submit [options] <app jar | python file | R file> [app arguments]
        |Usage: spark-submit --kill [submission ID] --master [spark://...]
        |Usage: spark-submit --status [submission ID] --master [spark://...]
        |Usage: spark-submit run-example [options] example-class [example args]""".stripMargin)
    logInfo(command)

    val mem_mb = Utils.DEFAULT_DRIVER_MEM_MB
    logInfo(
      s"""
        |Options:
        |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn,
        |                              k8s://https://host:port, or local (Default: local[*]).
        |  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
        |                              on one of the worker machines inside the cluster ("cluster")
        |                              (Default: client).
        |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
        |  --name NAME                 A name of your application.
        |  --jars JARS                 Comma-separated list of jars to include on the driver
        |                              and executor classpaths.
        |  --packages                  Comma-separated list of maven coordinates of jars to include
        |                              on the driver and executor classpaths. Will search the local
        |                              maven repo, then maven central and any additional remote
        |                              repositories given by --repositories. The format for the
        |                              coordinates should be groupId:artifactId:version.
        |  --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
        |                              resolving the dependencies provided in --packages to avoid
        |                              dependency conflicts.
        |  --repositories              Comma-separated list of additional remote repositories to
        |                              search for the maven coordinates given with --packages.
        |  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
        |                              on the PYTHONPATH for Python apps.
        |  --files FILES               Comma-separated list of files to be placed in the working
        |                              directory of each executor. File paths of these files
        |                              in executors can be accessed via SparkFiles.get(fileName).
        |
        |  --conf PROP=VALUE           Arbitrary Spark configuration property.
        |  --properties-file FILE      Path to a file from which to load extra properties. If not
        |                              specified, this will look for conf/spark-defaults.conf.
        |
        |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: ${mem_mb}M).
        |  --driver-java-options       Extra Java options to pass to the driver.
        |  --driver-library-path       Extra library path entries to pass to the driver.
        |  --driver-class-path         Extra class path entries to pass to the driver. Note that
        |                              jars added with --jars are automatically included in the
        |                              classpath.
        |
        |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).
        |
        |  --proxy-user NAME           User to impersonate when submitting the application.
        |                              This argument does not work with --principal / --keytab.
        |
        |  --help, -h                  Show this help message and exit.
        |  --verbose, -v               Print additional debug output.
        |  --version,                  Print the version of current Spark.
        |
        | Cluster deploy mode only:
        |  --driver-cores NUM          Number of cores used by the driver, only in cluster mode
        |                              (Default: 1).
        |
        | Spark standalone or Mesos with cluster deploy mode only:
        |  --supervise                 If given, restarts the driver on failure.
        |
        | Spark standalone, Mesos or K8s with cluster deploy mode only:
        |  --kill SUBMISSION_ID        If given, kills the driver specified.
        |  --status SUBMISSION_ID      If given, requests the status of the driver specified.
        |
        | Spark standalone, Mesos and Kubernetes only:
        |  --total-executor-cores NUM  Total cores for all executors.
        |
        | Spark standalone, YARN and Kubernetes only:
        |  --executor-cores NUM        Number of cores used by each executor. (Default: 1 in
        |                              YARN and K8S modes, or all available cores on the worker
        |                              in standalone mode).
        |
        | Spark on YARN and Kubernetes only:
        |  --num-executors NUM         Number of executors to launch (Default: 2).
        |                              If dynamic allocation is enabled, the initial number of
        |                              executors will be at least NUM.
        |  --principal PRINCIPAL       Principal to be used to login to KDC.
        |  --keytab KEYTAB             The full path to the file that contains the keytab for the
        |                              principal specified above.
        |
        | Spark on YARN only:
        |  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
        |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
        |                              working directory of each executor.
      """.stripMargin
    )

    if (SparkSubmit.isSqlShell(mainClass)) {
      logInfo("CLI options:")
      logInfo(getSqlShellOptions())
    }

    throw new SparkUserAppException(exitCode)
  }

  /**
   * Run the Spark SQL CLI main class with the "--help" option and catch its output. Then filter
   * the results to remove unwanted lines.
   *
   * Since the CLI will call `System.exit()`, we install a security manager to prevent that call
   * from working, and restore the original one afterwards.
   */
  private def getSqlShellOptions(): String = {
    val currentOut = System.out
    val currentErr = System.err
    val currentSm = System.getSecurityManager()
    try {
      val out = new ByteArrayOutputStream()
      val stream = new PrintStream(out)
      System.setOut(stream)
      System.setErr(stream)

      val sm = new SecurityManager() {
        override def checkExit(status: Int): Unit = {
          throw new SecurityException()
        }

        override def checkPermission(perm: java.security.Permission): Unit = {}
      }
      System.setSecurityManager(sm)

      try {
        Utils.classForName(mainClass).getMethod("main", classOf[Array[String]])
          .invoke(null, Array(HELP))
      } catch {
        case e: InvocationTargetException =>
          // Ignore SecurityException, since we throw it above.
          if (!e.getCause().isInstanceOf[SecurityException]) {
            throw e
          }
      }

      stream.flush()

      // Get the output and discard any unnecessary lines from it.
      Source.fromString(new String(out.toByteArray(), StandardCharsets.UTF_8)).getLines
        .filter { line =>
          !line.startsWith("log4j") && !line.startsWith("usage")
        }
        .mkString("\n")
    } finally {
      System.setSecurityManager(currentSm)
      System.setOut(currentOut)
      System.setErr(currentErr)
    }
  }

  private def error(msg: String): Unit = throw new SparkException(msg)

  private[deploy] def toSparkConf(sparkConf: Option[SparkConf] = None): SparkConf = {
    // either use an existing config or create a new empty one
    sparkProperties.foldLeft(sparkConf.getOrElse(new SparkConf())) {
      case (conf, (k, v)) => conf.set(k, v)
    }
  }
}
