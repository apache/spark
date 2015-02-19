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

import java.io.{File, PrintStream}
import java.lang.reflect.{InvocationTargetException, Modifier, UndeclaredThrowableException}
import java.net.URL
import java.security.PrivilegedExceptionAction

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ivy.Ivy
import org.apache.ivy.core.LogOptions
import org.apache.ivy.core.module.descriptor._
import org.apache.ivy.core.module.id.{ArtifactId, ModuleId, ModuleRevisionId}
import org.apache.ivy.core.report.ResolveReport
import org.apache.ivy.core.resolve.ResolveOptions
import org.apache.ivy.core.retrieve.RetrieveOptions
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.matcher.GlobPatternMatcher
import org.apache.ivy.plugins.resolver.{ChainResolver, IBiblioResolver}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.deploy.rest._
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils}

/**
 * Whether to submit, kill, or request the status of an application.
 * The latter two operations are currently supported only for standalone cluster mode.
 */
private[spark] object SparkSubmitAction extends Enumeration {
  type SparkSubmitAction = Value
  val SUBMIT, KILL, REQUEST_STATUS = Value
}

/**
 * Main gateway of launching a Spark application.
 *
 * This program handles setting up the classpath with relevant Spark dependencies and provides
 * a layer over the different cluster managers and deploy modes that Spark supports.
 */
object SparkSubmit {

  // Cluster managers
  private val YARN = 1
  private val STANDALONE = 2
  private val MESOS = 4
  private val LOCAL = 8
  private val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL

  // Deploy modes
  private val CLIENT = 1
  private val CLUSTER = 2
  private val ALL_DEPLOY_MODES = CLIENT | CLUSTER

  // A special jar name that indicates the class being run is inside of Spark itself, and therefore
  // no user jar is needed.
  private val SPARK_INTERNAL = "spark-internal"

  // Special primary resource names that represent shells rather than application jars.
  private val SPARK_SHELL = "spark-shell"
  private val PYSPARK_SHELL = "pyspark-shell"

  private val CLASS_NOT_FOUND_EXIT_STATUS = 101

  // Exposed for testing
  private[spark] var exitFn: () => Unit = () => System.exit(1)
  private[spark] var printStream: PrintStream = System.err
  private[spark] def printWarning(str: String): Unit = printStream.println("Warning: " + str)
  private[spark] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn()
  }
  private[spark] def printVersionAndExit(): Unit = {
    printStream.println("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format(SPARK_VERSION))
    printStream.println("Type --help for more information.")
    exitFn()
  }

  def main(args: Array[String]): Unit = {
    val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }
    appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
    }
  }

  /** Kill an existing submission using the REST protocol. Standalone cluster mode only. */
  private def kill(args: SparkSubmitArguments): Unit = {
    new StandaloneRestClient()
      .killSubmission(args.master, args.submissionToKill)
  }

  /**
   * Request the status of an existing submission using the REST protocol.
   * Standalone cluster mode only.
   */
  private def requestStatus(args: SparkSubmitArguments): Unit = {
    new StandaloneRestClient()
      .requestSubmissionStatus(args.master, args.submissionToRequestStatusFor)
  }

  /**
   * Submit the application using the provided parameters.
   *
   * This runs in two steps. First, we prepare the launch environment by setting up
   * the appropriate classpath, system properties, and application arguments for
   * running the child main class based on the cluster manager and the deploy mode.
   * Second, we use this launch environment to invoke the main method of the child
   * main class.
   */
  private[spark] def submit(args: SparkSubmitArguments): Unit = {
    val (childArgs, childClasspath, sysProps, childMainClass) = prepareSubmitEnvironment(args)

    def doRunMain(): Unit = {
      if (args.proxyUser != null) {
        val proxyUser = UserGroupInformation.createProxyUser(args.proxyUser,
          UserGroupInformation.getCurrentUser())
        try {
          proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
            }
          })
        } catch {
          case e: Exception =>
            // Hadoop's AuthorizationException suppresses the exception's stack trace, which
            // makes the message printed to the output by the JVM not very helpful. Instead,
            // detect exceptions with empty stack traces here, and treat them differently.
            if (e.getStackTrace().length == 0) {
              printStream.println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
              exitFn()
            } else {
              throw e
            }
        }
      } else {
        runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
      }
    }

     // In standalone cluster mode, there are two submission gateways:
     //   (1) The traditional Akka gateway using o.a.s.deploy.Client as a wrapper
     //   (2) The new REST-based gateway introduced in Spark 1.3
     // The latter is the default behavior as of Spark 1.3, but Spark submit will fail over
     // to use the legacy gateway if the master endpoint turns out to be not a REST server.
    if (args.isStandaloneCluster && args.useRest) {
      try {
        printStream.println("Running Spark using the REST application submission protocol.")
        doRunMain()
      } catch {
        // Fail over to use the legacy submission gateway
        case e: SubmitRestConnectionException =>
          printWarning(s"Master endpoint ${args.master} was not a REST server. " +
            "Falling back to legacy submission gateway instead.")
          args.useRest = false
          submit(args)
      }
    // In all other modes, just run the main class as prepared
    } else {
      doRunMain()
    }
  }

  /**
   * Prepare the environment for submitting an application.
   * This returns a 4-tuple:
   *   (1) the arguments for the child process,
   *   (2) a list of classpath entries for the child,
   *   (3) a map of system properties, and
   *   (4) the main class for the child
   * Exposed for testing.
   */
  private[spark] def prepareSubmitEnvironment(args: SparkSubmitArguments)
      : (Seq[String], Seq[String], Map[String, String], String) = {
    // Return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    val sysProps = new HashMap[String, String]()
    var childMainClass = ""

    // Set the cluster manager
    val clusterManager: Int = args.master match {
      case m if m.startsWith("yarn") => YARN
      case m if m.startsWith("spark") => STANDALONE
      case m if m.startsWith("mesos") => MESOS
      case m if m.startsWith("local") => LOCAL
      case _ => printErrorAndExit("Master must start with yarn, spark, mesos, or local"); -1
    }

    // Set the deploy mode; default is client mode
    var deployMode: Int = args.deployMode match {
      case "client" | null => CLIENT
      case "cluster" => CLUSTER
      case _ => printErrorAndExit("Deploy mode must be either client or cluster"); -1
    }

    // Because "yarn-cluster" and "yarn-client" encapsulate both the master
    // and deploy mode, we have some logic to infer the master and deploy mode
    // from each other if only one is specified, or exit early if they are at odds.
    if (clusterManager == YARN) {
      if (args.master == "yarn-standalone") {
        printWarning("\"yarn-standalone\" is deprecated. Use \"yarn-cluster\" instead.")
        args.master = "yarn-cluster"
      }
      (args.master, args.deployMode) match {
        case ("yarn-cluster", null) =>
          deployMode = CLUSTER
        case ("yarn-cluster", "client") =>
          printErrorAndExit("Client deploy mode is not compatible with master \"yarn-cluster\"")
        case ("yarn-client", "cluster") =>
          printErrorAndExit("Cluster deploy mode is not compatible with master \"yarn-client\"")
        case (_, mode) =>
          args.master = "yarn-" + Option(mode).getOrElse("client")
      }

      // Make sure YARN is included in our build if we're trying to use it
      if (!Utils.classIsLoadable("org.apache.spark.deploy.yarn.Client") && !Utils.isTesting) {
        printErrorAndExit(
          "Could not load YARN classes. " +
          "This copy of Spark may not have been compiled with YARN support.")
      }
    }

    val isYarnCluster = clusterManager == YARN && deployMode == CLUSTER

    // Resolve maven dependencies if there are any and add classpath to jars. Add them to py-files
    // too for packages that include Python code
    val resolvedMavenCoordinates =
      SparkSubmitUtils.resolveMavenCoordinates(
        args.packages, Option(args.repositories), Option(args.ivyRepoPath))
    if (!resolvedMavenCoordinates.trim.isEmpty) {
      if (args.jars == null || args.jars.trim.isEmpty) {
        args.jars = resolvedMavenCoordinates
      } else {
        args.jars += s",$resolvedMavenCoordinates"
      }
      if (args.isPython) {
        if (args.pyFiles == null || args.pyFiles.trim.isEmpty) {
          args.pyFiles = resolvedMavenCoordinates
        } else {
          args.pyFiles += s",$resolvedMavenCoordinates"
        }
      }
    }

    // Require all python files to be local, so we can add them to the PYTHONPATH
    // In YARN cluster mode, python files are distributed as regular files, which can be non-local
    if (args.isPython && !isYarnCluster) {
      if (Utils.nonLocalPaths(args.primaryResource).nonEmpty) {
        printErrorAndExit(s"Only local python files are supported: $args.primaryResource")
      }
      val nonLocalPyFiles = Utils.nonLocalPaths(args.pyFiles).mkString(",")
      if (nonLocalPyFiles.nonEmpty) {
        printErrorAndExit(s"Only local additional python files are supported: $nonLocalPyFiles")
      }
    }

    // The following modes are not supported or applicable
    (clusterManager, deployMode) match {
      case (MESOS, CLUSTER) =>
        printErrorAndExit("Cluster deploy mode is currently not supported for Mesos clusters.")
      case (STANDALONE, CLUSTER) if args.isPython =>
        printErrorAndExit("Cluster deploy mode is currently not supported for python " +
          "applications on standalone clusters.")
      case (_, CLUSTER) if isShell(args.primaryResource) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark shells.")
      case (_, CLUSTER) if isSqlShell(args.mainClass) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark SQL shell.")
      case (_, CLUSTER) if isThriftServer(args.mainClass) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark Thrift server.")
      case _ =>
    }

    // If we're running a python app, set the main class to our specific python runner
    if (args.isPython && deployMode == CLIENT) {
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = "org.apache.spark.api.python.PythonGatewayServer"
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(args.primaryResource, args.pyFiles) ++ args.childArgs
        args.files = mergeFileLists(args.files, args.primaryResource)
      }
      args.files = mergeFileLists(args.files, args.pyFiles)
      if (args.pyFiles != null) {
        sysProps("spark.submit.pyFiles") = args.pyFiles
      }
    }

    // In yarn-cluster mode for a python app, add primary resource and pyFiles to files
    // that can be distributed with the job
    if (args.isPython && isYarnCluster) {
      args.files = mergeFileLists(args.files, args.primaryResource)
      args.files = mergeFileLists(args.files, args.pyFiles)
    }

    // Special flag to avoid deprecation warnings at the client
    sysProps("SPARK_SUBMIT") = "true"

    // A list of rules to map each argument to system properties or command-line options in
    // each deploy mode; we iterate through these below
    val options = List[OptionAssigner](

      // All cluster managers
      OptionAssigner(args.master, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, sysProp = "spark.master"),
      OptionAssigner(args.name, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, sysProp = "spark.app.name"),
      OptionAssigner(args.jars, ALL_CLUSTER_MGRS, CLIENT, sysProp = "spark.jars"),
      OptionAssigner(args.ivyRepoPath, ALL_CLUSTER_MGRS, CLIENT, sysProp = "spark.jars.ivy"),
      OptionAssigner(args.driverMemory, ALL_CLUSTER_MGRS, CLIENT,
        sysProp = "spark.driver.memory"),
      OptionAssigner(args.driverExtraClassPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        sysProp = "spark.driver.extraClassPath"),
      OptionAssigner(args.driverExtraJavaOptions, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        sysProp = "spark.driver.extraJavaOptions"),
      OptionAssigner(args.driverExtraLibraryPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        sysProp = "spark.driver.extraLibraryPath"),

      // Standalone cluster only
      // Do not set CL arguments here because there are multiple possibilities for the main class
      OptionAssigner(args.jars, STANDALONE, CLUSTER, sysProp = "spark.jars"),
      OptionAssigner(args.ivyRepoPath, STANDALONE, CLUSTER, sysProp = "spark.jars.ivy"),
      OptionAssigner(args.driverMemory, STANDALONE, CLUSTER, sysProp = "spark.driver.memory"),
      OptionAssigner(args.driverCores, STANDALONE, CLUSTER, sysProp = "spark.driver.cores"),
      OptionAssigner(args.supervise.toString, STANDALONE, CLUSTER,
        sysProp = "spark.driver.supervise"),

      // Yarn client only
      OptionAssigner(args.queue, YARN, CLIENT, sysProp = "spark.yarn.queue"),
      OptionAssigner(args.numExecutors, YARN, CLIENT, sysProp = "spark.executor.instances"),
      OptionAssigner(args.executorCores, YARN, CLIENT, sysProp = "spark.executor.cores"),
      OptionAssigner(args.files, YARN, CLIENT, sysProp = "spark.yarn.dist.files"),
      OptionAssigner(args.archives, YARN, CLIENT, sysProp = "spark.yarn.dist.archives"),

      // Yarn cluster only
      OptionAssigner(args.name, YARN, CLUSTER, clOption = "--name"),
      OptionAssigner(args.driverMemory, YARN, CLUSTER, clOption = "--driver-memory"),
      OptionAssigner(args.driverCores, YARN, CLUSTER, clOption = "--driver-cores"),
      OptionAssigner(args.queue, YARN, CLUSTER, clOption = "--queue"),
      OptionAssigner(args.numExecutors, YARN, CLUSTER, clOption = "--num-executors"),
      OptionAssigner(args.executorMemory, YARN, CLUSTER, clOption = "--executor-memory"),
      OptionAssigner(args.executorCores, YARN, CLUSTER, clOption = "--executor-cores"),
      OptionAssigner(args.files, YARN, CLUSTER, clOption = "--files"),
      OptionAssigner(args.archives, YARN, CLUSTER, clOption = "--archives"),
      OptionAssigner(args.jars, YARN, CLUSTER, clOption = "--addJars"),

      // Other options
      OptionAssigner(args.executorMemory, STANDALONE | MESOS | YARN, ALL_DEPLOY_MODES,
        sysProp = "spark.executor.memory"),
      OptionAssigner(args.totalExecutorCores, STANDALONE | MESOS, ALL_DEPLOY_MODES,
        sysProp = "spark.cores.max"),
      OptionAssigner(args.files, LOCAL | STANDALONE | MESOS, ALL_DEPLOY_MODES,
        sysProp = "spark.files")
    )

    // In client mode, launch the application main class directly
    // In addition, add the main application jar and any added jars (if any) to the classpath
    if (deployMode == CLIENT) {
      childMainClass = args.mainClass
      if (isUserJar(args.primaryResource)) {
        childClasspath += args.primaryResource
      }
      if (args.jars != null) { childClasspath ++= args.jars.split(",") }
      if (args.childArgs != null) { childArgs ++= args.childArgs }
    }

    // Map all arguments to command-line options or system properties for our chosen mode
    for (opt <- options) {
      if (opt.value != null &&
          (deployMode & opt.deployMode) != 0 &&
          (clusterManager & opt.clusterManager) != 0) {
        if (opt.clOption != null) { childArgs += (opt.clOption, opt.value) }
        if (opt.sysProp != null) { sysProps.put(opt.sysProp, opt.value) }
      }
    }

    // Add the application jar automatically so the user doesn't have to call sc.addJar
    // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
    // For python files, the primary resource is already distributed as a regular file
    if (!isYarnCluster && !args.isPython) {
      var jars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq.empty)
      if (isUserJar(args.primaryResource)) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sysProps.put("spark.jars", jars.mkString(","))
    }

    // In standalone cluster mode, use the REST client to submit the application (Spark 1.3+).
    // All Spark parameters are expected to be passed to the client through system properties.
    if (args.isStandaloneCluster) {
      if (args.useRest) {
        childMainClass = "org.apache.spark.deploy.rest.StandaloneRestClient"
        childArgs += (args.primaryResource, args.mainClass)
      } else {
        // In legacy standalone cluster mode, use Client as a wrapper around the user class
        childMainClass = "org.apache.spark.deploy.Client"
        if (args.supervise) { childArgs += "--supervise" }
        Option(args.driverMemory).foreach { m => childArgs += ("--memory", m) }
        Option(args.driverCores).foreach { c => childArgs += ("--cores", c) }
        childArgs += "launch"
        childArgs += (args.master, args.primaryResource, args.mainClass)
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // In yarn-cluster mode, use yarn.Client as a wrapper around the user class
    if (isYarnCluster) {
      childMainClass = "org.apache.spark.deploy.yarn.Client"
      if (args.isPython) {
        val mainPyFile = new Path(args.primaryResource).getName
        childArgs += ("--primary-py-file", mainPyFile)
        if (args.pyFiles != null) {
          // These files will be distributed to each machine's working directory, so strip the
          // path prefix
          val pyFilesNames = args.pyFiles.split(",").map(p => (new Path(p)).getName).mkString(",")
          childArgs += ("--py-files", pyFilesNames)
        }
        childArgs += ("--class", "org.apache.spark.deploy.PythonRunner")
      } else {
        if (args.primaryResource != SPARK_INTERNAL) {
          childArgs += ("--jar", args.primaryResource)
        }
        childArgs += ("--class", args.mainClass)
      }
      if (args.childArgs != null) {
        args.childArgs.foreach { arg => childArgs += ("--arg", arg) }
      }
    }

    // Load any properties specified through --conf and the default properties file
    for ((k, v) <- args.sparkProperties) {
      sysProps.getOrElseUpdate(k, v)
    }

    // Ignore invalid spark.driver.host in cluster modes.
    if (deployMode == CLUSTER) {
      sysProps -= "spark.driver.host"
    }

    // Resolve paths in certain spark properties
    val pathConfigs = Seq(
      "spark.jars",
      "spark.files",
      "spark.yarn.jar",
      "spark.yarn.dist.files",
      "spark.yarn.dist.archives")
    pathConfigs.foreach { config =>
      // Replace old URIs with resolved URIs, if they exist
      sysProps.get(config).foreach { oldValue =>
        sysProps(config) = Utils.resolveURIs(oldValue)
      }
    }

    // Resolve and format python file paths properly before adding them to the PYTHONPATH.
    // The resolving part is redundant in the case of --py-files, but necessary if the user
    // explicitly sets `spark.submit.pyFiles` in his/her default properties file.
    sysProps.get("spark.submit.pyFiles").foreach { pyFiles =>
      val resolvedPyFiles = Utils.resolveURIs(pyFiles)
      val formattedPyFiles = PythonRunner.formatPaths(resolvedPyFiles).mkString(",")
      sysProps("spark.submit.pyFiles") = formattedPyFiles
    }

    (childArgs, childClasspath, sysProps, childMainClass)
  }

  /**
   * Run the main method of the child class using the provided launch environment.
   *
   * Note that this main class will not be the one provided by the user if we're
   * running cluster deploy mode or python applications.
   */
  private def runMain(
      childArgs: Seq[String],
      childClasspath: Seq[String],
      sysProps: Map[String, String],
      childMainClass: String,
      verbose: Boolean): Unit = {
    if (verbose) {
      printStream.println(s"Main class:\n$childMainClass")
      printStream.println(s"Arguments:\n${childArgs.mkString("\n")}")
      printStream.println(s"System properties:\n${sysProps.mkString("\n")}")
      printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      printStream.println("\n")
    }

    val loader =
      if (sysProps.getOrElse("spark.driver.userClassPathFirst", "false").toBoolean) {
        new ChildFirstURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      } else {
        new MutableURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      }
    Thread.currentThread.setContextClassLoader(loader)

    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    for ((key, value) <- sysProps) {
      System.setProperty(key, value)
    }

    var mainClass: Class[_] = null

    try {
      mainClass = Class.forName(childMainClass, true, loader)
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace(printStream)
        if (childMainClass.contains("thriftserver")) {
          printStream.println(s"Failed to load main class $childMainClass.")
          printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
    }

    // SPARK-4170
    if (classOf[scala.App].isAssignableFrom(mainClass)) {
      printWarning("Subclasses of scala.App may not work correctly. Use a main() method instead.")
    }

    val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the given main class must be static")
    }

    def findCause(t: Throwable): Throwable = t match {
      case e: UndeclaredThrowableException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: InvocationTargetException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: Throwable =>
        e
    }

    try {
      mainMethod.invoke(null, childArgs.toArray)
    } catch {
      case t: Throwable =>
        throw findCause(t)
    }
  }

  private def addJarToClasspath(localJar: String, loader: MutableURLClassLoader) {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          printWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        printWarning(s"Skip remote jar $uri.")
    }
  }

  /**
   * Return whether the given primary resource represents a user jar.
   */
  private def isUserJar(primaryResource: String): Boolean = {
    !isShell(primaryResource) && !isPython(primaryResource) && !isInternal(primaryResource)
  }

  /**
   * Return whether the given primary resource represents a shell.
   */
  private[spark] def isShell(primaryResource: String): Boolean = {
    primaryResource == SPARK_SHELL || primaryResource == PYSPARK_SHELL
  }

  /**
   * Return whether the given main class represents a sql shell.
   */
  private[spark] def isSqlShell(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
  }

  /**
   * Return whether the given main class represents a thrift server.
   */
  private[spark] def isThriftServer(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
  }

  /**
   * Return whether the given primary resource requires running python.
   */
  private[spark] def isPython(primaryResource: String): Boolean = {
    primaryResource.endsWith(".py") || primaryResource == PYSPARK_SHELL
  }

  private[spark] def isInternal(primaryResource: String): Boolean = {
    primaryResource == SPARK_INTERNAL
  }

  /**
   * Merge a sequence of comma-separated file lists, some of which may be null to indicate
   * no files, into a single comma-separated string.
   */
  private[spark] def mergeFileLists(lists: String*): String = {
    val merged = lists.filter(_ != null)
                      .flatMap(_.split(","))
                      .mkString(",")
    if (merged == "") null else merged
  }
}

/** Provides utility functions to be used inside SparkSubmit. */
private[spark] object SparkSubmitUtils {

  // Exposed for testing
  private[spark] var printStream = SparkSubmit.printStream

  /**
   * Represents a Maven Coordinate
   * @param groupId the groupId of the coordinate
   * @param artifactId the artifactId of the coordinate
   * @param version the version of the coordinate
   */
  private[spark] case class MavenCoordinate(groupId: String, artifactId: String, version: String)

/**
 * Extracts maven coordinates from a comma-delimited string. Coordinates should be provided
 * in the format `groupId:artifactId:version` or `groupId/artifactId:version`. The latter provides
 * simplicity for Spark Package users.
 * @param coordinates Comma-delimited string of maven coordinates
 * @return Sequence of Maven coordinates
 */
  private[spark] def extractMavenCoordinates(coordinates: String): Seq[MavenCoordinate] = {
    coordinates.split(",").map { p =>
      val splits = p.replace("/", ":").split(":")
      require(splits.length == 3, s"Provided Maven Coordinates must be in the form " +
        s"'groupId:artifactId:version'. The coordinate provided is: $p")
      require(splits(0) != null && splits(0).trim.nonEmpty, s"The groupId cannot be null or " +
        s"be whitespace. The groupId provided is: ${splits(0)}")
      require(splits(1) != null && splits(1).trim.nonEmpty, s"The artifactId cannot be null or " +
        s"be whitespace. The artifactId provided is: ${splits(1)}")
      require(splits(2) != null && splits(2).trim.nonEmpty, s"The version cannot be null or " +
        s"be whitespace. The version provided is: ${splits(2)}")
      new MavenCoordinate(splits(0), splits(1), splits(2))
    }
  }

  /**
   * Extracts maven coordinates from a comma-delimited string
   * @param remoteRepos Comma-delimited string of remote repositories
   * @return A ChainResolver used by Ivy to search for and resolve dependencies.
   */
  private[spark] def createRepoResolvers(remoteRepos: Option[String]): ChainResolver = {
    // We need a chain resolver if we want to check multiple repositories
    val cr = new ChainResolver
    cr.setName("list")

    // the biblio resolver resolves POM declared dependencies
    val br: IBiblioResolver = new IBiblioResolver
    br.setM2compatible(true)
    br.setUsepoms(true)
    br.setName("central")
    cr.add(br)

    val sp: IBiblioResolver = new IBiblioResolver
    sp.setM2compatible(true)
    sp.setUsepoms(true)
    sp.setRoot("http://dl.bintray.com/spark-packages/maven")
    sp.setName("spark-packages")
    cr.add(sp)

    val repositoryList = remoteRepos.getOrElse("")
    // add any other remote repositories other than maven central
    if (repositoryList.trim.nonEmpty) {
      repositoryList.split(",").zipWithIndex.foreach { case (repo, i) =>
        val brr: IBiblioResolver = new IBiblioResolver
        brr.setM2compatible(true)
        brr.setUsepoms(true)
        brr.setRoot(repo)
        brr.setName(s"repo-${i + 1}")
        cr.add(brr)
        printStream.println(s"$repo added as a remote repository with the name: ${brr.getName}")
      }
    }
    cr
  }

  /**
   * Output a comma-delimited list of paths for the downloaded jars to be added to the classpath
   * (will append to jars in SparkSubmit). The name of the jar is given
   * after a '!' by Ivy. It also sometimes contains '(bundle)' after '.jar'. Remove that as well.
   * @param artifacts Sequence of dependencies that were resolved and retrieved
   * @param cacheDirectory directory where jars are cached
   * @return a comma-delimited list of paths for the dependencies
   */
  private[spark] def resolveDependencyPaths(
      artifacts: Array[AnyRef],
      cacheDirectory: File): String = {
    artifacts.map { artifactInfo =>
      val artifactString = artifactInfo.toString
      val jarName = artifactString.drop(artifactString.lastIndexOf("!") + 1)
      cacheDirectory.getAbsolutePath + File.separator +
        jarName.substring(0, jarName.lastIndexOf(".jar") + 4)
    }.mkString(",")
  }

  /** Adds the given maven coordinates to Ivy's module descriptor. */
  private[spark] def addDependenciesToIvy(
      md: DefaultModuleDescriptor,
      artifacts: Seq[MavenCoordinate],
      ivyConfName: String): Unit = {
    artifacts.foreach { mvn =>
      val ri = ModuleRevisionId.newInstance(mvn.groupId, mvn.artifactId, mvn.version)
      val dd = new DefaultDependencyDescriptor(ri, false, false)
      dd.addDependencyConfiguration(ivyConfName, ivyConfName)
      printStream.println(s"${dd.getDependencyId} added as a dependency")
      md.addDependency(dd)
    }
  }

  /** A nice function to use in tests as well. Values are dummy strings. */
  private[spark] def getModuleDescriptor = DefaultModuleDescriptor.newDefaultInstance(
    ModuleRevisionId.newInstance("org.apache.spark", "spark-submit-parent", "1.0"))

  /**
   * Resolves any dependencies that were supplied through maven coordinates
   * @param coordinates Comma-delimited string of maven coordinates
   * @param remoteRepos Comma-delimited string of remote repositories other than maven central
   * @param ivyPath The path to the local ivy repository
   * @return The comma-delimited path to the jars of the given maven artifacts including their
   *         transitive dependencies
   */
  private[spark] def resolveMavenCoordinates(
      coordinates: String,
      remoteRepos: Option[String],
      ivyPath: Option[String],
      isTest: Boolean = false): String = {
    if (coordinates == null || coordinates.trim.isEmpty) {
      ""
    } else {
      val artifacts = extractMavenCoordinates(coordinates)
      // Default configuration name for ivy
      val ivyConfName = "default"
      // set ivy settings for location of cache
      val ivySettings: IvySettings = new IvySettings
      // Directories for caching downloads through ivy and storing the jars when maven coordinates
      // are supplied to spark-submit
      val alternateIvyCache = ivyPath.getOrElse("")
      val packagesDirectory: File =
        if (alternateIvyCache.trim.isEmpty) {
          new File(ivySettings.getDefaultIvyUserDir, "jars")
        } else {
          ivySettings.setDefaultCache(new File(alternateIvyCache, "cache"))
          new File(alternateIvyCache, "jars")
        }
      printStream.println(
        s"Ivy Default Cache set to: ${ivySettings.getDefaultCache.getAbsolutePath}")
      printStream.println(s"The jars for the packages stored in: $packagesDirectory")
      // create a pattern matcher
      ivySettings.addMatcher(new GlobPatternMatcher)
      // create the dependency resolvers
      val repoResolver = createRepoResolvers(remoteRepos)
      ivySettings.addResolver(repoResolver)
      ivySettings.setDefaultResolver(repoResolver.getName)

      val ivy = Ivy.newInstance(ivySettings)
      // Set resolve options to download transitive dependencies as well
      val resolveOptions = new ResolveOptions
      resolveOptions.setTransitive(true)
      val retrieveOptions = new RetrieveOptions
      // Turn downloading and logging off for testing
      if (isTest) {
        resolveOptions.setDownload(false)
        resolveOptions.setLog(LogOptions.LOG_QUIET)
        retrieveOptions.setLog(LogOptions.LOG_QUIET)
      } else {
        resolveOptions.setDownload(true)
      }

      // A Module descriptor must be specified. Entries are dummy strings
      val md = getModuleDescriptor
      md.setDefaultConf(ivyConfName)

      // Add an exclusion rule for Spark and Scala Library
      val sparkArtifacts = new ArtifactId(new ModuleId("org.apache.spark", "*"), "*", "*", "*")
      val sparkDependencyExcludeRule =
        new DefaultExcludeRule(sparkArtifacts, ivySettings.getMatcher("glob"), null)
      sparkDependencyExcludeRule.addConfiguration(ivyConfName)
      val scalaArtifacts = new ArtifactId(new ModuleId("*", "scala-library"), "*", "*", "*")
      val scalaDependencyExcludeRule =
        new DefaultExcludeRule(scalaArtifacts, ivySettings.getMatcher("glob"), null)
      scalaDependencyExcludeRule.addConfiguration(ivyConfName)

      // Exclude any Spark dependencies, and add all supplied maven artifacts as dependencies
      md.addExcludeRule(sparkDependencyExcludeRule)
      md.addExcludeRule(scalaDependencyExcludeRule)
      addDependenciesToIvy(md, artifacts, ivyConfName)

      // resolve dependencies
      val rr: ResolveReport = ivy.resolve(md, resolveOptions)
      if (rr.hasError) {
        throw new RuntimeException(rr.getAllProblemMessages.toString)
      }
      // retrieve all resolved dependencies
      ivy.retrieve(rr.getModuleDescriptor.getModuleRevisionId,
        packagesDirectory.getAbsolutePath + File.separator + "[artifact](-[classifier]).[ext]",
        retrieveOptions.setConfs(Array(ivyConfName)))

      resolveDependencyPaths(rr.getArtifacts.toArray, packagesDirectory)
    }
  }
}

/**
 * Provides an indirection layer for passing arguments as system properties or flags to
 * the user's driver program or to downstream launcher tools.
 */
private case class OptionAssigner(
    value: String,
    clusterManager: Int,
    deployMode: Int,
    clOption: String = null,
    sysProp: String = null)
