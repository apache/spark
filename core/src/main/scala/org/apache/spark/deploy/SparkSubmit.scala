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
import java.lang.reflect.InvocationTargetException
import java.net.URL

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import org.apache.spark.executor.ExecutorURLClassLoader
import org.apache.spark.util.Utils

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

  // Exposed for testing
  private[spark] var exitFn: () => Unit = () => System.exit(-1)
  private[spark] var printStream: PrintStream = System.err
  private[spark] def printWarning(str: String) = printStream.println("Warning: " + str)
  private[spark] def printErrorAndExit(str: String) = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn()
  }

  def main(args: Array[String]) {
    val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }
    val (childArgs, classpath, sysProps, mainClass) = createLaunchEnv(appArgs)
    launch(childArgs, classpath, sysProps, mainClass, appArgs.verbose)
  }

  /**
   * @return a tuple containing
   *           (1) the arguments for the child process,
   *           (2) a list of classpath entries for the child,
   *           (3) a list of system properties and env vars, and
   *           (4) the main class for the child
   */
  private[spark] def createLaunchEnv(args: SparkSubmitArguments)
      : (ArrayBuffer[String], ArrayBuffer[String], Map[String, String], String) = {

    // Values to return
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

    // The following modes are not supported or applicable
    (clusterManager, deployMode) match {
      case (MESOS, CLUSTER) =>
        printErrorAndExit("Cluster deploy mode is currently not supported for Mesos clusters.")
      case (_, CLUSTER) if args.isPython =>
        printErrorAndExit("Cluster deploy mode is currently not supported for python applications.")
      case (_, CLUSTER) if isShell(args.primaryResource) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark shells.")
      case _ =>
    }

    // If we're running a python app, set the main class to our specific python runner
    if (args.isPython) {
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = "py4j.GatewayServer"
        args.childArgs = ArrayBuffer("--die-on-broken-pipe", "0")
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(args.primaryResource, args.pyFiles) ++ args.childArgs
        args.files = mergeFileLists(args.files, args.primaryResource)
      }
      args.files = mergeFileLists(args.files, args.pyFiles)
      // Format python file paths properly before adding them to the PYTHONPATH
      sysProps("spark.submit.pyFiles") = PythonRunner.formatPaths(args.pyFiles).mkString(",")
    }

    // Special flag to avoid deprecation warnings at the client
    sysProps("SPARK_SUBMIT") = "true"

    // A list of rules to map each argument to system properties or command-line options in
    // each deploy mode; we iterate through these below
    val options = List[OptionAssigner](

      // All cluster managers
      OptionAssigner(args.master, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, sysProp = "spark.master"),
      OptionAssigner(args.name, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, sysProp = "spark.app.name"),
      OptionAssigner(args.jars, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, sysProp = "spark.jars"),

      // Standalone cluster only
      OptionAssigner(args.driverMemory, STANDALONE, CLUSTER, clOption = "--memory"),
      OptionAssigner(args.driverCores, STANDALONE, CLUSTER, clOption = "--cores"),

      // Yarn client only
      OptionAssigner(args.queue, YARN, CLIENT, sysProp = "spark.yarn.queue"),
      OptionAssigner(args.numExecutors, YARN, CLIENT, sysProp = "spark.executor.instances"),
      OptionAssigner(args.executorCores, YARN, CLIENT, sysProp = "spark.executor.cores"),
      OptionAssigner(args.files, YARN, CLIENT, sysProp = "spark.yarn.dist.files"),
      OptionAssigner(args.archives, YARN, CLIENT, sysProp = "spark.yarn.dist.archives"),

      // Yarn cluster only
      OptionAssigner(args.name, YARN, CLUSTER, clOption = "--name"),
      OptionAssigner(args.driverMemory, YARN, CLUSTER, clOption = "--driver-memory"),
      OptionAssigner(args.queue, YARN, CLUSTER, clOption = "--queue"),
      OptionAssigner(args.numExecutors, YARN, CLUSTER, clOption = "--num-executors"),
      OptionAssigner(args.executorMemory, YARN, CLUSTER, clOption = "--executor-memory"),
      OptionAssigner(args.executorCores, YARN, CLUSTER, clOption = "--executor-cores"),
      OptionAssigner(args.files, YARN, CLUSTER, clOption = "--files"),
      OptionAssigner(args.archives, YARN, CLUSTER, clOption = "--archives"),
      OptionAssigner(args.jars, YARN, CLUSTER, clOption = "--addJars"),

      // Other options
      OptionAssigner(args.driverExtraClassPath, STANDALONE | YARN, CLUSTER,
        sysProp = "spark.driver.extraClassPath"),
      OptionAssigner(args.driverExtraJavaOptions, STANDALONE | YARN, CLUSTER,
        sysProp = "spark.driver.extraJavaOptions"),
      OptionAssigner(args.driverExtraLibraryPath, STANDALONE | YARN, CLUSTER,
        sysProp = "spark.driver.extraLibraryPath"),
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
    val isYarnCluster = clusterManager == YARN && deployMode == CLUSTER
    if (!isYarnCluster && !args.isPython) {
      var jars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq.empty)
      if (isUserJar(args.primaryResource)) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sysProps.put("spark.jars", jars.mkString(","))
    }

    // In standalone-cluster mode, use Client as a wrapper around the user class
    if (clusterManager == STANDALONE && deployMode == CLUSTER) {
      childMainClass = "org.apache.spark.deploy.Client"
      if (args.supervise) {
        childArgs += "--supervise"
      }
      childArgs += "launch"
      childArgs += (args.master, args.primaryResource, args.mainClass)
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // In yarn-cluster mode, use yarn.Client as a wrapper around the user class
    if (clusterManager == YARN && deployMode == CLUSTER) {
      childMainClass = "org.apache.spark.deploy.yarn.Client"
      if (args.primaryResource != SPARK_INTERNAL) {
        childArgs += ("--jar", args.primaryResource)
      }
      childArgs += ("--class", args.mainClass)
      if (args.childArgs != null) {
        args.childArgs.foreach { arg => childArgs += ("--arg", arg) }
      }
    }

    // Properties given with --conf are superceded by other options, but take precedence over
    // properties in the defaults file.
    for ((k, v) <- args.sparkProperties) {
      sysProps.getOrElseUpdate(k, v)
    }

    // Read from default spark properties, if any
    for ((k, v) <- args.getDefaultSparkProperties) {
      sysProps.getOrElseUpdate(k, v)
    }

    (childArgs, childClasspath, sysProps, childMainClass)
  }

  private def launch(
      childArgs: ArrayBuffer[String],
      childClasspath: ArrayBuffer[String],
      sysProps: Map[String, String],
      childMainClass: String,
      verbose: Boolean = false) {
    if (verbose) {
      printStream.println(s"Main class:\n$childMainClass")
      printStream.println(s"Arguments:\n${childArgs.mkString("\n")}")
      printStream.println(s"System properties:\n${sysProps.mkString("\n")}")
      printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      printStream.println("\n")
    }

    val loader = new ExecutorURLClassLoader(new Array[URL](0),
      Thread.currentThread.getContextClassLoader)
    Thread.currentThread.setContextClassLoader(loader)

    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    for ((key, value) <- sysProps) {
      System.setProperty(key, value)
    }

    val mainClass = Class.forName(childMainClass, true, loader)
    val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
    try {
      mainMethod.invoke(null, childArgs.toArray)
    } catch {
      case e: InvocationTargetException => e.getCause match {
        case cause: Throwable => throw cause
        case null => throw e
      }
    }
  }

  private def addJarToClasspath(localJar: String, loader: ExecutorURLClassLoader) {
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

/**
 * Provides an indirection layer for passing arguments as system properties or flags to
 * the user's driver program or to downstream launcher tools.
 */
private[spark] case class OptionAssigner(
    value: String,
    clusterManager: Int,
    deployMode: Int,
    clOption: String = null,
    sysProp: String = null)
