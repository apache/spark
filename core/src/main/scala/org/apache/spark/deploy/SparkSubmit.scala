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
import java.lang.reflect.{Modifier, InvocationTargetException}
import java.net.URL

import scala.collection._

import org.apache.spark.executor.ExecutorURLClassLoader
import org.apache.spark.util.Utils
import org.apache.spark.deploy.ConfigConstants._

/**
 * Main gateway of launching a Spark application.
 *
 * This program handles setting up the classpath with relevant Spark dependencies and provides
 * a layer over the different cluster managers and deploy modes that Spark supports.
 */
object SparkSubmit {
  // A special jar name that indicates the class being run is inside of Spark itself, and therefore
  // no user jar is needed.
  val SPARK_INTERNAL = "spark-internal"

  // Special primary resource names that represent shells rather than application jars.
  val SPARK_SHELL = "spark-shell"
  val PYSPARK_SHELL = "pyspark-shell"

  // Special python classes
  val PY4J_GATEWAYSERVER: String = "py4j.GatewayServer"
  val PYTHON_RUNNER: String = "org.apache.spark.deploy.PythonRunner"

  private val CLASS_NOT_FOUND_EXIT_STATUS = 101

  // Unit tests for deploy currently disable exitFn() from working, so we need to stop execution
  // via generating this exception when we are testing
  private[spark] case class ApplicationExitException(s: String) extends Exception

  // Exposed for testing purposed.
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
      : (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String], Map[String, String], String) = {

    // Values to return
    val childArgs = new mutable.ArrayBuffer[String]()
    val childClasspath = new mutable.ArrayBuffer[String]()
    val sysProps = new mutable.HashMap[String, String]()
    var childMainClass = ""

    if (args.clusterManagerFlag == CM_YARN) {
      // Make sure YARN is included in our build if we're trying to use it
      if (!Utils.classIsLoadable("org.apache.spark.deploy.yarn.Client") && !Utils.isTesting) {
        printErrorAndExit(
          "Could not load YARN classes. " +
          "This copy of Spark may not have been compiled with YARN support.")
      }
      val hasHadoopEnv = sys.env.contains("HADOOP_CONF_DIR") || sys.env.contains("YARN_CONF_DIR")
      if (!hasHadoopEnv && !Utils.isTesting) {
        throw new Exception("When running with master '" + args.master + "'" +
          "either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.")
      }
    }

    // If we're running a python app, set the main class to our specific python runner
    if (args.isPython) {
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = PY4J_GATEWAYSERVER
        args.childArgs = mutable.ArrayBuffer("--die-on-broken-pipe", "0")
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = PYTHON_RUNNER
        args.childArgs = mutable.ArrayBuffer(args.primaryResource,
          args.pyFiles.getOrElse("")) ++ args.childArgs
        args.files = mergeFileLists(args.files.orNull, args.primaryResource)
      }
      args.files = mergeFileLists(args.files.orNull, args.pyFiles.orNull)
      // Format python file paths properly before adding them to the PYTHONPATH
      sysProps("spark.submit.pyFiles") = PythonRunner.formatPaths(
        args.pyFiles.getOrElse("")).mkString(",")
    }

    // As arguments get processed they are removed from this map.
    val unprocessedArgs = new mutable.HashSet ++= args.conf.keySet

    // Special flag to avoid deprecation warnings at the client
    sysProps("SPARK_SUBMIT") = "true"

    /* By default, spark config properties will be passed to child processes as system properties
     * unless they are mentioned in a list of rules below which map arguments to either
     * 1. a command line option (clOption=...)
     * 2. a differently named system property (sysProp=...)
     */
    val options = List[OptionAssigner](
      // Standalone cluster only
      OptionAssigner(SPARK_DRIVER_MEMORY, CM_STANDALONE, DM_CLUSTER, clOption = "--memory"),
      OptionAssigner(SPARK_DRIVER_CORES, CM_STANDALONE, DM_CLUSTER, clOption = "--cores"),

      //  yarn client
      OptionAssigner(SPARK_FILES, CM_YARN, DM_CLIENT, sysProp = SPARK_YARN_DIST_FILES,
        keepProperty=true),

      // Yarn cluster only
      OptionAssigner(SPARK_APP_NAME, CM_YARN, DM_CLUSTER, clOption = "--name", keepProperty = true),
      OptionAssigner(SPARK_DRIVER_MEMORY, CM_YARN, DM_CLUSTER, clOption = "--driver-memory"),
      OptionAssigner(SPARK_YARN_QUEUE, CM_YARN, DM_CLUSTER, clOption = "--queue"),
      OptionAssigner(SPARK_EXECUTOR_INSTANCES, CM_YARN, DM_CLUSTER, clOption = "--num-executors"),
      OptionAssigner(SPARK_EXECUTOR_MEMORY, CM_YARN, DM_CLUSTER, clOption = "--executor-memory",
        keepProperty=true),
      OptionAssigner(SPARK_EXECUTOR_CORES, CM_YARN, DM_CLUSTER, clOption = "--executor-cores"),
      OptionAssigner(SPARK_FILES, CM_YARN, DM_CLUSTER, clOption = "--files", keepProperty=true),
      OptionAssigner(SPARK_YARN_DIST_ARCHIVES, CM_YARN, DM_CLUSTER, clOption = "--archives",
        keepProperty=true),
      OptionAssigner(SPARK_JARS, CM_YARN, DM_CLUSTER, clOption = "--addJars")
    )

    // In client mode, launch the application main class directly
    // In addition, add the main application jar and any added jars (if any) to the classpath
    if (args.deployModeFlag == DM_CLIENT) {
      childMainClass = args.mainClass
      if (isUserJar(args.primaryResource)) {
        childClasspath += args.primaryResource
      }
      if (args.jars.isDefined) { childClasspath ++= args.jars.get.split(",") }
      if (args.childArgs != null) { childArgs ++= args.childArgs }
    }

    // Map all arguments to command-line options or system properties for our chosen mode
    for (opt <- options) {
      if (unprocessedArgs.contains(opt.configKey) &&
          (args.deployModeFlag & opt.deployMode) != 0 &&
          (args.clusterManagerFlag & opt.clusterManager) != 0) {
        val optValue = args.conf(opt.configKey)
        if (opt.clOption != null) { childArgs += (opt.clOption, optValue) }
        if (opt.sysProp != null) { sysProps.put(opt.sysProp, optValue) }
        if (!opt.keepProperty) {
          unprocessedArgs -= opt.configKey
        }
      }
    }

    // Add the application jar automatically so the user doesn't have to call sc.addJar
    // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
    // For python files, the primary resource is already distributed as a regular file
    if (!args.isYarnCluster && !args.isPython) {
      var jars = args.conf.get(SPARK_JARS).map(x => x.split(",").toSeq).getOrElse(Seq.empty)
      if (isUserJar(args.primaryResource)) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sysProps.put(SPARK_JARS, jars.mkString(","))
      unprocessedArgs -= SPARK_JARS
    }

    // In standalone-cluster mode, use Client as a wrapper around the user class
    if (args.clusterManagerFlag == CM_STANDALONE && args.deployModeFlag == DM_CLUSTER) {
      childMainClass = "org.apache.spark.deploy.Client"
      if (args.supervise) {
        childArgs += "--supervise"
      }
      childArgs += "launch"
      childArgs += (args.master, args.primaryResource, args.mainClass)

      unprocessedArgs --= Seq(SPARK_APP_PRIMARY_RESOURCE, SPARK_APP_CLASS,
        SPARK_DRIVER_SUPERVISE)

      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // In yarn-cluster mode, use yarn.Client as a wrapper around the user class
    if (args.isYarnCluster) {
      childMainClass = "org.apache.spark.deploy.yarn.Client"
      if (args.primaryResource != SPARK_INTERNAL) {
        childArgs += ("--jar", args.primaryResource)
        unprocessedArgs -= SPARK_APP_PRIMARY_RESOURCE
      }
      childArgs += ("--class", args.mainClass)
      unprocessedArgs -= SPARK_APP_CLASS
      if (args.childArgs != null) {
        args.childArgs.foreach { arg => childArgs += ("--arg", arg) }
      }
    }

    // Those config items that haven't already been processed will get passed as system properties.
    for (k <- unprocessedArgs) {
      sysProps.getOrElseUpdate(k, args.conf(k))
    }

    (childArgs, childClasspath, sysProps, childMainClass)
  }

  private def launch(
               childArgs: mutable.ArrayBuffer[String],
               childClasspath: mutable.ArrayBuffer[String],
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

    var mainClass: Class[_] = null

    try {
      mainClass = Class.forName(childMainClass, true, loader)
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace(printStream)
        if (childMainClass.contains("thriftserver")) {
          println(s"Failed to load main class $childMainClass.")
          println("You need to build Spark with -Phive.")
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
    }

    val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the given main class must be static")
    }
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
   * Return whether the given primary resource represents a shell.
   */
  private[spark] def isInternalOrShell(primaryResource: String): Boolean = {
    isInternal(primaryResource) || isShell(primaryResource)
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
private[spark] case class OptionAssigner(configKey: String,
                                         clusterManager: Int,
                                         deployMode: Int,
                                         clOption: String = null,
                                         keepProperty: Boolean = false,
                                         sysProp: String = null)
