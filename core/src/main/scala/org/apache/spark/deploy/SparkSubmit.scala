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
import java.net.{URI, URL}

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import org.apache.spark.executor.ExecutorURLClassLoader
import org.apache.spark.util.Utils

/**
 * Scala code behind the spark-submit script.  The script handles setting up the classpath with
 * relevant Spark dependencies and provides a layer over the different cluster managers and deploy
 * modes that Spark supports.
 */
object SparkSubmit {
  private val YARN = 1
  private val STANDALONE = 2
  private val MESOS = 4
  private val LOCAL = 8
  private val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL

  private var clusterManager: Int = LOCAL

  /**
   * A special jar name that indicates the class being run is inside of Spark itself,
   * and therefore no user jar is needed.
   */
  private val RESERVED_JAR_NAME = "spark-internal"

  def main(args: Array[String]) {
    val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }
    val (childArgs, classpath, sysProps, mainClass) = createLaunchEnv(appArgs)
    launch(childArgs, classpath, sysProps, mainClass, appArgs.verbose)
  }

  // Exposed for testing
  private[spark] var printStream: PrintStream = System.err
  private[spark] var exitFn: () => Unit = () => System.exit(-1)

  private[spark] def printErrorAndExit(str: String) = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn()
  }
  private[spark] def printWarning(str: String) = printStream.println("Warning: " + str)

  /**
   * @return a tuple containing the arguments for the child, a list of classpath
   *         entries for the child, a list of system properties, a list of env vars
   *         and the main class for the child
   */
  private[spark] def createLaunchEnv(args: SparkSubmitArguments): (ArrayBuffer[String],
      ArrayBuffer[String], Map[String, String], String) = {
    if (args.master.startsWith("local")) {
      clusterManager = LOCAL
    } else if (args.master.startsWith("yarn")) {
      clusterManager = YARN
    } else if (args.master.startsWith("spark")) {
      clusterManager = STANDALONE
    } else if (args.master.startsWith("mesos")) {
      clusterManager = MESOS
    } else {
      printErrorAndExit("Master must start with yarn, mesos, spark, or local")
    }

    // Because "yarn-cluster" and "yarn-client" encapsulate both the master
    // and deploy mode, we have some logic to infer the master and deploy mode
    // from each other if only one is specified, or exit early if they are at odds.
    if (args.deployMode == null &&
        (args.master == "yarn-standalone" || args.master == "yarn-cluster")) {
      args.deployMode = "cluster"
    }
    if (args.deployMode == "cluster" && args.master == "yarn-client") {
      printErrorAndExit("Deploy mode \"cluster\" and master \"yarn-client\" are not compatible")
    }
    if (args.deployMode == "client" &&
        (args.master == "yarn-standalone" || args.master == "yarn-cluster")) {
      printErrorAndExit("Deploy mode \"client\" and master \"" + args.master
        + "\" are not compatible")
    }
    if (args.deployMode == "cluster" && args.master.startsWith("yarn")) {
      args.master = "yarn-cluster"
    }
    if (args.deployMode != "cluster" && args.master.startsWith("yarn")) {
      args.master = "yarn-client"
    }

    val deployOnCluster = Option(args.deployMode).getOrElse("client") == "cluster"

    val childClasspath = new ArrayBuffer[String]()
    val childArgs = new ArrayBuffer[String]()
    val sysProps = new HashMap[String, String]()
    var childMainClass = ""

    val isPython = args.isPython
    val isYarnCluster = clusterManager == YARN && deployOnCluster

    if (clusterManager == MESOS && deployOnCluster) {
      printErrorAndExit("Cannot currently run driver on the cluster in Mesos")
    }

    // If we're running a Python app, set the Java class to run to be our PythonRunner, add
    // Python files to deployment list, and pass the main file and Python path to PythonRunner
    if (isPython) {
      if (deployOnCluster) {
        printErrorAndExit("Cannot currently run Python driver programs on cluster")
      }
      args.mainClass = "org.apache.spark.deploy.PythonRunner"
      args.files = mergeFileLists(args.files, args.pyFiles, args.primaryResource)
      val pyFiles = Option(args.pyFiles).getOrElse("")
      args.childArgs = ArrayBuffer(args.primaryResource, pyFiles) ++ args.childArgs
      args.primaryResource = RESERVED_JAR_NAME
      sysProps("spark.submit.pyFiles") = pyFiles
    }

    // If we're deploying into YARN, use yarn.Client as a wrapper around the user class
    if (!deployOnCluster) {
      childMainClass = args.mainClass
      if (args.primaryResource != RESERVED_JAR_NAME) {
        childClasspath += args.primaryResource
      }
    } else if (clusterManager == YARN) {
      childMainClass = "org.apache.spark.deploy.yarn.Client"
      childArgs += ("--jar", args.primaryResource)
      childArgs += ("--class", args.mainClass)
    }

    // Make sure YARN is included in our build if we're trying to use it
    if (clusterManager == YARN) {
      if (!Utils.classIsLoadable("org.apache.spark.deploy.yarn.Client") && !Utils.isTesting) {
        printErrorAndExit("Could not load YARN classes. " +
          "This copy of Spark may not have been compiled with YARN support.")
      }
    }

    // Special flag to avoid deprecation warnings at the client
    sysProps("SPARK_SUBMIT") = "true"

    // A list of rules to map each argument to system properties or command-line options in
    // each deploy mode; we iterate through these below
    val options = List[OptionAssigner](
      OptionAssigner(args.master, ALL_CLUSTER_MGRS, false, sysProp = "spark.master"),
      OptionAssigner(args.name, ALL_CLUSTER_MGRS, false, sysProp = "spark.app.name"),
      OptionAssigner(args.name, YARN, true, clOption = "--name", sysProp = "spark.app.name"),
      OptionAssigner(args.driverExtraClassPath, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraClassPath"),
      OptionAssigner(args.driverExtraJavaOptions, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraJavaOptions"),
      OptionAssigner(args.driverExtraLibraryPath, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraLibraryPath"),
      OptionAssigner(args.driverMemory, YARN, true, clOption = "--driver-memory"),
      OptionAssigner(args.driverMemory, STANDALONE, true, clOption = "--memory"),
      OptionAssigner(args.driverCores, STANDALONE, true, clOption = "--cores"),
      OptionAssigner(args.queue, YARN, true, clOption = "--queue"),
      OptionAssigner(args.queue, YARN, false, sysProp = "spark.yarn.queue"),
      OptionAssigner(args.numExecutors, YARN, true, clOption = "--num-executors"),
      OptionAssigner(args.numExecutors, YARN, false, sysProp = "spark.executor.instances"),
      OptionAssigner(args.executorMemory, YARN, true, clOption = "--executor-memory"),
      OptionAssigner(args.executorMemory, STANDALONE | MESOS | YARN, false,
        sysProp = "spark.executor.memory"),
      OptionAssigner(args.executorCores, YARN, true, clOption = "--executor-cores"),
      OptionAssigner(args.executorCores, YARN, false, sysProp = "spark.executor.cores"),
      OptionAssigner(args.totalExecutorCores, STANDALONE | MESOS, false,
        sysProp = "spark.cores.max"),
      OptionAssigner(args.files, YARN, false, sysProp = "spark.yarn.dist.files"),
      OptionAssigner(args.files, YARN, true, clOption = "--files"),
      OptionAssigner(args.files, LOCAL | STANDALONE | MESOS, false, sysProp = "spark.files"),
      OptionAssigner(args.files, LOCAL | STANDALONE | MESOS, true, sysProp = "spark.files"),
      OptionAssigner(args.archives, YARN, false, sysProp = "spark.yarn.dist.archives"),
      OptionAssigner(args.archives, YARN, true, clOption = "--archives"),
      OptionAssigner(args.jars, YARN, true, clOption = "--addJars"),
      OptionAssigner(args.jars, ALL_CLUSTER_MGRS, false, sysProp = "spark.jars")
    )

    // For client mode make any added jars immediately visible on the classpath
    if (args.jars != null && !deployOnCluster) {
      for (jar <- args.jars.split(",")) {
        childClasspath += jar
      }
    }

    // Map all arguments to command-line options or system properties for our chosen mode
    for (opt <- options) {
      if (opt.value != null && deployOnCluster == opt.deployOnCluster &&
          (clusterManager & opt.clusterManager) != 0) {
        if (opt.clOption != null) {
          childArgs += (opt.clOption, opt.value)
        }
        if (opt.sysProp != null) {
          sysProps.put(opt.sysProp, opt.value)
        }
      }
    }

    // Add the application jar automatically so the user doesn't have to call sc.addJar
    // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
    // For python files, the primary resource is already distributed as a regular file
    if (!isYarnCluster && !isPython) {
      var jars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq())
      if (args.primaryResource != RESERVED_JAR_NAME) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sysProps.put("spark.jars", jars.mkString(","))
    }

    // Standalone cluster specific configurations
    if (deployOnCluster && clusterManager == STANDALONE) {
      if (args.supervise) {
        childArgs += "--supervise"
      }
      childMainClass = "org.apache.spark.deploy.Client"
      childArgs += "launch"
      childArgs += (args.master, args.primaryResource, args.mainClass)
    }

    // Arguments to be passed to user program
    if (args.childArgs != null) {
      if (!deployOnCluster || clusterManager == STANDALONE) {
        childArgs ++= args.childArgs
      } else if (clusterManager == YARN) {
        for (arg <- args.childArgs) {
          childArgs += ("--arg", arg)
        }
      }
    }

    // Read from default spark properties, if any
    for ((k, v) <- args.getDefaultSparkProperties) {
      if (!sysProps.contains(k)) sysProps(k) = v
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
    val localJarFile = new File(new URI(localJar).getPath())
    if (!localJarFile.exists()) {
      printWarning(s"Jar $localJar does not exist, skipping.")
    }

    val url = localJarFile.getAbsoluteFile.toURI.toURL
    loader.addURL(url)
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
    deployOnCluster: Boolean,
    clOption: String = null,
    sysProp: String = null)
