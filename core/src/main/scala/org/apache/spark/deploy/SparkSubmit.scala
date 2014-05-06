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
   * @return
   *         a tuple containing the arguments for the child, a list of classpath
   *         entries for the child, a list of system propertes, a list of env vars
   *         and the main class for the child
   */
  private[spark] def createLaunchEnv(appArgs: SparkSubmitArguments): (ArrayBuffer[String],
      ArrayBuffer[String], Map[String, String], String) = {
    if (appArgs.master.startsWith("local")) {
      clusterManager = LOCAL
    } else if (appArgs.master.startsWith("yarn")) {
      clusterManager = YARN
    } else if (appArgs.master.startsWith("spark")) {
      clusterManager = STANDALONE
    } else if (appArgs.master.startsWith("mesos")) {
      clusterManager = MESOS
    } else {
      printErrorAndExit("Master must start with yarn, mesos, spark, or local")
    }

    // Because "yarn-cluster" and "yarn-client" encapsulate both the master
    // and deploy mode, we have some logic to infer the master and deploy mode
    // from each other if only one is specified, or exit early if they are at odds.
    if (appArgs.deployMode == null &&
        (appArgs.master == "yarn-standalone" || appArgs.master == "yarn-cluster")) {
      appArgs.deployMode = "cluster"
    }
    if (appArgs.deployMode == "cluster" && appArgs.master == "yarn-client") {
      printErrorAndExit("Deploy mode \"cluster\" and master \"yarn-client\" are not compatible")
    }
    if (appArgs.deployMode == "client" &&
        (appArgs.master == "yarn-standalone" || appArgs.master == "yarn-cluster")) {
      printErrorAndExit("Deploy mode \"client\" and master \"" + appArgs.master
        + "\" are not compatible")
    }
    if (appArgs.deployMode == "cluster" && appArgs.master.startsWith("yarn")) {
      appArgs.master = "yarn-cluster"
    }
    if (appArgs.deployMode != "cluster" && appArgs.master.startsWith("yarn")) {
      appArgs.master = "yarn-client"
    }

    val deployOnCluster = Option(appArgs.deployMode).getOrElse("client") == "cluster"

    val childClasspath = new ArrayBuffer[String]()
    val childArgs = new ArrayBuffer[String]()
    val sysProps = new HashMap[String, String]()
    var childMainClass = ""

    if (clusterManager == MESOS && deployOnCluster) {
      printErrorAndExit("Cannot currently run driver on the cluster in Mesos")
    }

    // If we're running a Python app, set the Java class to run to be our PythonRunner, add
    // Python files to deployment list, and pass the main file and Python path to PythonRunner
    if (appArgs.isPython) {
      if (deployOnCluster) {
        printErrorAndExit("Cannot currently run Python driver programs on cluster")
      }
      appArgs.mainClass = "org.apache.spark.deploy.PythonRunner"
      appArgs.files = mergeFileLists(appArgs.files, appArgs.pyFiles, appArgs.primaryResource)
      val pyFiles = Option(appArgs.pyFiles).getOrElse("")
      appArgs.childArgs = ArrayBuffer(appArgs.primaryResource, pyFiles) ++ appArgs.childArgs
      appArgs.primaryResource = RESERVED_JAR_NAME
      sysProps("spark.submit.pyFiles") = pyFiles
    }

    // If we're deploying into YARN, use yarn.Client as a wrapper around the user class
    if (!deployOnCluster) {
      childMainClass = appArgs.mainClass
      if (appArgs.primaryResource != RESERVED_JAR_NAME) {
        childClasspath += appArgs.primaryResource
      }
    } else if (clusterManager == YARN) {
      childMainClass = "org.apache.spark.deploy.yarn.Client"
      childArgs += ("--jar", appArgs.primaryResource)
      childArgs += ("--class", appArgs.mainClass)
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
      OptionAssigner(appArgs.master, ALL_CLUSTER_MGRS, false, sysProp = "spark.master"),
      OptionAssigner(appArgs.driverExtraClassPath, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraClassPath"),
      OptionAssigner(appArgs.driverExtraJavaOptions, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraJavaOptions"),
      OptionAssigner(appArgs.driverExtraLibraryPath, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraLibraryPath"),
      OptionAssigner(appArgs.driverMemory, YARN, true, clOption = "--driver-memory"),
      OptionAssigner(appArgs.name, YARN, true, clOption = "--name"),
      OptionAssigner(appArgs.queue, YARN, true, clOption = "--queue"),
      OptionAssigner(appArgs.queue, YARN, false, sysProp = "spark.yarn.queue"),
      OptionAssigner(appArgs.numExecutors, YARN, true, clOption = "--num-executors"),
      OptionAssigner(appArgs.numExecutors, YARN, false, sysProp = "spark.executor.instances"),
      OptionAssigner(appArgs.executorMemory, YARN, true, clOption = "--executor-memory"),
      OptionAssigner(appArgs.executorMemory, STANDALONE | MESOS | YARN, false,
        sysProp = "spark.executor.memory"),
      OptionAssigner(appArgs.driverMemory, STANDALONE, true, clOption = "--memory"),
      OptionAssigner(appArgs.driverCores, STANDALONE, true, clOption = "--cores"),
      OptionAssigner(appArgs.executorCores, YARN, true, clOption = "--executor-cores"),
      OptionAssigner(appArgs.executorCores, YARN, false, sysProp = "spark.executor.cores"),
      OptionAssigner(appArgs.totalExecutorCores, STANDALONE | MESOS, false,
        sysProp = "spark.cores.max"),
      OptionAssigner(appArgs.files, YARN, false, sysProp = "spark.yarn.dist.files"),
      OptionAssigner(appArgs.files, YARN, true, clOption = "--files"),
      OptionAssigner(appArgs.archives, YARN, false, sysProp = "spark.yarn.dist.archives"),
      OptionAssigner(appArgs.archives, YARN, true, clOption = "--archives"),
      OptionAssigner(appArgs.jars, YARN, true, clOption = "--addJars"),
      OptionAssigner(appArgs.files, LOCAL | STANDALONE | MESOS, false, sysProp = "spark.files"),
      OptionAssigner(appArgs.files, LOCAL | STANDALONE | MESOS, true, sysProp = "spark.files"),
      OptionAssigner(appArgs.jars, LOCAL | STANDALONE | MESOS, false, sysProp = "spark.jars"),
      OptionAssigner(appArgs.name, LOCAL | STANDALONE | MESOS, false, sysProp = "spark.app.name")
    )

    // For client mode make any added jars immediately visible on the classpath
    if (appArgs.jars != null && !deployOnCluster) {
      for (jar <- appArgs.jars.split(",")) {
        childClasspath += jar
      }
    }

    // Map all arguments to command-line options or system properties for our chosen mode
    for (opt <- options) {
      if (opt.value != null && deployOnCluster == opt.deployOnCluster &&
          (clusterManager & opt.clusterManager) != 0) {
        if (opt.clOption != null) {
          childArgs += (opt.clOption, opt.value)
        } else if (opt.sysProp != null) {
          sysProps.put(opt.sysProp, opt.value)
        }
      }
    }

    // For standalone mode, add the application jar automatically so the user doesn't have to
    // call sc.addJar. TODO: Standalone mode in the cluster
    if (clusterManager == STANDALONE) {
      var jars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq())
      if (appArgs.primaryResource != RESERVED_JAR_NAME) {
        jars = jars ++ Seq(appArgs.primaryResource)
      }
      sysProps.put("spark.jars", jars.mkString(","))
    }

    if (deployOnCluster && clusterManager == STANDALONE) {
      if (appArgs.supervise) {
        childArgs += "--supervise"
      }

      childMainClass = "org.apache.spark.deploy.Client"
      childArgs += "launch"
      childArgs += (appArgs.master, appArgs.primaryResource, appArgs.mainClass)
    }

    // Arguments to be passed to user program
    if (appArgs.childArgs != null) {
      if (!deployOnCluster || clusterManager == STANDALONE) {
        childArgs ++= appArgs.childArgs
      } else if (clusterManager == YARN) {
        for (arg <- appArgs.childArgs) {
          childArgs += ("--arg", arg)
        }
      }
    }

    for ((k, v) <- appArgs.getDefaultSparkProperties) {
      if (!sysProps.contains(k)) sysProps(k) = v
    }

    (childArgs, childClasspath, sysProps, childMainClass)
  }

  private def launch(childArgs: ArrayBuffer[String], childClasspath: ArrayBuffer[String],
      sysProps: Map[String, String], childMainClass: String, verbose: Boolean = false)
  {
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
