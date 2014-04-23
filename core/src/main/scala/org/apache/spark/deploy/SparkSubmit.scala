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
import java.net.{URI, URL}

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import org.apache.spark.executor.ExecutorURLClassLoader

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
    printStream.println("error: " + str)
    printStream.println("run with --help for more information or --verbose for debugging output")
    exitFn()
  }
  private[spark] def printWarning(str: String) = printStream.println("warning: " + str)

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
      printErrorAndExit("master must start with yarn, mesos, spark, or local")
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
      printErrorAndExit("Mesos does not support running the driver on the cluster")
    }

    if (!deployOnCluster) {
      childMainClass = appArgs.mainClass
      childClasspath += appArgs.primaryResource
    } else if (clusterManager == YARN) {
      childMainClass = "org.apache.spark.deploy.yarn.Client"
      childArgs += ("--jar", appArgs.primaryResource)
      childArgs += ("--class", appArgs.mainClass)
    }

    val options = List[OptionAssigner](
      new OptionAssigner(appArgs.master, ALL_CLUSTER_MGRS, false, sysProp = "spark.master"),
      new OptionAssigner(appArgs.driverExtraClassPath, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraClassPath"),
      new OptionAssigner(appArgs.driverExtraJavaOptions, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraJavaOptions"),
      new OptionAssigner(appArgs.driverExtraLibraryPath, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraLibraryPath"),
      new OptionAssigner(appArgs.driverMemory, YARN, true, clOption = "--driver-memory"),
      new OptionAssigner(appArgs.name, YARN, true, clOption = "--name"),
      new OptionAssigner(appArgs.queue, YARN, true, clOption = "--queue"),
      new OptionAssigner(appArgs.queue, YARN, false, sysProp = "spark.yarn.queue"),
      new OptionAssigner(appArgs.numExecutors, YARN, true, clOption = "--num-executors"),
      new OptionAssigner(appArgs.numExecutors, YARN, false, sysProp = "spark.executor.instances"),
      new OptionAssigner(appArgs.executorMemory, YARN, true, clOption = "--executor-memory"),
      new OptionAssigner(appArgs.executorMemory, STANDALONE | MESOS | YARN, false,
        sysProp = "spark.executor.memory"),
      new OptionAssigner(appArgs.driverMemory, STANDALONE, true, clOption = "--memory"),
      new OptionAssigner(appArgs.driverCores, STANDALONE, true, clOption = "--cores"),
      new OptionAssigner(appArgs.executorCores, YARN, true, clOption = "--executor-cores"),
      new OptionAssigner(appArgs.executorCores, YARN, false, sysProp = "spark.executor.cores"),
      new OptionAssigner(appArgs.totalExecutorCores, STANDALONE | MESOS, false,
        sysProp = "spark.cores.max"),
      new OptionAssigner(appArgs.files, YARN, false, sysProp = "spark.yarn.dist.files"),
      new OptionAssigner(appArgs.files, YARN, true, clOption = "--files"),
      new OptionAssigner(appArgs.archives, YARN, false, sysProp = "spark.yarn.dist.archives"),
      new OptionAssigner(appArgs.archives, YARN, true, clOption = "--archives"),
      new OptionAssigner(appArgs.jars, YARN, true, clOption = "--addJars"),
      new OptionAssigner(appArgs.files, LOCAL | STANDALONE | MESOS, true, sysProp = "spark.files"),
      new OptionAssigner(appArgs.jars, LOCAL | STANDALONE | MESOS, false, sysProp = "spark.jars"),
      new OptionAssigner(appArgs.name, LOCAL | STANDALONE | MESOS, false,
        sysProp = "spark.app.name")
    )

    // For client mode make any added jars immediately visible on the classpath
    if (appArgs.jars != null && !deployOnCluster) {
      for (jar <- appArgs.jars.split(",")) {
        childClasspath += jar
      }
    }

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
      val existingJars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq())
      sysProps.put("spark.jars", (existingJars ++ Seq(appArgs.primaryResource)).mkString(","))
      println("SPARK JARS" + sysProps.get("spark.jars"))
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
      sysProps: Map[String, String], childMainClass: String, verbose: Boolean = false) {

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
    mainMethod.invoke(null, childArgs.toArray)
  }

  private def addJarToClasspath(localJar: String, loader: ExecutorURLClassLoader) {
    val localJarFile = new File(new URI(localJar).getPath())
    if (!localJarFile.exists()) {
      printWarning(s"Jar $localJar does not exist, skipping.")
    }

    val url = localJarFile.getAbsoluteFile.toURI.toURL
    loader.addURL(url)
  }
}

/**
 * Provides an indirection layer for passing arguments as system properties or flags to
 * the user's driver program or to downstream launcher tools.
 */
private[spark] class OptionAssigner(val value: String,
  val clusterManager: Int,
  val deployOnCluster: Boolean,
  val clOption: String = null,
  val sysProp: String = null
) { }
