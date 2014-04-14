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

import scala.collection.mutable.ArrayBuffer

/**
 * Parses and encapsulates arguments from the spark-submit script.
 */
private[spark] class SparkSubmitArguments(args: Array[String]) {
  var master: String = "local"
  var deployMode: String = null
  var executorMemory: String = null
  var executorCores: String = null
  var totalExecutorCores: String = null
  var driverMemory: String = null
  var driverCores: String = null
  var supervise: Boolean = false
  var queue: String = null
  var numExecutors: String = null
  var files: String = null
  var archives: String = null
  var mainClass: String = null
  var primaryResource: String = null
  var name: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var jars: String = null

  loadEnvVars()
  parseArgs(args.toList)

  def loadEnvVars() {
    master = System.getenv("MASTER")
    deployMode = System.getenv("DEPLOY_MODE")
  }

  def parseArgs(args: List[String]) {
    if (args.size == 0) {
      printUsageAndExit(1)
      System.exit(1)
    }
    primaryResource = args(0)
    parseOpts(args.tail)
  }

  def parseOpts(opts: List[String]): Unit = opts match {
    case ("--name") :: value :: tail =>
      name = value
      parseOpts(tail)

    case ("--master") :: value :: tail =>
      master = value
      parseOpts(tail)

    case ("--class") :: value :: tail =>
      mainClass = value
      parseOpts(tail)

    case ("--deploy-mode") :: value :: tail =>
      if (value != "client" && value != "cluster") {
        System.err.println("--deploy-mode must be either \"client\" or \"cluster\"")
        System.exit(1)
      }
      deployMode = value
      parseOpts(tail)

    case ("--num-executors") :: value :: tail =>
      numExecutors = value
      parseOpts(tail)

    case ("--total-executor-cores") :: value :: tail =>
      totalExecutorCores = value
      parseOpts(tail)

    case ("--executor-cores") :: value :: tail =>
      executorCores = value
      parseOpts(tail)

    case ("--executor-memory") :: value :: tail =>
      executorMemory = value
      parseOpts(tail)

    case ("--driver-memory") :: value :: tail =>
      driverMemory = value
      parseOpts(tail)

    case ("--driver-cores") :: value :: tail =>
      driverCores = value
      parseOpts(tail)

    case ("--supervise") :: tail =>
      supervise = true
      parseOpts(tail)

    case ("--queue") :: value :: tail =>
      queue = value
      parseOpts(tail)

    case ("--files") :: value :: tail =>
      files = value
      parseOpts(tail)

    case ("--archives") :: value :: tail =>
      archives = value
      parseOpts(tail)

    case ("--arg") :: value :: tail =>
      childArgs += value
      parseOpts(tail)

    case ("--jars") :: value :: tail =>
      jars = value
      parseOpts(tail)

    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case Nil =>

    case _ =>
      printUsageAndExit(1, opts)
  }

  def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    if (unknownParam != null) {
      System.err.println("Unknown/unsupported param " + unknownParam)
    }
    System.err.println(
      """Usage: spark-submit <primary binary> [options]
        |Options:
        |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
        |  --deploy-mode DEPLOY_MODE   Mode to deploy the app in, either 'client' or 'cluster'.
        |  --class CLASS_NAME          Name of your app's main class (required for Java apps).
        |  --arg ARG                   Argument to be passed to your application's main class. This
        |                              option can be specified multiple times for multiple args.
        |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 512M).
        |  --name NAME                 The name of your application (Default: 'Spark').
        |  --jars JARS                 A comma-separated list of local jars to include on the
        |                              driver classpath and that SparkContext.addJar will work
        |                              with. Doesn't work on standalone with 'cluster' deploy mode.
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
        |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).
        |  --queue QUEUE_NAME          The YARN queue to submit to (Default: 'default').
        |  --num-executors NUM         Number of executors to (Default: 2).
        |  --files FILES               Comma separated list of files to be placed in the working dir
        |                              of each executor.
        |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
        |                              working dir of each executor.""".stripMargin
    )
    System.exit(exitCode)
  }
}
