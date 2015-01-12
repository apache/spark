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

package org.apache.spark.deploy.yarn

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.util.{Utils, IntParam, MemoryParam}

// TODO: Add code and support for ensuring that yarn resource 'tasks' are location aware !
private[spark] class ClientArguments(args: Array[String], sparkConf: SparkConf) {
  var addJars: String = null
  var files: String = null
  var archives: String = null
  var userJar: String = null
  var userClass: String = null
  var userArgs: Seq[String] = Seq[String]()
  var executorMemory = 1024 // MB
  var executorCores = 1
  var numExecutors = DEFAULT_NUMBER_EXECUTORS
  var amQueue = sparkConf.get("spark.yarn.queue", "default")
  var amMemory: Int = 512 // MB
  var appName: String = "Spark"
  var priority = 0
  def isClusterMode: Boolean = userClass != null

  private var driverMemory: Int = 512 // MB
  private val driverMemOverheadKey = "spark.yarn.driver.memoryOverhead"
  private val amMemKey = "spark.yarn.am.memory"
  private val amMemOverheadKey = "spark.yarn.am.memoryOverhead"
  private val isDynamicAllocationEnabled =
    sparkConf.getBoolean("spark.dynamicAllocation.enabled", false)

  parseArgs(args.toList)
  loadEnvironmentArgs()
  validateArgs()

  // Additional memory to allocate to containers
  val amMemoryOverheadConf = if (isClusterMode) driverMemOverheadKey else amMemOverheadKey
  val amMemoryOverhead = sparkConf.getInt(amMemoryOverheadConf,
    math.max((MEMORY_OVERHEAD_FACTOR * amMemory).toInt, MEMORY_OVERHEAD_MIN))

  val executorMemoryOverhead = sparkConf.getInt("spark.yarn.executor.memoryOverhead",
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN))

  /** Load any default arguments provided through environment variables and Spark properties. */
  private def loadEnvironmentArgs(): Unit = {
    // For backward compatibility, SPARK_YARN_DIST_{ARCHIVES/FILES} should be resolved to hdfs://,
    // while spark.yarn.dist.{archives/files} should be resolved to file:// (SPARK-2051).
    files = Option(files)
      .orElse(sys.env.get("SPARK_YARN_DIST_FILES"))
      .orElse(sparkConf.getOption("spark.yarn.dist.files").map(p => Utils.resolveURIs(p)))
      .orNull
    archives = Option(archives)
      .orElse(sys.env.get("SPARK_YARN_DIST_ARCHIVES"))
      .orElse(sparkConf.getOption("spark.yarn.dist.archives").map(p => Utils.resolveURIs(p)))
      .orNull
    // If dynamic allocation is enabled, start at the max number of executors
    if (isDynamicAllocationEnabled) {
      val maxExecutorsConf = "spark.dynamicAllocation.maxExecutors"
      if (!sparkConf.contains(maxExecutorsConf)) {
        throw new IllegalArgumentException(
          s"$maxExecutorsConf must be set if dynamic allocation is enabled!")
      }
      numExecutors = sparkConf.get(maxExecutorsConf).toInt
    }
  }

  /**
   * Fail fast if any arguments provided are invalid.
   * This is intended to be called only after the provided arguments have been parsed.
   */
  private def validateArgs(): Unit = {
    if (numExecutors <= 0) {
      throw new IllegalArgumentException(
        "You must specify at least 1 executor!\n" + getUsageMessage())
    }
    if (isClusterMode) {
      for (key <- Seq(amMemKey, amMemOverheadKey)) {
        if (sparkConf.contains(key)) {
          println(s"$key is set but does not apply in cluster mode.")
        }
      }
      amMemory = driverMemory
    } else {
      if (sparkConf.contains(driverMemOverheadKey)) {
        println(s"$driverMemOverheadKey is set but does not apply in client mode.")
      }
      sparkConf.getOption(amMemKey)
        .map(Utils.memoryStringToMb)
        .foreach { mem => amMemory = mem }
    }
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    val userArgsBuffer = new ArrayBuffer[String]()
    var args = inputArgs

    while (!args.isEmpty) {
      args match {
        case ("--jar") :: value :: tail =>
          userJar = value
          args = tail

        case ("--class") :: value :: tail =>
          userClass = value
          args = tail

        case ("--args" | "--arg") :: value :: tail =>
          if (args(0) == "--args") {
            println("--args is deprecated. Use --arg instead.")
          }
          userArgsBuffer += value
          args = tail

        case ("--master-class" | "--am-class") :: value :: tail =>
          println(s"${args(0)} is deprecated and is not used anymore.")
          args = tail

        case ("--master-memory" | "--driver-memory") :: MemoryParam(value) :: tail =>
          if (args(0) == "--master-memory") {
            println("--master-memory is deprecated. Use --driver-memory instead.")
          }
          driverMemory = value
          args = tail

        case ("--num-workers" | "--num-executors") :: IntParam(value) :: tail =>
          if (args(0) == "--num-workers") {
            println("--num-workers is deprecated. Use --num-executors instead.")
          }
          // Dynamic allocation is not compatible with this option
          if (isDynamicAllocationEnabled) {
            throw new IllegalArgumentException("Explicitly setting the number " +
              "of executors is not compatible with spark.dynamicAllocation.enabled!")
          }
          numExecutors = value
          args = tail

        case ("--worker-memory" | "--executor-memory") :: MemoryParam(value) :: tail =>
          if (args(0) == "--worker-memory") {
            println("--worker-memory is deprecated. Use --executor-memory instead.")
          }
          executorMemory = value
          args = tail

        case ("--worker-cores" | "--executor-cores") :: IntParam(value) :: tail =>
          if (args(0) == "--worker-cores") {
            println("--worker-cores is deprecated. Use --executor-cores instead.")
          }
          executorCores = value
          args = tail

        case ("--queue") :: value :: tail =>
          amQueue = value
          args = tail

        case ("--name") :: value :: tail =>
          appName = value
          args = tail

        case ("--addJars") :: value :: tail =>
          addJars = value
          args = tail

        case ("--files") :: value :: tail =>
          files = value
          args = tail

        case ("--archives") :: value :: tail =>
          archives = value
          args = tail

        case Nil =>

        case _ =>
          throw new IllegalArgumentException(getUsageMessage(args))
      }
    }

    userArgs = userArgsBuffer.readOnly
  }

  private def getUsageMessage(unknownParam: List[String] = null): String = {
    val message = if (unknownParam != null) s"Unknown/unsupported param $unknownParam\n" else ""
    message + """
      |Usage: org.apache.spark.deploy.yarn.Client [options]
      |Options:
      |  --jar JAR_PATH           Path to your application's JAR file (required in yarn-cluster
      |                           mode)
      |  --class CLASS_NAME       Name of your application's main class (required)
      |  --arg ARG                Argument to be passed to your application's main class.
      |                           Multiple invocations are possible, each will be passed in order.
      |  --num-executors NUM      Number of executors to start (Default: 2)
      |  --executor-cores NUM     Number of cores for the executors (Default: 1).
      |  --driver-memory MEM      Memory for driver (e.g. 1000M, 2G) (Default: 512 Mb)
      |  --executor-memory MEM    Memory per executor (e.g. 1000M, 2G) (Default: 1G)
      |  --name NAME              The name of your application (Default: Spark)
      |  --queue QUEUE            The hadoop queue to use for allocation requests (Default:
      |                           'default')
      |  --addJars jars           Comma separated list of local jars that want SparkContext.addJar
      |                           to work with.
      |  --files files            Comma separated list of files to be distributed with the job.
      |  --archives archives      Comma separated list of archives to be distributed with the job.
      """.stripMargin
  }
}
