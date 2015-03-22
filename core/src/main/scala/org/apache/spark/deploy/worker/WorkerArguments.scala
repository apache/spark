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

package org.apache.spark.deploy.worker

import java.lang.management.ManagementFactory

import org.apache.spark.util.{IntParam, MemoryParam, Utils}
import org.apache.spark.SparkConf
import org.apache.spark.util.expression.{BaseParser, Parsers}
import org.apache.spark.util.expression.quantity.ByteQuantity

/**
 * Command-line parser for the worker.
 */
private[spark] class WorkerArguments(args: Array[String], conf: SparkConf) {
  var host = Utils.localHostName()
  var port = 0
  var webUiPort = 8081
  var workerCores: String = System.getenv("SPARK_WORKER_CORES")
  var workerMem: String = conf.getenv("SPARK_WORKER_MEMORY")
  var masters: Array[String] = null
  var workDir: String = null
  var propertiesFile: String = null

  // Check for settings in environment variables
  if (System.getenv("SPARK_WORKER_PORT") != null) {
    port = System.getenv("SPARK_WORKER_PORT").toInt
  }

  if (System.getenv("SPARK_WORKER_WEBUI_PORT") != null) {
    webUiPort = System.getenv("SPARK_WORKER_WEBUI_PORT").toInt
  }
  if (System.getenv("SPARK_WORKER_DIR") != null) {
    workDir = System.getenv("SPARK_WORKER_DIR")
  }

  parse(args.toList)

  val cores: Int = Option(workerCores)
    .flatMap(Parsers.NumberParser.parse)
    // cores should always be at least 1 if an expression is given
    .map(x => if (x.round >= 1) x.round.toInt else 1)
    .getOrElse(inferDefaultCores)

  def getNewStyleMemMB(mem: String): Option[Int] = {
    Parsers.ByteParser.parse(mem).map(x => ByteQuantity(x, "B").toMB.toInt)
  }

  def getOldStyleMemMB(mem: String): Option[Int] = {
    Some(Utils.memoryStringToMb(mem))
  }

  val memory: Int = Option(workerMem)
    // Explicitly revert to old code path if string does not have tag indicating expression
    .flatMap(x => if (x.head == BaseParser.EXP_TAG) getNewStyleMemMB(x) else getOldStyleMemMB(x))
    .getOrElse(inferDefaultMemory())

  // NB - We are currently not parsing expressions from Spark defaults file

  // This mutates the SparkConf, so all accesses to it must be made after this line
  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)

  if (conf.contains("spark.worker.ui.port")) {
    webUiPort = conf.get("spark.worker.ui.port").toInt
  }

  checkWorkerMemory()

  def parse(args: List[String]): Unit = args match {
    case ("--ip" | "-i") :: value :: tail =>
      Utils.checkHost(value, "ip no longer supported, please use hostname " + value)
      host = value
      parse(tail)

    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value, "Please use hostname " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--cores" | "-c") :: value :: tail =>
      workerCores = value
      parse(tail)

    case ("--memory" | "-m") :: value :: tail =>
      workerMem = value
      parse(tail)

    case ("--work-dir" | "-d") :: value :: tail =>
      workDir = value
      parse(tail)

    case "--webui-port" :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case value :: tail =>
      if (masters != null) {  // Two positional arguments were given
        printUsageAndExit(1)
      }
      masters = value.stripPrefix("spark://").split(",").map("spark://" + _)
      parse(tail)

    case Nil =>
      if (masters == null) {  // No positional argument was given
        printUsageAndExit(1)
      }

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: Worker [options] <master>\n" +
      "\n" +
      "Master must be a URL of the form spark://hostname:port\n" +
      "\n" +
      "Options:\n" +
      "  -c CORES, --cores CORES  Number of cores to use\n" +
      "  -m MEM, --memory MEM     Amount of memory to use (e.g. 1000M, 2G)\n" +
      "  -d DIR, --work-dir DIR   Directory to run apps in (default: SPARK_HOME/work)\n" +
      "  -i HOST, --ip IP         Hostname to listen on (deprecated, please use --host or -h)\n" +
      "  -h HOST, --host HOST     Hostname to listen on\n" +
      "  -p PORT, --port PORT     Port to listen on (default: random)\n" +
      "  --webui-port PORT        Port for web UI (default: 8081)\n" +
      "  --properties-file FILE   Path to a custom Spark properties file.\n" +
      "                           Default is conf/spark-defaults.conf.")
    System.exit(exitCode)
  }

  def inferDefaultCores(): Int = {
    Runtime.getRuntime.availableProcessors()
  }

  def inferDefaultMemory(): Int = {
    val ibmVendor = System.getProperty("java.vendor").contains("IBM")
    var totalMb = 0
    try {
      val bean = ManagementFactory.getOperatingSystemMXBean()
      if (ibmVendor) {
        val beanClass = Class.forName("com.ibm.lang.management.OperatingSystemMXBean")
        val method = beanClass.getDeclaredMethod("getTotalPhysicalMemory")
        totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
      } else {
        val beanClass = Class.forName("com.sun.management.OperatingSystemMXBean")
        val method = beanClass.getDeclaredMethod("getTotalPhysicalMemorySize")
        totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
      }
    } catch {
      case e: Exception => {
        totalMb = 2*1024
        System.out.println("Failed to get total physical memory. Using " + totalMb + " MB")
      }
    }
    // Leave out 1 GB for the operating system, but don't return a negative memory size
    math.max(totalMb - 1024, 512)
  }

  def checkWorkerMemory(): Unit = {
    if (memory <= 0) {
      val message = "Memory can't be 0, missing a M or G on the end of the memory specification?"
      throw new IllegalStateException(message)
    }
  }
}
