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

import java.net.{URI, URISyntaxException}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

import org.apache.log4j.Level

import org.apache.spark.util.{IntParam, MemoryParam, Utils}

/**
 * Command-line parser for the driver client.
 */
private[deploy] class ClientArguments(args: Array[String]) {
  import ClientArguments._

  var cmd: String = "" // 'launch' or 'kill'
  var logLevel = Level.WARN

  // launch parameters
  var masters: Array[String] = null
  var jarUrl: String = ""
  var mainClass: String = ""
  var supervise: Boolean = DEFAULT_SUPERVISE
  var memory: Int = DEFAULT_MEMORY
  var cores: Int = DEFAULT_CORES
  private var _driverOptions = ListBuffer[String]()
  def driverOptions: Seq[String] = _driverOptions.toSeq

  // kill parameters
  var driverId: String = ""

  parse(args.toList)

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--cores" | "-c") :: IntParam(value) :: tail =>
      cores = value
      parse(tail)

    case ("--memory" | "-m") :: MemoryParam(value) :: tail =>
      memory = value
      parse(tail)

    case ("--supervise" | "-s") :: tail =>
      supervise = true
      parse(tail)

    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case ("--verbose" | "-v") :: tail =>
      logLevel = Level.INFO
      parse(tail)

    case "launch" :: _master :: _jarUrl :: _mainClass :: tail =>
      cmd = "launch"

      if (!ClientArguments.isValidJarUrl(_jarUrl)) {
        // scalastyle:off println
        println(s"Jar url '${_jarUrl}' is not in valid format.")
        println(s"Must be a jar file path in URL format " +
          "(e.g. hdfs://host:port/XX.jar, file:///XX.jar)")
        // scalastyle:on println
        printUsageAndExit(-1)
      }

      jarUrl = _jarUrl
      masters = Utils.parseStandaloneMasterUrls(_master)
      mainClass = _mainClass
      _driverOptions ++= tail

    case "kill" :: _master :: _driverId :: tail =>
      cmd = "kill"
      masters = Utils.parseStandaloneMasterUrls(_master)
      driverId = _driverId

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  private def printUsageAndExit(exitCode: Int): Unit = {
    // TODO: It wouldn't be too hard to allow users to submit their app and dependency jars
    //       separately similar to in the YARN client.
    val usage =
     s"""
      |Usage: DriverClient [options] launch <active-master> <jar-url> <main-class> [driver options]
      |Usage: DriverClient kill <active-master> <driver-id>
      |
      |Options:
      |   -c CORES, --cores CORES        Number of cores to request (default: $DEFAULT_CORES)
      |   -m MEMORY, --memory MEMORY     Megabytes of memory to request (default: $DEFAULT_MEMORY)
      |   -s, --supervise                Whether to restart the driver on failure
      |                                  (default: $DEFAULT_SUPERVISE)
      |   -v, --verbose                  Print more debugging output
     """.stripMargin
    // scalastyle:off println
    System.err.println(usage)
    // scalastyle:on println
    System.exit(exitCode)
  }
}

private[deploy] object ClientArguments {
  val DEFAULT_CORES = 1
  val DEFAULT_MEMORY = Utils.DEFAULT_DRIVER_MEM_MB // MB
  val DEFAULT_SUPERVISE = false

  def isValidJarUrl(s: String): Boolean = {
    try {
      val uri = new URI(s)
      uri.getScheme != null && uri.getPath != null && uri.getPath.endsWith(".jar")
    } catch {
      case _: URISyntaxException => false
    }
  }
}
