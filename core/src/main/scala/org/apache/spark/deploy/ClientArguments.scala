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

import scala.collection.mutable.ListBuffer

import org.apache.log4j.Level

import org.apache.spark.util.MemoryParam

/**
 * Command-line parser for the driver client.
 */
private[spark] class ClientArguments(args: Array[String]) {
  val defaultCores = 1
  val defaultMemory = 512

  var cmd: String = "" // 'launch' or 'kill'
  var logLevel = Level.WARN

  // launch parameters
  var master: String = ""
  var jarUrl: String = ""
  var mainClass: String = ""
  var supervise: Boolean = false
  var memory: Int = defaultMemory
  var cores: Int = defaultCores
  private var _driverOptions = ListBuffer[String]()
  def driverOptions = _driverOptions.toSeq

  // kill parameters
  var driverId: String = ""

  parse(args.toList)

  def parse(args: List[String]): Unit = args match {
    case ("--cores" | "-c") :: value :: tail =>
      cores = value.toInt
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
        println(s"Jar url '${_jarUrl}' is not in valid format.")
        println(s"Must be a jar file path in URL format (e.g. hdfs://XX.jar, file://XX.jar)")
        printUsageAndExit(-1)
      }

      jarUrl = _jarUrl
      master = _master
      mainClass = _mainClass
      _driverOptions ++= tail

    case "kill" :: _master :: _driverId :: tail =>
      cmd = "kill"
      master = _master
      driverId = _driverId

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    // TODO: It wouldn't be too hard to allow users to submit their app and dependency jars
    //       separately similar to in the YARN client.
    val usage =
     s"""
      |Usage: DriverClient [options] launch <active-master> <jar-url> <main-class> [driver options]
      |Usage: DriverClient kill <active-master> <driver-id>
      |
      |Options:
      |   -c CORES, --cores CORES        Number of cores to request (default: $defaultCores)
      |   -m MEMORY, --memory MEMORY     Megabytes of memory to request (default: $defaultMemory)
      |   -s, --supervise                Whether to restart the driver on failure
      |   -v, --verbose                  Print more debugging output
     """.stripMargin
    System.err.println(usage)
    System.exit(exitCode)
  }
}

object ClientArguments {
  def isValidJarUrl(s: String): Boolean = {
    try {
      val uri = new URI(s)
      uri.getScheme != null && uri.getAuthority != null && s.endsWith("jar")
    } catch {
      case _: URISyntaxException => false
    }
  }
}
