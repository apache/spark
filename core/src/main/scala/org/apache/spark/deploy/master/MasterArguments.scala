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

package org.apache.spark.deploy.master

import org.apache.spark.SparkConf
import org.apache.spark.util.{IntParam, Utils}

/**
 * Arguments parser for the master.
 */
private[spark] class MasterArguments(args: Array[String], conf: SparkConf) {
  var host: String = null
  var port: Int = -1
  var webUiPort: Int = -1
  var propertiesFile: String = null

  parse(args.toList)

  // This mutates the SparkConf, so all accesses to it must be made after this line
  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)

  loadEnvironmentArguments()

  /**
   * Load arguments from environment variables, Spark properties etc.
   */
  private def loadEnvironmentArguments(): Unit = {
    host = Option(host)
      .orElse(conf.getOption("spark.master.host"))
      .orElse(Option(conf.getenv("SPARK_MASTER_HOST")))
      .getOrElse(Utils.localHostName())
    if (port < 0) {
      port = conf.getOption("spark.master.port")
        .orElse(Option(conf.getenv("SPARK_MASTER_PORT")))
        .getOrElse("7077")
        .toInt
    }
    if (webUiPort < 0) {
      webUiPort = conf.getOption("spark.master.ui.port")
        .orElse(Option(conf.getenv("SPARK_MASTER_WEBUI_PORT")))
        .getOrElse("8080")
        .toInt
    }
  }

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

    case "--webui-port" :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil => {}

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      """Usage: Master [options]
        |
        |Options:
        |  -i HOST, --ip HOST     Hostname to listen on (deprecated, please use --host or -h).
        |  -h HOST, --host HOST   Hostname to listen on.
        |  -p PORT, --port PORT   Port to listen on (default: 7077).
        |  --webui-port PORT      Port for web UI (default: 8080).
        |  --properties-file FILE Path to a custom Spark properties file.
        |                         Default is conf/spark-defaults.conf.
      """.stripMargin)
    System.exit(exitCode)
  }
}
