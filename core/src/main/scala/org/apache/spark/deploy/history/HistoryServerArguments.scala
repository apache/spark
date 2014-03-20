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

package org.apache.spark.deploy.history

import java.net.URI

import org.apache.spark.SparkConf
import org.apache.spark.util.{Utils, IntParam}
import org.apache.hadoop.fs.Path

/**
 * Command-line parser for the master.
 */
private[spark] class HistoryServerArguments(args: Array[String], conf: SparkConf) {
  var port = 18080
  var logDir = ""

  parse(args.toList)

  def parse(args: List[String]): Unit = {
    args match {
      case ("--port" | "-p") :: IntParam(value) :: tail =>
        port = value
        parse(tail)

      case ("--dir" | "-d") :: value :: tail =>
        logDir = value
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case Nil => {}

      case _ =>
        printUsageAndExit(1)
    }
    validateLogDir()
  }

  def validateLogDir() {
    if (logDir == "") {
      System.err.println("Logging directory must be specified.")
      printUsageAndExit(1)
    }
    val fileSystem = Utils.getHadoopFileSystem(new URI(logDir))
    val path = new Path(logDir)
    if (!fileSystem.exists(path) || !fileSystem.getFileStatus(path).isDir) {
      System.err.println("Logging directory specified is invalid: %s".format(logDir))
      printUsageAndExit(1)
    }
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: HistoryServer [options]\n" +
      "\n" +
      "Options:\n" +
      "  -p PORT, --port PORT   Port for web server (default: 18080)\n" +
      "  -d DIR,  --dir DIR     Location of event log files")
    System.exit(exitCode)
  }
}
