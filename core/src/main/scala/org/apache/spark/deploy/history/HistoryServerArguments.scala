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

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

/**
 * Command-line parser for the master.
 */
private[spark] class HistoryServerArguments(conf: SparkConf, args: Array[String]) {
  var logDir = conf.get("spark.history.fs.logDirectory", null)

  parse(args.toList)

  private def parse(args: List[String]): Unit = {
    args match {
      case ("--dir" | "-d") :: value :: tail =>
        logDir = value
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case Nil =>

      case _ =>
        printUsageAndExit(1)
    }
    validateLogDir()
    conf.set("spark.history.fs.logDirectory", logDir)
  }

  private def validateLogDir() {
    if (logDir == null) {
      System.err.println("Logging directory must be specified.")
      printUsageAndExit(1)
    }
    val fileSystem = Utils.getHadoopFileSystem(new URI(logDir))
    val path = new Path(logDir)
    if (!fileSystem.exists(path)) {
      System.err.println("Logging directory specified does not exist: %s".format(logDir))
      printUsageAndExit(1)
    }
    if (!fileSystem.getFileStatus(path).isDir) {
      System.err.println("Logging directory specified is not a directory: %s".format(logDir))
      printUsageAndExit(1)
    }
  }

  private def printUsageAndExit(exitCode: Int) {
    System.err.println(
      """
      |Usage: HistoryServer [-d logDir]
      |
      |The preferred way to pass options is to set the configuration below using
      |SPARK_HISTORY_OPTS. The "-d" command line argument is avalable for backwards
      |compatibility, and overrides "spark.history.fs.logDirectory".
      |
      |History Server options are always available; additional options depend on the provider.
      |
      |History Server options:
      |
      |  spark.history.ui.port      Port where server will listen for connections (default 18080)
      |  spark.history.acls.enable  Whether to enable view acls for all applications (default false)
      |  spark.history.provider     Name of history provider class (defaults to file system-based
      |                             provider)
      |
      |FsHistoryProvider options:
      |
      |  spark.history.fs.logDirectory    Directory where app logs are stored (required)
      |  spark.history.fs.updateInterval  How often to reload log data from storage (seconds,
      |                                   default 10)
      |""".stripMargin)
    System.exit(exitCode)
  }

}
