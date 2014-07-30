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

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

/**
 * Command-line parser for the master.
 */
private[spark] class HistoryServerArguments(conf: SparkConf, args: Array[String]) {
  private var logDir: String = null
  private var propertiesFile: String = null

  parse(args.toList)

  private def parse(args: List[String]): Unit = {
    args match {
      case ("--dir" | "-d") :: value :: tail =>
        logDir = value
        conf.set("spark.history.fs.logDirectory", value)
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case ("--properties-file") :: value :: tail =>
        propertiesFile = value
        parse(tail)

      case Nil =>

      case _ =>
        printUsageAndExit(1)
    }
  }

  // Use common defaults file, if not specified by user
  propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultConfigFile)
  Option(propertiesFile).foreach(f => conf.loadPropertiesFromFile(f))

  private def printUsageAndExit(exitCode: Int) {
    System.err.println(
      """
      |Usage: HistoryServer [options]
      |
      |Options:
      |  -d DIR, --dir DIR           Directory where app logs are stored.
      |  --properties-file FILE      Path to a file from which to load extra properties. If not
      |                              specified, this will look for conf/spark-defaults.conf.
      |""".stripMargin)
    System.exit(exitCode)
  }

}
