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

import scala.annotation.tailrec

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{ConfigEntry, History}
import org.apache.spark.util.Utils

/**
 * Command-line parser for the [[HistoryServer]].
 */
private[history] class HistoryServerArguments(conf: SparkConf, args: Array[String])
  extends Logging {
  private var propertiesFile: String = null

  parse(args.toList)

  @tailrec
  private def parse(args: List[String]): Unit = {
    args match {
      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case ("--properties-file") :: value :: tail =>
        propertiesFile = value
        parse(tail)

      case Nil =>

      case other =>
        val errorMsg = s"Unrecognized options: ${other.mkString(" ")}\n"
        printUsageAndExit(1, errorMsg)
    }
  }

  // This mutates the SparkConf, so all accesses to it must be made after this line
  Utils.loadDefaultSparkProperties(conf, propertiesFile)
  // Initialize logging system again after `spark.log.structuredLogging.enabled` takes effect
  Utils.resetStructuredLogging(conf)
  Logging.uninitialize()

  // scalastyle:off line.size.limit println
  private def printUsageAndExit(exitCode: Int, error: String = ""): Unit = {
    val configs = History.getClass.getDeclaredFields
      .filter(f => classOf[ConfigEntry[_]].isAssignableFrom(f.getType))
      .map { f =>
        f.setAccessible(true)
        f.get(History).asInstanceOf[ConfigEntry[_]]
      }
    val maxConfigLength = configs.map(_.key.length).max
    val sb = new StringBuilder(
      s"""
         |${error}Usage: HistoryServer [options]
         |
         |Options:
         |  ${"--properties-file FILE".padTo(maxConfigLength, ' ')} Path to a custom Spark properties file.
         |  ${"".padTo(maxConfigLength, ' ')} Default is conf/spark-defaults.conf.
         |
         |Configuration options can be set by setting the corresponding JVM system property.
         |History Server options are always available; additional options depend on the provider.
         |
         |""".stripMargin)

    def printConfigs(configs: Array[ConfigEntry[_]]): Unit = {
      configs.sortBy(_.key).foreach { conf =>
        sb.append("  ").append(conf.key.padTo(maxConfigLength, ' '))
        var currentDocLen = 0
        val intention = "\n" + " " * (maxConfigLength + 2)
        conf.doc.split("\\s+").foreach { word =>
          if (currentDocLen + word.length > 60) {
            sb.append(intention).append(" ").append(word)
            currentDocLen = word.length + 1
          } else {
            sb.append(" ").append(word)
            currentDocLen += word.length + 1
          }
        }
        sb.append(intention).append(" (Default: ").append(conf.defaultValueString).append(")\n")
      }
    }
    val (common, fs) = configs.partition(!_.key.startsWith("spark.history.fs."))
    sb.append("History Server options:\n")
    printConfigs(common)
    sb.append("FsHistoryProvider options:\n")
    printConfigs(fs)
    System.err.println(sb.toString())
    // scalastyle:on line.size.limit println
    System.exit(exitCode)
  }
}

