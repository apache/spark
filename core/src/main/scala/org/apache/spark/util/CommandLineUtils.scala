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

package org.apache.spark.util

import java.io.PrintStream

import org.apache.spark.SparkException

/**
 * Contains basic command line parsing functionality and methods to parse some common Spark CLI
 * options.
 */
private[spark] trait CommandLineUtils {

  // Exposed for testing
  private[spark] var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)

  private[spark] var printStream: PrintStream = System.err

  // scalastyle:off println

  private[spark] def printWarning(str: String): Unit = printStream.println("Warning: " + str)

  private[spark] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }

  // scalastyle:on println

  private[spark] def parseSparkConfProperty(pair: String): (String, String) = {
    pair.split("=", 2).toSeq match {
      case Seq(k, v) => (k, v)
      case _ => printErrorAndExit(s"Spark config without '=': $pair")
        throw new SparkException(s"Spark config without '=': $pair")
    }
  }

  def main(args: Array[String]): Unit
}
