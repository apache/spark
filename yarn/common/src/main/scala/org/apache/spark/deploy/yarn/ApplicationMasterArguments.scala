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

import org.apache.spark.util.IntParam
import collection.mutable.ArrayBuffer

class ApplicationMasterArguments(val args: Array[String]) {
  var userJar: String = null
  var userClass: String = null
  var userArgs: Seq[String] = Seq[String]()
  var executorMemory = 1024
  var executorCores = 1
  var numExecutors = ApplicationMasterArguments.DEFAULT_NUMBER_EXECUTORS

  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    val userArgsBuffer = new ArrayBuffer[String]()

    var args = inputArgs

    while (! args.isEmpty) {
      // --num-workers, --worker-memory, and --worker-cores are deprecated since 1.0,
      // the properties with executor in their names are preferred.
      args match {
        case ("--jar") :: value :: tail =>
          userJar = value
          args = tail

        case ("--class") :: value :: tail =>
          userClass = value
          args = tail

        case ("--args" | "--arg") :: value :: tail =>
          userArgsBuffer += value
          args = tail

        case ("--num-workers" | "--num-executors") :: IntParam(value) :: tail =>
          numExecutors = value
          args = tail

        case ("--worker-memory" | "--executor-memory") :: IntParam(value) :: tail =>
          executorMemory = value
          args = tail

        case ("--worker-cores" | "--executor-cores") :: IntParam(value) :: tail =>
          executorCores = value
          args = tail

        case Nil =>
          if (userJar == null || userClass == null) {
            printUsageAndExit(1)
          }

        case _ =>
          printUsageAndExit(1, args)
      }
    }

    userArgs = userArgsBuffer.readOnly
  }

  def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    if (unknownParam != null) {
      System.err.println("Unknown/unsupported param " + unknownParam)
    }
    System.err.println(
      "Usage: org.apache.spark.deploy.yarn.ApplicationMaster [options] \n" +
      "Options:\n" +
      "  --jar JAR_PATH       Path to your application's JAR file (required)\n" +
      "  --class CLASS_NAME   Name of your application's main class (required)\n" +
      "  --args ARGS          Arguments to be passed to your application's main class.\n" +
      "                       Mutliple invocations are possible, each will be passed in order.\n" +
      "  --num-executors NUM    Number of executors to start (Default: 2)\n" +
      "  --executor-cores NUM   Number of cores for the executors (Default: 1)\n" +
      "  --executor-memory MEM  Memory per executor (e.g. 1000M, 2G) (Default: 1G)\n")
    System.exit(exitCode)
  }
}

object ApplicationMasterArguments {
  val DEFAULT_NUMBER_EXECUTORS = 2
}
