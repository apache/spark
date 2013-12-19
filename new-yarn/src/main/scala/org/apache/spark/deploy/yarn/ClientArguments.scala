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

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.scheduler.{InputFormatInfo, SplitInfo}
import org.apache.spark.util.IntParam
import org.apache.spark.util.MemoryParam


// TODO: Add code and support for ensuring that yarn resource 'tasks' are location aware !
class ClientArguments(val args: Array[String]) {
  var addJars: String = null
  var files: String = null
  var archives: String = null
  var userJar: String = null
  var userClass: String = null
  var userArgs: Seq[String] = Seq[String]()
  var workerMemory = 1024 // MB
  var workerCores = 1
  var numWorkers = 2
  var amQueue = System.getProperty("QUEUE", "default")
  var amMemory: Int = 512 // MB
  var amClass: String = "org.apache.spark.deploy.yarn.ApplicationMaster"
  var appName: String = "Spark"
  // TODO
  var inputFormatInfo: List[InputFormatInfo] = null
  // TODO(harvey)
  var priority = 0

  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    val userArgsBuffer: ArrayBuffer[String] = new ArrayBuffer[String]()
    val inputFormatMap: HashMap[String, InputFormatInfo] = new HashMap[String, InputFormatInfo]()

    var args = inputArgs

    while (!args.isEmpty) {
      args match {
        case ("--jar") :: value :: tail =>
          userJar = value
          args = tail

        case ("--class") :: value :: tail =>
          userClass = value
          args = tail

        case ("--args") :: value :: tail =>
          userArgsBuffer += value
          args = tail

        case ("--master-class") :: value :: tail =>
          amClass = value
          args = tail

        case ("--master-memory") :: MemoryParam(value) :: tail =>
          amMemory = value
          args = tail

        case ("--num-workers") :: IntParam(value) :: tail =>
          numWorkers = value
          args = tail

        case ("--worker-memory") :: MemoryParam(value) :: tail =>
          workerMemory = value
          args = tail

        case ("--worker-cores") :: IntParam(value) :: tail =>
          workerCores = value
          args = tail

        case ("--queue") :: value :: tail =>
          amQueue = value
          args = tail

        case ("--name") :: value :: tail =>
          appName = value
          args = tail

        case ("--addJars") :: value :: tail =>
          addJars = value
          args = tail

        case ("--files") :: value :: tail =>
          files = value
          args = tail

        case ("--archives") :: value :: tail =>
          archives = value
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
    inputFormatInfo = inputFormatMap.values.toList
  }


  def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    if (unknownParam != null) {
      System.err.println("Unknown/unsupported param " + unknownParam)
    }
    System.err.println(
      "Usage: org.apache.spark.deploy.yarn.Client [options] \n" +
      "Options:\n" +
      "  --jar JAR_PATH             Path to your application's JAR file (required)\n" +
      "  --class CLASS_NAME         Name of your application's main class (required)\n" +
      "  --args ARGS                Arguments to be passed to your application's main class.\n" +
      "                             Mutliple invocations are possible, each will be passed in order.\n" +
      "  --num-workers NUM          Number of workers to start (Default: 2)\n" +
      "  --worker-cores NUM         Number of cores for the workers (Default: 1). This is unsused right now.\n" +
      "  --master-class CLASS_NAME  Class Name for Master (Default: spark.deploy.yarn.ApplicationMaster)\n" +
      "  --master-memory MEM        Memory for Master (e.g. 1000M, 2G) (Default: 512 Mb)\n" +
      "  --worker-memory MEM        Memory per Worker (e.g. 1000M, 2G) (Default: 1G)\n" +
      "  --name NAME                The name of your application (Default: Spark)\n" +
      "  --queue QUEUE              The hadoop queue to use for allocation requests (Default: 'default')\n" +
      "  --addJars jars             Comma separated list of local jars that want SparkContext.addJar to work with.\n" +
      "  --files files              Comma separated list of files to be distributed with the job.\n" +
      "  --archives archives        Comma separated list of archives to be distributed with the job."
      )
    System.exit(exitCode)
  }

}
