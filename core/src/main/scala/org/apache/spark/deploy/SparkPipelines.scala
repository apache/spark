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

import java.util.{Arrays => JArrays, List => JList}
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkUserAppException
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher.SPARK_API_MODE
import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.util.SparkExitCode

/**
 * Outer implementation of the spark-pipelines command line interface. Responsible for routing
 * spark-submit args to spark-submit, and pipeline-specific args to the inner Python CLI
 * implementation that loads the user code and submits it to the backend.
 */
object SparkPipelines extends Logging {
  def main(args: Array[String]): Unit = {
    val pipelinesCliFile = args(0)
    val sparkSubmitAndPipelinesArgs = args.slice(1, args.length)
    SparkSubmit.main(
      constructSparkSubmitArgs(pipelinesCliFile, sparkSubmitAndPipelinesArgs).toArray)
  }

  protected[deploy] def constructSparkSubmitArgs(
      pipelinesCliFile: String,
      args: Array[String]): Seq[String] = {
    val (sparkSubmitArgs, pipelinesArgs) = splitArgs(args)
    sparkSubmitArgs ++ Seq(pipelinesCliFile) ++ pipelinesArgs
  }

  /**
   * Split the arguments into spark-submit args (--master, --remote, etc.) and pipeline args
   * (run, --spec, etc.).
   */
  private def splitArgs(args: Array[String]): (Seq[String], Seq[String]) = {
    val sparkSubmitArgs = new ArrayBuffer[String]()
    val pipelinesArgs = new ArrayBuffer[String]()
    var remote = "local"

    new SparkSubmitArgumentsParser() {
      parse(JArrays.asList(args: _*))

      override protected def handle(opt: String, value: String): Boolean = {
        if (opt == "--remote") {
          remote = value
        } else if (opt == "--class") {
          logError("--class argument not supported.")
          throw SparkUserAppException(SparkExitCode.EXIT_FAILURE)
        } else if ((opt == "--conf" || opt == "-c") && value.startsWith(s"$SPARK_API_MODE=")) {
          val apiMode = value.stripPrefix(s"$SPARK_API_MODE=").trim
          if (apiMode.toLowerCase(Locale.ROOT) != "connect") {
            logError(
              s"$SPARK_API_MODE must be 'connect' (was '$apiMode'). " +
                "Declarative Pipelines currently only supports Spark Connect."
            )
            throw SparkUserAppException(SparkExitCode.EXIT_FAILURE)
          }
        } else if (Seq("--name", "-h", "--help").contains(opt)) {
          pipelinesArgs += opt
          if (value != null && value.nonEmpty) {
            pipelinesArgs += value
          }
        } else {
          sparkSubmitArgs += opt
          if (value != null) {
            sparkSubmitArgs += value
          }
        }

        true
      }

      override protected def handleExtraArgs(extra: JList[String]): Unit = {
        pipelinesArgs.appendAll(extra.asScala)
      }

      override protected def handleUnknown(opt: String): Boolean = {
        pipelinesArgs += opt
        true
      }
    }

    sparkSubmitArgs += "--conf"
    sparkSubmitArgs += s"$SPARK_API_MODE=connect"
    sparkSubmitArgs += "--remote"
    sparkSubmitArgs += remote
    (sparkSubmitArgs.toSeq, pipelinesArgs.toSeq)
  }

}
