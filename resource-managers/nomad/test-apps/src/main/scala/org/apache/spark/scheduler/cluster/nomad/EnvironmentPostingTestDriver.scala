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

package org.apache.spark.scheduler.cluster.nomad

import org.apache.spark.{SparkConf, SparkContext}

private[spark] object EnvironmentPostingTestDriver extends TestApplication {

  def main(args: Array[String]): Unit = {
    val rootDriverUrl +: rootExecutorUrl +: environmentVariables = args.toSeq

    val sc = new SparkContext(new SparkConf())
    try {
      sc.parallelize(Seq(1))
        .foreach(_ => putEnvironmentVariables(rootExecutorUrl, environmentVariables))
    } catch { case e: Throwable => s"ERROR: $e" }

    try putEnvironmentVariables(rootDriverUrl, environmentVariables)
    finally sc.stop()
  }

  def putEnvironmentVariables(rootUrl: String, environmentVariables: Seq[String]): Unit = {

    logInfo("---- START ENVIRONMENT VARIABLES ----")
    sys.env.foreach { case (k, v) =>
      logInfo(s"$k=$v")
    }
    logInfo("---- END ENVIRONMENT VARIABLES ----")

    environmentVariables.foreach { environmentVariable =>
      httpPut(rootUrl + environmentVariable) {
        sys.env.getOrElse(environmentVariable, "NOT SET")
      }
    }
  }

}
