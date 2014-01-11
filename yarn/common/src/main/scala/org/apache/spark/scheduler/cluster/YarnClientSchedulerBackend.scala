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

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.records.{ApplicationId, YarnApplicationState}
import org.apache.spark.{SparkException, Logging, SparkContext}
import org.apache.spark.deploy.yarn.{Client, ClientArguments}
import org.apache.spark.scheduler.TaskSchedulerImpl

import scala.collection.mutable.ArrayBuffer

private[spark] class YarnClientSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
  with Logging {

  var client: Client = null
  var appId: ApplicationId = null

  private[spark] def addArg(optionName: String, optionalParam: String, arrayBuf: ArrayBuffer[String]) {
    Option(System.getenv(optionalParam)) foreach {
      optParam => {
        arrayBuf += (optionName, optParam)
      }
    }
  }

  override def start() {
    super.start()

    val userJar = System.getenv("SPARK_YARN_APP_JAR")
    if (userJar == null)
      throw new SparkException("env SPARK_YARN_APP_JAR is not set")

    val driverHost = conf.get("spark.driver.host")
    val driverPort = conf.get("spark.driver.port")
    val hostport = driverHost + ":" + driverPort

    val argsArrayBuf = new ArrayBuffer[String]()
    argsArrayBuf += (
      "--class", "notused",
      "--jar", userJar,
      "--args", hostport,
      "--master-class", "org.apache.spark.deploy.yarn.WorkerLauncher"
    )

    // process any optional arguments, use the defaults already defined in ClientArguments 
    // if things aren't specified
    Map("--master-memory" -> "SPARK_MASTER_MEMORY",
      "--num-workers" -> "SPARK_WORKER_INSTANCES",
      "--worker-memory" -> "SPARK_WORKER_MEMORY",
      "--worker-cores" -> "SPARK_WORKER_CORES",
      "--queue" -> "SPARK_YARN_QUEUE",
      "--name" -> "SPARK_YARN_APP_NAME",
      "--files" -> "SPARK_YARN_DIST_FILES",
      "--archives" -> "SPARK_YARN_DIST_ARCHIVES")
    .foreach { case (optName, optParam) => addArg(optName, optParam, argsArrayBuf) }
      
    logDebug("ClientArguments called with: " + argsArrayBuf)
    val args = new ClientArguments(argsArrayBuf.toArray, conf)
    client = new Client(args, conf)
    appId = client.runApp()
    waitForApp()
  }

  def waitForApp() {

    // TODO : need a better way to find out whether the workers are ready or not
    // maybe by resource usage report?
    while(true) {
      val report = client.getApplicationReport(appId)

      logInfo("Application report from ASM: \n" +
        "\t appMasterRpcPort: " + report.getRpcPort() + "\n" +
        "\t appStartTime: " + report.getStartTime() + "\n" +
        "\t yarnAppState: " + report.getYarnApplicationState() + "\n"
      )

      // Ready to go, or already gone.
      val state = report.getYarnApplicationState()
      if (state == YarnApplicationState.RUNNING) {
        return
      } else if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        throw new SparkException("Yarn application already ended," +
          "might be killed or not able to launch application master.")
      }

      Thread.sleep(1000)
    }
  }

  override def stop() {
    super.stop()
    client.stop()
    logInfo("Stoped")
  }

}
