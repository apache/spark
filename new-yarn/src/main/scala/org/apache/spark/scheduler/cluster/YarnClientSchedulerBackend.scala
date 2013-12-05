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

private[spark] class YarnClientSchedulerBackend(
    scheduler: ClusterScheduler,
    sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
  with Logging {

  var client: Client = null
  var appId: ApplicationId = null

  override def start() {
    super.start()

    val defalutWorkerCores = "2"
    val defalutWorkerMemory = "512m"
    val defaultWorkerNumber = "1"

    val userJar = System.getenv("SPARK_YARN_APP_JAR")
    var workerCores = System.getenv("SPARK_WORKER_CORES")
    var workerMemory = System.getenv("SPARK_WORKER_MEMORY")
    var workerNumber = System.getenv("SPARK_WORKER_INSTANCES")

    if (userJar == null)
      throw new SparkException("env SPARK_YARN_APP_JAR is not set")

    if (workerCores == null)
      workerCores = defalutWorkerCores
    if (workerMemory == null)
      workerMemory = defalutWorkerMemory
    if (workerNumber == null)
      workerNumber = defaultWorkerNumber

    val driverHost = System.getProperty("spark.driver.host")
    val driverPort = System.getProperty("spark.driver.port")
    val hostport = driverHost + ":" + driverPort

    val argsArray = Array[String](
      "--class", "notused",
      "--jar", userJar,
      "--args", hostport,
      "--worker-memory", workerMemory,
      "--worker-cores", workerCores,
      "--num-workers", workerNumber,
      "--master-class", "org.apache.spark.deploy.yarn.WorkerLauncher"
    )

    val args = new ClientArguments(argsArray)
    client = new Client(args)
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
