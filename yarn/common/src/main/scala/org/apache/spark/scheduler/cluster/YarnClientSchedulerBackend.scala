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
import org.apache.spark.deploy.yarn.{Client, ClientArguments, ExecutorLauncher, YarnSparkHadoopUtil}
import org.apache.spark.scheduler.TaskSchedulerImpl

import scala.collection.mutable.ArrayBuffer

private[spark] class YarnClientSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
  with Logging {

  if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
    minRegisteredRatio = 0.8
  }

  var client: Client = null
  var appId: ApplicationId = null
  var checkerThread: Thread = null
  var stopping: Boolean = false
  var totalExpectedExecutors = 0

  private[spark] def addArg(optionName: String, envVar: String, sysProp: String,
      arrayBuf: ArrayBuffer[String]) {
    if (System.getenv(envVar) != null) {
      arrayBuf += (optionName, System.getenv(envVar))
    } else if (sc.getConf.contains(sysProp)) {
      arrayBuf += (optionName, sc.getConf.get(sysProp))
    }
  }

  override def start() {
    super.start()

    val driverHost = conf.get("spark.driver.host")
    val driverPort = conf.get("spark.driver.port")
    val hostport = driverHost + ":" + driverPort
    conf.set("spark.driver.appUIAddress", sc.ui.appUIHostPort)
    conf.set("spark.driver.appUIHistoryAddress", YarnSparkHadoopUtil.getUIHistoryAddress(sc, conf))

    val argsArrayBuf = new ArrayBuffer[String]()
    argsArrayBuf += (
      "--class", "notused",
      "--jar", null, // The primary jar will be added dynamically in SparkContext.
      "--args", hostport,
      "--am-class", classOf[ExecutorLauncher].getName
    )

    // process any optional arguments, given either as environment variables
    // or system properties. use the defaults already defined in ClientArguments
    // if things aren't specified. system properties override environment
    // variables.
    List(("--driver-memory", "SPARK_MASTER_MEMORY", "spark.master.memory"),
      ("--driver-memory", "SPARK_DRIVER_MEMORY", "spark.driver.memory"),
      ("--num-executors", "SPARK_WORKER_INSTANCES", "spark.executor.instances"),
      ("--num-executors", "SPARK_EXECUTOR_INSTANCES", "spark.executor.instances"),
      ("--executor-memory", "SPARK_WORKER_MEMORY", "spark.executor.memory"),
      ("--executor-memory", "SPARK_EXECUTOR_MEMORY", "spark.executor.memory"),
      ("--executor-cores", "SPARK_WORKER_CORES", "spark.executor.cores"),
      ("--executor-cores", "SPARK_EXECUTOR_CORES", "spark.executor.cores"),
      ("--queue", "SPARK_YARN_QUEUE", "spark.yarn.queue"),
      ("--name", "SPARK_YARN_APP_NAME", "spark.app.name"))
    .foreach { case (optName, envVar, sysProp) => addArg(optName, envVar, sysProp, argsArrayBuf) }

    logDebug("ClientArguments called with: " + argsArrayBuf)
    val args = new ClientArguments(argsArrayBuf.toArray, conf)
    totalExpectedExecutors = args.numExecutors
    client = new Client(args, conf)
    appId = client.runApp()
    waitForApp()
    checkerThread = yarnApplicationStateCheckerThread()
  }

  def waitForApp() {

    // TODO : need a better way to find out whether the executors are ready or not
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

  private def yarnApplicationStateCheckerThread(): Thread = {
    val t = new Thread {
      override def run() {
        while (!stopping) {
          val report = client.getApplicationReport(appId)
          val state = report.getYarnApplicationState()
          if (state == YarnApplicationState.FINISHED || state == YarnApplicationState.KILLED
            || state == YarnApplicationState.FAILED) {
            logError(s"Yarn application already ended: $state")
            sc.stop()
            stopping = true
          }
          Thread.sleep(1000L)
        }
        checkerThread = null
        Thread.currentThread().interrupt()
      }
    }
    t.setName("Yarn Application State Checker")
    t.setDaemon(true)
    t.start()
    t
  }

  override def stop() {
    stopping = true
    super.stop()
    client.stop
    logInfo("Stopped")
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= totalExpectedExecutors * minRegisteredRatio
  }
}
