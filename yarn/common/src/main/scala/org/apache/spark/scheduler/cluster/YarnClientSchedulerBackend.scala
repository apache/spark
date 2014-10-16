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

  if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
    minRegisteredRatio = 0.8
  }

  private var client: Client = null
  private var appId: ApplicationId = null
  private var stopping: Boolean = false
  private var totalExpectedExecutors = 0

  /**
   * Create a Yarn client to submit an application to the ResourceManager.
   * This waits until the application is running.
   */
  override def start() {
    super.start()
    val driverHost = conf.get("spark.driver.host")
    val driverPort = conf.get("spark.driver.port")
    val hostport = driverHost + ":" + driverPort
    sc.ui.foreach { ui => conf.set("spark.driver.appUIAddress", ui.appUIHostPort) }

    val argsArrayBuf = new ArrayBuffer[String]()
    argsArrayBuf += ("--arg", hostport)
    argsArrayBuf ++= getExtraClientArguments

    logDebug("ClientArguments called with: " + argsArrayBuf.mkString(" "))
    val args = new ClientArguments(argsArrayBuf.toArray, conf)
    totalExpectedExecutors = args.numExecutors
    client = new Client(args, conf)
    appId = client.submitApplication()
    waitForApplication()
    asyncMonitorApplication()
  }

  /**
   * Return any extra command line arguments to be passed to Client provided in the form of
   * environment variables or Spark properties.
   */
  private def getExtraClientArguments: Seq[String] = {
    val extraArgs = new ArrayBuffer[String]
    val optionTuples = // List of (target Client argument, environment variable, Spark property)
      List(
        ("--driver-memory", "SPARK_MASTER_MEMORY", "spark.master.memory"),
        ("--driver-memory", "SPARK_DRIVER_MEMORY", "spark.driver.memory"),
        ("--num-executors", "SPARK_WORKER_INSTANCES", "spark.executor.instances"),
        ("--num-executors", "SPARK_EXECUTOR_INSTANCES", "spark.executor.instances"),
        ("--executor-memory", "SPARK_WORKER_MEMORY", "spark.executor.memory"),
        ("--executor-memory", "SPARK_EXECUTOR_MEMORY", "spark.executor.memory"),
        ("--executor-cores", "SPARK_WORKER_CORES", "spark.executor.cores"),
        ("--executor-cores", "SPARK_EXECUTOR_CORES", "spark.executor.cores"),
        ("--queue", "SPARK_YARN_QUEUE", "spark.yarn.queue"),
        ("--name", "SPARK_YARN_APP_NAME", "spark.app.name")
      )
    optionTuples.foreach { case (optionName, envVar, sparkProp) =>
      if (System.getenv(envVar) != null) {
        extraArgs += (optionName, System.getenv(envVar))
      } else if (sc.getConf.contains(sparkProp)) {
        extraArgs += (optionName, sc.getConf.get(sparkProp))
      }
    }
    extraArgs
  }

  /**
   * Report the state of the application until it is running.
   * If the application has finished, failed or been killed in the process, throw an exception.
   * This assumes both `client` and `appId` have already been set.
   */
  private def waitForApplication(): Unit = {
    assert(client != null && appId != null, "Application has not been submitted yet!")
    val state = client.monitorApplication(appId, returnOnRunning = true) // blocking
    if (state == YarnApplicationState.FINISHED ||
      state == YarnApplicationState.FAILED ||
      state == YarnApplicationState.KILLED) {
      throw new SparkException("Yarn application has already ended! " +
        "It might have been killed or unable to launch application master.")
    }
    if (state == YarnApplicationState.RUNNING) {
      logInfo(s"Application $appId has started running.")
    }
  }

  /**
   * Monitor the application state in a separate thread.
   * If the application has exited for any reason, stop the SparkContext.
   * This assumes both `client` and `appId` have already been set.
   */
  private def asyncMonitorApplication(): Unit = {
    assert(client != null && appId != null, "Application has not been submitted yet!")
    val t = new Thread {
      override def run() {
        while (!stopping) {
          val report = client.getApplicationReport(appId)
          val state = report.getYarnApplicationState()
          if (state == YarnApplicationState.FINISHED ||
            state == YarnApplicationState.KILLED ||
            state == YarnApplicationState.FAILED) {
            logError(s"Yarn application has already exited with state $state!")
            sc.stop()
            stopping = true
          }
          Thread.sleep(1000L)
        }
        Thread.currentThread().interrupt()
      }
    }
    t.setName("Yarn application state monitor")
    t.setDaemon(true)
    t.start()
  }

  /**
   * Stop the scheduler. This assumes `start()` has already been called.
   */
  override def stop() {
    assert(client != null, "Attempted to stop this scheduler before starting it!")
    stopping = true
    super.stop()
    client.stop()
    logInfo("Stopped")
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= totalExpectedExecutors * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).map(_.toString).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

}
