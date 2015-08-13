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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.yarn.api.records.{ApplicationId, YarnApplicationState}

import org.apache.spark.{SparkException, Logging, SparkContext}
import org.apache.spark.deploy.yarn.{Client, ClientArguments, YarnSparkHadoopUtil}
import org.apache.spark.scheduler.TaskSchedulerImpl

private[spark] class YarnClientSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends YarnSchedulerBackend(scheduler, sc)
  with Logging {

  private var client: Client = null
  private var appId: ApplicationId = null
  private var monitorThread: MonitorThread = null

  /**
   * Create a Yarn client to submit an application to the ResourceManager.
   * This waits until the application is running.
   */
  override def start() {
    val driverHost = conf.get("spark.driver.host")
    val driverPort = conf.get("spark.driver.port")
    val hostport = driverHost + ":" + driverPort
    sc.ui.foreach { ui => conf.set("spark.driver.appUIAddress", ui.appUIAddress) }

    val argsArrayBuf = new ArrayBuffer[String]()
    argsArrayBuf += ("--arg", hostport)
    argsArrayBuf ++= getExtraClientArguments

    logDebug("ClientArguments called with: " + argsArrayBuf.mkString(" "))
    val args = new ClientArguments(argsArrayBuf.toArray, conf)
    totalExpectedExecutors = args.numExecutors
    client = new Client(args, conf)
    appId = client.submitApplication()

    // SPARK-8687: Ensure all necessary properties have already been set before
    // we initialize our driver scheduler backend, which serves these properties
    // to the executors
    super.start()

    waitForApplication()

    // SPARK-8851: In yarn-client mode, the AM still does the credentials refresh. The driver
    // reads the credentials from HDFS, just like the executors and updates its own credentials
    // cache.
    if (conf.contains("spark.yarn.credentials.file")) {
      YarnSparkHadoopUtil.get.startExecutorDelegationTokenRenewer(conf)
    }
    monitorThread = asyncMonitorApplication()
    monitorThread.start()
  }

  /**
   * Return any extra command line arguments to be passed to Client provided in the form of
   * environment variables or Spark properties.
   */
  private def getExtraClientArguments: Seq[String] = {
    val extraArgs = new ArrayBuffer[String]
    // List of (target Client argument, environment variable, Spark property)
    val optionTuples =
      List(
        ("--executor-memory", "SPARK_WORKER_MEMORY", "spark.executor.memory"),
        ("--executor-memory", "SPARK_EXECUTOR_MEMORY", "spark.executor.memory"),
        ("--executor-cores", "SPARK_WORKER_CORES", "spark.executor.cores"),
        ("--executor-cores", "SPARK_EXECUTOR_CORES", "spark.executor.cores"),
        ("--queue", "SPARK_YARN_QUEUE", "spark.yarn.queue"),
        ("--py-files", null, "spark.submit.pyFiles")
      )
    // Warn against the following deprecated environment variables: env var -> suggestion
    val deprecatedEnvVars = Map(
      "SPARK_WORKER_MEMORY" -> "SPARK_EXECUTOR_MEMORY or --executor-memory through spark-submit",
      "SPARK_WORKER_CORES" -> "SPARK_EXECUTOR_CORES or --executor-cores through spark-submit")
    optionTuples.foreach { case (optionName, envVar, sparkProp) =>
      if (sc.getConf.contains(sparkProp)) {
        extraArgs += (optionName, sc.getConf.get(sparkProp))
      } else if (envVar != null && System.getenv(envVar) != null) {
        extraArgs += (optionName, System.getenv(envVar))
        if (deprecatedEnvVars.contains(envVar)) {
          logWarning(s"NOTE: $envVar is deprecated. Use ${deprecatedEnvVars(envVar)} instead.")
        }
      }
    }
    // The app name is a special case because "spark.app.name" is required of all applications.
    // As a result, the corresponding "SPARK_YARN_APP_NAME" is already handled preemptively in
    // SparkSubmitArguments if "spark.app.name" is not explicitly set by the user. (SPARK-5222)
    sc.getConf.getOption("spark.app.name").foreach(v => extraArgs += ("--name", v))
    extraArgs
  }

  /**
   * Report the state of the application until it is running.
   * If the application has finished, failed or been killed in the process, throw an exception.
   * This assumes both `client` and `appId` have already been set.
   */
  private def waitForApplication(): Unit = {
    assert(client != null && appId != null, "Application has not been submitted yet!")
    val (state, _) = client.monitorApplication(appId, returnOnRunning = true) // blocking
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
   * We create this class for SPARK-9519. Basically when we interrupt the monitor thread it's
   * because the SparkContext is being shut down(sc.stop() called by user code), but if
   * monitorApplication return, it means the Yarn application finished before sc.stop() was called,
   * which means we should call sc.stop() here, and we don't allow the monitor to be interrupted
   * before SparkContext stops successfully.
   */
  private class MonitorThread extends Thread {
    private var allowInterrupt = true

    override def run() {
      try {
        val (state, _) = client.monitorApplication(appId, logApplicationReport = false)
        logError(s"Yarn application has already exited with state $state!")
        allowInterrupt = false
        sc.stop()
      } catch {
        case e: InterruptedException => logInfo("Interrupting monitor thread")
      }
    }

    def stopMonitor(): Unit = {
      if (allowInterrupt) {
        this.interrupt()
      }
    }
  }

  /**
   * Monitor the application state in a separate thread.
   * If the application has exited for any reason, stop the SparkContext.
   * This assumes both `client` and `appId` have already been set.
   */
  private def asyncMonitorApplication(): MonitorThread = {
    assert(client != null && appId != null, "Application has not been submitted yet!")
    val t = new MonitorThread
    t.setName("Yarn application state monitor")
    t.setDaemon(true)
    t
  }

  /**
   * Stop the scheduler. This assumes `start()` has already been called.
   */
  override def stop() {
    assert(client != null, "Attempted to stop this scheduler before starting it!")
    if (monitorThread != null) {
      monitorThread.stopMonitor()
    }
    super.stop()
    client.stop()
    YarnSparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
    logInfo("Stopped")
  }

  override def applicationId(): String = {
    Option(appId).map(_.toString).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }
  }
}
