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

import java.io.InterruptedIOException

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.yarn.api.records.{FinalApplicationStatus, YarnApplicationState}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.deploy.yarn.{Client, ClientArguments, YarnAppReport}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.launcher.SparkAppHandle
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._

private[spark] class YarnClientSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends YarnSchedulerBackend(scheduler, sc)
  with Logging {

  private var client: Client = null
  private var monitorThread: MonitorThread = null

  /**
   * Create a Yarn client to submit an application to the ResourceManager.
   * This waits until the application is running.
   */
  override def start(): Unit = {
    super.start()

    val driverHost = conf.get(config.DRIVER_HOST_ADDRESS)
    val driverPort = conf.get(config.DRIVER_PORT)
    val hostport = driverHost + ":" + driverPort
    sc.ui.foreach { ui => conf.set(DRIVER_APP_UI_ADDRESS, ui.webUrl) }

    val argsArrayBuf = new ArrayBuffer[String]()
    argsArrayBuf += ("--arg", hostport)

    logDebug("ClientArguments called with: " + argsArrayBuf.mkString(" "))
    val args = new ClientArguments(argsArrayBuf.toArray)
    totalExpectedExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)
    client = new Client(args, conf, sc.env.rpcEnv)
    client.submitApplication()
    bindToYarn(client.getApplicationId(), None)

    waitForApplication()

    monitorThread = asyncMonitorApplication()
    monitorThread.start()

    startBindings()
  }

  /**
   * Report the state of the application until it is running.
   * If the application has finished, failed or been killed in the process, throw an exception.
   * This assumes both `client` and `appId` have already been set.
   */
  private def waitForApplication(): Unit = {
    val monitorInterval = conf.get(CLIENT_LAUNCH_MONITOR_INTERVAL)

    assert(client != null && appId.isDefined, "Application has not been submitted yet!")
    val YarnAppReport(state, _, diags) =
      client.monitorApplication(returnOnRunning = true, interval = monitorInterval)
    if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
      val genericMessage = "The YARN application has already ended! " +
        "It might have been killed or the Application Master may have failed to start. " +
        "Check the YARN application logs for more details."
      val exceptionMsg = diags match {
        case Some(msg) =>
          logError(genericMessage)
          msg

        case None =>
          genericMessage
      }
      throw new SparkException(exceptionMsg)
    }
    if (state == YarnApplicationState.RUNNING) {
      logInfo(s"Application ${appId.get} has started running.")
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

    override def run(): Unit = {
      try {
        val YarnAppReport(_, state, diags) =
          client.monitorApplication(logApplicationReport = false)
        logError(s"YARN application has exited unexpectedly with state $state! " +
          "Check the YARN application logs for more details.")
        diags.foreach { err =>
          logError(s"Diagnostics message: $err")
        }
        allowInterrupt = false
        sc.stop()
        state match {
          case FinalApplicationStatus.FAILED | FinalApplicationStatus.KILLED
            if conf.get(AM_CLIENT_MODE_EXIT_ON_ERROR) =>
            logWarning(s"ApplicationMaster finished with status ${state}, " +
              s"SparkContext should exit with code 1.")
            System.exit(1)
          case _ =>
        }
      } catch {
        case _: InterruptedException | _: InterruptedIOException =>
          logInfo("Interrupting monitor thread")
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
    assert(client != null && appId.isDefined, "Application has not been submitted yet!")
    val t = new MonitorThread
    t.setName("YARN application state monitor")
    t.setDaemon(true)
    t
  }

  /**
   * Stop the scheduler. This assumes `start()` has already been called.
   */
  override def stop(): Unit = {
    assert(client != null, "Attempted to stop this scheduler before starting it!")
    yarnSchedulerEndpoint.handleClientModeDriverStop()
    if (monitorThread != null) {
      monitorThread.stopMonitor()
    }

    // Report a final state to the launcher if one is connected. This is needed since in client
    // mode this backend doesn't let the app monitor loop run to completion, so it does not report
    // the final state itself.
    //
    // Note: there's not enough information at this point to provide a better final state,
    // so assume the application was successful.
    client.reportLauncherState(SparkAppHandle.State.FINISHED)

    super.stop()
    client.stop()
    logInfo("YARN client scheduler backend Stopped")
  }

  override protected def updateDelegationTokens(tokens: Array[Byte]): Unit = {
    super.updateDelegationTokens(tokens)
    amEndpoint.foreach(_.send(UpdateDelegationTokens(tokens)))
  }

}
