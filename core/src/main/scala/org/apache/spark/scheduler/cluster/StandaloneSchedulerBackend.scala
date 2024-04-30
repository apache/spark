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

import java.util.Locale
import java.util.concurrent.{RejectedExecutionException, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{StandaloneAppClient, StandaloneAppClientListener}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKeys.REASON
import org.apache.spark.internal.config.EXECUTOR_REMOVE_DELAY
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcAddress, RpcEndpointAddress}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RemoveExecutor
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * A [[SchedulerBackend]] implementation for Spark's standalone cluster manager.
 */
private[spark] class StandaloneSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with StandaloneAppClientListener
  with Logging {

  private[spark] var client: StandaloneAppClient = null
  private val stopping = new AtomicBoolean(false)
  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sc.conf
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  @volatile var shutdownCallback: StandaloneSchedulerBackend => Unit = _
  @volatile private var appId: String = _

  private val registrationBarrier = new Semaphore(0)

  private val maxCores = conf.get(config.CORES_MAX)
  private val totalExpectedCores = maxCores.getOrElse(0)
  private val defaultProf = sc.resourceProfileManager.defaultResourceProfile

  private val executorDelayRemoveThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-executor-delay-remove-thread")
  private val _executorRemoveDelay = conf.get(EXECUTOR_REMOVE_DELAY)

  override def start(): Unit = {
    super.start()

    // SPARK-21159. The scheduler backend should only try to connect to the launcher when in client
    // mode. In cluster mode, the code that submits the application to the Master needs to connect
    // to the launcher instead.
    if (sc.deployMode == "client") {
      launcherBackend.connect()
    }

    // The endpoint for executors to talk to us
    val driverUrl = RpcEndpointAddress(
      sc.conf.get(config.DRIVER_HOST_ADDRESS),
      sc.conf.get(config.DRIVER_PORT),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}",
      "--resourceProfileId", "{{RESOURCE_PROFILE_ID}}")
    val extraJavaOpts = sc.conf.get(config.EXECUTOR_JAVA_OPTIONS)
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.get(config.EXECUTOR_CLASS_PATH)
      .map(_.split(java.io.File.pathSeparator).toImmutableArraySeq).getOrElse(Nil)
    val libraryPathEntries = sc.conf.get(config.EXECUTOR_LIBRARY_PATH)
      .map(_.split(java.io.File.pathSeparator).toImmutableArraySeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath =
      if (sys.props.contains(IS_TESTING.key)) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toImmutableArraySeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val webUrl = sc.ui.map(_.webUrl).getOrElse("")
    val coresPerExecutor = conf.getOption(config.EXECUTOR_CORES.key).map(_.toInt)
    // If we're using dynamic allocation, set our initial executor limit to 0 for now.
    // ExecutorAllocationManager will send the real initial limit to the Master later.
    val initialExecutorLimit =
      if (Utils.isDynamicAllocationEnabled(conf)) {
        if (coresPerExecutor.isEmpty) {
          logWarning("Dynamic allocation enabled without spark.executor.cores explicitly " +
            "set, you may get more executors allocated than expected. It's recommended to " +
            "set spark.executor.cores explicitly. Please check SPARK-30299 for more details.")
        }

        Some(0)
      } else {
        None
      }
    val appDesc = ApplicationDescription(sc.appName, maxCores, command,
      webUrl, defaultProfile = defaultProf, sc.eventLogDir, sc.eventLogCodec, initialExecutorLimit)
    client = new StandaloneAppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
    client.start()
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    waitForRegistration()
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop(): Unit = {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def connected(appId: String): Unit = {
    logInfo("Connected to Spark cluster with app ID " + appId)
    this.appId = appId
    notifyContext()
    launcherBackend.setAppId(appId)
  }

  override def disconnected(): Unit = {
    notifyContext()
    if (!stopping.get) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String): Unit = {
    notifyContext()
    if (!stopping.get) {
      launcherBackend.setState(SparkAppHandle.State.KILLED)
      logError(log"Application has been killed. Reason: ${MDC(REASON, reason)}")
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stopInNewThread()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int): Unit = {
    logInfo("Granted executor ID %s on hostPort %s with %d core(s), %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(
      fullId: String,
      message: String,
      exitStatus: Option[Int],
      workerHost: Option[String]): Unit = {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(ExecutorExitCode.HEARTBEAT_FAILURE) =>
        ExecutorExited(ExecutorExitCode.HEARTBEAT_FAILURE, exitCausedByApp = false, message)
      case Some(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR) =>
        ExecutorExited(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR,
          exitCausedByApp = false, message)
      case Some(code) => ExecutorExited(code, exitCausedByApp = true, message)
      case None => ExecutorProcessLost(message, workerHost, causedByApp = workerHost.isEmpty)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason)
  }

  override def executorDecommissioned(fullId: String,
      decommissionInfo: ExecutorDecommissionInfo): Unit = {
    logInfo(s"Asked to decommission executor $fullId")
    val execId = fullId.split("/")(1)
    decommissionExecutors(
      Array((execId, decommissionInfo)),
      adjustTargetNumExecutors = false,
      triggeredByExecutor = false)
    logInfo("Executor %s decommissioned: %s".format(fullId, decommissionInfo))
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo("Worker %s removed: %s".format(workerId, message))
    removeWorker(workerId, host, message)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId()
    }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  protected override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    // resources profiles not supported
    Option(client) match {
      case Some(c) =>
        c.requestTotalExecutors(resourceProfileToTotalExecs)
      case None =>
        logWarning("Attempted to request executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  protected override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.killExecutors(executorIds)
      case None =>
        logWarning("Attempted to kill executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    val prefix = "SPARK_DRIVER_LOG_URL_"
    val driverLogUrls = sys.env.filter { case (k, _) => k.startsWith(prefix) }
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2))
    if (driverLogUrls.nonEmpty) Some(driverLogUrls) else None
  }

  private def waitForRegistration() = {
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    registrationBarrier.release()
  }

  private def stop(finalState: SparkAppHandle.State): Unit = {
    if (stopping.compareAndSet(false, true)) {
      try {
        executorDelayRemoveThread.shutdownNow()
        super.stop()
        if (client != null) {
          client.stop()
        }
        val callback = shutdownCallback
        if (callback != null) {
          callback(this)
        }
      } finally {
        launcherBackend.setState(finalState)
        launcherBackend.close()
      }
    }
  }

  override def createDriverEndpoint(): DriverEndpoint = {
    new StandaloneDriverEndpoint()
  }

  private class StandaloneDriverEndpoint extends DriverEndpoint {
    // [SC-104659]: There are two paths to detect executor loss.
    // (1) (fast path) `onDisconnected`: Executor -> Driver
    //     When Executor closes its JVM, the socket (Netty's channel) will be closed. The
    //     function onDisconnected will be triggered when driver knows the channel is closed.
    //
    // (2) (slow path) ExecutorRunner -> Worker -> Master -> Driver
    //     When executor exits with ExecutorExitCode, the exit code will be passed from
    //     ExecutorRunner to Driver. (Check [SC-104335] PR for details)
    //
    // Both path will call the function `removeExecutor` to remove the lost executor. The main
    // difference between these two paths is ExecutorExitCode. To elaborate, the ExecutorLossReason
    // of slow path has the information of ExecutorExitCode, but fast path does not have. Hence,
    // slow path can determine the category of the executor loss with more information.
    //
    // Typically, fast path will be triggered prior to slow path. That is, when driver receives the
    // ExecutorExitCode from slow path, the lost executor has already been removed from
    // executorDataMap by fast path. Hence, we delay to send RemoveExecutor(executorId, lossReason)
    // by _executorRemoveDelay milliseconds when the function onDisconnected is triggered, and hope
    // to receive ExecutorExitCode from slow path during the delay.
    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId.get(remoteAddress).foreach { executorId =>
        // [SC-104659]:
        // When driver detects executor loss by fast path (`onDisconnected`), we need to notify
        // task scheduler to avoid assigning new tasks on this lost executor and wait slow path
        // for `_executorRemoveDelay` seconds. To prevent assigning tasks to the lost executor,
        // we added the executor to `executorsPendingLossReason`. Hence, the executor will be
        // filtered out from `activeExecutors` in the function `getWorkerOffers`.
        executorsPendingLossReason += executorId
        val lossReason = ExecutorProcessLost("Remote RPC client disassociated. Likely due to " +
          "containers exceeding thresholds, or network issues. Check driver logs for WARN " +
          "messages.")
        val removeExecutorTask = new Runnable() {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // If the executor is not removed by slow path, fast path will send a `RemoveExecutor`
            // message to the scheduler backend.
            //
            // [Note]: Here may have race condition because `executorsPendingLossReason` will be
            //         operated in the following 3 cases for standalone scheduler.
            //
            //  1. `removeExecutor`: executorsPendingLossReason -= executorId (remove)
            //  2. `onDisconnected`: executorsPendingLossReason += executorId (add)
            //  3. `executorDelayRemoveThread`: executorsPendingLossReason.contains(executorId)
            //
            // Case 1 & case 3 may have race condition. Case 2 & case 3 may also have. However,
            // race condition is okay because `removeExecutor` will check whether the executor is
            // existing or not. If the executor has been removed, the extra `RemoveExecutor`
            // message will have no effectiveness.
            if (executorsPendingLossReason.contains(executorId)) {
              driverEndpoint.send(RemoveExecutor(executorId, lossReason))
            }
          }
        }
        try {
          executorDelayRemoveThread.schedule(removeExecutorTask,
            _executorRemoveDelay, TimeUnit.MILLISECONDS)
        } catch {
          case _: RejectedExecutionException if stopping.get() =>
            logWarning(
              "Skipping onDisconnected RemoveExecutor call because the scheduler is stopping")
        }
      }
    }
  }
}
