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

import java.util.concurrent.Semaphore

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.deploy.client.{StandaloneAppClient, StandaloneAppClientListener}
import org.apache.spark.deploy.security.{AMCredentialRenewer, ConfigurableCredentialManager}
import org.apache.spark.deploy.{ApplicationDescription, Command, SparkHadoopUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Future

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

  private var client: StandaloneAppClient = null
  private var stopping = false
  private val launcherBackend = new LauncherBackend() {
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  @volatile var shutdownCallback: StandaloneSchedulerBackend => Unit = _
  @volatile private var appId: String = _

  private val registrationBarrier = new Semaphore(0)

  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)
  private val totalExpectedCores = maxCores.getOrElse(0)


  private var loginFromKeytab = false
  private var principal: String = null
  private var keytab: String = null
  private var credentials: Credentials = null

  private var credentialRenewer: AMCredentialRenewer = _


  def setupCredentials(): Unit = {
    loginFromKeytab = conf.contains(PRINCIPAL.key)
    if (loginFromKeytab) {
      principal = conf.get(PRINCIPAL).get
      keytab = conf.get(KEYTAB).orNull
    }
    // Defensive copy of the credentials
    credentials = new Credentials(UserGroupInformation.getCurrentUser.getCredentials)
  }


  override def start() {

    setupCredentials()

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val credentialManager = new ConfigurableCredentialManager(conf, hadoopConf)

    // Merge credentials obtained from registered providers
    val nearestTimeOfNextRenewal = credentialManager.obtainCredentials(hadoopConf, credentials)

    if (credentials != null) {
      logDebug(SparkHadoopUtil.get.dumpTokens(credentials).mkString("\n"))
    }

    // If we use principal and keytab to login, also credentials can be renewed some time
    // after current time, we should pass the next renewal and updating time to credential
    // renewer and updater.

    if (loginFromKeytab && nearestTimeOfNextRenewal > System.currentTimeMillis() &&
      nearestTimeOfNextRenewal != Long.MaxValue) {

      // Valid renewal time is 75% of next renewal time, and the valid update time will be
      // slightly later then renewal time (80% of next renewal time). This is to make sure
      // credentials are renewed and updated before expired.
      val currTime = System.currentTimeMillis()
      val renewalTime = (nearestTimeOfNextRenewal - currTime) * 0.75 + currTime
      val updateTime = (nearestTimeOfNextRenewal - currTime) * 0.8 + currTime

      logInfo(s"Setting credential renewal time: ${renewalTime.toLong} ms,"
        + s" update time ${updateTime.toLong} ms")

      conf.set(CREDENTIALS_RENEWAL_TIME, renewalTime.toLong)
      conf.set(CREDENTIALS_UPDATE_TIME, updateTime.toLong)
    }

    // If the credentials file config is present, we must periodically renew tokens. So create
    // a new AMDelegationTokenRenewer
    if (conf.contains(CREDENTIALS_FILE_PATH.key)) {
      // If a principal and keytab have been set, use that to create new credentials for executors
      // periodically
      credentialRenewer = credentialManager.credentialRenewer()
      credentialRenewer.scheduleLoginFromKeytab()
    }
    // NOTE we don't need an updater since the above accomplishes it already

    super.start()
    launcherBackend.connect()

    // The endpoint for executors to talk to us
    val driverUrl = RpcEndpointAddress(
      sc.conf.get("spark.driver.host"),
      sc.conf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val libraryPathEntries = sc.conf.getOption("spark.executor.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath =
      if (sys.props.contains("spark.testing")) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }


    //////////////////////
    //////////////////////

    def base64EncodedValue(fn: DataOutputBuffer => Unit): String = {
      val dob = new DataOutputBuffer
      fn(dob)
      dob.close()
      new String(Base64.encodeBase64(dob.getData))
    }

    val bootstrap = if (credentials.getAllTokens.size() > 0) {
      val bootstrapCredentials = base64EncodedValue { dob =>
        credentials.writeTokenStorageToStream(dob)
      }

      logInfo("Security tokens will be sent to executors")
      Map(BOOTSTRAP_TOKENS.key -> bootstrapCredentials)
    } else Map.empty[String, String]

    val executorEnv = sc.executorEnvs.toMap ++ bootstrap

    //////////////////////
    //////////////////////

    // Start executors with a few necessary configs for registering with the scheduler
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, executorEnv, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val webUrl = sc.ui.map(_.webUrl).getOrElse("")
    val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
    // If we're using dynamic allocation, set our initial executor limit to 0 for now.
    // ExecutorAllocationManager will send the real initial limit to the Master later.
    val initialExecutorLimit =
      if (Utils.isDynamicAllocationEnabled(conf)) {
        Some(0)
      } else {
        None
      }
    val appDesc = ApplicationDescription(sc.appName, maxCores, sc.executorMemory, command,
      webUrl, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor, initialExecutorLimit)
    client = new StandaloneAppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
    client.start()
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    waitForRegistration()
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop(): Unit = synchronized {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    this.appId = appId
    notifyContext()
    launcherBackend.setAppId(appId)
  }

  override def disconnected() {
    notifyContext()
    if (!stopping) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String) {
    notifyContext()
    if (!stopping) {
      launcherBackend.setState(SparkAppHandle.State.KILLED)
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stopInNewThread()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d cores, %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code, exitCausedByApp = true, message)
      case None => SlaveLost(message, workerLost = workerLost)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  protected override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.requestTotalExecutors(requestedTotal)
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

  private def waitForRegistration() = {
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    registrationBarrier.release()
  }

  private def stop(finalState: SparkAppHandle.State): Unit = synchronized {
    try {
      stopping = true

      super.stop()
      client.stop()

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
