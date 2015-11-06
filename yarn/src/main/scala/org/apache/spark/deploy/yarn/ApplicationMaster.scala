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

package org.apache.spark.deploy.yarn

import scala.util.control.NonFatal

import java.io.{File, IOException}
import java.lang.reflect.InvocationTargetException
import java.net.{Socket, URL}
import java.util.concurrent.atomic.AtomicReference

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.rpc._
import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext, SparkEnv,
  SparkException, SparkUserAppException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, YarnSchedulerBackend}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util._

/**
 * Common application master functionality for Spark on Yarn.
 */
private[spark] class ApplicationMaster(
    args: ApplicationMasterArguments,
    client: YarnRMClient)
  extends Logging {

  // Load the properties file with the Spark configuration and set entries as system properties,
  // so that user code run inside the AM also has access to them.
  if (args.propertiesFile != null) {
    Utils.getPropertiesFromFile(args.propertiesFile).foreach { case (k, v) =>
      sys.props(k) = v
    }
  }

  // TODO: Currently, task to container is computed once (TaskSetManager) - which need not be
  // optimal as more containers are available. Might need to handle this better.

  private val sparkConf = new SparkConf()
  private val yarnConf: YarnConfiguration = SparkHadoopUtil.get.newConfiguration(sparkConf)
    .asInstanceOf[YarnConfiguration]
  private val isClusterMode = args.userClass != null

  // Default to twice the number of executors (twice the maximum number of executors if dynamic
  // allocation is enabled), with a minimum of 3.

  private val maxNumExecutorFailures = {
    val defaultKey =
      if (Utils.isDynamicAllocationEnabled(sparkConf)) {
        "spark.dynamicAllocation.maxExecutors"
      } else {
        "spark.executor.instances"
      }
    val effectiveNumExecutors = sparkConf.getInt(defaultKey, 0)
    val defaultMaxNumExecutorFailures = math.max(3, 2 * effectiveNumExecutors)

    sparkConf.getInt("spark.yarn.max.executor.failures", defaultMaxNumExecutorFailures)
  }

  @volatile private var exitCode = 0
  @volatile private var unregistered = false
  @volatile private var finished = false
  @volatile private var finalStatus = getDefaultFinalStatus
  @volatile private var finalMsg: String = ""
  @volatile private var userClassThread: Thread = _

  @volatile private var reporterThread: Thread = _
  @volatile private var allocator: YarnAllocator = _

  // Lock for controlling the allocator (heartbeat) thread.
  private val allocatorLock = new Object()

  // Steady state heartbeat interval. We want to be reasonably responsive without causing too many
  // requests to RM.
  private val heartbeatInterval = {
    // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.
    val expiryInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)
    math.max(0, math.min(expiryInterval / 2,
      sparkConf.getTimeAsMs("spark.yarn.scheduler.heartbeat.interval-ms", "3s")))
  }

  // Initial wait interval before allocator poll, to allow for quicker ramp up when executors are
  // being requested.
  private val initialAllocationInterval = math.min(heartbeatInterval,
    sparkConf.getTimeAsMs("spark.yarn.scheduler.initial-allocation.interval", "200ms"))

  // Next wait interval before allocator poll.
  private var nextAllocationInterval = initialAllocationInterval

  // Fields used in client mode.
  private var rpcEnv: RpcEnv = null
  private var amEndpoint: RpcEndpointRef = _

  // Fields used in cluster mode.
  private val sparkContextRef = new AtomicReference[SparkContext](null)

  private var delegationTokenRenewerOption: Option[AMDelegationTokenRenewer] = None

  final def run(): Int = {
    try {
      val appAttemptId = client.getAttemptId()

      if (isClusterMode) {
        // Set the web ui port to be ephemeral for yarn so we don't conflict with
        // other spark processes running on the same box
        System.setProperty("spark.ui.port", "0")

        // Set the master property to match the requested mode.
        System.setProperty("spark.master", "yarn-cluster")

        // Propagate the application ID so that YarnClusterSchedulerBackend can pick it up.
        System.setProperty("spark.yarn.app.id", appAttemptId.getApplicationId().toString())

        // Propagate the attempt if, so that in case of event logging,
        // different attempt's logs gets created in different directory
        System.setProperty("spark.yarn.app.attemptId", appAttemptId.getAttemptId().toString())
      }

      logInfo("ApplicationAttemptId: " + appAttemptId)

      val fs = FileSystem.get(yarnConf)

      // This shutdown hook should run *after* the SparkContext is shut down.
      val priority = ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY - 1
      ShutdownHookManager.addShutdownHook(priority) { () =>
        val maxAppAttempts = client.getMaxRegAttempts(sparkConf, yarnConf)
        val isLastAttempt = client.getAttemptId().getAttemptId() >= maxAppAttempts

        if (!finished) {
          // This happens when the user application calls System.exit(). We have the choice
          // of either failing or succeeding at this point. We report success to avoid
          // retrying applications that have succeeded (System.exit(0)), which means that
          // applications that explicitly exit with a non-zero status will also show up as
          // succeeded in the RM UI.
          finish(finalStatus,
            ApplicationMaster.EXIT_SUCCESS,
            "Shutdown hook called before final status was reported.")
        }

        if (!unregistered) {
          // we only want to unregister if we don't want the RM to retry
          if (finalStatus == FinalApplicationStatus.SUCCEEDED || isLastAttempt) {
            unregister(finalStatus, finalMsg)
            cleanupStagingDir(fs)
          }
        }
      }

      // Call this to force generation of secret so it gets populated into the
      // Hadoop UGI. This has to happen before the startUserApplication which does a
      // doAs in order for the credentials to be passed on to the executor containers.
      val securityMgr = new SecurityManager(sparkConf)

      // If the credentials file config is present, we must periodically renew tokens. So create
      // a new AMDelegationTokenRenewer
      if (sparkConf.contains("spark.yarn.credentials.file")) {
        delegationTokenRenewerOption = Some(new AMDelegationTokenRenewer(sparkConf, yarnConf))
        // If a principal and keytab have been set, use that to create new credentials for executors
        // periodically
        delegationTokenRenewerOption.foreach(_.scheduleLoginFromKeytab())
      }

      if (isClusterMode) {
        runDriver(securityMgr)
      } else {
        runExecutorLauncher(securityMgr)
      }
    } catch {
      case e: Exception =>
        // catch everything else if not specifically handled
        logError("Uncaught exception: ", e)
        finish(FinalApplicationStatus.FAILED,
          ApplicationMaster.EXIT_UNCAUGHT_EXCEPTION,
          "Uncaught exception: " + e)
    }
    exitCode
  }

  /**
   * Set the default final application status for client mode to UNDEFINED to handle
   * if YARN HA restarts the application so that it properly retries. Set the final
   * status to SUCCEEDED in cluster mode to handle if the user calls System.exit
   * from the application code.
   */
  final def getDefaultFinalStatus(): FinalApplicationStatus = {
    if (isClusterMode) {
      FinalApplicationStatus.SUCCEEDED
    } else {
      FinalApplicationStatus.UNDEFINED
    }
  }

  /**
   * unregister is used to completely unregister the application from the ResourceManager.
   * This means the ResourceManager will not retry the application attempt on your behalf if
   * a failure occurred.
   */
  final def unregister(status: FinalApplicationStatus, diagnostics: String = null): Unit = {
    synchronized {
      if (!unregistered) {
        logInfo(s"Unregistering ApplicationMaster with $status" +
          Option(diagnostics).map(msg => s" (diag message: $msg)").getOrElse(""))
        unregistered = true
        client.unregister(status, Option(diagnostics).getOrElse(""))
      }
    }
  }

  final def finish(status: FinalApplicationStatus, code: Int, msg: String = null): Unit = {
    synchronized {
      if (!finished) {
        val inShutdown = ShutdownHookManager.inShutdown()
        logInfo(s"Final app status: $status, exitCode: $code" +
          Option(msg).map(msg => s", (reason: $msg)").getOrElse(""))
        exitCode = code
        finalStatus = status
        finalMsg = msg
        finished = true
        if (!inShutdown && Thread.currentThread() != reporterThread && reporterThread != null) {
          logDebug("shutting down reporter thread")
          reporterThread.interrupt()
        }
        if (!inShutdown && Thread.currentThread() != userClassThread && userClassThread != null) {
          logDebug("shutting down user thread")
          userClassThread.interrupt()
        }
        if (!inShutdown) delegationTokenRenewerOption.foreach(_.stop())
      }
    }
  }

  private def sparkContextInitialized(sc: SparkContext) = {
    sparkContextRef.synchronized {
      sparkContextRef.compareAndSet(null, sc)
      sparkContextRef.notifyAll()
    }
  }

  private def sparkContextStopped(sc: SparkContext) = {
    sparkContextRef.compareAndSet(sc, null)
  }

  private def registerAM(
      _rpcEnv: RpcEnv,
      driverRef: RpcEndpointRef,
      uiAddress: String,
      securityMgr: SecurityManager) = {
    val sc = sparkContextRef.get()

    val appId = client.getAttemptId().getApplicationId().toString()
    val attemptId = client.getAttemptId().getAttemptId().toString()
    val historyAddress =
      sparkConf.getOption("spark.yarn.historyServer.address")
        .map { text => SparkHadoopUtil.get.substituteHadoopVariables(text, yarnConf) }
        .map { address => s"${address}${HistoryServer.UI_PATH_PREFIX}/${appId}/${attemptId}" }
        .getOrElse("")

    val _sparkConf = if (sc != null) sc.getConf else sparkConf
    val driverUrl = _rpcEnv.uriOf(
        SparkEnv.driverActorSystemName,
        RpcAddress(_sparkConf.get("spark.driver.host"), _sparkConf.get("spark.driver.port").toInt),
        CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    allocator = client.register(driverUrl,
      driverRef,
      yarnConf,
      _sparkConf,
      uiAddress,
      historyAddress,
      securityMgr)

    allocator.allocateResources()
    reporterThread = launchReporterThread()
  }

  /**
   * Create an [[RpcEndpoint]] that communicates with the driver.
   *
   * In cluster mode, the AM and the driver belong to same process
   * so the AMEndpoint need not monitor lifecycle of the driver.
   *
   * @return A reference to the driver's RPC endpoint.
   */
  private def runAMEndpoint(
      host: String,
      port: String,
      isClusterMode: Boolean): RpcEndpointRef = {
    val driverEndpoint = rpcEnv.setupEndpointRef(
      SparkEnv.driverActorSystemName,
      RpcAddress(host, port.toInt),
      YarnSchedulerBackend.ENDPOINT_NAME)
    amEndpoint =
      rpcEnv.setupEndpoint("YarnAM", new AMEndpoint(rpcEnv, driverEndpoint, isClusterMode))
    driverEndpoint
  }

  private def runDriver(securityMgr: SecurityManager): Unit = {
    addAmIpFilter()
    userClassThread = startUserApplication()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.
    val sc = waitForSparkContextInitialized()

    // If there is no SparkContext at this point, just fail the app.
    if (sc == null) {
      finish(FinalApplicationStatus.FAILED,
        ApplicationMaster.EXIT_SC_NOT_INITED,
        "Timed out waiting for SparkContext.")
    } else {
      rpcEnv = sc.env.rpcEnv
      val driverRef = runAMEndpoint(
        sc.getConf.get("spark.driver.host"),
        sc.getConf.get("spark.driver.port"),
        isClusterMode = true)
      registerAM(rpcEnv, driverRef, sc.ui.map(_.appUIAddress).getOrElse(""), securityMgr)
      userClassThread.join()
    }
  }

  private def runExecutorLauncher(securityMgr: SecurityManager): Unit = {
    val port = sparkConf.getInt("spark.yarn.am.port", 0)
    rpcEnv = RpcEnv.create("sparkYarnAM", Utils.localHostName, port, sparkConf, securityMgr,
      clientMode = true)
    val driverRef = waitForSparkDriver()
    addAmIpFilter()
    registerAM(rpcEnv, driverRef, sparkConf.get("spark.driver.appUIAddress", ""), securityMgr)

    // In client mode the actor will stop the reporter thread.
    reporterThread.join()
  }

  private def launchReporterThread(): Thread = {
    // The number of failures in a row until Reporter thread give up
    val reporterMaxFailures = sparkConf.getInt("spark.yarn.scheduler.reporterThread.maxFailures", 5)

    val t = new Thread {
      override def run() {
        var failureCount = 0
        while (!finished) {
          try {
            if (allocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
              finish(FinalApplicationStatus.FAILED,
                ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES,
                s"Max number of executor failures ($maxNumExecutorFailures) reached")
            } else {
              logDebug("Sending progress")
              allocator.allocateResources()
            }
            failureCount = 0
          } catch {
            case i: InterruptedException =>
            case e: Throwable => {
              failureCount += 1
              if (!NonFatal(e) || failureCount >= reporterMaxFailures) {
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_REPORTER_FAILURE, "Exception was thrown " +
                    s"$failureCount time(s) from Reporter thread.")
              } else {
                logWarning(s"Reporter thread fails $failureCount time(s) in a row.", e)
              }
            }
          }
          try {
            val numPendingAllocate = allocator.getPendingAllocate.size
            allocatorLock.synchronized {
              val sleepInterval =
                if (numPendingAllocate > 0 || allocator.getNumPendingLossReasonRequests > 0) {
                  val currentAllocationInterval =
                    math.min(heartbeatInterval, nextAllocationInterval)
                  nextAllocationInterval = currentAllocationInterval * 2 // avoid overflow
                  currentAllocationInterval
                } else {
                  nextAllocationInterval = initialAllocationInterval
                  heartbeatInterval
                }
              logDebug(s"Number of pending allocations is $numPendingAllocate. " +
                       s"Sleeping for $sleepInterval.")
              allocatorLock.wait(sleepInterval)
            }
          } catch {
            case e: InterruptedException =>
          }
        }
      }
    }
    // setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.setName("Reporter")
    t.start()
    logInfo(s"Started progress reporter thread with (heartbeat : $heartbeatInterval, " +
            s"initial allocation : $initialAllocationInterval) intervals")
    t
  }

  /**
   * Clean up the staging directory.
   */
  private def cleanupStagingDir(fs: FileSystem) {
    var stagingDirPath: Path = null
    try {
      val preserveFiles = sparkConf.getBoolean("spark.yarn.preserve.staging.files", false)
      if (!preserveFiles) {
        stagingDirPath = new Path(System.getenv("SPARK_YARN_STAGING_DIR"))
        if (stagingDirPath == null) {
          logError("Staging directory is null")
          return
        }
        logInfo("Deleting staging directory " + stagingDirPath)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        logError("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  private def waitForSparkContextInitialized(): SparkContext = {
    logInfo("Waiting for spark context initialization")
    sparkContextRef.synchronized {
      val totalWaitTime = sparkConf.getTimeAsMs("spark.yarn.am.waitTime", "100s")
      val deadline = System.currentTimeMillis() + totalWaitTime

      while (sparkContextRef.get() == null && System.currentTimeMillis < deadline && !finished) {
        logInfo("Waiting for spark context initialization ... ")
        sparkContextRef.wait(10000L)
      }

      val sparkContext = sparkContextRef.get()
      if (sparkContext == null) {
        logError(("SparkContext did not initialize after waiting for %d ms. Please check earlier"
          + " log output for errors. Failing the application.").format(totalWaitTime))
      }
      sparkContext
    }
  }

  private def waitForSparkDriver(): RpcEndpointRef = {
    logInfo("Waiting for Spark driver to be reachable.")
    var driverUp = false
    val hostport = args.userArgs(0)
    val (driverHost, driverPort) = Utils.parseHostPort(hostport)

    // Spark driver should already be up since it launched us, but we don't want to
    // wait forever, so wait 100 seconds max to match the cluster mode setting.
    val totalWaitTimeMs = sparkConf.getTimeAsMs("spark.yarn.am.waitTime", "100s")
    val deadline = System.currentTimeMillis + totalWaitTimeMs

    while (!driverUp && !finished && System.currentTimeMillis < deadline) {
      try {
        val socket = new Socket(driverHost, driverPort)
        socket.close()
        logInfo("Driver now available: %s:%s".format(driverHost, driverPort))
        driverUp = true
      } catch {
        case e: Exception =>
          logError("Failed to connect to driver at %s:%s, retrying ...".
            format(driverHost, driverPort))
          Thread.sleep(100L)
      }
    }

    if (!driverUp) {
      throw new SparkException("Failed to connect to driver!")
    }

    sparkConf.set("spark.driver.host", driverHost)
    sparkConf.set("spark.driver.port", driverPort.toString)

    runAMEndpoint(driverHost, driverPort.toString, isClusterMode = false)
  }

  /** Add the Yarn IP filter that is required for properly securing the UI. */
  private def addAmIpFilter() = {
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val params = client.getAmIpFilterParams(yarnConf, proxyBase)
    if (isClusterMode) {
      System.setProperty("spark.ui.filters", amFilter)
      params.foreach { case (k, v) => System.setProperty(s"spark.$amFilter.param.$k", v) }
    } else {
      amEndpoint.send(AddWebUIFilter(amFilter, params.toMap, proxyBase))
    }
  }

  /**
   * Start the user class, which contains the spark driver, in a separate Thread.
   * If the main routine exits cleanly or exits with System.exit(N) for any N
   * we assume it was successful, for all other cases we assume failure.
   *
   * Returns the user thread that was started.
   */
  private def startUserApplication(): Thread = {
    logInfo("Starting the user application in a separate Thread")

    val classpath = Client.getUserClasspath(sparkConf)
    val urls = classpath.map { entry =>
      new URL("file:" + new File(entry.getPath()).getAbsolutePath())
    }
    val userClassLoader =
      if (Client.isUserClassPathFirst(sparkConf, isDriver = true)) {
        new ChildFirstURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      } else {
        new MutableURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      }

    var userArgs = args.userArgs
    if (args.primaryPyFile != null && args.primaryPyFile.endsWith(".py")) {
      // When running pyspark, the app is run using PythonRunner. The second argument is the list
      // of files to add to PYTHONPATH, which Client.scala already handles, so it's empty.
      userArgs = Seq(args.primaryPyFile, "") ++ userArgs
    }
    if (args.primaryRFile != null && args.primaryRFile.endsWith(".R")) {
      // TODO(davies): add R dependencies here
    }
    val mainMethod = userClassLoader.loadClass(args.userClass)
      .getMethod("main", classOf[Array[String]])

    val userThread = new Thread {
      override def run() {
        try {
          mainMethod.invoke(null, userArgs.toArray)
          finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
          logDebug("Done running users class")
        } catch {
          case e: InvocationTargetException =>
            e.getCause match {
              case _: InterruptedException =>
                // Reporter thread can interrupt to stop user class
              case SparkUserAppException(exitCode) =>
                val msg = s"User application exited with status $exitCode"
                logError(msg)
                finish(FinalApplicationStatus.FAILED, exitCode, msg)
              case cause: Throwable =>
                logError("User class threw exception: " + cause, cause)
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_EXCEPTION_USER_CLASS,
                  "User class threw exception: " + cause)
            }
        }
      }
    }
    userThread.setContextClassLoader(userClassLoader)
    userThread.setName("Driver")
    userThread.start()
    userThread
  }

  private def resetAllocatorInterval(): Unit = allocatorLock.synchronized {
    nextAllocationInterval = initialAllocationInterval
    allocatorLock.notifyAll()
  }

  /**
   * An [[RpcEndpoint]] that communicates with the driver's scheduler backend.
   */
  private class AMEndpoint(
      override val rpcEnv: RpcEnv, driver: RpcEndpointRef, isClusterMode: Boolean)
    extends RpcEndpoint with Logging {

    override def onStart(): Unit = {
      driver.send(RegisterClusterManager(self))
    }

    override def receive: PartialFunction[Any, Unit] = {
      case x: AddWebUIFilter =>
        logInfo(s"Add WebUI Filter. $x")
        driver.send(x)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RequestExecutors(requestedTotal, localityAwareTasks, hostToLocalTaskCount) =>
        Option(allocator) match {
          case Some(a) =>
            if (a.requestTotalExecutorsWithPreferredLocalities(requestedTotal,
              localityAwareTasks, hostToLocalTaskCount)) {
              resetAllocatorInterval()
            }

          case None =>
            logWarning("Container allocator is not ready to request executors yet.")
        }
        context.reply(true)

      case KillExecutors(executorIds) =>
        logInfo(s"Driver requested to kill executor(s) ${executorIds.mkString(", ")}.")
        Option(allocator) match {
          case Some(a) => executorIds.foreach(a.killExecutor)
          case None => logWarning("Container allocator is not ready to kill executors yet.")
        }
        context.reply(true)

      case GetExecutorLossReason(eid) =>
        Option(allocator) match {
          case Some(a) =>
            a.enqueueGetLossReasonRequest(eid, context)
            resetAllocatorInterval()
          case None =>
            logWarning("Container allocator is not ready to find executor loss reasons yet.")
        }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      // In cluster mode, do not rely on the disassociated event to exit
      // This avoids potentially reporting incorrect exit codes if the driver fails
      if (!isClusterMode) {
        logInfo(s"Driver terminated or disconnected! Shutting down. $remoteAddress")
        finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
      }
    }
  }

}

object ApplicationMaster extends Logging {

  // exit codes for different causes, no reason behind the values
  private val EXIT_SUCCESS = 0
  private val EXIT_UNCAUGHT_EXCEPTION = 10
  private val EXIT_MAX_EXECUTOR_FAILURES = 11
  private val EXIT_REPORTER_FAILURE = 12
  private val EXIT_SC_NOT_INITED = 13
  private val EXIT_SECURITY = 14
  private val EXIT_EXCEPTION_USER_CLASS = 15

  private var master: ApplicationMaster = _

  def main(args: Array[String]): Unit = {
    SignalLogger.register(log)
    val amArgs = new ApplicationMasterArguments(args)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      master = new ApplicationMaster(amArgs, new YarnRMClient(amArgs))
      System.exit(master.run())
    }
  }

  private[spark] def sparkContextInitialized(sc: SparkContext): Unit = {
    master.sparkContextInitialized(sc)
  }

  private[spark] def sparkContextStopped(sc: SparkContext): Boolean = {
    master.sparkContextStopped(sc)
  }

}

/**
 * This object does not provide any special functionality. It exists so that it's easy to tell
 * apart the client-mode AM from the cluster-mode AM when using tools such as ps or jps.
 */
object ExecutorLauncher {

  def main(args: Array[String]): Unit = {
    ApplicationMaster.main(args)
  }

}
