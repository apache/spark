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

import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.net.Socket
import java.util.concurrent.atomic.AtomicReference

import akka.actor._
import akka.remote._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.SparkException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.scheduler.cluster.YarnSchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{AkkaUtils, SignalLogger, Utils}

/**
 * Common application master functionality for Spark on Yarn.
 */
private[spark] class ApplicationMaster(args: ApplicationMasterArguments,
  client: YarnRMClient) extends Logging {
  // TODO: Currently, task to container is computed once (TaskSetManager) - which need not be
  // optimal as more containers are available. Might need to handle this better.

  private val sparkConf = new SparkConf()
  private val yarnConf: YarnConfiguration = SparkHadoopUtil.get.newConfiguration(sparkConf)
    .asInstanceOf[YarnConfiguration]
  private val isDriver = args.userClass != null

  // Default to numExecutors * 2, with minimum of 3
  private val maxNumExecutorFailures = sparkConf.getInt("spark.yarn.max.executor.failures",
    sparkConf.getInt("spark.yarn.max.worker.failures", math.max(args.numExecutors * 2, 3)))

  @volatile private var exitCode = 0
  @volatile private var unregistered = false
  @volatile private var finished = false
  @volatile private var finalStatus = getDefaultFinalStatus
  @volatile private var finalMsg: String = ""
  @volatile private var userClassThread: Thread = _

  private var reporterThread: Thread = _
  private var allocator: YarnAllocator = _

  // Fields used in client mode.
  private var actorSystem: ActorSystem = null
  private var actor: ActorRef = _

  // Fields used in cluster mode.
  private val sparkContextRef = new AtomicReference[SparkContext](null)

  final def run(): Int = {
    try {
      val appAttemptId = client.getAttemptId()

      if (isDriver) {
        // Set the web ui port to be ephemeral for yarn so we don't conflict with
        // other spark processes running on the same box
        System.setProperty("spark.ui.port", "0")

        // Set the master property to match the requested mode.
        System.setProperty("spark.master", "yarn-cluster")

        // Propagate the application ID so that YarnClusterSchedulerBackend can pick it up.
        System.setProperty("spark.yarn.app.id", appAttemptId.getApplicationId().toString())
      }

      logInfo("ApplicationAttemptId: " + appAttemptId)

      val fs = FileSystem.get(yarnConf)
      val cleanupHook = new Runnable {
        override def run() {
          // If the SparkContext is still registered, shut it down as a best case effort in case
          // users do not call sc.stop or do System.exit().
          val sc = sparkContextRef.get()
          if (sc != null) {
            logInfo("Invoking sc stop from shutdown hook")
            sc.stop()
          }
          val maxAppAttempts = client.getMaxRegAttempts(yarnConf)
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
      }

      // Use higher priority than FileSystem.
      assert(ApplicationMaster.SHUTDOWN_HOOK_PRIORITY > FileSystem.SHUTDOWN_HOOK_PRIORITY)
      ShutdownHookManager
        .get().addShutdownHook(cleanupHook, ApplicationMaster.SHUTDOWN_HOOK_PRIORITY)

      // Call this to force generation of secret so it gets populated into the
      // Hadoop UGI. This has to happen before the startUserClass which does a
      // doAs in order for the credentials to be passed on to the executor containers.
      val securityMgr = new SecurityManager(sparkConf)

      if (isDriver) {
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
          "Uncaught exception: " + e.getMessage())
    }
    exitCode
  }

  /**
   * Set the default final application status for client mode to UNDEFINED to handle
   * if YARN HA restarts the application so that it properly retries. Set the final
   * status to SUCCEEDED in cluster mode to handle if the user calls System.exit
   * from the application code.
   */
  final def getDefaultFinalStatus() = {
    if (isDriver) {
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
  final def unregister(status: FinalApplicationStatus, diagnostics: String = null) = synchronized {
    if (!unregistered) {
      logInfo(s"Unregistering ApplicationMaster with $status" +
        Option(diagnostics).map(msg => s" (diag message: $msg)").getOrElse(""))
      unregistered = true
      client.unregister(status, Option(diagnostics).getOrElse(""))
    }
  }

  final def finish(status: FinalApplicationStatus, code: Int, msg: String = null) = synchronized {
    if (!finished) {
      val inShutdown = Utils.inShutdown()
      logInfo(s"Final app status: ${status}, exitCode: ${code}" +
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

  private def registerAM(uiAddress: String, securityMgr: SecurityManager) = {
    val sc = sparkContextRef.get()

    val appId = client.getAttemptId().getApplicationId().toString()
    val historyAddress =
      sparkConf.getOption("spark.yarn.historyServer.address")
        .map { address => s"${address}${HistoryServer.UI_PATH_PREFIX}/${appId}" }
        .getOrElse("")

    allocator = client.register(yarnConf,
      if (sc != null) sc.getConf else sparkConf,
      if (sc != null) sc.preferredNodeLocationData else Map(),
      uiAddress,
      historyAddress,
      securityMgr)

    allocator.allocateResources()
    reporterThread = launchReporterThread()
  }

  private def runDriver(securityMgr: SecurityManager): Unit = {
    addAmIpFilter()
    userClassThread = startUserClass()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.
    val sc = waitForSparkContextInitialized()

    // If there is no SparkContext at this point, just fail the app.
    if (sc == null) {
      finish(FinalApplicationStatus.FAILED,
        ApplicationMaster.EXIT_SC_NOT_INITED,
        "Timed out waiting for SparkContext.")
    } else {
      registerAM(sc.ui.map(_.appUIAddress).getOrElse(""), securityMgr)
      userClassThread.join()
    }
  }

  private def runExecutorLauncher(securityMgr: SecurityManager): Unit = {
    actorSystem = AkkaUtils.createActorSystem("sparkYarnAM", Utils.localHostName, 0,
      conf = sparkConf, securityManager = securityMgr)._1
    actor = waitForSparkDriver()
    addAmIpFilter()
    registerAM(sparkConf.get("spark.driver.appUIAddress", ""), securityMgr)

    // In client mode the actor will stop the reporter thread.
    reporterThread.join()
  }

  private def launchReporterThread(): Thread = {
    // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.
    val expiryInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)

    // we want to be reasonably responsive without causing too many requests to RM.
    val schedulerInterval =
      sparkConf.getLong("spark.yarn.scheduler.heartbeat.interval-ms", 5000)

    // must be <= expiryInterval / 2.
    val interval = math.max(0, math.min(expiryInterval / 2, schedulerInterval))

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
                "Max number of executor failures reached")
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
                    s"${failureCount} time(s) from Reporter thread.")

              } else {
                logWarning(s"Reporter thread fails ${failureCount} time(s) in a row.", e)
              }
            }
          }
          try {
            Thread.sleep(interval)
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
    logInfo("Started progress reporter thread - sleep time : " + interval)
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
      val waitTries = sparkConf.getOption("spark.yarn.applicationMaster.waitTries")
        .map(_.toLong * 10000L)
      if (waitTries.isDefined) {
        logWarning(
          "spark.yarn.applicationMaster.waitTries is deprecated, use spark.yarn.am.waitTime")
      }
      val totalWaitTime = sparkConf.getLong("spark.yarn.am.waitTime", waitTries.getOrElse(100000L))
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

  private def waitForSparkDriver(): ActorRef = {
    logInfo("Waiting for Spark driver to be reachable.")
    var driverUp = false
    val hostport = args.userArgs(0)
    val (driverHost, driverPort) = Utils.parseHostPort(hostport)

    // Spark driver should already be up since it launched us, but we don't want to
    // wait forever, so wait 100 seconds max to match the cluster mode setting.
    val totalWaitTime = sparkConf.getLong("spark.yarn.am.waitTime", 100000L)
    val deadline = System.currentTimeMillis + totalWaitTime

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

    val driverUrl = "akka.tcp://%s@%s:%s/user/%s".format(
      SparkEnv.driverActorSystemName,
      driverHost,
      driverPort.toString,
      YarnSchedulerBackend.ACTOR_NAME)
    actorSystem.actorOf(Props(new AMActor(driverUrl)), name = "YarnAM")
  }

  /** Add the Yarn IP filter that is required for properly securing the UI. */
  private def addAmIpFilter() = {
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val params = client.getAmIpFilterParams(yarnConf, proxyBase)
    if (isDriver) {
      System.setProperty("spark.ui.filters", amFilter)
      params.foreach { case (k, v) => System.setProperty(s"spark.$amFilter.param.$k", v) }
    } else {
      actor ! AddWebUIFilter(amFilter, params.toMap, proxyBase)
    }
  }

  /**
   * Start the user class, which contains the spark driver, in a separate Thread.
   * If the main routine exits cleanly or exits with System.exit(N) for any N
   * we assume it was successful, for all other cases we assume failure.
   *
   * Returns the user thread that was started.
   */
  private def startUserClass(): Thread = {
    logInfo("Starting the user JAR in a separate Thread")
    System.setProperty("spark.executor.instances", args.numExecutors.toString)
    val mainMethod = Class.forName(args.userClass, false,
      Thread.currentThread.getContextClassLoader).getMethod("main", classOf[Array[String]])

    val userThread = new Thread {
      override def run() {
        try {
          val mainArgs = new Array[String](args.userArgs.size)
          args.userArgs.copyToArray(mainArgs, 0, args.userArgs.size)
          mainMethod.invoke(null, mainArgs)
          finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
          logDebug("Done running users class")
        } catch {
          case e: InvocationTargetException =>
            e.getCause match {
              case _: InterruptedException =>
                // Reporter thread can interrupt to stop user class
              case e: Exception =>
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_EXCEPTION_USER_CLASS,
                  "User class threw exception: " + e.getMessage)
                // re-throw to get it logged
                throw e
            }
        }
      }
    }
    userThread.setName("Driver")
    userThread.start()
    userThread
  }

  /**
   * Actor that communicates with the driver in client deploy mode.
   */
  private class AMActor(driverUrl: String) extends Actor {
    var driver: ActorSelection = _

    override def preStart() = {
      logInfo("Listen to driver: " + driverUrl)
      driver = context.actorSelection(driverUrl)
      // Send a hello message to establish the connection, after which
      // we can monitor Lifecycle Events.
      driver ! "Hello"
      driver ! RegisterClusterManager
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    override def receive = {
      case x: DisassociatedEvent =>
        logInfo(s"Driver terminated or disconnected! Shutting down. $x")
        finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)

      case x: AddWebUIFilter =>
        logInfo(s"Add WebUI Filter. $x")
        driver ! x

      case RequestExecutors(requestedTotal) =>
        logInfo(s"Driver requested a total number of $requestedTotal executor(s).")
        Option(allocator) match {
          case Some(a) => a.requestTotalExecutors(requestedTotal)
          case None => logWarning("Container allocator is not ready to request executors yet.")
        }
        sender ! true

      case KillExecutors(executorIds) =>
        logInfo(s"Driver requested to kill executor(s) ${executorIds.mkString(", ")}.")
        Option(allocator) match {
          case Some(a) => executorIds.foreach(a.killExecutor)
          case None => logWarning("Container allocator is not ready to kill executors yet.")
        }
        sender ! true
    }
  }

}

object ApplicationMaster extends Logging {

  val SHUTDOWN_HOOK_PRIORITY: Int = 30

  // exit codes for different causes, no reason behind the values
  private val EXIT_SUCCESS = 0
  private val EXIT_UNCAUGHT_EXCEPTION = 10
  private val EXIT_MAX_EXECUTOR_FAILURES = 11
  private val EXIT_REPORTER_FAILURE = 12
  private val EXIT_SC_NOT_INITED = 13
  private val EXIT_SECURITY = 14
  private val EXIT_EXCEPTION_USER_CLASS = 15

  private var master: ApplicationMaster = _

  def main(args: Array[String]) = {
    SignalLogger.register(log)
    val amArgs = new ApplicationMasterArguments(args)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      master = new ApplicationMaster(amArgs, new YarnRMClient(amArgs))
      System.exit(master.run())
    }
  }

  private[spark] def sparkContextInitialized(sc: SparkContext) = {
    master.sparkContextInitialized(sc)
  }

  private[spark] def sparkContextStopped(sc: SparkContext) = {
    master.sparkContextStopped(sc)
  }

}

/**
 * This object does not provide any special functionality. It exists so that it's easy to tell
 * apart the client-mode AM from the cluster-mode AM when using tools such as ps or jps.
 */
object ExecutorLauncher {

  def main(args: Array[String]) = {
    ApplicationMaster.main(args)
  }

}
