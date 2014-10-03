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
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.AddWebUIFilter
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

  @volatile private var finished = false
  @volatile private var finalStatus = FinalApplicationStatus.UNDEFINED
  @volatile private var userClassThread: Thread = _

  private var reporterThread: Thread = _
  private var allocator: YarnAllocator = _

  // Fields used in client mode.
  private var actorSystem: ActorSystem = null
  private var actor: ActorRef = _

  // Fields used in cluster mode.
  private val sparkContextRef = new AtomicReference[SparkContext](null)

  final def run(): Int = {
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

    val cleanupHook = new Runnable {
      override def run() {
        // If the SparkContext is still registered, shut it down as a best case effort in case
        // users do not call sc.stop or do System.exit().
        val sc = sparkContextRef.get()
        if (sc != null) {
          logInfo("Invoking sc stop from shutdown hook")
          sc.stop()
          finish(FinalApplicationStatus.SUCCEEDED)
        }

        // Cleanup the staging dir after the app is finished, or if it's the last attempt at
        // running the AM.
        val maxAppAttempts = client.getMaxRegAttempts(yarnConf)
        val isLastAttempt = client.getAttemptId().getAttemptId() >= maxAppAttempts
        if (finished || isLastAttempt) {
          cleanupStagingDir()
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

    if (finalStatus != FinalApplicationStatus.UNDEFINED) {
      finish(finalStatus)
      0
    } else {
      1
    }
  }

  final def finish(status: FinalApplicationStatus, diagnostics: String = null) = synchronized {
    if (!finished) {
      logInfo(s"Finishing ApplicationMaster with $status"  +
        Option(diagnostics).map(msg => s" (diag message: $msg)").getOrElse(""))
      finished = true
      finalStatus = status
      try {
        if (Thread.currentThread() != reporterThread) {
          reporterThread.interrupt()
          reporterThread.join()
        }
      } finally {
        client.shutdown(status, Option(diagnostics).getOrElse(""))
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
    val userThread = startUserClass()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.
    val sc = waitForSparkContextInitialized()

    // If there is no SparkContext at this point, just fail the app.
    if (sc == null) {
      finish(FinalApplicationStatus.FAILED, "Timed out waiting for SparkContext.")
    } else {
      registerAM(sc.ui.map(_.appUIAddress).getOrElse(""), securityMgr)
      try {
        userThread.join()
      } finally {
        // In cluster mode, ask the reporter thread to stop since the user app is finished.
        reporterThread.interrupt()
      }
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
    finalStatus = FinalApplicationStatus.SUCCEEDED
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
            checkNumExecutorsFailed()
            if (!finished) {
              logDebug("Sending progress")
              allocator.allocateResources()
            }
            failureCount = 0
          } catch {
            case e: Throwable => {
              failureCount += 1
              if (!NonFatal(e) || failureCount >= reporterMaxFailures) {
                logError("Exception was thrown from Reporter thread.", e)
                finish(FinalApplicationStatus.FAILED, "Exception was thrown" +
                  s"${failureCount} time(s) from Reporter thread.")

                /**
                 * If exception is thrown from ReporterThread,
                 * interrupt user class to stop.
                 * Without this interrupting, if exception is
                 * thrown before allocating enough executors,
                 * YarnClusterScheduler waits until timeout even though
                 * we cannot allocate executors.
                 */
                logInfo("Interrupting user class to stop.")
                userClassThread.interrupt
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
  private def cleanupStagingDir() {
    val fs = FileSystem.get(yarnConf)
    var stagingDirPath: Path = null
    try {
      val preserveFiles = sparkConf.get("spark.yarn.preserve.staging.files", "false").toBoolean
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
    try {
      sparkContextRef.synchronized {
        var count = 0
        val waitTime = 10000L
        val numTries = sparkConf.getInt("spark.yarn.ApplicationMaster.waitTries", 10)
        while (sparkContextRef.get() == null && count < numTries && !finished) {
          logInfo("Waiting for spark context initialization ... " + count)
          count = count + 1
          sparkContextRef.wait(waitTime)
        }

        val sparkContext = sparkContextRef.get()
        if (sparkContext == null) {
          logError(("SparkContext did not initialize after waiting for %d ms. Please check earlier"
            + " log output for errors. Failing the application.").format(numTries * waitTime))
        }
        sparkContext
      }
    }
  }

  private def waitForSparkDriver(): ActorRef = {
    logInfo("Waiting for Spark driver to be reachable.")
    var driverUp = false
    val hostport = args.userArgs(0)
    val (driverHost, driverPort) = Utils.parseHostPort(hostport)
    while (!driverUp) {
      try {
        val socket = new Socket(driverHost, driverPort)
        socket.close()
        logInfo("Driver now available: %s:%s".format(driverHost, driverPort))
        driverUp = true
      } catch {
        case e: Exception =>
          logError("Failed to connect to driver at %s:%s, retrying ...".
            format(driverHost, driverPort))
          Thread.sleep(100)
      }
    }
    sparkConf.set("spark.driver.host", driverHost)
    sparkConf.set("spark.driver.port", driverPort.toString)

    val driverUrl = "akka.tcp://%s@%s:%s/user/%s".format(
      SparkEnv.driverActorSystemName,
      driverHost,
      driverPort.toString,
      CoarseGrainedSchedulerBackend.ACTOR_NAME)
    actorSystem.actorOf(Props(new MonitorActor(driverUrl)), name = "YarnAM")
  }

  private def checkNumExecutorsFailed() = {
    if (allocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
      finish(FinalApplicationStatus.FAILED, "Max number of executor failures reached.")

      val sc = sparkContextRef.get()
      if (sc != null) {
        logInfo("Invoking sc stop from checkNumExecutorsFailed")
        sc.stop()
      }
    }
  }

  /** Add the Yarn IP filter that is required for properly securing the UI. */
  private def addAmIpFilter() = {
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val proxy = client.getProxyHostAndPort(yarnConf)
    val parts = proxy.split(":")
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val uriBase = "http://" + proxy + proxyBase
    val params = "PROXY_HOST=" + parts(0) + "," + "PROXY_URI_BASE=" + uriBase

    if (isDriver) {
      System.setProperty("spark.ui.filters", amFilter)
      System.setProperty(s"spark.$amFilter.params", params)
    } else {
      actor ! AddWebUIFilter(amFilter, params, proxyBase)
    }
  }

  private def startUserClass(): Thread = {
    logInfo("Starting the user JAR in a separate Thread")
    System.setProperty("spark.executor.instances", args.numExecutors.toString)
    val mainMethod = Class.forName(args.userClass, false,
      Thread.currentThread.getContextClassLoader).getMethod("main", classOf[Array[String]])

    userClassThread = new Thread {
      override def run() {
        var status = FinalApplicationStatus.FAILED
        try {
          // Copy
          val mainArgs = new Array[String](args.userArgs.size)
          args.userArgs.copyToArray(mainArgs, 0, args.userArgs.size)
          mainMethod.invoke(null, mainArgs)
          // Some apps have "System.exit(0)" at the end.  The user thread will stop here unless
          // it has an uncaught exception thrown out.  It needs a shutdown hook to set SUCCEEDED.
          status = FinalApplicationStatus.SUCCEEDED
        } catch {
          case e: InvocationTargetException =>
            e.getCause match {
              case _: InterruptedException =>
                // Reporter thread can interrupt to stop user class

              case e => throw e
            }
        } finally {
          logDebug("Finishing main")
          finalStatus = status
        }
      }
    }
    userClassThread.setName("Driver")
    userClassThread.start()
    userClassThread
  }

  // Actor used to monitor the driver when running in client deploy mode.
  private class MonitorActor(driverUrl: String) extends Actor {

    var driver: ActorSelection = _

    override def preStart() = {
      logInfo("Listen to driver: " + driverUrl)
      driver = context.actorSelection(driverUrl)
      // Send a hello message to establish the connection, after which
      // we can monitor Lifecycle Events.
      driver ! "Hello"
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    override def receive = {
      case x: DisassociatedEvent =>
        logInfo(s"Driver terminated or disconnected! Shutting down. $x")
        finish(FinalApplicationStatus.SUCCEEDED)
      case x: AddWebUIFilter =>
        logInfo(s"Add WebUI Filter. $x")
        driver ! x
    }

  }

}

object ApplicationMaster extends Logging {

  val SHUTDOWN_HOOK_PRIORITY: Int = 30

  private var master: ApplicationMaster = _

  def main(args: Array[String]) = {
    SignalLogger.register(log)
    val amArgs = new ApplicationMasterArguments(args)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      master = new ApplicationMaster(amArgs, new YarnRMClientImpl(amArgs))
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
