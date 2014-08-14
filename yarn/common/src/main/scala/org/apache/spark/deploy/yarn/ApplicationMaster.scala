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

import java.io.IOException
import java.net.Socket
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConversions._
import scala.util.Try

import akka.actor._
import akka.remote._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
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
  private val ALLOCATE_HEARTBEAT_INTERVAL = 100

  private val sparkConf = new SparkConf()
  private val yarnConf: YarnConfiguration = new YarnConfiguration(new Configuration())
  private val isDriver = args.userClass != null

  // Default to numExecutors * 2, with minimum of 3
  private val maxNumExecutorFailures = sparkConf.getInt("spark.yarn.max.executor.failures",
    sparkConf.getInt("spark.yarn.max.worker.failures", math.max(args.numExecutors * 2, 3)))

  private var finished = false
  private var registered = false
  private var reporterThread: Thread = _
  private var allocator: YarnAllocator = _

  // Fields used in client mode.
  private var actorSystem: ActorSystem = null

  // Fields used in cluster mode.
  private var userThread: Thread = _
  private val sparkContextRef = new AtomicReference[SparkContext](null)
  private val userResult = new AtomicBoolean(false)

  final def run() = {
    // Setup the directories so things go to YARN approved directories rather
    // than user specified and /tmp.
    System.setProperty("spark.local.dir", getLocalDirs())

    if (isDriver) {
      // Set the web ui port to be ephemeral for yarn so we don't conflict with
      // other spark processes running on the same box
      System.setProperty("spark.ui.port", "0")

      // When running the AM, the Spark master is always "yarn-cluster"
      System.setProperty("spark.master", "yarn-cluster")
    }

    val attemptId = client.getAttemptId()
    logInfo("ApplicationAttemptId: " + attemptId)

    // Call this to force generation of secret so it gets populated into the
    // Hadoop UGI. This has to happen before the startUserClass which does a
    // doAs in order for the credentials to be passed on to the executor containers.
    val securityMgr = new SecurityManager(sparkConf)

    val (uiAddress, uiHistoryAddress) = if (isDriver) {
      // Start the user's JAR
      userThread = startUserClass()

      // This a bit hacky, but we need to wait until the spark.driver.port property has
      // been set by the Thread executing the user class.
      waitForSparkContextInitialized()

      val sc = sparkContextRef.get()
      (sc.ui.appUIHostPort, YarnSparkHadoopUtil.getUIHistoryAddress(sc, sparkConf))
    } else {
      actorSystem = AkkaUtils.createActorSystem("sparkYarnAM", Utils.localHostName, 0,
        conf = sparkConf, securityManager = securityMgr)._1
      waitForSparkMaster()
      (sparkConf.get("spark.driver.appUIAddress", ""), "")
    }

    Utils.logUncaughtExceptions {
      val sc = sparkContextRef.get()
      allocator = client.register(yarnConf,
        if (sc != null) sc.getConf else sparkConf,
        if (sc != null) sc.preferredNodeLocationData else Map(),
        uiAddress,
        uiHistoryAddress)
      registered = true
    }

    addAmIpFilter()

    if (registered) {
      // Launch thread that will heartbeat to the RM so it won't think the app has died.
      reporterThread = launchReporterThread()

      // Allocate all containers
      allocateExecutors()
    }

    val success =
      if (isDriver) {
        try {
          userThread.join()
          userResult.get()
        } finally {
          // In cluster mode, ask the reporter thread to stop since the user app is finished.
          reporterThread.interrupt()
        }
      } else {
        // In client mode the actor will stop the reporter thread.
        reporterThread.join()
        true
      }

    finish(if (success) FinalApplicationStatus.SUCCEEDED else FinalApplicationStatus.FAILED)

    val shouldCleanup =
      if (success) {
        true
      } else {
        val maxAppAttempts = client.getMaxRegAttempts(yarnConf)
        attemptId.getAttemptId() >= maxAppAttempts
      }

    if (shouldCleanup) {
      cleanupStagingDir()
    }
  }

  final def finish(status: FinalApplicationStatus, diagnostics: String = "") = synchronized {
    if (!finished) {
      logInfo(s"Finishing ApplicationMaster with $status")
      finished = true
      reporterThread.interrupt()
      reporterThread.join()
      client.shutdown(status, diagnostics)
    }
  }

  private[spark] def sparkContextInitialized(sc: SparkContext) = {
    var modified = false
    sparkContextRef.synchronized {
      modified = sparkContextRef.compareAndSet(null, sc)
      sparkContextRef.notifyAll()
    }

    // Add a shutdown hook - as a best case effort in case users do not call sc.stop or do
    // System.exit.
    // Should not really have to do this, but it helps YARN to evict resources earlier.
    // Not to mention, prevent the Client from declaring failure even though we exited properly.
    // Note that this will unfortunately not properly clean up the staging files because it gets
    // called too late, after the filesystem is already shutdown.
    if (modified) {
      Runtime.getRuntime().addShutdownHook(new Thread with Logging {
        // This is not only logs, but also ensures that log system is initialized for this instance
        // when we are actually 'run'-ing.
        logInfo("Adding shutdown hook for context " + sc)

        override def run() {
          logInfo("Invoking sc stop from shutdown hook")
          sc.stop()
          finish(FinalApplicationStatus.SUCCEEDED)
        }
      })
    }
  }

  /** Get the Yarn approved local directories. */
  private def getLocalDirs(): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(System.getenv("YARN_LOCAL_DIRS"))
      .orElse(Option(System.getenv("LOCAL_DIRS")))

    localDirs match {
      case None => throw new Exception("Yarn Local dirs can't be empty")
      case Some(l) => l
    }
  }

  private def launchReporterThread(): Thread = {
    // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.
    val expiryInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)

    // we want to be reasonably responsive without causing too many requests to RM.
    val schedulerInterval =
      sparkConf.getLong("spark.yarn.scheduler.heartbeat.interval-ms", 5000)

    // must be <= timeoutInterval / 2.
    val interval = math.max(0, math.min(expiryInterval / 2, schedulerInterval))

    val t = new Thread {
      override def run() {
        while (!finished) {
          checkNumExecutorsFailed()
          logDebug("Sending progress")
          allocator.allocateResources()
          Try(Thread.sleep(interval))
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

  // Note: this need to happen before allocateExecutors.
  private def waitForSparkContextInitialized() {
    logInfo("Waiting for spark context initialization")
    try {
      var sparkContext: SparkContext = null
      sparkContextRef.synchronized {
        var count = 0
        val waitTime = 10000L
        val numTries = sparkConf.getInt("spark.yarn.ApplicationMaster.waitTries", 10)
        while (sparkContextRef.get() == null && count < numTries && !finished) {
          logInfo("Waiting for spark context initialization ... " + count)
          count = count + 1
          sparkContextRef.wait(waitTime)
        }
        sparkContext = sparkContextRef.get()
        assert(sparkContext != null || count >= numTries)

        if (sparkContext == null) {
          throw new IllegalStateException(
            "Unable to retrieve sparkContext inspite of waiting for %d, numTries = %d".format(
              count * waitTime, numTries))
        }
      }
    }
  }

  private def waitForSparkMaster() {
    logInfo("Waiting for Spark driver to be reachable.")
    var driverUp = false
    val hostport = args.userArgs(0)
    val (driverHost, driverPort) = Utils.parseHostPort(hostport)
    while(!driverUp) {
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

    val driverUrl = "akka.tcp://spark@%s:%s/user/%s".format(
      driverHost, driverPort.toString, CoarseGrainedSchedulerBackend.ACTOR_NAME)

    actorSystem.actorOf(Props(new MonitorActor(driverUrl)), name = "YarnAM")
  }

  private def allocateExecutors() = {
    try {
      logInfo("Requesting" + args.numExecutors + " executors.")
      allocator.allocateResources()

      var iters = 0
      while (allocator.getNumExecutorsRunning < args.numExecutors && !finished) {
        checkNumExecutorsFailed()
        allocator.allocateResources()
        Thread.sleep(ALLOCATE_HEARTBEAT_INTERVAL)
        iters += 1
      }
    }
    logInfo("All executors have launched.")
  }

  private def checkNumExecutorsFailed() = {
    if (allocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
      finish(FinalApplicationStatus.FAILED, "Max number of executor failures reached.")
    }
  }

  // add the yarn amIpFilter that Yarn requires for properly securing the UI
  private def addAmIpFilter() = {
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    System.setProperty("spark.ui.filters", amFilter)
    val proxy = client.getProxyHostAndPort(yarnConf)
    val parts : Array[String] = proxy.split(":")
    val uriBase = "http://" + proxy +
      System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)

    val params = "PROXY_HOST=" + parts(0) + "," + "PROXY_URI_BASE=" + uriBase
    System.setProperty("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.params",
      params)
  }

  private def startUserClass(): Thread = {
    logInfo("Starting the user JAR in a separate Thread")
    System.setProperty("spark.executor.instances", args.numExecutors.toString)
    val mainMethod = Class.forName(
      args.userClass,
      false,
      Thread.currentThread.getContextClassLoader).getMethod("main", classOf[Array[String]])

    val t = new Thread {
      override def run() {
        try {
          // Copy
          val mainArgs = new Array[String](args.userArgs.size)
          args.userArgs.copyToArray(mainArgs, 0, args.userArgs.size)
          mainMethod.invoke(null, mainArgs)
          // Some apps have "System.exit(0)" at the end.  The user thread will stop here unless
          // it has an uncaught exception thrown out.  It needs a shutdown hook to set SUCCEEDED.
          userResult.set(true)
        } finally {
          logDebug("Finishing main")
        }
      }
    }
    t.setName("Driver")
    t.start()
    t
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

  private var master: ApplicationMaster = _

  def main(argStrings: Array[String]) = {
    SignalLogger.register(log)
    val args = new ApplicationMasterArguments(argStrings)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      master = new ApplicationMaster(args, new YarnRMClientImpl(args))
      master.run()
    }
  }

  private[spark] def sparkContextInitialized(sc: SparkContext) = {
    master.sparkContextInitialized(sc)
  }

}
