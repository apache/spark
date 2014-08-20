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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.webapp.util.WebAppUtils

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{SignalLogger, Utils}


/**
 * An application master that runs the user's driver program and allocates executors.
 */
class ApplicationMaster(args: ApplicationMasterArguments, conf: Configuration,
                        sparkConf: SparkConf) extends Logging {

  def this(args: ApplicationMasterArguments, sparkConf: SparkConf) =
    this(args, new Configuration(), sparkConf)

  def this(args: ApplicationMasterArguments) = this(args, new SparkConf())

  private val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  private var appAttemptId: ApplicationAttemptId = _
  private var userThread: Thread = _
  private val fs = FileSystem.get(yarnConf)

  private var yarnAllocator: YarnAllocationHandler = _
  private var isFinished: Boolean = false
  private var uiAddress: String = _
  private var uiHistoryAddress: String = _
  private val maxAppAttempts: Int = conf.getInt(
    YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
  private var isLastAMRetry: Boolean = true
  private var amClient: AMRMClient[ContainerRequest] = _

  // Default to numExecutors * 2, with minimum of 3
  private val maxNumExecutorFailures = sparkConf.getInt("spark.yarn.max.executor.failures",
    sparkConf.getInt("spark.yarn.max.worker.failures", math.max(args.numExecutors * 2, 3)))

  private var registered = false

  def run() {
    // Set the web ui port to be ephemeral for yarn so we don't conflict with
    // other spark processes running on the same box
    System.setProperty("spark.ui.port", "0")

    // When running the AM, the Spark master is always "yarn-cluster"
    System.setProperty("spark.master", "yarn-cluster")

    // Use priority 30 as it's higher than HDFS. It's the same priority MapReduce is using.
    ShutdownHookManager.get().addShutdownHook(new AppMasterShutdownHook(this), 30)

    appAttemptId = ApplicationMaster.getApplicationAttemptId()
    logInfo("ApplicationAttemptId: " + appAttemptId)
    isLastAMRetry = appAttemptId.getAttemptId() >= maxAppAttempts
    amClient = AMRMClient.createAMRMClient()
    amClient.init(yarnConf)
    amClient.start()

    // setup AmIpFilter for the SparkUI - do this before we start the UI
    addAmIpFilter()

    ApplicationMaster.register(this)

    // Call this to force generation of secret so it gets populated into the
    // Hadoop UGI. This has to happen before the startUserClass which does a
    // doAs in order for the credentials to be passed on to the executor containers.
    val securityMgr = new SecurityManager(sparkConf)

    // Start the user's JAR
    userThread = startUserClass()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.
    waitForSparkContextInitialized()

    // Do this after Spark master is up and SparkContext is created so that we can register UI Url.
    synchronized {
      if (!isFinished) {
        registerApplicationMaster()
        registered = true
      }
    }

    // Allocate all containers
    allocateExecutors()

    // Launch thread that will heartbeat to the RM so it won't think the app has died.
    launchReporterThread()

    // Wait for the user class to finish
    userThread.join()

    System.exit(0)
  }

  // add the yarn amIpFilter that Yarn requires for properly securing the UI
  private def addAmIpFilter() {
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    System.setProperty("spark.ui.filters", amFilter)
    val proxy = WebAppUtils.getProxyHostAndPort(conf)
    val parts : Array[String] = proxy.split(":")
    val uriBase = "http://" + proxy +
      System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)

    val params = "PROXY_HOST=" + parts(0) + "," + "PROXY_URI_BASE=" + uriBase
    System.setProperty(
      "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.params", params)
  }

  private def registerApplicationMaster(): RegisterApplicationMasterResponse = {
    logInfo("Registering the ApplicationMaster")
    amClient.registerApplicationMaster(Utils.localHostName(), 0, uiAddress)
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
        var succeeded = false
        try {
          // Copy
          val mainArgs = new Array[String](args.userArgs.size)
          args.userArgs.copyToArray(mainArgs, 0, args.userArgs.size)
          mainMethod.invoke(null, mainArgs)
          // Some apps have "System.exit(0)" at the end.  The user thread will stop here unless
          // it has an uncaught exception thrown out.  It needs a shutdown hook to set SUCCEEDED.
          succeeded = true
        } finally {
          logDebug("Finishing main")
          isLastAMRetry = true
          if (succeeded) {
            ApplicationMaster.this.finishApplicationMaster(FinalApplicationStatus.SUCCEEDED)
          } else {
            ApplicationMaster.this.finishApplicationMaster(FinalApplicationStatus.FAILED)
          }
        }
      }
    }
    t.setName("Driver")
    t.start()
    t
  }

  // This needs to happen before allocateExecutors()
  private def waitForSparkContextInitialized() {
    logInfo("Waiting for Spark context initialization")
    try {
      var sparkContext: SparkContext = null
      ApplicationMaster.sparkContextRef.synchronized {
        var numTries = 0
        val waitTime = 10000L
        val maxNumTries = sparkConf.getInt("spark.yarn.applicationMaster.waitTries", 10)
        while (ApplicationMaster.sparkContextRef.get() == null && numTries < maxNumTries
            && !isFinished) {
          logInfo("Waiting for Spark context initialization ... " + numTries)
          numTries = numTries + 1
          ApplicationMaster.sparkContextRef.wait(waitTime)
        }
        sparkContext = ApplicationMaster.sparkContextRef.get()
        assert(sparkContext != null || numTries >= maxNumTries)

        if (sparkContext != null) {
          uiAddress = sparkContext.ui.appUIHostPort
          uiHistoryAddress = YarnSparkHadoopUtil.getUIHistoryAddress(sparkContext, sparkConf)
          this.yarnAllocator = YarnAllocationHandler.newAllocator(
            yarnConf,
            amClient,
            appAttemptId,
            args,
            sparkContext.preferredNodeLocationData,
            sparkContext.getConf)
        } else {
          logWarning("Unable to retrieve SparkContext in spite of waiting for %d, maxNumTries = %d".
            format(numTries * waitTime, maxNumTries))
          this.yarnAllocator = YarnAllocationHandler.newAllocator(
            yarnConf,
            amClient,
            appAttemptId,
            args,
            sparkContext.getConf)
        }
      }
    }
  }

  private def allocateExecutors() {
    try {
      logInfo("Requesting" + args.numExecutors + " executors.")
      // Wait until all containers have launched
      yarnAllocator.addResourceRequests(args.numExecutors)
      yarnAllocator.allocateResources()
      // Exits the loop if the user thread exits.

      while (yarnAllocator.getNumExecutorsRunning < args.numExecutors && userThread.isAlive
          && !isFinished) {
        checkNumExecutorsFailed()
        allocateMissingExecutor()
        yarnAllocator.allocateResources()
        Thread.sleep(ApplicationMaster.ALLOCATE_HEARTBEAT_INTERVAL)
      }
    }
    logInfo("All executors have launched.")
  }

  private def allocateMissingExecutor() {
    val missingExecutorCount = args.numExecutors - yarnAllocator.getNumExecutorsRunning -
      yarnAllocator.getNumPendingAllocate
    if (missingExecutorCount > 0) {
      logInfo("Allocating %d containers to make up for (potentially) lost containers".
        format(missingExecutorCount))
      yarnAllocator.addResourceRequests(missingExecutorCount)
    }
  }

  private def checkNumExecutorsFailed() {
    if (yarnAllocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
      logInfo("max number of executor failures reached")
      finishApplicationMaster(FinalApplicationStatus.FAILED,
        "max number of executor failures reached")
      // make sure to stop the user thread
      val sparkContext = ApplicationMaster.sparkContextRef.get()
      if (sparkContext != null) {
        logInfo("Invoking sc stop from checkNumExecutorsFailed")
        sparkContext.stop()
      } else {
        logError("sparkContext is null when should shutdown")
      }
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
        while (userThread.isAlive && !isFinished) {
          checkNumExecutorsFailed()
          allocateMissingExecutor()
          logDebug("Sending progress")
          yarnAllocator.allocateResources()
          Thread.sleep(interval)
        }
      }
    }
    // Setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.start()
    logInfo("Started progress reporter thread - heartbeat interval : " + interval)
    t
  }

  def finishApplicationMaster(status: FinalApplicationStatus, diagnostics: String = "") {
    synchronized {
      if (isFinished) {
        return
      }
      isFinished = true

      logInfo("Unregistering ApplicationMaster with " + status)
      if (registered) {
        amClient.unregisterApplicationMaster(status, diagnostics, uiHistoryAddress)
      }
    }
  }

  /**
   * Clean up the staging directory.
   */
  private def cleanupStagingDir() {
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

  // The shutdown hook that runs when a signal is received AND during normal close of the JVM.
  class AppMasterShutdownHook(appMaster: ApplicationMaster) extends Runnable {

    def run() {
      logInfo("AppMaster received a signal.")
      // We need to clean up staging dir before HDFS is shut down
      // make sure we don't delete it until this is the last AM
      if (appMaster.isLastAMRetry) appMaster.cleanupStagingDir()
    }
  }

}

object ApplicationMaster extends Logging {
  // TODO: Currently, task to container is computed once (TaskSetManager) - which need not be
  // optimal as more containers are available. Might need to handle this better.
  private val ALLOCATE_HEARTBEAT_INTERVAL = 100

  private val applicationMasters = new CopyOnWriteArrayList[ApplicationMaster]()

  val sparkContextRef: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)

  def register(master: ApplicationMaster) {
    applicationMasters.add(master)
  }

  /**
   * Called from YarnClusterScheduler to notify the AM code that a SparkContext has been
   * initialized in the user code.
   */
  def sparkContextInitialized(sc: SparkContext): Boolean = {
    var modified = false
    sparkContextRef.synchronized {
      modified = sparkContextRef.compareAndSet(null, sc)
      sparkContextRef.notifyAll()
    }

    // Add a shutdown hook - as a best effort in case users do not call sc.stop or do
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
          // Best case ...
          for (master <- applicationMasters) {
            master.finishApplicationMaster(FinalApplicationStatus.SUCCEEDED)
          }
        }
      })
    }

    // Wait for initialization to complete and at least 'some' nodes to get allocated.
    modified
  }

  def getApplicationAttemptId(): ApplicationAttemptId = {
    val containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name())
    val containerId = ConverterUtils.toContainerId(containerIdString)
    val appAttemptId = containerId.getApplicationAttemptId()
    appAttemptId
  }

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val args = new ApplicationMasterArguments(argStrings)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      new ApplicationMaster(args).run()
    }
  }
}
