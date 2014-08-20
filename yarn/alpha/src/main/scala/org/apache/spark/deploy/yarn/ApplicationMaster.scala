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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{SignalLogger, Utils}

/**
 * An application master that runs the users driver program and allocates executors.
 */
class ApplicationMaster(args: ApplicationMasterArguments, conf: Configuration,
                        sparkConf: SparkConf) extends Logging {

  def this(args: ApplicationMasterArguments, sparkConf: SparkConf) =
    this(args, new Configuration(), sparkConf)

  def this(args: ApplicationMasterArguments) = this(args, new SparkConf())

  private val rpc: YarnRPC = YarnRPC.create(conf)
  private var resourceManager: AMRMProtocol = _
  private var appAttemptId: ApplicationAttemptId = _
  private var userThread: Thread = _
  private val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  private val fs = FileSystem.get(yarnConf)

  private var yarnAllocator: YarnAllocationHandler = _
  private var isFinished: Boolean = false
  private var uiAddress: String = _
  private var uiHistoryAddress: String = _
  private val maxAppAttempts: Int = conf.getInt(YarnConfiguration.RM_AM_MAX_RETRIES,
    YarnConfiguration.DEFAULT_RM_AM_MAX_RETRIES)
  private var isLastAMRetry: Boolean = true

  // Default to numExecutors * 2, with minimum of 3
  private val maxNumExecutorFailures = sparkConf.getInt("spark.yarn.max.executor.failures",
    sparkConf.getInt("spark.yarn.max.worker.failures", math.max(args.numExecutors * 2, 3)))

  private var registered = false

  def run() {
    // set the web ui port to be ephemeral for yarn so we don't conflict with
    // other spark processes running on the same box
    System.setProperty("spark.ui.port", "0")

    // when running the AM, the Spark master is always "yarn-cluster"
    System.setProperty("spark.master", "yarn-cluster")

    // Use priority 30 as its higher then HDFS. Its same priority as MapReduce is using.
    ShutdownHookManager.get().addShutdownHook(new AppMasterShutdownHook(this), 30)

    appAttemptId = getApplicationAttemptId()
    isLastAMRetry = appAttemptId.getAttemptId() >= maxAppAttempts
    resourceManager = registerWithResourceManager()

    // setup AmIpFilter for the SparkUI - do this before we start the UI
    addAmIpFilter()

    ApplicationMaster.register(this)

    // Call this to force generation of secret so it gets populated into the
    // hadoop UGI. This has to happen before the startUserClass which does a
    // doAs in order for the credentials to be passed on to the executor containers.
    val securityMgr = new SecurityManager(sparkConf)

    // Start the user's JAR
    userThread = startUserClass()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.
    waitForSparkContextInitialized()

    // Do this after spark master is up and SparkContext is created so that we can register UI Url
    synchronized {
      if (!isFinished) {
        registerApplicationMaster()
        registered = true
      }
    }

    // Allocate all containers
    allocateExecutors()

    // Wait for the user class to Finish
    userThread.join()

    System.exit(0)
  }

  // add the yarn amIpFilter that Yarn requires for properly securing the UI
  private def addAmIpFilter() {
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    System.setProperty("spark.ui.filters", amFilter)
    val proxy = YarnConfiguration.getProxyHostAndPort(conf)
    val parts : Array[String] = proxy.split(":")
    val uriBase = "http://" + proxy +
      System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)

    val params = "PROXY_HOST=" + parts(0) + "," + "PROXY_URI_BASE=" + uriBase
    System.setProperty("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.params",
      params)
  }

  private def getApplicationAttemptId(): ApplicationAttemptId = {
    val envs = System.getenv()
    val containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV)
    val containerId = ConverterUtils.toContainerId(containerIdString)
    val appAttemptId = containerId.getApplicationAttemptId()
    logInfo("ApplicationAttemptId: " + appAttemptId)
    appAttemptId
  }

  private def registerWithResourceManager(): AMRMProtocol = {
    val rmAddress = NetUtils.createSocketAddr(yarnConf.get(
      YarnConfiguration.RM_SCHEDULER_ADDRESS,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))
    logInfo("Connecting to ResourceManager at " + rmAddress)
    rpc.getProxy(classOf[AMRMProtocol], rmAddress, conf).asInstanceOf[AMRMProtocol]
  }

  private def registerApplicationMaster(): RegisterApplicationMasterResponse = {
    logInfo("Registering the ApplicationMaster")
    val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest])
      .asInstanceOf[RegisterApplicationMasterRequest]
    appMasterRequest.setApplicationAttemptId(appAttemptId)
    // Setting this to master host,port - so that the ApplicationReport at client has some
    // sensible info.
    // Users can then monitor stderr/stdout on that node if required.
    appMasterRequest.setHost(Utils.localHostName())
    appMasterRequest.setRpcPort(0)
    appMasterRequest.setTrackingUrl(uiAddress)
    resourceManager.registerApplicationMaster(appMasterRequest)
  }

  private def startUserClass(): Thread = {
    logInfo("Starting the user JAR in a separate Thread")
    System.setProperty("spark.executor.instances", args.numExecutors.toString)
    val mainMethod = Class.forName(
      args.userClass,
      false /* initialize */ ,
      Thread.currentThread.getContextClassLoader).getMethod("main", classOf[Array[String]])
    val t = new Thread {
      override def run() {

        var successed = false
        try {
          // Copy
          var mainArgs: Array[String] = new Array[String](args.userArgs.size)
          args.userArgs.copyToArray(mainArgs, 0, args.userArgs.size)
          mainMethod.invoke(null, mainArgs)
          // some job script has "System.exit(0)" at the end, for example SparkPi, SparkLR
          // userThread will stop here unless it has uncaught exception thrown out
          // It need shutdown hook to set SUCCEEDED
          successed = true
        } finally {
          logDebug("finishing main")
          isLastAMRetry = true
          if (successed) {
            ApplicationMaster.this.finishApplicationMaster(FinalApplicationStatus.SUCCEEDED)
          } else {
            ApplicationMaster.this.finishApplicationMaster(FinalApplicationStatus.FAILED)
          }
        }
      }
    }
    t.start()
    t
  }

  // this need to happen before allocateExecutors
  private def waitForSparkContextInitialized() {
    logInfo("Waiting for spark context initialization")
    try {
      var sparkContext: SparkContext = null
      ApplicationMaster.sparkContextRef.synchronized {
        var count = 0
        val waitTime = 10000L
        val numTries = sparkConf.getInt("spark.yarn.ApplicationMaster.waitTries", 10)
        while (ApplicationMaster.sparkContextRef.get() == null && count < numTries
            && !isFinished) {
          logInfo("Waiting for spark context initialization ... " + count)
          count = count + 1
          ApplicationMaster.sparkContextRef.wait(waitTime)
        }
        sparkContext = ApplicationMaster.sparkContextRef.get()
        assert(sparkContext != null || count >= numTries)

        if (null != sparkContext) {
          uiAddress = sparkContext.ui.appUIHostPort
          uiHistoryAddress = YarnSparkHadoopUtil.getUIHistoryAddress(sparkContext, sparkConf)
          this.yarnAllocator = YarnAllocationHandler.newAllocator(
            yarnConf,
            resourceManager,
            appAttemptId,
            args,
            sparkContext.preferredNodeLocationData,
            sparkContext.getConf)
        } else {
          logWarning("Unable to retrieve sparkContext inspite of waiting for %d, numTries = %d".
            format(count * waitTime, numTries))
          this.yarnAllocator = YarnAllocationHandler.newAllocator(
            yarnConf,
            resourceManager,
            appAttemptId,
            args,
            sparkContext.getConf)
        }
      }
    }
  }

  private def allocateExecutors() {
    try {
      logInfo("Allocating " + args.numExecutors + " executors.")
      // Wait until all containers have finished
      // TODO: This is a bit ugly. Can we make it nicer?
      // TODO: Handle container failure

      // Exits the loop if the user thread exits.
      while (yarnAllocator.getNumExecutorsRunning < args.numExecutors && userThread.isAlive
          && !isFinished) {
        checkNumExecutorsFailed()
        yarnAllocator.allocateContainers(
          math.max(args.numExecutors - yarnAllocator.getNumExecutorsRunning, 0))
        Thread.sleep(ApplicationMaster.ALLOCATE_HEARTBEAT_INTERVAL)
      }
    }
    logInfo("All executors have launched.")

    // Launch a progress reporter thread, else the app will get killed after expiration
    // (def: 10mins) timeout.
    // TODO(harvey): Verify the timeout
    if (userThread.isAlive) {
      // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.
      val timeoutInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)

      // we want to be reasonably responsive without causing too many requests to RM.
      val schedulerInterval =
        sparkConf.getLong("spark.yarn.scheduler.heartbeat.interval-ms", 5000)

      // must be <= timeoutInterval / 2.
      val interval = math.min(timeoutInterval / 2, schedulerInterval)

      launchReporterThread(interval)
    }
  }

  private def launchReporterThread(_sleepTime: Long): Thread = {
    val sleepTime = if (_sleepTime <= 0) 0 else _sleepTime

    val t = new Thread {
      override def run() {
        while (userThread.isAlive && !isFinished) {
          checkNumExecutorsFailed()
          val missingExecutorCount = args.numExecutors - yarnAllocator.getNumExecutorsRunning
          if (missingExecutorCount > 0) {
            logInfo("Allocating %d containers to make up for (potentially) lost containers".
              format(missingExecutorCount))
            yarnAllocator.allocateContainers(missingExecutorCount)
          } else {
            sendProgress()
          }
          Thread.sleep(sleepTime)
        }
      }
    }
    // Setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.start()
    logInfo("Started progress reporter thread - sleep time : " + sleepTime)
    t
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

  private def sendProgress() {
    logDebug("Sending progress")
    // Simulated with an allocate request with no nodes requested ...
    yarnAllocator.allocateContainers(0)
  }

  /*
  def printContainers(containers: List[Container]) = {
    for (container <- containers) {
      logInfo("Launching shell command on a new container."
        + ", containerId=" + container.getId()
        + ", containerNode=" + container.getNodeId().getHost()
        + ":" + container.getNodeId().getPort()
        + ", containerNodeURI=" + container.getNodeHttpAddress()
        + ", containerState" + container.getState()
        + ", containerResourceMemory"
        + container.getResource().getMemory())
    }
  }
  */

  def finishApplicationMaster(status: FinalApplicationStatus, diagnostics: String = "") {
    synchronized {
      if (isFinished) {
        return
      }
      isFinished = true

      logInfo("finishApplicationMaster with " + status)
      if (registered) {
        val finishReq = Records.newRecord(classOf[FinishApplicationMasterRequest])
          .asInstanceOf[FinishApplicationMasterRequest]
        finishReq.setAppAttemptId(appAttemptId)
        finishReq.setFinishApplicationStatus(status)
        finishReq.setDiagnostics(diagnostics)
        finishReq.setTrackingUrl(uiHistoryAddress)
        resourceManager.finishApplicationMaster(finishReq)
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
      // we need to clean up staging dir before HDFS is shut down
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

  def register(master: ApplicationMaster) {
    applicationMasters.add(master)
  }

  val sparkContextRef: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null /* initialValue */)

  def sparkContextInitialized(sc: SparkContext): Boolean = {
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
          // Best case ...
          for (master <- applicationMasters) {
            master.finishApplicationMaster(FinalApplicationStatus.SUCCEEDED)
          }
        }
      })
    }

    modified
  }

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val args = new ApplicationMasterArguments(argStrings)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      new ApplicationMaster(args).run()
    }
  }
}
