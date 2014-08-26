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

import java.net.Socket
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}
import akka.actor._
import akka.remote._
import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.util.{Utils, AkkaUtils}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.AddWebUIFilter
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * An application master that allocates executors on behalf of a driver that is running outside
 * the cluster.
 *
 * This is used only in yarn-client mode.
 */
class ExecutorLauncher(args: ApplicationMasterArguments, conf: Configuration, sparkConf: SparkConf)
  extends Logging {

  def this(args: ApplicationMasterArguments, sparkConf: SparkConf) =
    this(args, new Configuration(), sparkConf)

  def this(args: ApplicationMasterArguments) = this(args, new SparkConf())

  private val rpc: YarnRPC = YarnRPC.create(conf)
  private var resourceManager: AMRMProtocol = _
  private var appAttemptId: ApplicationAttemptId = _
  private var reporterThread: Thread = _
  private val yarnConf: YarnConfiguration = new YarnConfiguration(conf)

  private var yarnAllocator: YarnAllocationHandler = _

  private var driverClosed: Boolean = false
  private var isFinished: Boolean = false
  private var registered: Boolean = false

  // Default to numExecutors * 2, with minimum of 3
  private val maxNumExecutorFailures = sparkConf.getInt("spark.yarn.max.executor.failures",
    sparkConf.getInt("spark.yarn.max.worker.failures", math.max(args.numExecutors * 2, 3)))

  val securityManager = new SecurityManager(sparkConf)
  val actorSystem: ActorSystem = AkkaUtils.createActorSystem("sparkYarnAM", Utils.localHostName, 0,
    conf = sparkConf, securityManager = securityManager)._1
  var actor: ActorRef = _

  // This actor just working as a monitor to watch on Driver Actor.
  class MonitorActor(driverUrl: String) extends Actor {

    var driver: ActorSelection = _

    override def preStart() {
      logInfo("Listen to driver: " + driverUrl)
      driver = context.actorSelection(driverUrl)
      // Send a hello message thus the connection is actually established, thus we can
      // monitor Lifecycle Events.
      driver ! "Hello"
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    override def receive = {
      case x: DisassociatedEvent =>
        logInfo(s"Driver terminated or disconnected! Shutting down. $x")
        driverClosed = true
      case x: AddWebUIFilter =>
        logInfo(s"Add WebUI Filter. $x")
        driver ! x
    }
  }

  def run() {
    appAttemptId = getApplicationAttemptId()
    resourceManager = registerWithResourceManager()

    synchronized {
      if (!isFinished) {
        val appMasterResponse: RegisterApplicationMasterResponse = registerApplicationMaster()
        // Compute number of threads for akka
        val minimumMemory = appMasterResponse.getMinimumResourceCapability().getMemory()

        if (minimumMemory > 0) {
          val mem = args.executorMemory + sparkConf.getInt("spark.yarn.executor.memoryOverhead",
            YarnAllocationHandler.MEMORY_OVERHEAD)
          val numCore = (mem  / minimumMemory) + (if (0 != (mem % minimumMemory)) 1 else 0)

          if (numCore > 0) {
            // do not override - hits https://issues.apache.org/jira/browse/HADOOP-8406
            // TODO: Uncomment when hadoop is on a version which has this fixed.
            // args.workerCores = numCore
          }
        }
        registered = true
      }
    }
    waitForSparkMaster()
    addAmIpFilter()
    // Allocate all containers
    allocateExecutors()

    // Launch a progress reporter thread, else app will get killed after expiration
    // (def: 10mins) timeout ensure that progress is sent before
    // YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapse.

    val timeoutInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)
    // we want to be reasonably responsive without causing too many requests to RM.
    val schedulerInterval =
      System.getProperty("spark.yarn.scheduler.heartbeat.interval-ms", "5000").toLong

    // must be <= timeoutInterval / 2.
    val interval = math.min(timeoutInterval / 2, schedulerInterval)

    reporterThread = launchReporterThread(interval)

    // Wait for the reporter thread to Finish.
    reporterThread.join()

    finishApplicationMaster(FinalApplicationStatus.SUCCEEDED)
    actorSystem.shutdown()

    logInfo("Exited")
    System.exit(0)
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
    val appUIAddress = sparkConf.get("spark.driver.appUIAddress", "")
    logInfo(s"Registering the ApplicationMaster with appUIAddress: $appUIAddress")
    val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest])
      .asInstanceOf[RegisterApplicationMasterRequest]
    appMasterRequest.setApplicationAttemptId(appAttemptId)
    // Setting this to master host,port - so that the ApplicationReport at client has
    // some sensible info. Users can then monitor stderr/stdout on that node if required.
    appMasterRequest.setHost(Utils.localHostName())
    appMasterRequest.setRpcPort(0)
    // What do we provide here ? Might make sense to expose something sensible later ?
    appMasterRequest.setTrackingUrl(appUIAddress)
    resourceManager.registerApplicationMaster(appMasterRequest)
  }

  // add the yarn amIpFilter that Yarn requires for properly securing the UI
  private def addAmIpFilter() {
    val proxy = YarnConfiguration.getProxyHostAndPort(conf)
    val parts = proxy.split(":")
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val uriBase = "http://" + proxy + proxyBase
    val amFilter = "PROXY_HOST=" + parts(0) + "," + "PROXY_URI_BASE=" + uriBase
    val amFilterName = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    actor ! AddWebUIFilter(amFilterName, amFilter, proxyBase)
  }

  private def waitForSparkMaster() {
    logInfo("Waiting for spark driver to be reachable.")
    var driverUp = false
    val hostport = args.userArgs(0)
    val (driverHost, driverPort) = Utils.parseHostPort(hostport)
    while(!driverUp) {
      try {
        val socket = new Socket(driverHost, driverPort)
        socket.close()
        logInfo("Master now available: " + driverHost + ":" + driverPort)
        driverUp = true
      } catch {
        case e: Exception =>
          logError("Failed to connect to driver at " + driverHost + ":" + driverPort)
        Thread.sleep(100)
      }
    }
    sparkConf.set("spark.driver.host",  driverHost)
    sparkConf.set("spark.driver.port",  driverPort.toString)

    val driverUrl = "akka.tcp://%s@%s:%s/user/%s".format(
      SparkEnv.driverActorSystemName,
      driverHost,
      driverPort.toString,
      CoarseGrainedSchedulerBackend.ACTOR_NAME)

    actor = actorSystem.actorOf(Props(new MonitorActor(driverUrl)), name = "YarnAM")
  }


  private def allocateExecutors() {

    // Fixme: should get preferredNodeLocationData from SparkContext, just fake a empty one for now.
    val preferredNodeLocationData: scala.collection.Map[String, scala.collection.Set[SplitInfo]] =
      scala.collection.immutable.Map()

    yarnAllocator = YarnAllocationHandler.newAllocator(yarnConf, resourceManager, appAttemptId,
      args, preferredNodeLocationData, sparkConf)

    logInfo("Allocating " + args.numExecutors + " executors.")
    // Wait until all containers have finished
    // TODO: This is a bit ugly. Can we make it nicer?
    // TODO: Handle container failure
    while ((yarnAllocator.getNumExecutorsRunning < args.numExecutors) && (!driverClosed) &&
        !isFinished) {
      yarnAllocator.allocateContainers(
        math.max(args.numExecutors - yarnAllocator.getNumExecutorsRunning, 0))
      checkNumExecutorsFailed()
      Thread.sleep(100)
    }

    logInfo("All executors have launched.")
  }
  private def checkNumExecutorsFailed() {
    if (yarnAllocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
      finishApplicationMaster(FinalApplicationStatus.FAILED,
        "max number of executor failures reached")
    }
  }

  // TODO: We might want to extend this to allocate more containers in case they die !
  private def launchReporterThread(_sleepTime: Long): Thread = {
    val sleepTime = if (_sleepTime <= 0 ) 0 else _sleepTime

    val t = new Thread {
      override def run() {
        while (!driverClosed && !isFinished) {
          checkNumExecutorsFailed()
          val missingExecutorCount = args.numExecutors - yarnAllocator.getNumExecutorsRunning
          if (missingExecutorCount > 0) {
            logInfo("Allocating " + missingExecutorCount +
              " containers to make up for (potentially ?) lost containers")
            yarnAllocator.allocateContainers(missingExecutorCount)
          } else {
            sendProgress()
          }
          Thread.sleep(sleepTime)
        }
      }
    }
    // setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.start()
    logInfo("Started progress reporter thread - sleep time : " + sleepTime)
    t
  }

  private def sendProgress() {
    logDebug("Sending progress")
    // simulated with an allocate request with no nodes requested ...
    yarnAllocator.allocateContainers(0)
  }

  def finishApplicationMaster(status: FinalApplicationStatus, appMessage: String = "") {
    synchronized {
      if (isFinished) {
        return
      }
      logInfo("Unregistering ApplicationMaster with " + status)
      if (registered) {
        val finishReq = Records.newRecord(classOf[FinishApplicationMasterRequest])
          .asInstanceOf[FinishApplicationMasterRequest]
        finishReq.setAppAttemptId(appAttemptId)
        finishReq.setFinishApplicationStatus(status)
        finishReq.setTrackingUrl(sparkConf.get("spark.yarn.historyServer.address", ""))
        finishReq.setDiagnostics(appMessage)
        resourceManager.finishApplicationMaster(finishReq)
      }
      isFinished = true
    }
  }

}


object ExecutorLauncher {
  def main(argStrings: Array[String]) {
    val args = new ApplicationMasterArguments(argStrings)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      new ExecutorLauncher(args).run()
    }
  }
}
