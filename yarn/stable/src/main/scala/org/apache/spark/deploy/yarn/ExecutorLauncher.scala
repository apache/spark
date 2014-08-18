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
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import akka.actor._
import akka.remote._
import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.util.{Utils, AkkaUtils}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.AddWebUIFilter
import org.apache.spark.scheduler.SplitInfo
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.yarn.webapp.util.WebAppUtils

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

  private var appAttemptId: ApplicationAttemptId = _
  private var reporterThread: Thread = _
  private val yarnConf: YarnConfiguration = new YarnConfiguration(conf)

  private var yarnAllocator: YarnAllocationHandler = _
  private var driverClosed: Boolean = false
  private var isFinished: Boolean = false
  private var registered: Boolean = false

  private var amClient: AMRMClient[ContainerRequest] = _

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
      // Send a hello message to establish the connection, after which
      // we can monitor Lifecycle Events.
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

    // Setup the directories so things go to yarn approved directories rather
    // then user specified and /tmp.
    System.setProperty("spark.local.dir", getLocalDirs())

    amClient = AMRMClient.createAMRMClient()
    amClient.init(yarnConf)
    amClient.start()

    appAttemptId = ApplicationMaster.getApplicationAttemptId()
    synchronized {
      if (!isFinished) {
        registerApplicationMaster()
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

  private def registerApplicationMaster(): RegisterApplicationMasterResponse = {
    val appUIAddress = sparkConf.get("spark.driver.appUIAddress", "")
    logInfo(s"Registering the ApplicationMaster with appUIAddress: $appUIAddress")
    amClient.registerApplicationMaster(Utils.localHostName(), 0, appUIAddress)
  }

  // add the yarn amIpFilter that Yarn requires for properly securing the UI
  private def addAmIpFilter() {
    val proxy = WebAppUtils.getProxyHostAndPort(conf)
    val parts = proxy.split(":")
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val uriBase = "http://" + proxy + proxyBase
    val amFilter = "PROXY_HOST=" + parts(0) + "," + "PROXY_URI_BASE=" + uriBase
    val amFilterName = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    actor ! AddWebUIFilter(amFilterName, amFilter, proxyBase)
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

    actor = actorSystem.actorOf(Props(new MonitorActor(driverUrl)), name = "YarnAM")
  }


  private def allocateExecutors() {
    // TODO: should get preferredNodeLocationData from SparkContext, just fake a empty one for now.
    val preferredNodeLocationData: scala.collection.Map[String, scala.collection.Set[SplitInfo]] =
      scala.collection.immutable.Map()

    yarnAllocator = YarnAllocationHandler.newAllocator(
      yarnConf,
      amClient,
      appAttemptId,
      args,
      preferredNodeLocationData,
      sparkConf)

    logInfo("Requesting " + args.numExecutors + " executors.")
    // Wait until all containers have launched
    yarnAllocator.addResourceRequests(args.numExecutors)
    yarnAllocator.allocateResources()
    while ((yarnAllocator.getNumExecutorsRunning < args.numExecutors) && (!driverClosed)) {
      checkNumExecutorsFailed()
      allocateMissingExecutor()
      yarnAllocator.allocateResources()
      Thread.sleep(100)
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
      finishApplicationMaster(FinalApplicationStatus.FAILED,
        "max number of executor failures reached")
    }
  }

  private def launchReporterThread(_sleepTime: Long): Thread = {
    val sleepTime = if (_sleepTime <= 0) 0 else _sleepTime

    val t = new Thread {
      override def run() {
        while (!driverClosed) {
          checkNumExecutorsFailed()
          allocateMissingExecutor()
          logDebug("Sending progress")
          yarnAllocator.allocateResources()
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

  def finishApplicationMaster(status: FinalApplicationStatus, appMessage: String = "") {
    synchronized {
      if (isFinished) {
        return
      }
      logInfo("Unregistering ApplicationMaster with " + status)
      if (registered) {
        val trackingUrl = sparkConf.get("spark.yarn.historyServer.address", "")
        amClient.unregisterApplicationMaster(status, appMessage, trackingUrl)
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
