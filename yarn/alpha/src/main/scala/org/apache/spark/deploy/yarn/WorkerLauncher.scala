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
import akka.actor.Terminated
import org.apache.spark.{SparkConf, SparkContext, Logging}
import org.apache.spark.util.{Utils, AkkaUtils}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.SplitInfo

class WorkerLauncher(args: ApplicationMasterArguments, conf: Configuration, sparkConf: SparkConf)
  extends Logging {

  def this(args: ApplicationMasterArguments, sparkConf: SparkConf) = this(args, new Configuration(), sparkConf)

  def this(args: ApplicationMasterArguments) = this(args, new SparkConf())

  private val rpc: YarnRPC = YarnRPC.create(conf)
  private var resourceManager: AMRMProtocol = _
  private var appAttemptId: ApplicationAttemptId = _
  private var reporterThread: Thread = _
  private val yarnConf: YarnConfiguration = new YarnConfiguration(conf)

  private var yarnAllocator: YarnAllocationHandler = _
  private var driverClosed:Boolean = false

  val actorSystem : ActorSystem = AkkaUtils.createActorSystem("sparkYarnAM", Utils.localHostName, 0,
    conf = sparkConf)._1
  var actor: ActorRef = _

  // This actor just working as a monitor to watch on Driver Actor.
  class MonitorActor(driverUrl: String) extends Actor {

    var driver: ActorSelection = _

    override def preStart() {
      logInfo("Listen to driver: " + driverUrl)
      driver = context.actorSelection(driverUrl)
      // Send a hello message thus the connection is actually established, thus we can monitor Lifecycle Events.
      driver ! "Hello"
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    override def receive = {
      case x: DisassociatedEvent =>
        logInfo(s"Driver terminated or disconnected! Shutting down. $x")
        driverClosed = true
    }
  }

  def run() {

    // Setup the directories so things go to yarn approved directories rather
    // then user specified and /tmp.
    System.setProperty("spark.local.dir", getLocalDirs())

    appAttemptId = getApplicationAttemptId()
    resourceManager = registerWithResourceManager()
    val appMasterResponse: RegisterApplicationMasterResponse = registerApplicationMaster()

    // Compute number of threads for akka
    val minimumMemory = appMasterResponse.getMinimumResourceCapability().getMemory()

    if (minimumMemory > 0) {
      val mem = args.workerMemory + YarnAllocationHandler.MEMORY_OVERHEAD
      val numCore = (mem  / minimumMemory) + (if (0 != (mem % minimumMemory)) 1 else 0)

      if (numCore > 0) {
        // do not override - hits https://issues.apache.org/jira/browse/HADOOP-8406
        // TODO: Uncomment when hadoop is on a version which has this fixed.
        // args.workerCores = numCore
      }
    }

    waitForSparkMaster()

    // Allocate all containers
    allocateWorkers()

    // Launch a progress reporter thread, else app will get killed after expiration (def: 10mins) timeout
    // ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapse.

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
    // Setting this to master host,port - so that the ApplicationReport at client has some sensible info.
    // Users can then monitor stderr/stdout on that node if required.
    appMasterRequest.setHost(Utils.localHostName())
    appMasterRequest.setRpcPort(0)
    // What do we provide here ? Might make sense to expose something sensible later ?
    appMasterRequest.setTrackingUrl("")
    resourceManager.registerApplicationMaster(appMasterRequest)
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

    val driverUrl = "akka.tcp://spark@%s:%s/user/%s".format(
      driverHost, driverPort.toString, CoarseGrainedSchedulerBackend.ACTOR_NAME)

    actor = actorSystem.actorOf(Props(new MonitorActor(driverUrl)), name = "YarnAM")
  }


  private def allocateWorkers() {

    // Fixme: should get preferredNodeLocationData from SparkContext, just fake a empty one for now.
    val preferredNodeLocationData: scala.collection.Map[String, scala.collection.Set[SplitInfo]] =
      scala.collection.immutable.Map()

    yarnAllocator = YarnAllocationHandler.newAllocator(yarnConf, resourceManager, appAttemptId,
      args, preferredNodeLocationData, sparkConf)

    logInfo("Allocating " + args.numWorkers + " workers.")
    // Wait until all containers have finished
    // TODO: This is a bit ugly. Can we make it nicer?
    // TODO: Handle container failure
    while(yarnAllocator.getNumWorkersRunning < args.numWorkers) {
      yarnAllocator.allocateContainers(math.max(args.numWorkers - yarnAllocator.getNumWorkersRunning, 0))
      Thread.sleep(100)
    }

    logInfo("All workers have launched.")

  }

  // TODO: We might want to extend this to allocate more containers in case they die !
  private def launchReporterThread(_sleepTime: Long): Thread = {
    val sleepTime = if (_sleepTime <= 0 ) 0 else _sleepTime

    val t = new Thread {
      override def run() {
        while (!driverClosed) {
          val missingWorkerCount = args.numWorkers - yarnAllocator.getNumWorkersRunning
          if (missingWorkerCount > 0) {
            logInfo("Allocating " + missingWorkerCount + " containers to make up for (potentially ?) lost containers")
            yarnAllocator.allocateContainers(missingWorkerCount)
          }
          else sendProgress()
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

  def finishApplicationMaster(status: FinalApplicationStatus) {

    logInfo("finish ApplicationMaster with " + status)
    val finishReq = Records.newRecord(classOf[FinishApplicationMasterRequest])
      .asInstanceOf[FinishApplicationMasterRequest]
    finishReq.setAppAttemptId(appAttemptId)
    finishReq.setFinishApplicationStatus(status)
    resourceManager.finishApplicationMaster(finishReq)
  }

}


object WorkerLauncher {
  def main(argStrings: Array[String]) {
    val args = new ApplicationMasterArguments(argStrings)
    new WorkerLauncher(args).run()
  }
}
