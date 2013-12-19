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
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}
import akka.actor._
import akka.remote._
import akka.actor.Terminated
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.util.{Utils, AkkaUtils}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.SplitInfo
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest

class WorkerLauncher(args: ApplicationMasterArguments, conf: Configuration) extends Logging {

  def this(args: ApplicationMasterArguments) = this(args, new Configuration())

  private var appAttemptId: ApplicationAttemptId = _
  private var reporterThread: Thread = _
  private val yarnConf: YarnConfiguration = new YarnConfiguration(conf)

  private var yarnAllocator: YarnAllocationHandler = _
  private var driverClosed:Boolean = false

  private var amClient: AMRMClient[ContainerRequest] = _

  val actorSystem : ActorSystem = AkkaUtils.createActorSystem("sparkYarnAM", Utils.localHostName, 0)._1
  var actor: ActorRef = _

  // This actor just working as a monitor to watch on Driver Actor.
  class MonitorActor(driverUrl: String) extends Actor {

    var driver: ActorSelection = null

    override def preStart() {
      logInfo("Listen to driver: " + driverUrl)
      driver = context.actorSelection(driverUrl)
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    override def receive = {
      case x: DisassociatedEvent =>
        logInfo("Driver terminated or disconnected! Shutting down.")
        driverClosed = true
    }
  }

  def run() {

    amClient = AMRMClient.createAMRMClient()
    amClient.init(yarnConf)
    amClient.start()

    appAttemptId = getApplicationAttemptId()
    registerApplicationMaster()

    waitForSparkMaster()

    // Allocate all containers
    allocateWorkers()

    // Launch a progress reporter thread, else app will get killed after expiration (def: 10mins) timeout
    // ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapse.

    val timeoutInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)
    // must be <= timeoutInterval/ 2.
    // On other hand, also ensure that we are reasonably responsive without causing too many requests to RM.
    // so atleast 1 minute or timeoutInterval / 10 - whichever is higher.
    val interval = math.min(timeoutInterval / 2, math.max(timeoutInterval/ 10, 60000L))
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
    val containerIdString = envs.get(ApplicationConstants.Environment.CONTAINER_ID.name())
    val containerId = ConverterUtils.toContainerId(containerIdString)
    val appAttemptId = containerId.getApplicationAttemptId()
    logInfo("ApplicationAttemptId: " + appAttemptId)
    appAttemptId
  }

  private def registerApplicationMaster(): RegisterApplicationMasterResponse = {
    logInfo("Registering the ApplicationMaster")
    // TODO:(Raymond) Find out Spark UI address and fill in here?
    amClient.registerApplicationMaster(Utils.localHostName(), 0, "")
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
    System.setProperty("spark.driver.host", driverHost)
    System.setProperty("spark.driver.port", driverPort.toString)

    val driverUrl = "akka.tcp://spark@%s:%s/user/%s".format(
      driverHost, driverPort.toString, CoarseGrainedSchedulerBackend.ACTOR_NAME)

    actor = actorSystem.actorOf(Props(new MonitorActor(driverUrl)), name = "YarnAM")
  }


  private def allocateWorkers() {

    // Fixme: should get preferredNodeLocationData from SparkContext, just fake a empty one for now.
    val preferredNodeLocationData: scala.collection.Map[String, scala.collection.Set[SplitInfo]] =
      scala.collection.immutable.Map()

    yarnAllocator = YarnAllocationHandler.newAllocator(
      yarnConf,
      amClient,
      appAttemptId,
      args,
      preferredNodeLocationData)

    logInfo("Allocating " + args.numWorkers + " workers.")
    // Wait until all containers have finished
    // TODO: This is a bit ugly. Can we make it nicer?
    // TODO: Handle container failure

    yarnAllocator.addResourceRequests(args.numWorkers)
    while(yarnAllocator.getNumWorkersRunning < args.numWorkers) {
      yarnAllocator.allocateResources()
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
          val missingWorkerCount = args.numWorkers - yarnAllocator.getNumWorkersRunning -
            yarnAllocator.getNumPendingAllocate
          if (missingWorkerCount > 0) {
            logInfo("Allocating %d containers to make up for (potentially) lost containers".
              format(missingWorkerCount))
            yarnAllocator.addResourceRequests(missingWorkerCount)
          }
          sendProgress()
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
    yarnAllocator.allocateResources()
  }

  def finishApplicationMaster(status: FinalApplicationStatus) {
    logInfo("finish ApplicationMaster with " + status)
    amClient.unregisterApplicationMaster(status, "" /* appMessage */, "" /* appTrackingUrl */)
  }

}


object WorkerLauncher {
  def main(argStrings: Array[String]) {
    val args = new ApplicationMasterArguments(argStrings)
    new WorkerLauncher(args).run()
  }
}
