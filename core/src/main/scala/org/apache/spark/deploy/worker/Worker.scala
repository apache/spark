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

package org.apache.spark.deploy.worker

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.HashMap
import scala.concurrent.duration._

import akka.actor._
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.deploy.{ExecutorDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.{AkkaUtils, Utils}

/**
  * @param masterUrls Each url should look like spark://host:port.
  */
private[spark] class Worker(
    host: String,
    port: Int,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterUrls: Array[String],
    actorSystemName: String,
    actorName: String,
    workDirPath: String = null,
    val conf: SparkConf)
  extends Actor with Logging {
  import context.dispatcher

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For worker and executor IDs

  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4

  val REGISTRATION_TIMEOUT = 20.seconds
  val REGISTRATION_RETRIES = 3

  // Index into masterUrls that we're currently trying to register with.
  var masterIndex = 0

  val masterLock: Object = new Object()
  var master: ActorSelection = null
  var masterAddress: Address = null
  var activeMasterUrl: String = ""
  var activeMasterWebUiUrl : String = ""
  val akkaUrl = "akka.tcp://%s@%s:%s/user/%s".format(actorSystemName, host, port, actorName)
  @volatile var registered = false
  @volatile var connected = false
  val workerId = generateWorkerId()
  var sparkHome: File = null
  var workDir: File = null
  val executors = new HashMap[String, ExecutorRunner]
  val finishedExecutors = new HashMap[String, ExecutorRunner]
  val drivers = new HashMap[String, DriverRunner]
  val finishedDrivers = new HashMap[String, DriverRunner]

  val publicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }
  var webUi: WorkerWebUI = null

  var coresUsed = 0
  var memoryUsed = 0

  val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf)
  val workerSource = new WorkerSource(this)

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  def createWorkDir() {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      workDir.mkdirs()
      if ( !workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert (workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  override def preStart() {
    assert(!registered)
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    sparkHome = new File(Option(System.getenv("SPARK_HOME")).getOrElse("."))
    logInfo("Spark home: " + sparkHome)
    createWorkDir()
    webUi = new WorkerWebUI(this, workDir, Some(webUiPort))
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    webUi.start()
    registerWithMaster()

    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
  }

  def changeMaster(url: String, uiUrl: String) {
    masterLock.synchronized {
      activeMasterUrl = url
      activeMasterWebUiUrl = uiUrl
      master = context.actorSelection(Master.toAkkaUrl(activeMasterUrl))
      masterAddress = activeMasterUrl match {
        case Master.sparkUrlRegex(_host, _port) =>
          Address("akka.tcp", Master.systemName, _host, _port.toInt)
        case x =>
          throw new SparkException("Invalid spark URL: " + x)
      }
      connected = true
    }
  }

  def tryRegisterAllMasters() {
    for (masterUrl <- masterUrls) {
      logInfo("Connecting to master " + masterUrl + "...")
      val actor = context.actorSelection(Master.toAkkaUrl(masterUrl))
      actor ! RegisterWorker(workerId, host, port, cores, memory, webUi.boundPort.get,
        publicAddress)
    }
  }

  def registerWithMaster() {
    tryRegisterAllMasters()

    var retries = 0
    lazy val retryTimer: Cancellable =
      context.system.scheduler.schedule(REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT) {
        retries += 1
        if (registered) {
          retryTimer.cancel()
        } else if (retries >= REGISTRATION_RETRIES) {
          logError("All masters are unresponsive! Giving up.")
          System.exit(1)
        } else {
          tryRegisterAllMasters()
        }
      }
    retryTimer // start timer
  }

  override def receive = {
    case RegisteredWorker(masterUrl, masterWebUiUrl) =>
      logInfo("Successfully registered with master " + masterUrl)
      registered = true
      changeMaster(masterUrl, masterWebUiUrl)
      context.system.scheduler.schedule(0 millis, HEARTBEAT_MILLIS millis, self, SendHeartbeat)

    case SendHeartbeat =>
      masterLock.synchronized {
        if (connected) { master ! Heartbeat(workerId) }
      }

    case MasterChanged(masterUrl, masterWebUiUrl) =>
      logInfo("Master has changed, new master is at " + masterUrl)
      changeMaster(masterUrl, masterWebUiUrl)

      val execs = executors.values.
        map(e => new ExecutorDescription(e.appId, e.execId, e.cores, e.state))
      sender ! WorkerSchedulerStateResponse(workerId, execs.toList, drivers.keys.toSeq)

    case Heartbeat =>
      logInfo(s"Received heartbeat from driver ${sender.path}")

    case RegisterWorkerFailed(message) =>
      if (!registered) {
        logError("Worker registration failed: " + message)
        System.exit(1)
      }

    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_, execSparkHome_) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))
        // TODO (pwendell): We shuld make sparkHome an Option[String] in
        // ApplicationDescription to be more explicit about this.
        val effectiveSparkHome = Option(execSparkHome_).getOrElse(sparkHome.getAbsolutePath)
        val manager = new ExecutorRunner(appId, execId, appDesc, cores_, memory_,
          self, workerId, host, new File(effectiveSparkHome), workDir, akkaUrl, ExecutorState.RUNNING)
        executors(appId + "/" + execId) = manager
        manager.start()
        coresUsed += cores_
        memoryUsed += memory_
        masterLock.synchronized {
          master ! ExecutorStateChanged(appId, execId, manager.state, None, None)
        }
      }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      masterLock.synchronized {
        master ! ExecutorStateChanged(appId, execId, state, message, exitStatus)
      }
      val fullId = appId + "/" + execId
      if (ExecutorState.isFinished(state)) {
        val executor = executors(fullId)
        logInfo("Executor " + fullId + " finished with state " + state +
          message.map(" message " + _).getOrElse("") +
          exitStatus.map(" exitStatus " + _).getOrElse(""))
        executors -= fullId
        finishedExecutors(fullId) = executor
        coresUsed -= executor.cores
        memoryUsed -= executor.memory
      }

    case KillExecutor(masterUrl, appId, execId) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor " + execId)
      } else {
        val fullId = appId + "/" + execId
        executors.get(fullId) match {
          case Some(executor) =>
            logInfo("Asked to kill executor " + fullId)
            executor.kill()
          case None =>
            logInfo("Asked to kill unknown executor " + fullId)
        }
      }

    case LaunchDriver(driverId, driverDesc) => {
      logInfo(s"Asked to launch driver $driverId")
      val driver = new DriverRunner(driverId, workDir, sparkHome, driverDesc, self, akkaUrl)
      drivers(driverId) = driver
      driver.start()

      coresUsed += driverDesc.cores
      memoryUsed += driverDesc.mem
    }

    case KillDriver(driverId) => {
      logInfo(s"Asked to kill driver $driverId")
      drivers.get(driverId) match {
        case Some(runner) =>
          runner.kill()
        case None =>
          logError(s"Asked to kill unknown driver $driverId")
      }
    }

    case DriverStateChanged(driverId, state, exception) => {
      state match {
        case DriverState.ERROR =>
          logWarning(s"Driver $driverId failed with unrecoverable exception: ${exception.get}")
        case DriverState.FINISHED =>
          logInfo(s"Driver $driverId exited successfully")
        case DriverState.KILLED =>
          logInfo(s"Driver $driverId was killed by user")
      }
      masterLock.synchronized {
        master ! DriverStateChanged(driverId, state, exception)
      }
      val driver = drivers.remove(driverId).get
      finishedDrivers(driverId) = driver
      memoryUsed -= driver.driverDesc.mem
      coresUsed -= driver.driverDesc.cores
    }

    case x: DisassociatedEvent if x.remoteAddress == masterAddress =>
      logInfo(s"$x Disassociated !")
      masterDisconnected()

    case RequestWorkerState => {
      sender ! WorkerStateResponse(host, port, workerId, executors.values.toList,
        finishedExecutors.values.toList, drivers.values.toList,
        finishedDrivers.values.toList, activeMasterUrl, cores, memory,
        coresUsed, memoryUsed, activeMasterWebUiUrl)
    }
  }

  def masterDisconnected() {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
  }

  def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(DATE_FORMAT.format(new Date), host, port)
  }

  override def postStop() {
    executors.values.foreach(_.kill())
    drivers.values.foreach(_.kill())
    webUi.stop()
    metricsSystem.stop()
  }
}

private[spark] object Worker {

  def main(argStrings: Array[String]) {
    val args = new WorkerArguments(argStrings)
    val (actorSystem, _) = startSystemAndActor(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir)
    actorSystem.awaitTermination()
  }

  def startSystemAndActor(host: String, port: Int, webUiPort: Int, cores: Int, memory: Int,
      masterUrls: Array[String], workDir: String, workerNumber: Option[Int] = None)
      : (ActorSystem, Int) =
  {
    // The LocalSparkCluster runs multiple local sparkWorkerX actor systems
    val conf = new SparkConf
    val systemName = "sparkWorker" + workerNumber.map(_.toString).getOrElse("")
    val actorName = "Worker"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port,
      conf = conf)
    actorSystem.actorOf(Props(classOf[Worker], host, boundPort, webUiPort, cores, memory,
      masterUrls, systemName, actorName,  workDir, conf), name = actorName)
    (actorSystem, boundPort)
  }

}
