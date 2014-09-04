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
import scala.language.postfixOps

import akka.actor._
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ExecutorDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, SignalLogger, Utils}

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
    val conf: SparkConf,
    val securityMgr: SecurityManager)
  extends Actor with ActorLogReceive with Logging {
  import context.dispatcher

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")  // For worker and executor IDs

  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4

  val REGISTRATION_TIMEOUT = 20.seconds
  val REGISTRATION_RETRIES = 3

  val CLEANUP_ENABLED = conf.getBoolean("spark.worker.cleanup.enabled", false)
  // How often worker will clean up old app folders
  val CLEANUP_INTERVAL_MILLIS = conf.getLong("spark.worker.cleanup.interval", 60 * 30) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  val APP_DATA_RETENTION_SECS = conf.getLong("spark.worker.cleanup.appDataTtl", 7 * 24 * 3600)

  val testing: Boolean = sys.props.contains("spark.testing")
  var master: ActorSelection = null
  var masterAddress: Address = null
  var activeMasterUrl: String = ""
  var activeMasterWebUiUrl : String = ""
  val akkaUrl = "akka.tcp://%s@%s:%s/user/%s".format(actorSystemName, host, port, actorName)
  @volatile var registered = false
  @volatile var connected = false
  val workerId = generateWorkerId()
  val sparkHome =
    if (testing) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.get("SPARK_HOME").getOrElse("."))
    }
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

  val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, securityMgr)
  val workerSource = new WorkerSource(this)

  var registrationRetryTimer: Option[Cancellable] = None

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
    logInfo("Spark home: " + sparkHome)
    createWorkDir()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    webUi = new WorkerWebUI(this, workDir, webUiPort)
    webUi.bind()
    registerWithMaster()

    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
  }

  def changeMaster(url: String, uiUrl: String) {
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

  def tryRegisterAllMasters() {
    for (masterUrl <- masterUrls) {
      logInfo("Connecting to master " + masterUrl + "...")
      val actor = context.actorSelection(Master.toAkkaUrl(masterUrl))
      actor ! RegisterWorker(workerId, host, port, cores, memory, webUi.boundPort, publicAddress)
    }
  }

  def registerWithMaster() {
    tryRegisterAllMasters()
    var retries = 0
    registrationRetryTimer = Some {
      context.system.scheduler.schedule(REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT) {
        Utils.tryOrExit {
          retries += 1
          if (registered) {
            registrationRetryTimer.foreach(_.cancel())
          } else if (retries >= REGISTRATION_RETRIES) {
            logError("All masters are unresponsive! Giving up.")
            System.exit(1)
          } else {
            tryRegisterAllMasters()
          }
        }
      }
    }
  }

  override def receiveWithLogging = {
    case RegisteredWorker(masterUrl, masterWebUiUrl) =>
      logInfo("Successfully registered with master " + masterUrl)
      registered = true
      changeMaster(masterUrl, masterWebUiUrl)
      context.system.scheduler.schedule(0 millis, HEARTBEAT_MILLIS millis, self, SendHeartbeat)
      if (CLEANUP_ENABLED) {
        context.system.scheduler.schedule(CLEANUP_INTERVAL_MILLIS millis,
          CLEANUP_INTERVAL_MILLIS millis, self, WorkDirCleanup)
      }

    case SendHeartbeat =>
      if (connected) { master ! Heartbeat(workerId) }

    case WorkDirCleanup =>
      // Spin up a separate thread (in a future) to do the dir cleanup; don't tie up worker actor
      val cleanupFuture = concurrent.future {
        logInfo("Cleaning up oldest application directories in " + workDir + " ...")
        Utils.findOldFiles(workDir, APP_DATA_RETENTION_SECS)
          .foreach(Utils.deleteRecursively)
      }
      cleanupFuture onFailure {
        case e: Throwable =>
          logError("App dir cleanup failed: " + e.getMessage, e)
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

    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        try {
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))
          val manager = new ExecutorRunner(appId, execId, appDesc, cores_, memory_,
            self, workerId, host, sparkHome, workDir, akkaUrl, conf, ExecutorState.RUNNING)
          executors(appId + "/" + execId) = manager
          manager.start()
          coresUsed += cores_
          memoryUsed += memory_
          master ! ExecutorStateChanged(appId, execId, manager.state, None, None)
        } catch {
          case e: Exception => {
            logError("Failed to launch executor %s/%d for %s".format(appId, execId, appDesc.name))
            if (executors.contains(appId + "/" + execId)) {
              executors(appId + "/" + execId).kill()
              executors -= appId + "/" + execId
            }
            master ! ExecutorStateChanged(appId, execId, ExecutorState.FAILED, None, None)
          }
        }
      }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      master ! ExecutorStateChanged(appId, execId, state, message, exitStatus)
      val fullId = appId + "/" + execId
      if (ExecutorState.isFinished(state)) {
        executors.get(fullId) match {
          case Some(executor) =>
            logInfo("Executor " + fullId + " finished with state " + state +
              message.map(" message " + _).getOrElse("") +
              exitStatus.map(" exitStatus " + _).getOrElse(""))
            executors -= fullId
            finishedExecutors(fullId) = executor
            coresUsed -= executor.cores
            memoryUsed -= executor.memory
          case None =>
            logInfo("Unknown Executor " + fullId + " finished with state " + state +
              message.map(" message " + _).getOrElse("") +
              exitStatus.map(" exitStatus " + _).getOrElse(""))
        }
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
      val driver = new DriverRunner(conf, driverId, workDir, sparkHome, driverDesc, self, akkaUrl)
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
        case DriverState.FAILED =>
          logWarning(s"Driver $driverId exited with failure")
        case DriverState.FINISHED =>
          logInfo(s"Driver $driverId exited successfully")
        case DriverState.KILLED =>
          logInfo(s"Driver $driverId was killed by user")
        case _ =>
          logDebug(s"Driver $driverId changed state to $state")
      }
      master ! DriverStateChanged(driverId, state, exception)
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
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  override def postStop() {
    metricsSystem.report()
    registrationRetryTimer.foreach(_.cancel())
    executors.values.foreach(_.kill())
    drivers.values.foreach(_.kill())
    webUi.stop()
    metricsSystem.stop()
  }
}

private[spark] object Worker extends Logging {
  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new SparkConf
    val args = new WorkerArguments(argStrings, conf)
    val (actorSystem, _) = startSystemAndActor(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir)
    actorSystem.awaitTermination()
  }

  def startSystemAndActor(
      host: String,
      port: Int,
      webUiPort: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],
      workDir: String, workerNumber: Option[Int] = None): (ActorSystem, Int) = {

    // The LocalSparkCluster runs multiple local sparkWorkerX actor systems
    val conf = new SparkConf
    val systemName = "sparkWorker" + workerNumber.map(_.toString).getOrElse("")
    val actorName = "Worker"
    val securityMgr = new SecurityManager(conf)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port,
      conf = conf, securityManager = securityMgr)
    actorSystem.actorOf(Props(classOf[Worker], host, boundPort, webUiPort, cores, memory,
      masterUrls, systemName, actorName,  workDir, conf, securityMgr), name = actorName)
    (actorSystem, boundPort)
  }

}
