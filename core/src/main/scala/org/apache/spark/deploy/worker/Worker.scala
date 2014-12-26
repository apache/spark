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
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{UUID, Date}

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

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

  // Model retries to connect to the master, after Hadoop's model.
  // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
  // Afterwards, the next 10 attempts are between 30 and 90 seconds.
  // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
  // the same time.
  val INITIAL_REGISTRATION_RETRIES = 6
  val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
  val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  val INITIAL_REGISTRATION_RETRY_INTERVAL = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER)).seconds
  val PROLONGED_REGISTRATION_RETRY_INTERVAL = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER)).seconds

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
  val appDirectories = new HashMap[String, Seq[String]]
  val finishedApps = new HashSet[String]

  // The shuffle service is not actually started unless configured.
  val shuffleService = new StandaloneWorkerShuffleService(conf, securityMgr)

  val publicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }
  var webUi: WorkerWebUI = null

  var coresUsed = 0
  var memoryUsed = 0
  var connectionAttemptCount = 0

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
    shuffleService.startIfEnabled()
    webUi = new WorkerWebUI(this, workDir, webUiPort)
    webUi.bind()
    registerWithMaster()

    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
    // Attach the worker metrics servlet handler to the web ui after the metrics system is started.
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
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
    // Cancel any outstanding re-registration attempts because we found a new master
    registrationRetryTimer.foreach(_.cancel())
    registrationRetryTimer = None
  }

  private def tryRegisterAllMasters() {
    for (masterUrl <- masterUrls) {
      logInfo("Connecting to master " + masterUrl + "...")
      val actor = context.actorSelection(Master.toAkkaUrl(masterUrl))
      actor ! RegisterWorker(workerId, host, port, cores, memory, webUi.boundPort, publicAddress)
    }
  }

  /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * If the re-registration attempt threshold is exceeded, the worker exits with error.
   * Note that for thread-safety this should only be called from the actor.
   */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        registrationRetryTimer.foreach(_.cancel())
        registrationRetryTimer = None
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        /**
         * Re-register with the active master this worker has been communicating with. If there
         * is none, then it means this worker is still bootstrapping and hasn't established a
         * connection with a master yet, in which case we should re-register with all masters.
         *
         * It is important to re-register only with the active master during failures. Otherwise,
         * if the worker unconditionally attempts to re-register with all masters, the following
         * race condition may arise and cause a "duplicate worker" error detailed in SPARK-4592:
         *
         *   (1) Master A fails and Worker attempts to reconnect to all masters
         *   (2) Master B takes over and notifies Worker
         *   (3) Worker responds by registering with Master B
         *   (4) Meanwhile, Worker's previous reconnection attempt reaches Master B,
         *       causing the same Worker to register with Master B twice
         *
         * Instead, if we only register with the known active master, we can assume that the
         * old master must have died because another master has taken over. Note that this is
         * still not safe if the old master recovers within this interval, but this is a much
         * less likely scenario.
         */
        if (master != null) {
          master ! RegisterWorker(
            workerId, host, port, cores, memory, webUi.boundPort, publicAddress)
        } else {
          // We are retrying the initial registration
          tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel())
          registrationRetryTimer = Some {
            context.system.scheduler.schedule(PROLONGED_REGISTRATION_RETRY_INTERVAL,
              PROLONGED_REGISTRATION_RETRY_INTERVAL, self, ReregisterWithMaster)
          }
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  def registerWithMaster() {
    // DisassociatedEvent may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false
        tryRegisterAllMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some {
          context.system.scheduler.schedule(INITIAL_REGISTRATION_RETRY_INTERVAL,
            INITIAL_REGISTRATION_RETRY_INTERVAL, self, ReregisterWithMaster)
        }
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  override def receiveWithLogging = {
    case RegisteredWorker(masterUrl, masterWebUiUrl) =>
      logInfo("Successfully registered with master " + masterUrl)
      registered = true
      changeMaster(masterUrl, masterWebUiUrl)
      context.system.scheduler.schedule(0 millis, HEARTBEAT_MILLIS millis, self, SendHeartbeat)
      if (CLEANUP_ENABLED) {
        logInfo(s"Worker cleanup enabled; old application directories will be deleted in: $workDir")
        context.system.scheduler.schedule(CLEANUP_INTERVAL_MILLIS millis,
          CLEANUP_INTERVAL_MILLIS millis, self, WorkDirCleanup)
      }

    case SendHeartbeat =>
      if (connected) { master ! Heartbeat(workerId) }

    case WorkDirCleanup =>
      // Spin up a separate thread (in a future) to do the dir cleanup; don't tie up worker actor
      val cleanupFuture = concurrent.future {
        val appDirs = workDir.listFiles()
        if (appDirs == null) {
          throw new IOException("ERROR: Failed to list files in " + appDirs)
        }
        appDirs.filter { dir =>
          // the directory is used by an application - check that the application is not running
          // when cleaning up
          val appIdFromDir = dir.getName
          val isAppStillRunning = executors.values.map(_.appId).contains(appIdFromDir)
          dir.isDirectory && !isAppStillRunning &&
          !Utils.doesDirectoryContainAnyNewFiles(dir, APP_DATA_RETENTION_SECS)
        }.foreach { dir =>
          logInfo(s"Removing directory: ${dir.getPath}")
          Utils.deleteRecursively(dir)
        }
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

    case ReconnectWorker(masterUrl) =>
      logInfo(s"Master with url $masterUrl requested this worker to reconnect.")
      registerWithMaster()

    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        try {
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))

          // Create the executor's working directory
          val executorDir = new File(workDir, appId + "/" + execId)
          if (!executorDir.mkdirs()) {
            throw new IOException("Failed to create directory " + executorDir)
          }

          // Create local dirs for the executor. These are passed to the executor via the
          // SPARK_LOCAL_DIRS environment variable, and deleted by the Worker when the
          // application finishes.
          val appLocalDirs = appDirectories.get(appId).getOrElse {
            Utils.getOrCreateLocalRootDirs(conf).map { dir =>
              Utils.createDirectory(dir).getAbsolutePath()
            }.toSeq
          }
          appDirectories(appId) = appLocalDirs

          val manager = new ExecutorRunner(appId, execId, appDesc, cores_, memory_,
            self, workerId, host, sparkHome, executorDir, akkaUrl, conf, appLocalDirs,
            ExecutorState.LOADING)
          executors(appId + "/" + execId) = manager
          manager.start()
          coresUsed += cores_
          memoryUsed += memory_
          master ! ExecutorStateChanged(appId, execId, manager.state, None, None)
        } catch {
          case e: Exception => {
            logError(s"Failed to launch executor $appId/$execId for ${appDesc.name}.", e)
            if (executors.contains(appId + "/" + execId)) {
              executors(appId + "/" + execId).kill()
              executors -= appId + "/" + execId
            }
            master ! ExecutorStateChanged(appId, execId, ExecutorState.FAILED,
              Some(e.toString), None)
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
        maybeCleanupApplication(appId)
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

    case RequestWorkerState =>
      sender ! WorkerStateResponse(host, port, workerId, executors.values.toList,
        finishedExecutors.values.toList, drivers.values.toList,
        finishedDrivers.values.toList, activeMasterUrl, cores, memory,
        coresUsed, memoryUsed, activeMasterWebUiUrl)

    case ReregisterWithMaster =>
      reregisterWithMaster()

    case ApplicationFinished(id) =>
      finishedApps += id
      maybeCleanupApplication(id)
  }

  private def masterDisconnected() {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  private def maybeCleanupApplication(id: String): Unit = {
    val shouldCleanup = finishedApps.contains(id) && !executors.values.exists(_.appId == id)
    if (shouldCleanup) {
      finishedApps -= id
      appDirectories.remove(id).foreach { dirList =>
        logInfo(s"Cleaning up local directories for application $id")
        dirList.foreach { dir =>
          Utils.deleteRecursively(new File(dir))
        }
      }
    }
  }

  def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  override def postStop() {
    metricsSystem.report()
    registrationRetryTimer.foreach(_.cancel())
    executors.values.foreach(_.kill())
    drivers.values.foreach(_.kill())
    shuffleService.stop()
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
      workDir: String,
      workerNumber: Option[Int] = None): (ActorSystem, Int) = {

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
