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

package org.apache.spark.deploy.master

import java.io.FileNotFoundException
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import akka.pattern.ask
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,
  ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, SignalLogger, Utils}

private[master] class Master(
    host: String,
    port: Int,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends Actor with ActorLogReceive with Logging with LeaderElectable {

  import context.dispatcher   // to use Akka's scheduler.schedule()

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")  // For application IDs
  
  private val WORKER_TIMEOUT = conf.getLong("spark.worker.timeout", 60) * 1000
  private val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  private val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
  private val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")

  val workers = new HashSet[WorkerInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  val waitingApps = new ArrayBuffer[ApplicationInfo]
  val apps = new HashSet[ApplicationInfo]

  private val idToWorker = new HashMap[String, WorkerInfo]
  private val addressToWorker = new HashMap[Address, WorkerInfo]

  private val actorToApp = new HashMap[ActorRef, ApplicationInfo]
  private val addressToApp = new HashMap[Address, ApplicationInfo]
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0
  private val appIdToUI = new HashMap[String, SparkUI]

  private val drivers = new HashSet[DriverInfo]
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  private val waitingDrivers = new ArrayBuffer[DriverInfo] 
  private var nextDriverNumber = 0

  Utils.checkHost(host, "Expected hostname")

  private val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  private val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  private val masterSource = new MasterSource(this)

  private val webUi = new MasterWebUI(this, webUiPort)

  private val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  private val masterUrl = "spark://" + host + ":" + port
  private var masterWebUiUrl: String = _

  private var state = RecoveryState.STANDBY

  private var persistenceEngine: PersistenceEngine = _

  private var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: Cancellable = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  private val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  private val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
  private val restServer =
    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      Some(new StandaloneRestServer(host, port, self, masterUrl, conf))
    } else {
      None
    }
  private val restServerBoundPort = restServer.map(_.start())

  override def preStart() {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis, self, CheckForWorkerTimeOut)

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, SerializationExtension(context.system))
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, SerializationExtension(context.system))
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Class.forName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(conf.getClass, Serialization.getClass)
          .newInstance(conf, SerializationExtension(context.system))
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message) // calls postStop()!
    logError("Master actor restarted due to exception", reason)
  }

  override def postStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel()
    }
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    self ! ElectedLeader
  }

  override def revokedLeadership() {
    self ! RevokedLeadership
  }

  override def receiveWithLogging: PartialFunction[Any, Unit] = {
    case ElectedLeader => {
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData()
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = context.system.scheduler.scheduleOnce(WORKER_TIMEOUT millis, self,
          CompleteRecovery)
      }
    }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership => {
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)
    }

    case RegisterWorker(id, workerHost, workerPort, cores, memory, workerUiPort, publicAddress) =>
    {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          sender, workerUiPort, publicAddress)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          sender ! RegisteredWorker(masterUrl, masterWebUiUrl)
          schedule()
        } else {
          val workerAddress = worker.actor.path.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          sender ! RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress)
        }
      }
    }

    case RequestSubmitDriver(description) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"Can only accept driver submissions in ALIVE state. Current state: $state."
        sender ! SubmitDriverResponse(false, None, msg)
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        sender ! SubmitDriverResponse(true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}")
      }
    }

    case RequestKillDriver(driverId) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"Can only kill drivers in ALIVE state. Current state: $state."
        sender ! KillDriverResponse(driverId, success = false, msg)
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self ! DriverStateChanged(driverId, DriverState.KILLED, None)
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.actor ! KillDriver(driverId)
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            sender ! KillDriverResponse(driverId, success = true, msg)
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            sender ! KillDriverResponse(driverId, success = false, msg)
        }
      }
    }

    case RequestDriverStatus(driverId) => {
      (drivers ++ completedDrivers).find(_.id == driverId) match {
        case Some(driver) =>
          sender ! DriverStatusResponse(found = true, Some(driver.state),
            driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception)
        case None =>
          sender ! DriverStatusResponse(found = false, None, None, None, None)
      }
    }

    case RegisterApplication(description) => {
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        val app = createApplication(description, sender)
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        persistenceEngine.addApplication(app)
        sender ! RegisteredApplication(app.id, masterUrl)
        schedule()
      }
    }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) => {
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) => {
          val appInfo = idToApp(appId)
          exec.state = state
          if (state == ExecutorState.RUNNING) { appInfo.resetRetryCount() }
          exec.application.driver ! ExecutorUpdated(execId, state, message, exitStatus)
          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            // If an application has already finished, preserve its
            // state to display its information properly on the UI
            if (!appInfo.isFinished) {
              appInfo.removeExecutor(exec)
            }
            exec.worker.removeExecutor(exec)

            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            if (!normalExit) {
              if (appInfo.incrementRetryCount() < ApplicationState.MAX_NUM_RETRY) {
                schedule()
              } else {
                val execs = appInfo.executors.values
                if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                  logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                    s"${appInfo.retryCount} times; removing it")
                  removeApplication(appInfo, ApplicationState.FAILED)
                }
              }
            }
          }
        }
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }
    }

    case DriverStateChanged(driverId, state, exception) => {
      state match {
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }
    }

    case Heartbeat(workerId) => {
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            sender ! ReconnectWorker(masterUrl)
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }
    }

    case MasterChangeAcknowledged(appId) => {
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    case WorkerSchedulerStateResponse(workerId, executors, driverIds) => {
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.drivers(driverId) = driver
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    case UnregisterApplication(applicationId) =>
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)

    case DisassociatedEvent(_, address, _) => {
      // The disconnected client could've been either a worker or an app; remove whichever it was
      logInfo(s"$address got disassociated, removing it.")
      addressToWorker.get(address).foreach(removeWorker)
      addressToApp.get(address).foreach(finishApplication)
      if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
    }

    case RequestMasterState => {
      sender ! MasterStateResponse(
        host, port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state)
    }

    case CheckForWorkerTimeOut => {
      timeOutDeadWorkers()
    }

    case BoundPortsRequest => {
      sender ! BoundPortsResponse(port, webUi.boundPort, restServerBoundPort)
    }
  }

  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]) {
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        app.driver ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.actor ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  private def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    synchronized {
      if (state != RecoveryState.RECOVERING) { return }
      state = RecoveryState.COMPLETING_RECOVERY
    }

    // Kill off any workers and apps that didn't respond to us.
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Reschedule drivers which were not claimed by any workers
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d)
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Can an app use the given worker? True if the worker has enough memory and we haven't already
   * launched an executor for the app on it (right now the standalone backend doesn't like having
   * two executors on the same worker).
   */
  private def canUse(app: ApplicationInfo, worker: WorkerInfo): Boolean = {
    worker.memoryFree >= app.desc.memoryPerSlave && !worker.hasExecutor(app)
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  private def schedule() {
    if (state != RecoveryState.ALIVE) { return }

    // First schedule drivers, they take strict precedence over applications
    // Randomization helps balance drivers
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0

    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numWorkersVisited = 0
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          launchDriver(worker, driver)
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }

    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    if (spreadOutApps) {
      // Try to spread out each app among all the nodes, until it has all its cores
      for (app <- waitingApps if app.coresLeft > 0) {
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(canUse(app, _)).sortBy(_.coresFree).reverse
        val numUsable = usableWorkers.length
        val assigned = new Array[Int](numUsable) // Number of cores to give on each node
        var toAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
        var pos = 0
        while (toAssign > 0) {
          if (usableWorkers(pos).coresFree - assigned(pos) > 0) {
            toAssign -= 1
            assigned(pos) += 1
          }
          pos = (pos + 1) % numUsable
        }
        // Now that we've decided how many cores to give on each node, let's actually give them
        for (pos <- 0 until numUsable) {
          if (assigned(pos) > 0) {
            val exec = app.addExecutor(usableWorkers(pos), assigned(pos))
            launchExecutor(usableWorkers(pos), exec)
            app.state = ApplicationState.RUNNING
          }
        }
      }
    } else {
      // Pack each app into as few nodes as possible until we've assigned all its cores
      for (worker <- workers if worker.coresFree > 0 && worker.state == WorkerState.ALIVE) {
        for (app <- waitingApps if app.coresLeft > 0) {
          if (canUse(app, worker)) {
            val coresToUse = math.min(worker.coresFree, app.coresLeft)
            if (coresToUse > 0) {
              val exec = app.addExecutor(worker, coresToUse)
              launchExecutor(worker, exec)
              app.state = ApplicationState.RUNNING
            }
          }
        }
      }
    }
  }

  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc) {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)
    worker.actor ! LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory)
    exec.application.driver ! ExecutorAdded(
      exec.id, worker.id, worker.hostPort, exec.cores, exec.memory)
  }

  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.actor.path.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  private def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.actor.path.address
    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      exec.application.driver ! ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None)
      exec.application.removeExecutor(exec)
    }
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    persistenceEngine.removeWorker(worker)
  }

  private def relaunchDriver(driver: DriverInfo) {
    driver.worker = None
    driver.state = DriverState.RELAUNCHING
    waitingDrivers += driver
    schedule()
  }

  private def createApplication(desc: ApplicationDescription, driver: ActorRef): ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new ApplicationInfo(now, newApplicationId(date), desc, date, driver, defaultCores)
  }

  private def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.path.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    idToApp(app.id) = app
    actorToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
  }

  private def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      actorToApp -= app.driver
      addressToApp -= app.driver.path.address
      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach( a => {
          appIdToUI.remove(a.id).foreach { ui => webUi.detachSparkUI(ui) }
          applicationMetricsSystem.removeSource(a.appSource)
        })
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      // If application events are logged, use them to rebuild the UI
      rebuildSparkUI(app)

      for (exec <- app.executors.values) {
        exec.worker.removeExecutor(exec)
        exec.worker.actor ! KillExecutor(masterUrl, exec.application.id, exec.id)
        exec.state = ExecutorState.KILLED
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver ! ApplicationRemoved(state.toString)
      }
      persistenceEngine.removeApplication(app)
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      workers.foreach { w =>
        w.actor ! ApplicationFinished(app.id)
      }
    }
  }

  /**
   * Rebuild a new SparkUI from the given application's event logs.
   * Return whether this is successful.
   */
  private def rebuildSparkUI(app: ApplicationInfo): Boolean = {
    val appName = app.desc.name
    val notFoundBasePath = HistoryServer.UI_PATH_PREFIX + "/not-found"
    try {
      val eventLogDir = app.desc.eventLogDir
        .getOrElse {
          // Event logging is not enabled for this application
          app.desc.appUiUrl = notFoundBasePath
          return false
        }
      
      val eventLogFilePrefix = EventLoggingListener.getLogPath(
          eventLogDir, app.id, app.desc.eventLogCodec)
      val fs = Utils.getHadoopFileSystem(eventLogDir, hadoopConf)
      val inProgressExists = fs.exists(new Path(eventLogFilePrefix + 
          EventLoggingListener.IN_PROGRESS))
      
      if (inProgressExists) {
        // Event logging is enabled for this application, but the application is still in progress
        logWarning(s"Application $appName is still in progress, it may be terminated abnormally.")
      }
      
      val (eventLogFile, status) = if (inProgressExists) {
        (eventLogFilePrefix + EventLoggingListener.IN_PROGRESS, " (in progress)")
      } else {
        (eventLogFilePrefix, " (completed)")
      }
      
      val logInput = EventLoggingListener.openEventLog(new Path(eventLogFile), fs)
      val replayBus = new ReplayListenerBus()
      val ui = SparkUI.createHistoryUI(new SparkConf, replayBus, new SecurityManager(conf),
        appName + status, HistoryServer.UI_PATH_PREFIX + s"/${app.id}")
      val maybeTruncated = eventLogFile.endsWith(EventLoggingListener.IN_PROGRESS)
      try {
        replayBus.replay(logInput, eventLogFile, maybeTruncated)
      } finally {
        logInput.close()
      }
      appIdToUI(app.id) = ui
      webUi.attachSparkUI(ui)
      // Application UI is successfully rebuilt, so link the Master UI to it
      app.desc.appUiUrl = ui.basePath
      true
    } catch {
      case fnf: FileNotFoundException =>
        // Event logging is enabled for this application, but no event logs are found
        val title = s"Application history not found (${app.id})"
        var msg = s"No event logs found for application $appName in ${app.desc.eventLogDir.get}."
        logWarning(msg)
        msg += " Did you specify the correct logging directory?"
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
        false
      case e: Exception =>
        // Relay exception message to application UI page
        val title = s"Application history load error (${app.id})"
        val exception = URLEncoder.encode(Utils.exceptionString(e), "UTF-8")
        var msg = s"Exception in replaying log for application $appName!"
        logError(msg, e)
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&exception=$exception&title=$title"
        false
    }
  }

  /** Generate a new app ID given a app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT/1000))
        removeWorker(worker)
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  private def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    worker.addDriver(driver)
    driver.worker = Some(worker)
    worker.actor ! LaunchDriver(driver.id, driver.desc)
    driver.state = DriverState.RUNNING
  }

  private def removeDriver(
      driverId: String, 
      finalState: DriverState, 
      exception: Option[Exception]) {
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        drivers -= driver
        if (completedDrivers.size >= RETAINED_DRIVERS) {
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        completedDrivers += driver
        persistenceEngine.removeDriver(driver)
        driver.state = finalState
        driver.exception = exception
        driver.worker.foreach(w => w.removeDriver(driver))
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[deploy] object Master extends Logging {
  val systemName = "sparkMaster"
  private val actorName = "Master"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    val (actorSystem, _, _, _) = startSystemAndActor(args.host, args.port, args.webUiPort, conf)
    actorSystem.awaitTermination()
  }

  /**
   * Returns an `akka.tcp://...` URL for the Master actor given a sparkUrl `spark://host:port`.
   *
   * @throws SparkException if the url is invalid
   */
  def toAkkaUrl(sparkUrl: String, protocol: String): String = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    AkkaUtils.address(protocol, systemName, host, port, actorName)
  }

  /**
   * Returns an akka `Address` for the Master actor given a sparkUrl `spark://host:port`.
   *
   * @throws SparkException if the url is invalid
   */
  def toAkkaAddress(sparkUrl: String, protocol: String): Address = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    Address(protocol, systemName, host, port)
  }

  /**
   * Start the Master and return a four tuple of:
   *   (1) The Master actor system
   *   (2) The bound port
   *   (3) The web UI bound port
   *   (4) The REST server bound port, if any
   */
  def startSystemAndActor(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (ActorSystem, Int, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port, conf = conf,
      securityManager = securityMgr)
    val actor = actorSystem.actorOf(
      Props(classOf[Master], host, boundPort, webUiPort, securityMgr, conf), actorName)
    val timeout = AkkaUtils.askTimeout(conf)
    val portsRequest = actor.ask(BoundPortsRequest)(timeout)
    val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
    (actorSystem, boundPort, portsResponse.webUIPort, portsResponse.restPort)
  }
}
