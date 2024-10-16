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

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.internal.config.Deploy.WorkerSelectionPolicy._
import org.apache.spark.internal.config.UI._
import org.apache.spark.internal.config.Worker._
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.resource.{ResourceInformation, ResourceProfile, ResourceRequirement, ResourceUtils}
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.util.{SparkUncaughtExceptionHandler, ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  private val driverIdPattern = conf.get(DRIVER_ID_PATTERN)
  private val appIdPattern = conf.get(APP_ID_PATTERN)
  private val workerTimeoutMs = conf.get(WORKER_TIMEOUT) * 1000
  private val retainedApplications = conf.get(RETAINED_APPLICATIONS)
  private val retainedDrivers = conf.get(RETAINED_DRIVERS)
  private val maxDrivers = conf.get(MAX_DRIVERS)
  private val reaperIterations = conf.get(REAPER_ITERATIONS)
  private val recoveryTimeoutMs =
    conf.get(RECOVERY_TIMEOUT).map(_ * 1000).getOrElse(workerTimeoutMs)
  private val recoveryMode = conf.get(RECOVERY_MODE)
  private val maxExecutorRetries = conf.get(MAX_EXECUTOR_RETRIES)

  val workers = new HashSet[WorkerInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  private val waitingApps = new ArrayBuffer[ApplicationInfo]
  val apps = new HashSet[ApplicationInfo]

  // Visible for testing
  private[master] val idToWorker = new HashMap[String, WorkerInfo]
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]

  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0
  private val moduloAppNumber = conf.get(APP_NUMBER_MODULO).getOrElse(0)

  private val drivers = new HashSet[DriverInfo]
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  private val waitingDrivers = new ArrayBuffer[DriverInfo]
  private var nextDriverNumber = 0

  Utils.checkHost(address.host)

  private val masterMetricsSystem =
    MetricsSystem.createMetricsSystem(MetricsSystemInstances.MASTER, conf)
  private val applicationMetricsSystem =
    MetricsSystem.createMetricsSystem(MetricsSystemInstances.APPLICATIONS, conf)
  private val masterSource = new MasterSource(this)

  // After onStart, webUi will be set
  private var webUi: MasterWebUI = null

  private val masterUrl = address.toSparkURL
  private var masterWebUiUrl: String = _

  private[master] var state = RecoveryState.STANDBY

  private[master] var persistenceEngine: PersistenceEngine = _

  private var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: ScheduledFuture[_] = _

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  private val spreadOutDrivers = conf.get(SPREAD_OUT_DRIVERS)

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  private val spreadOutApps = conf.get(SPREAD_OUT_APPS)
  private val workerSelectionPolicy =
    WorkerSelectionPolicy.withName(conf.get(WORKER_SELECTION_POLICY))

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  private val defaultCores = conf.get(DEFAULT_CORES)
  val reverseProxy = conf.get(UI_REVERSE_PROXY)
  val historyServerUrl = conf.get(MASTER_UI_HISTORY_SERVER_URL)
  val useAppNameAsAppId = conf.get(MASTER_USE_APP_NAME_AS_APP_ID)

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.get(MASTER_REST_SERVER_ENABLED)
  private var restServer: Option[StandaloneRestServer] = None
  private var restServerBoundPort: Option[Int] = None

  {
    val authKey = SecurityManager.SPARK_AUTH_SECRET_CONF
    require(conf.getOption(authKey).isEmpty || !restServerEnabled,
      s"The RestSubmissionServer does not support authentication via ${authKey}.  Either turn " +
        "off the RestSubmissionServer with spark.master.rest.enabled=false, or do not use " +
        "authentication.")
  }

  override def onStart(): Unit = {
    logInfo(log"Starting Spark master at ${MDC(LogKeys.MASTER_URL, masterUrl)}")
    logInfo(log"Running Spark version" +
      log" ${MDC(LogKeys.SPARK_VERSION, org.apache.spark.SPARK_VERSION)}")
    webUi = new MasterWebUI(this, webUiPort)
    webUi.bind()
    masterWebUiUrl = webUi.webUrl
    if (reverseProxy) {
      val uiReverseProxyUrl = conf.get(UI_REVERSE_PROXY_URL).map(_.stripSuffix("/"))
      if (uiReverseProxyUrl.nonEmpty) {
        System.setProperty("spark.ui.proxyBase", uiReverseProxyUrl.get)
        // If the master URL has a path component, it must end with a slash.
        // Otherwise the browser generates incorrect relative links
        masterWebUiUrl = uiReverseProxyUrl.get + "/"
      }
      webUi.addProxy()
      logInfo(log"Spark Master is acting as a reverse proxy. Master, Workers and " +
       log"Applications UIs are available at ${MDC(LogKeys.WEB_URL, masterWebUiUrl)}")
    }
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(
      () => Utils.tryLogNonFatalError { self.send(CheckForWorkerTimeOut) },
      0, workerTimeoutMs, TimeUnit.MILLISECONDS)

    if (restServerEnabled) {
      val port = conf.get(MASTER_REST_SERVER_PORT)
      val host = conf.get(MASTER_REST_SERVER_HOST).getOrElse(address.host)
      restServer = Some(new StandaloneRestServer(host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = recoveryMode match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "ROCKSDB" =>
        val rdbFactory =
          new RocksDBRecoveryModeFactory(conf, serializer)
        (rdbFactory.createPersistenceEngine(), rdbFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get(RECOVERY_MODE_FACTORY))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def onStop(): Unit = {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader(): Unit = {
    self.send(ElectedLeader)
  }

  override def revokedLeadership(): Unit = {
    self.send(RevokedLeadership)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case ElectedLeader =>
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo(log"I have been elected leader! New state: ${MDC(LogKeys.RECOVERY_STATE, state)}")
      if (state == RecoveryState.RECOVERING) {
        if (beginRecovery(storedApps, storedDrivers, storedWorkers)) {
          recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              self.send(CompleteRecovery)
            }
          }, recoveryTimeoutMs, TimeUnit.MILLISECONDS)
        }
      }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership =>
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)

    case WorkerDecommissioning(id, workerRef) =>
      if (state == RecoveryState.STANDBY) {
        workerRef.send(MasterInStandby)
      } else {
        // We use foreach since get gives us an option and we can skip the failures.
        idToWorker.get(id).foreach(decommissionWorker)
      }

    case DecommissionWorkers(ids) =>
      // The caller has already checked the state when handling DecommissionWorkersOnHosts,
      // so it should not be the STANDBY
      assert(state != RecoveryState.STANDBY)
      ids.foreach ( id =>
        // We use foreach since get gives us an option and we can skip the failures.
        idToWorker.get(id).foreach { w =>
          decommissionWorker(w)
          // Also send a message to the worker node to notify.
          w.endpoint.send(DecommissionWorker)
        }
      )

    case RegisterWorker(
      id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl,
      masterAddress, resources) =>
      handleRegisterWorker(id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl,
        masterAddress, resources)

    case RegisterApplication(description, driver) =>
      // TODO Prevent repeated registrations from some driver
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo(log"Registering app ${MDC(LogKeys.APP_NAME, description.name)}")
        val app = createApplication(description, driver)
        registerApplication(app)
        logInfo(log"Registered app ${MDC(LogKeys.APP_NAME, description.name)} with" +
          log" ID ${MDC(LogKeys.APP_ID, app.id)}")
        persistenceEngine.addApplication(app)
        driver.send(RegisteredApplication(app.id, self))
        schedule()
      }

    case DriverStateChanged(driverId, state, exception) =>
      state match {
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }

    case Heartbeat(workerId, worker) =>
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(log"Got heartbeat from unregistered worker " +
              log"${MDC(LogKeys.WORKER_ID, workerId)}. Asking it to re-register.")
            worker.send(ReconnectWorker(masterUrl))
          } else {
            logWarning(log"Got heartbeat from unregistered worker " +
              log"${MDC(LogKeys.WORKER_ID, workerId)}. " +
              log"This worker was never registered, so ignoring the heartbeat.")
          }
      }

    case MasterChangeAcknowledged(appId) =>
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo(log"Application has been re-registered: ${MDC(LogKeys.APP_ID, appId)}")
          app.state = ApplicationState.WAITING
        case None =>
          logWarning(log"Master change ack from unknown app: ${MDC(LogKeys.APP_ID, appId)}")
      }

      if (canCompleteRecovery) { completeRecovery() }

    case WorkerSchedulerStateResponse(workerId, execResponses, driverResponses) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo(log"Worker has been re-registered: ${MDC(LogKeys.WORKER_ID, workerId)}")
          worker.state = WorkerState.ALIVE

          val validExecutors = execResponses.filter(
            exec => idToApp.get(exec.desc.appId).isDefined)
          for (exec <- validExecutors) {
            val (execDesc, execResources) = (exec.desc, exec.resources)
            val app = idToApp(execDesc.appId)
            val execInfo = app.addExecutor(worker, execDesc.cores,
              execDesc.memoryMb, execResources, execDesc.rpId, Some(execDesc.execId))
            worker.addExecutor(execInfo)
            worker.recoverResources(execResources)
            execInfo.copyState(execDesc)
          }

          for (driver <- driverResponses) {
            val (driverId, driverResource) = (driver.driverId, driver.resources)
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              driver.withResources(driverResource)
              worker.recoverResources(driverResource)
              worker.addDriver(driver)
            }
          }
        case None =>
          logWarning(log"Scheduler state from unknown worker: ${MDC(LogKeys.WORKER_ID, workerId)}")
      }

      if (canCompleteRecovery) { completeRecovery() }

    case WorkerLatestState(workerId, executors, driverIds) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          for (exec <- executors) {
            val executorMatches = worker.executors.exists {
              case (_, e) => e.application.id == exec.appId && e.id == exec.execId
            }
            if (!executorMatches) {
              // master doesn't recognize this executor. So just tell worker to kill it.
              worker.endpoint.send(KillExecutor(masterUrl, exec.appId, exec.execId))
            }
          }

          for (driverId <- driverIds) {
            val driverMatches = worker.drivers.exists { case (id, _) => id == driverId }
            if (!driverMatches) {
              // master doesn't recognize this driver. So just tell worker to kill it.
              worker.endpoint.send(KillDriver(driverId))
            }
          }
        case None =>
          logWarning(log"Worker state from unknown worker: ${MDC(LogKeys.WORKER_ID, workerId)}")
      }

    case UnregisterApplication(applicationId) =>
      logInfo(log"Received unregister request from application" +
        log" ${MDC(LogKeys.APP_ID, applicationId)}")
      idToApp.get(applicationId).foreach(finishApplication)

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestSubmitDriver(description) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitDriverResponse(self, false, None, msg))
      } else {
        logInfo(log"Driver submitted ${MDC(LogKeys.CLASS_NAME, description.command.mainClass)}")
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }

    case RequestKillDriver(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo(log"Asked to kill driver ${MDC(LogKeys.DRIVER_ID, driverId)}")
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self.send(DriverStateChanged(driverId, DriverState.KILLED, None))
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.endpoint.send(KillDriver(driverId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = log"Kill request for ${MDC(LogKeys.DRIVER_ID, driverId)} submitted"
            logInfo(msg)
            context.reply(KillDriverResponse(self, driverId, success = true, msg.message))
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(log"Driver ${MDC(LogKeys.DRIVER_ID, driverId)} " +
              log"has already finished or does not exist")
            context.reply(KillDriverResponse(self, driverId, success = false, msg))
        }
      }

    case RequestKillAllDrivers =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillAllDriversResponse(self, success = false, msg))
      } else {
        logInfo("Asked to kill all drivers")
        drivers.foreach { d =>
          val driverId = d.id
          if (waitingDrivers.contains(d)) {
            waitingDrivers -= d
            self.send(DriverStateChanged(driverId, DriverState.KILLED, None))
          } else {
            // We just notify the worker to kill the driver here. The final bookkeeping occurs
            // on the return path when the worker submits a state change back to the master
            // to notify it that the driver was successfully killed.
            d.worker.foreach { w =>
              w.endpoint.send(KillDriver(driverId))
            }
          }
          logInfo(log"Kill request for ${MDC(LogKeys.DRIVER_ID, driverId)} submitted")
        }
        context.reply(KillAllDriversResponse(self, true, "Kill request for all drivers submitted"))
      }

    case RequestClearCompletedDriversAndApps =>
      val numDrivers = completedDrivers.length
      val numApps = completedApps.length
      logInfo(log"Asked to clear ${MDC(LogKeys.NUM_DRIVERS, numDrivers)} completed drivers and" +
        log" ${MDC(LogKeys.NUM_APPS, numApps)} completed apps.")
      completedDrivers.clear()
      completedApps.clear()
      context.reply(true)

    case RequestDriverStatus(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          DriverStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (drivers ++ completedDrivers).find(_.id == driverId) match {
          case Some(driver) =>
            context.reply(DriverStatusResponse(found = true, Some(driver.state),
              driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception))
          case None =>
            context.reply(DriverStatusResponse(found = false, None, None, None, None))
        }
      }

    case RequestMasterState =>
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state))

    case RequestReadyz =>
      context.reply(state != RecoveryState.STANDBY)

    case BoundPortsRequest =>
      context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))

    case RequestExecutors(appId, resourceProfileToTotalExecs: Map[ResourceProfile, Int]) =>
      context.reply(handleRequestExecutors(appId, resourceProfileToTotalExecs))

    case KillExecutors(appId, executorIds) =>
      val formattedExecutorIds = formatExecutorIds(executorIds)
      context.reply(handleKillExecutors(appId, formattedExecutorIds))

    case DecommissionWorkersOnHosts(hostnames) =>
      if (state != RecoveryState.STANDBY) {
        context.reply(decommissionWorkersOnHosts(hostnames))
      } else {
        context.reply(0)
      }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) =>
          val appInfo = idToApp(appId)
          val oldState = exec.state
          exec.state = state

          if (state == ExecutorState.RUNNING) {
            assert(oldState == ExecutorState.LAUNCHING,
              s"executor $execId state transfer from $oldState to RUNNING is illegal")
            appInfo.resetRetryCount()
          }

          exec.application.driver.send(ExecutorUpdated(execId, state, message, exitStatus, None))

          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            logInfo(log"Removing executor ${MDC(LogKeys.EXECUTOR_ID, exec.fullId)}" +
              log" because it is ${MDC(LogKeys.EXECUTOR_STATE, state)}")
            // If an application has already finished, preserve its
            // state to display its information properly on the UI
            if (!appInfo.isFinished) {
              appInfo.removeExecutor(exec)
            }
            exec.worker.removeExecutor(exec)

            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            // Important note: this code path is not exercised by tests, so be very careful when
            // changing this `if` condition.
            // We also don't count failures from decommissioned workers since they are "expected."
            if (!normalExit
              && oldState != ExecutorState.DECOMMISSIONED
              && appInfo.incrementRetryCount() >= maxExecutorRetries
              && maxExecutorRetries >= 0) { // < 0 disables this application-killing path
              val execs = appInfo.executors.values
              if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                logError(log"Application ${MDC(LogKeys.APP_DESC, appInfo.desc.name)} " +
                  log"with ID ${MDC(LogKeys.APP_ID, appInfo.id)} " +
                  log"failed ${MDC(LogKeys.NUM_RETRY, appInfo.retryCount)} times; removing it")
                removeApplication(appInfo, ApplicationState.FAILED)
              }
            }
          }
          schedule()
        case None =>
          logWarning(log"Got status update for unknown executor ${MDC(LogKeys.APP_ID, appId)}" +
            log"/${MDC(LogKeys.EXECUTOR_ID, execId)}")
      }
      context.reply(true)
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(log"${MDC(LogKeys.RPC_ADDRESS, address)} got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker(_, s"${address} got disassociated"))
    addressToApp.get(address).foreach(finishApplication)
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  private var recoveryStartTimeMs = 0L

  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]): Boolean = {
    recoveryStartTimeMs = System.currentTimeMillis()
    for (app <- storedApps) {
      logInfo(log"Trying to recover app: ${MDC(LogKeys.APP_ID, app.id)}")
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        app.driver.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo(log"App ${MDC(LogKeys.APP_ID, app.id)}" +
          log" had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo(log"Trying to recover worker: ${MDC(LogKeys.WORKER_ID, worker.id)}")
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo(log"Worker ${MDC(LogKeys.WORKER_ID, worker.id)}" +
          log" had exception on reconnect")
      }
    }

    // In case of zero workers and apps, we can complete recovery.
    if (canCompleteRecovery) {
      completeRecovery()
      false
    } else {
      true
    }
  }

  private def completeRecovery(): Unit = {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    if (state != RecoveryState.RECOVERING) { return }
    state = RecoveryState.COMPLETING_RECOVERY

    // Kill off any workers and apps that didn't respond to us.
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(
      removeWorker(_, "Not responding for recovery"))
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Update the state of recovered apps to RUNNING
    apps.filter(_.state == ApplicationState.WAITING).foreach(_.state = ApplicationState.RUNNING)

    // Reschedule drivers which were not claimed by any workers
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(log"Driver ${MDC(LogKeys.DRIVER_ID, d.id)} " +
        log"was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(log"Re-launching ${MDC(LogKeys.DRIVER_ID, d.id)}")
        relaunchDriver(d)
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(log"Did not re-launch " +
          log"${MDC(LogKeys.DRIVER_ID, d.id)} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    schedule()
    val timeTakenMs = System.currentTimeMillis() - recoveryStartTimeMs
    logInfo(log"Recovery complete in ${MDC(LogKeys.TOTAL_TIME, timeTakenMs)} ms" +
      log" - resuming operations!")
  }

  private[master] def handleRegisterWorker(
      id: String,
      workerHost: String,
      workerPort: Int,
      workerRef: RpcEndpointRef,
      cores: Int,
      memory: Int,
      workerWebUiUrl: String,
      masterAddress: RpcAddress,
      resources: Map[String, ResourceInformation]): Unit = {
    logInfo(log"Registering worker" +
      log" ${MDC(LogKeys.WORKER_HOST, workerHost)}:${MDC(LogKeys.WORKER_PORT, workerPort)}" +
      log" with ${MDC(LogKeys.NUM_CORES, cores)} cores," +
      log" ${MDC(LogKeys.MEMORY_SIZE, Utils.megabytesToString(memory))} RAM")
    if (state == RecoveryState.STANDBY) {
      workerRef.send(MasterInStandby)
    } else if (idToWorker.contains(id)) {
      if (idToWorker(id).state == WorkerState.UNKNOWN) {
        logInfo(log"Worker has been re-registered: ${MDC(LogKeys.WORKER_ID, id)}")
        idToWorker(id).state = WorkerState.ALIVE
      }
      workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress, true))
    } else {
      val workerResources =
        resources.map(r => r._1 -> WorkerResourceInfo(r._1, r._2.addresses.toImmutableArraySeq))
      val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
        workerRef, workerWebUiUrl, workerResources)
      if (registerWorker(worker)) {
        persistenceEngine.addWorker(worker)
        workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress, false))
        schedule()
      } else {
        val workerAddress = worker.endpoint.address
        logWarning(log"Worker registration failed. Attempted to re-register worker at same " +
          log"address: ${MDC(LogKeys.WORKER_URL, workerAddress)}")
        workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
          + workerAddress))
      }
    }
  }

  /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor per application may be launched on each
   * worker during one single schedule iteration.
   * Note that when `spark.executor.cores` is not set, we may still launch multiple executors from
   * the same application on the same worker. Consider appA and appB both have one executor running
   * on worker1, and appA.coresLeft > 0, then appB is finished and release all its cores on worker1,
   * thus for the next schedule iteration, appA launches a new executor that grabs all the free
   * cores on worker1, therefore we get multiple executors from appA running on worker1.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   */
  private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      rpId: Int,
      resourceDesc: ExecutorResourceDescription,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = resourceDesc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = resourceDesc.memoryMbPerExecutor
    val resourceReqsPerExecutor = resourceDesc.customResourcesPerExecutor
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    def canLaunchExecutorForApp(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor
      val assignedExecutorNum = assignedExecutors(pos)

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutorNum == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutorNum * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val assignedResources = resourceReqsPerExecutor.map {
          req => req.resourceName -> req.amount * assignedExecutorNum
        }.toMap
        val resourcesFree = usableWorkers(pos).resourcesAmountFree.map {
          case (rName, free) => rName -> (free - assignedResources.getOrElse(rName, 0))
        }
        val enoughResources = ResourceUtils.resourcesMeetRequirements(
          resourcesFree, resourceReqsPerExecutor)
        val executorNum = app.getOrUpdateExecutorsForRPId(rpId).size
        val executorLimit = app.getTargetExecutorNumForRPId(rpId)
        val underLimit = assignedExecutors.sum + executorNum < executorLimit
        keepScheduling && enoughCores && enoughMemory && enoughResources && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutorForApp)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutorForApp(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutorForApp)
    }
    assignedCores
  }

  /**
   * Schedule and launch executors on workers
   */
  private def startExecutorsOnWorkers(): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc. And for each app, we will schedule base on
    // resource profiles also with a simple FIFO scheduler, resource profile with smaller id
    // first.
    for (app <- waitingApps) {
      for (rpId <- app.getRequestedRPIds()) {
        logInfo(log"Start scheduling for app ${MDC(LogKeys.APP_ID, app.id)} with" +
          log" rpId: ${MDC(LogKeys.RESOURCE_PROFILE_ID, rpId)}")
        val resourceDesc = app.getResourceDescriptionForRpId(rpId)
        val coresPerExecutor = resourceDesc.coresPerExecutor.getOrElse(1)

        // If the cores left is less than the coresPerExecutor,the cores left will not be allocated
        if (app.coresLeft >= coresPerExecutor) {
          // Filter out workers that don't have enough resources to launch an executor
          val aliveWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
            .filter(canLaunchExecutor(_, resourceDesc))
          val usableWorkers = workerSelectionPolicy match {
            case CORES_FREE_ASC => aliveWorkers.sortBy(w => (w.coresFree, w.id))
            case CORES_FREE_DESC => aliveWorkers.sortBy(w => (w.coresFree, w.id)).reverse
            case MEMORY_FREE_ASC => aliveWorkers.sortBy(w => (w.memoryFree, w.id))
            case MEMORY_FREE_DESC => aliveWorkers.sortBy(w => (w.memoryFree, w.id)).reverse
            case WorkerSelectionPolicy.WORKER_ID => aliveWorkers.sortBy(_.id)
          }
          val appMayHang = waitingApps.length == 1 &&
            waitingApps.head.executors.isEmpty && usableWorkers.isEmpty
          if (appMayHang) {
            logWarning(log"App ${MDC(LogKeys.APP_ID, app.id)} requires more resource " +
              log"than any of Workers could have.")
          }
          val assignedCores =
            scheduleExecutorsOnWorkers(app, rpId, resourceDesc, usableWorkers, spreadOutApps)

          // Now that we've decided how many cores to allocate on each worker, let's allocate them
          for (pos <- usableWorkers.indices if assignedCores(pos) > 0) {
            allocateWorkerResourceToExecutors(
              app,
              assignedCores(pos),
              resourceDesc,
              usableWorkers(pos),
              rpId)
          }
        }
      }
    }
  }

  /**
   * Allocate a worker's resources to one or more executors.
   * @param app the info of the application which the executors belong to
   * @param assignedCores number of cores on this worker for this application
   * @param resourceDesc resources requested for the executor
   * @param worker the worker info
   * @param rpId resource profile id for the executor
   */
  private def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      resourceDesc: ExecutorResourceDescription,
      worker: WorkerInfo,
      rpId: Int): Unit = {
    val coresPerExecutor = resourceDesc.coresPerExecutor
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    for (i <- 1 to numExecutors) {
      val allocated = worker.acquireResources(resourceDesc.customResourcesPerExecutor)
      val exec = app.addExecutor(
        worker, coresToAssign, resourceDesc.memoryMbPerExecutor, allocated, rpId)
      launchExecutor(worker, exec)
      app.state = ApplicationState.RUNNING
    }
  }

  private def canLaunch(
      worker: WorkerInfo,
      memoryReq: Int,
      coresReq: Int,
      resourceRequirements: Seq[ResourceRequirement])
    : Boolean = {
    val enoughMem = worker.memoryFree >= memoryReq
    val enoughCores = worker.coresFree >= coresReq
    val enoughResources = ResourceUtils.resourcesMeetRequirements(
      worker.resourcesAmountFree, resourceRequirements)
    enoughMem && enoughCores && enoughResources
  }

  /**
   * @return whether the worker could launch the driver represented by DriverDescription
   */
  private def canLaunchDriver(worker: WorkerInfo, desc: DriverDescription): Boolean = {
    canLaunch(worker, desc.mem, desc.cores, desc.resourceReqs)
  }

  /**
   * @return whether the worker could launch the executor according to application's requirement
   */
  private def canLaunchExecutor(
      worker: WorkerInfo,
      resourceDesc: ExecutorResourceDescription): Boolean = {
    canLaunch(
      worker,
      resourceDesc.memoryMbPerExecutor,
      resourceDesc.coresPerExecutor.getOrElse(1),
      resourceDesc.customResourcesPerExecutor)
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Drivers take strict precedence over executors
    if (spreadOutDrivers) {
      val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
      val numWorkersAlive = shuffledAliveWorkers.size
      var curPos = 0
      for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
        // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
        // start from the last worker that was assigned a driver, and continue onwards until we have
        // explored all alive workers.
        var launched = (drivers.size - waitingDrivers.size) >= maxDrivers
        var isClusterIdle = !launched
        var numWorkersVisited = 0
        while (numWorkersVisited < numWorkersAlive && !launched) {
          val worker = shuffledAliveWorkers(curPos)
          isClusterIdle = worker.drivers.isEmpty && worker.executors.isEmpty
          numWorkersVisited += 1
          if (canLaunchDriver(worker, driver.desc)) {
            val allocated = worker.acquireResources(driver.desc.resourceReqs)
            driver.withResources(allocated)
            launchDriver(worker, driver)
            waitingDrivers -= driver
            launched = true
          }
          curPos = (curPos + 1) % numWorkersAlive
        }
        if (!launched && isClusterIdle) {
          logWarning(log"Driver ${MDC(LogKeys.DRIVER_ID, driver.id)} " +
            log"requires more resource than any of Workers could have.")
        }
      }
    } else {
      // Sort by worker ID as a way to be deterministic
      val aliveWorkers = workers.toSeq.filter(_.state == WorkerState.ALIVE).sortBy(_.id)
      for (driver <- waitingDrivers.toList) {
        if ((drivers.size - waitingDrivers.size) < maxDrivers) {
          aliveWorkers.find(canLaunchDriver(_, driver.desc)) match {
            case Some(worker) =>
              driver.withResources(worker.acquireResources(driver.desc.resourceReqs))
              launchDriver(worker, driver)
              waitingDrivers -= driver
            case _ =>
              logWarning(log"Driver ${MDC(LogKeys.DRIVER_ID, driver.id)} " +
                log"requires more resource than any of Workers could have.")
          }
        }
      }
    }
    startExecutorsOnWorkers()
  }

  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo(log"Launching executor ${MDC(LogKeys.EXECUTOR_ID, exec.fullId)}" +
      log" on worker ${MDC(LogKeys.WORKER_ID, worker.id)}")
    worker.addExecutor(exec)
    worker.endpoint.send(LaunchExecutor(masterUrl, exec.application.id, exec.id,
      exec.rpId, exec.application.desc, exec.cores, exec.memory, exec.resources))
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }

  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker, "Worker replaced by a new worker with same address")
      } else {
        logInfo(log"Attempted to re-register worker at same address:" +
          log" ${MDC(LogKeys.RPC_ADDRESS, workerAddress)}")
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  /**
   * Decommission all workers that are active on any of the given hostnames. The decommissioning is
   * asynchronously done by enqueueing WorkerDecommission messages to self. No checks are done about
   * the prior state of the worker. So an already decommissioned worker will match as well.
   *
   * @param hostnames: A list of hostnames without the ports. Like "localhost", "foo.bar.com" etc
   *
   * Returns the number of workers that matched the hostnames.
   */
  private def decommissionWorkersOnHosts(hostnames: Seq[String]): Integer = {
    val hostnamesSet = hostnames.map(_.toLowerCase(Locale.ROOT)).toSet
    val workersToRemove = addressToWorker
      .filter { case (addr, _) => hostnamesSet.contains(addr.host.toLowerCase(Locale.ROOT)) }
      .values

    val workersToRemoveHostPorts = workersToRemove.map(_.hostPort)
    logInfo(log"Decommissioning the workers with host:ports" +
      log" ${MDC(LogKeys.HOST_PORT, workersToRemoveHostPorts)}")

    // The workers are removed async to avoid blocking the receive loop for the entire batch
    self.send(DecommissionWorkers(workersToRemove.map(_.id).toSeq))

    // Return the count of workers actually removed
    workersToRemove.size
  }

  private def decommissionWorker(worker: WorkerInfo): Unit = {
    if (worker.state != WorkerState.DECOMMISSIONED) {
      logInfo(log"Decommissioning worker ${MDC(LogKeys.WORKER_ID, worker.id)}" +
        log" on ${MDC(LogKeys.WORKER_HOST, worker.host)}:${MDC(LogKeys.WORKER_PORT, worker.port)}")
      worker.setState(WorkerState.DECOMMISSIONED)
      for (exec <- worker.executors.values) {
        logInfo("Telling app of decommission executors")
        exec.application.driver.send(ExecutorUpdated(
          exec.id, ExecutorState.DECOMMISSIONED,
          Some("worker decommissioned"), None,
          // worker host is being set here to let the driver know that the host (aka. worker)
          // is also being decommissioned. So the driver can unregister all the shuffle map
          // statues located at this host when it receives the executor lost event.
          Some(worker.host)))
        exec.state = ExecutorState.DECOMMISSIONED
        exec.application.removeExecutor(exec)
      }
      // On recovery do not add a decommissioned executor
      persistenceEngine.removeWorker(worker)
    } else {
      logWarning(log"Skipping decommissioning worker ${MDC(LogKeys.WORKER_ID, worker.id)} " +
        log"on ${MDC(LogKeys.WORKER_HOST, worker.host)}:" +
        log"${MDC(LogKeys.WORKER_PORT, worker.port)} as worker is already decommissioned")
    }
  }

  private def removeWorker(worker: WorkerInfo, msg: String): Unit = {
    logInfo(log"Removing worker ${MDC(LogKeys.WORKER_ID, worker.id)} on" +
      log" ${MDC(LogKeys.WORKER_HOST, worker.host)}:${MDC(LogKeys.WORKER_PORT, worker.port)}")
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address

    for (exec <- worker.executors.values) {
      logInfo(log"Telling app of lost executor: ${MDC(LogKeys.EXECUTOR_ID, exec.id)}")
      exec.application.driver.send(ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some(s"worker lost: $msg"), None, Some(worker.host)))
      exec.state = ExecutorState.LOST
      exec.application.removeExecutor(exec)
    }
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(log"Re-launching ${MDC(LogKeys.DRIVER_ID, driver.id)}")
        relaunchDriver(driver)
      } else {
        logInfo(log"Not re-launching ${MDC(LogKeys.DRIVER_ID, driver.id)}" +
          log" because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    logInfo(log"Telling app of lost worker: ${MDC(LogKeys.WORKER_ID, worker.id)}")
    apps.filterNot(completedApps.contains(_)).foreach { app =>
      app.driver.send(WorkerRemoved(worker.id, worker.host, msg))
    }
    persistenceEngine.removeWorker(worker)
    schedule()
  }

  private def relaunchDriver(driver: DriverInfo): Unit = {
    // We must setup a new driver with a new driver id here, because the original driver may
    // be still running. Consider this scenario: a worker is network partitioned with master,
    // the master then relaunches driver driverID1 with a driver id driverID2, then the worker
    // reconnects to master. From this point on, if driverID2 is equal to driverID1, then master
    // can not distinguish the statusUpdate of the original driver and the newly relaunched one,
    // for example, when DriverStateChanged(driverID1, KILLED) arrives at master, master will
    // remove driverID1, so the newly relaunched driver disappears too. See SPARK-19900 for details.
    removeDriver(driver.id, DriverState.RELAUNCHING, None)
    val newDriver = createDriver(driver.desc)
    persistenceEngine.addDriver(newDriver)
    drivers.add(newDriver)
    waitingDrivers += newDriver

    schedule()
  }

  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = if (useAppNameAsAppId) {
      desc.name.toLowerCase().replaceAll("\\s+", "")
    } else {
      newApplicationId(date)
    }
    new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
  }

  private[master] def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.address
    if (addressToApp.contains(appAddress)) {
      logInfo(log"Attempted to re-register application at same" +
        log" address: ${MDC(LogKeys.RPC_ADDRESS, appAddress)}")
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    idToApp(app.id) = app
    endpointToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
  }

  private def finishApplication(app: ApplicationInfo): Unit = {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value): Unit = {
    if (apps.contains(app)) {
      logInfo(log"Removing app ${MDC(LogKeys.APP_ID, app.id)}")
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address

      if (completedApps.size >= retainedApplications) {
        val toRemove = math.max(retainedApplications / 10, 1)
        completedApps.take(toRemove).foreach { a =>
          applicationMetricsSystem.removeSource(a.appSource)
        }
        completedApps.dropInPlace(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      for (exec <- app.executors.values) {
        killExecutor(exec)
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver.send(ApplicationRemoved(state.toString))
      }
      persistenceEngine.removeApplication(app)
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(app.id))
      }
    }
  }

  /**
   * Handle a request to set the target number of executors for this application.
   *
   * If the executor limit is adjusted upwards, new executors will be launched provided
   * that there are workers with sufficient resources. If it is adjusted downwards, however,
   * we do not kill existing executors until we explicitly receive a kill request.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleRequestExecutors(
      appId: String,
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(log"Application ${MDC(LogKeys.APP_ID, appId)} requested executors:" +
          log" ${MDC(LogKeys.RESOURCE_PROFILE_TO_TOTAL_EXECS, resourceProfileToTotalExecs)}.")
        appInfo.requestExecutors(resourceProfileToTotalExecs)
        schedule()
        true
      case None =>
        logWarning(log"Unknown application " +
          log"${MDC(LogKeys.APP_ID, appId)} requested executors:" +
          log" ${MDC(LogKeys.RESOURCE_PROFILE_TO_TOTAL_EXECS, resourceProfileToTotalExecs)}.")
        false
    }
  }

  /**
   * Handle a kill request from the given application.
   *
   * This method assumes the executor limit has already been adjusted downwards through
   * a separate [[RequestExecutors]] message, such that we do not launch new executors
   * immediately after the old ones are removed.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(log"Application ${MDC(LogKeys.APP_ID, appId)} requests to kill" +
          log" executors: ${MDC(LogKeys.EXECUTOR_IDS, executorIds.mkString(", "))}")
        val (known, unknown) = executorIds.partition(appInfo.executors.contains)
        known.foreach { executorId =>
          val desc = appInfo.executors(executorId)
          appInfo.removeExecutor(desc)
          killExecutor(desc)
        }
        if (unknown.nonEmpty) {
          logWarning(log"Application ${MDC(LogKeys.APP_ID, appId)} attempted to kill " +
            log"non-existent executors: " +
            log"${MDC(LogKeys.EXECUTOR_IDS, unknown.mkString(", "))}")
        }
        schedule()
        true
      case None =>
        logWarning(log"Unregistered application ${MDC(LogKeys.APP_ID, appId)} " +
          log"requested us to kill executors!")
        false
    }
  }

  /**
   * Cast the given executor IDs to integers and filter out the ones that fail.
   *
   * All executors IDs should be integers since we launched these executors. However,
   * the kill interface on the driver side accepts arbitrary strings, so we need to
   * handle non-integer executor IDs just to be safe.
   */
  private def formatExecutorIds(executorIds: Seq[String]): Seq[Int] = {
    executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          // scalastyle:off line.size.limit
          logError(log"Encountered executor with a non-integer ID: " +
            log"${MDC(LogKeys.EXECUTOR_ID, executorId)}. Ignoring")
          // scalastyle:on
          None
      }
    }
  }

  /**
   * Ask the worker on which the specified executor is launched to kill the executor.
   */
  private def killExecutor(exec: ExecutorDesc): Unit = {
    exec.worker.removeExecutor(exec)
    exec.worker.endpoint.send(KillExecutor(masterUrl, exec.application.id, exec.id))
    exec.state = ExecutorState.KILLED
  }

  /** Generate a new app ID given an app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = appIdPattern.format(
      Master.DATE_TIME_FORMATTER.format(submitDate.toInstant), nextAppNumber)
    nextAppNumber += 1
    if (moduloAppNumber > 0) {
      nextAppNumber %= moduloAppNumber
    }
    appId
  }

  /** Check for, and remove, any timed-out workers */
  private def timeOutDeadWorkers(): Unit = {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - workerTimeoutMs).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        val workerTimeoutSecs = TimeUnit.MILLISECONDS.toSeconds(workerTimeoutMs)
        logWarning(log"Removing ${MDC(LogKeys.WORKER_ID, worker.id)} because we got no heartbeat " +
          log"in ${MDC(LogKeys.TIME_UNITS, workerTimeoutMs)} ms")
        removeWorker(worker, s"Not receiving heartbeat for $workerTimeoutSecs seconds")
      } else {
        if (worker.lastHeartbeat < currentTime - ((reaperIterations + 1) * workerTimeoutMs)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  private def newDriverId(submitDate: Date): String = {
    val appId = driverIdPattern.format(
      Master.DATE_TIME_FORMATTER.format(submitDate.toInstant), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  private def launchDriver(worker: WorkerInfo, driver: DriverInfo): Unit = {
    logInfo(log"Launching driver ${MDC(LogKeys.DRIVER_ID, driver.id)} on worker ${MDC(LogKeys.WORKER_ID, worker.id)}")
    worker.addDriver(driver)
    driver.worker = Some(worker)
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc, driver.resources))
    driver.state = DriverState.RUNNING
  }

  private def removeDriver(
      driverId: String,
      finalState: DriverState,
      exception: Option[Exception]): Unit = {
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(log"Removing driver: ${MDC(LogKeys.DRIVER_ID, driverId)}" +
          log" (${MDC(LogKeys.DRIVER_STATE, finalState)})")
        drivers -= driver
        if (completedDrivers.size >= retainedDrivers) {
          val toRemove = math.max(retainedDrivers / 10, 1)
          completedDrivers.dropInPlace(toRemove)
        }
        completedDrivers += driver
        persistenceEngine.removeDriver(driver)
        driver.state = finalState
        driver.exception = exception
        driver.worker.foreach(w => w.removeDriver(driver))
        schedule()
      case None =>
        logWarning(log"Asked to remove unknown driver: ${MDC(LogKeys.DRIVER_ID, driverId)}")
    }
  }
}

private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  // For application IDs
  private val DATE_TIME_FORMATTER =
    DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss", Locale.US)
      .withZone(ZoneId.systemDefault())

  def main(argStrings: Array[String]): Unit = {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.resetStructuredLogging()
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
