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

import java.io.{File, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}
import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}
import java.util.function.Supplier

import scala.collection.mutable.{HashMap, HashSet, LinkedHashMap}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Success}
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{Command, ExecutorDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.StandaloneResourceUtils._
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.internal.config.UI._
import org.apache.spark.internal.config.Worker._
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.rpc._
import org.apache.spark.util.{RpcUtils, SignalUtils, SparkUncaughtExceptionHandler, ThreadUtils, Utils}

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterRpcAddresses: Array[RpcAddress],
    endpointName: String,
    workDirPath: String = null,
    val conf: SparkConf,
    val securityMgr: SecurityManager,
    resourceFileOpt: Option[String] = None,
    externalShuffleServiceSupplier: Supplier[ExternalShuffleService] = null)
  extends ThreadSafeRpcEndpoint with Logging {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  Utils.checkHost(host)
  assert (port > 0)

  // If worker decommissioning is enabled register a handler on the configured signal to shutdown.
  if (conf.get(config.DECOMMISSION_ENABLED)) {
    val signal = conf.get(config.Worker.WORKER_DECOMMISSION_SIGNAL)
    logInfo(s"Registering SIG$signal handler to trigger decommissioning.")
    SignalUtils.register(signal, s"Failed to register SIG$signal handler - " +
      "disabling worker decommission feature.") {
       self.send(WorkerDecommissionSigReceived)
       true
    }
  } else {
    logInfo("Worker decommissioning not enabled.")
  }

  // A scheduled executor used to send messages at the specified time.
  private val forwardMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  // A separated thread to clean up the workDir and the directories of finished applications.
  // Used to provide the implicit parameter of `Future` methods.
  private val cleanupThreadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor("worker-cleanup-thread"))

  // For worker and executor IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  private val HEARTBEAT_MILLIS = conf.get(WORKER_TIMEOUT) * 1000 / 4

  // Model retries to connect to the master, after Hadoop's model.
  // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
  // Afterwards, the next 10 attempts are between 30 and 90 seconds.
  // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
  // the same time.
  private val INITIAL_REGISTRATION_RETRIES = 6
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER))
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER))

  private val CLEANUP_ENABLED = conf.get(WORKER_CLEANUP_ENABLED)
  // How often worker will clean up old app folders
  private val CLEANUP_INTERVAL_MILLIS = conf.get(WORKER_CLEANUP_INTERVAL) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  private val APP_DATA_RETENTION_SECONDS = conf.get(APP_DATA_RETENTION)

  // Whether or not cleanup the non-shuffle service served files on executor exits.
  private val CLEANUP_FILES_AFTER_EXECUTOR_EXIT =
    conf.get(config.STORAGE_CLEANUP_FILES_AFTER_EXECUTOR_EXIT)

  private var master: Option[RpcEndpointRef] = None

  /**
   * Whether to use the master address in `masterRpcAddresses` if possible. If it's disabled, Worker
   * will just use the address received from Master.
   */
  private val preferConfiguredMasterAddress = conf.get(PREFER_CONFIGURED_MASTER_ADDRESS)
  /**
   * The master address to connect in case of failure. When the connection is broken, worker will
   * use this address to connect. This is usually just one of `masterRpcAddresses`. However, when
   * a master is restarted or takes over leadership, it will be an address sent from master, which
   * may not be in `masterRpcAddresses`.
   */
  private var masterAddressToConnect: Option[RpcAddress] = None
  private var activeMasterUrl: String = ""
  private[worker] var activeMasterWebUiUrl : String = ""
  private var workerWebUiUrl: String = ""
  private val workerUri = RpcEndpointAddress(rpcEnv.address, endpointName).toString
  private var registered = false
  private var connected = false
  private var decommissioned = false
  // expose for test
  private[spark] val workerId = generateWorkerId()
  private val sparkHome =
    if (sys.props.contains(IS_TESTING.key)) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.getOrElse("SPARK_HOME", "."))
    }

  var workDir: File = null
  val finishedExecutors = new LinkedHashMap[String, ExecutorRunner]
  val drivers = new HashMap[String, DriverRunner]
  val executors = new HashMap[String, ExecutorRunner]
  val finishedDrivers = new LinkedHashMap[String, DriverRunner]
  val appDirectories = new HashMap[String, Seq[String]]
  val finishedApps = new HashSet[String]

  // Record the consecutive failure attempts of executor state change syncing with Master,
  // so we don't try it endless. We will exit the Worker process at the end if the failure
  // attempts reach the max attempts. In that case, it's highly possible the Worker
  // suffers a severe network issue, and the Worker would exit finally either reaches max
  // re-register attempts or max state syncing attempts.
  // Map from executor fullId to its consecutive failure attempts number. It's supposed
  // to be very small since it's only used for the temporary network drop, which doesn't
  // happen frequently and recover soon.
  private val executorStateSyncFailureAttempts = new HashMap[String, Int]()
  lazy private val executorStateSyncFailureHandler = ExecutionContext.fromExecutor(
    ThreadUtils.newDaemonSingleThreadExecutor("executor-state-sync-failure-handler"))
  private val executorStateSyncMaxAttempts = conf.get(config.EXECUTOR_STATE_SYNC_MAX_ATTEMPTS)
  private val defaultAskTimeout = RpcUtils.askRpcTimeout(conf).duration.toMillis

  val retainedExecutors = conf.get(WORKER_UI_RETAINED_EXECUTORS)
  val retainedDrivers = conf.get(WORKER_UI_RETAINED_DRIVERS)

  // The shuffle service is not actually started unless configured.
  private val shuffleService = if (externalShuffleServiceSupplier != null) {
    externalShuffleServiceSupplier.get()
  } else {
    new ExternalShuffleService(conf, securityMgr)
  }

  private val publicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }
  private var webUi: WorkerWebUI = null

  private var connectionAttemptCount = 0

  private val metricsSystem =
    MetricsSystem.createMetricsSystem(MetricsSystemInstances.WORKER, conf)
  private val workerSource = new WorkerSource(this)

  val reverseProxy = conf.get(UI_REVERSE_PROXY)

  private var registerMasterFutures: Array[JFuture[_]] = null
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None

  // A thread pool for registering with masters. Because registering with a master is a blocking
  // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
  // time so that we can register with all masters.
  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-master-threadpool",
    masterRpcAddresses.length // Make sure we can register with all masters at the same time
  )

  // visible for tests
  private[deploy] var resources: Map[String, ResourceInformation] = Map.empty

  var coresUsed = 0
  var memoryUsed = 0
  val resourcesUsed = new HashMap[String, MutableResourceInfo]()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  private def createWorkDir(): Unit = {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    if (!Utils.createDirectory(workDir)) {
      System.exit(1)
    }
  }

  override def onStart(): Unit = {
    assert(!registered)
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    logInfo("Spark home: " + sparkHome)
    createWorkDir()
    startExternalShuffleService()
    setupWorkerResources()
    webUi = new WorkerWebUI(this, workDir, webUiPort)
    webUi.bind()

    workerWebUiUrl = s"${webUi.scheme}$publicAddress:${webUi.boundPort}"
    registerWithMaster()

    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
    // Attach the worker metrics servlet handler to the web ui after the metrics system is started.
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
  }

  private def setupWorkerResources(): Unit = {
    try {
      resources = getOrDiscoverAllResources(conf, SPARK_WORKER_PREFIX, resourceFileOpt)
      logResourceInfo(SPARK_WORKER_PREFIX, resources)
    } catch {
      case e: Exception =>
        logError("Failed to setup worker resources: ", e)
        if (!Utils.isTesting) {
          System.exit(1)
        }
    }
    resources.keys.foreach { rName =>
      resourcesUsed(rName) = MutableResourceInfo(rName, new HashSet[String])
    }
  }

  private def addResourcesUsed(deltaInfo: Map[String, ResourceInformation]): Unit = {
    deltaInfo.foreach { case (rName, rInfo) =>
      resourcesUsed(rName) += rInfo
    }
  }

  private def removeResourcesUsed(deltaInfo: Map[String, ResourceInformation]): Unit = {
    deltaInfo.foreach { case (rName, rInfo) =>
      resourcesUsed(rName) -= rInfo
    }
  }

  /**
   * Change to use the new master.
   *
   * @param masterRef the new master ref
   * @param uiUrl the new master Web UI address
   * @param masterAddress the new master address which the worker should use to connect in case of
   *                      failure
   */
  private def changeMaster(masterRef: RpcEndpointRef, uiUrl: String,
      masterAddress: RpcAddress): Unit = {
    // activeMasterUrl it's a valid Spark url since we receive it from master.
    activeMasterUrl = masterRef.address.toSparkURL
    activeMasterWebUiUrl = uiUrl
    masterAddressToConnect = Some(masterAddress)
    master = Some(masterRef)
    connected = true
    if (reverseProxy) {
      logInfo("WorkerWebUI is available at %s/proxy/%s".format(
        activeMasterWebUiUrl.stripSuffix("/"), workerId))
      // if reverseProxyUrl is not set, then we continue to generate relative URLs
      // starting with "/" throughout the UI and do not use activeMasterWebUiUrl
      val proxyUrl = conf.get(UI_REVERSE_PROXY_URL.key, "").stripSuffix("/")
      // In the method `UIUtils.makeHref`, the URL segment "/proxy/$worker_id" will be appended
      // after `proxyUrl`, so no need to set the worker ID in the `spark.ui.proxyBase` here.
      System.setProperty("spark.ui.proxyBase", proxyUrl)
    }
    // Cancel any outstanding re-registration attempts because we found a new master
    cancelLastRegistrationRetry()
  }

  private def tryRegisterAllMasters(): Array[JFuture[_]] = {
    masterRpcAddresses.map { masterAddress =>
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            logInfo("Connecting to master " + masterAddress + "...")
            val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            sendRegisterMessageToMaster(masterEndpoint)
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        }
      })
    }
  }

  /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * If the re-registration attempt threshold is exceeded, the worker exits with error.
   * Note that for thread-safety this should only be called from the rpcEndpoint.
   */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        cancelLastRegistrationRetry()
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
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            val masterAddress =
              if (preferConfiguredMasterAddress) masterAddressToConnect.get else masterRef.address
            registerMasterFutures = Array(registerMasterThreadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterAddress + "...")
                  val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
                  sendRegisterMessageToMaster(masterEndpoint)
                } catch {
                  case ie: InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
                }
              }
            }))
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            // We are retrying the initial registration
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(
            forwardMessageScheduler.scheduleAtFixedRate(
              () => Utils.tryLogNonFatalError { self.send(ReregisterWithMaster) },
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  /**
   * Cancel last registration retry, or do nothing if no retry
   */
  private def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  private def registerWithMaster(): Unit = {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some(forwardMessageScheduler.scheduleAtFixedRate(
          () => Utils.tryLogNonFatalError { Option(self).foreach(_.send(ReregisterWithMaster)) },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  private def startExternalShuffleService(): Unit = {
    try {
      shuffleService.startIfEnabled()
    } catch {
      case e: Exception =>
        logError("Failed to start external shuffle service", e)
        System.exit(1)
    }
  }

  private def sendRegisterMessageToMaster(masterEndpoint: RpcEndpointRef): Unit = {
    masterEndpoint.send(RegisterWorker(
      workerId,
      host,
      port,
      self,
      cores,
      memory,
      workerWebUiUrl,
      masterEndpoint.address,
      resources))
  }

  private def handleRegisterResponse(msg: RegisterWorkerResponse): Unit = synchronized {
    msg match {
      case RegisteredWorker(masterRef, masterWebUiUrl, masterAddress, duplicate) =>
        val preferredMasterAddress = if (preferConfiguredMasterAddress) {
          masterAddress.toSparkURL
        } else {
          masterRef.address.toSparkURL
        }

        // there're corner cases which we could hardly avoid duplicate worker registration,
        // e.g. Master disconnect(maybe due to network drop) and recover immediately, see
        // SPARK-23191 for more details.
        if (duplicate) {
          logWarning(s"Duplicate registration at master $preferredMasterAddress")
        }

        logInfo(s"Successfully registered with master $preferredMasterAddress")
        registered = true
        changeMaster(masterRef, masterWebUiUrl, masterAddress)
        forwardMessageScheduler.scheduleAtFixedRate(
          () => Utils.tryLogNonFatalError { self.send(SendHeartbeat) },
          0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
        if (CLEANUP_ENABLED) {
          logInfo(
            s"Worker cleanup enabled; old application directories will be deleted in: $workDir")
          forwardMessageScheduler.scheduleAtFixedRate(
            () => Utils.tryLogNonFatalError { self.send(WorkDirCleanup) },
            CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
        }

        val execs = executors.values.map { e =>
          new ExecutorDescription(e.appId, e.execId, e.rpId, e.cores, e.memory, e.state)
        }
        masterRef.send(WorkerLatestState(workerId, execs.toList, drivers.keys.toSeq))

      case RegisterWorkerFailed(message) =>
        if (!registered) {
          logError("Worker registration failed: " + message)
          System.exit(1)
        }

      case MasterInStandby =>
        // Ignore. Master not yet ready.
    }
  }

  override def receive: PartialFunction[Any, Unit] = synchronized {
    case msg: RegisterWorkerResponse =>
      handleRegisterResponse(msg)

    case SendHeartbeat =>
      if (connected) { sendToMaster(Heartbeat(workerId, self)) }

    case WorkDirCleanup =>
      // Spin up a separate thread (in a future) to do the dir cleanup; don't tie up worker
      // rpcEndpoint.
      // Copy ids so that it can be used in the cleanup thread.
      val appIds = (executors.values.map(_.appId) ++ drivers.values.map(_.driverId)).toSet
      try {
        val cleanupFuture: concurrent.Future[Unit] = concurrent.Future {
          val appDirs = workDir.listFiles()
          if (appDirs == null) {
            throw new IOException(
              s"ERROR: Failed to list files in ${appDirs.mkString("dirs(", ", ", ")")}")
          }
          appDirs.filter { dir =>
            // the directory is used by an application - check that the application is not running
            // when cleaning up
            val appIdFromDir = dir.getName
            val isAppStillRunning = appIds.contains(appIdFromDir)
            dir.isDirectory && !isAppStillRunning &&
              !Utils.doesDirectoryContainAnyNewFiles(dir, APP_DATA_RETENTION_SECONDS)
          }.foreach { dir =>
            logInfo(s"Removing directory: ${dir.getPath}")
            Utils.deleteRecursively(dir)

            // Remove some registeredExecutors information of DB in external shuffle service when
            // #spark.shuffle.service.db.enabled=true, the one which comes to mind is, what happens
            // if an application is stopped while the external shuffle service is down?
            // So then it'll leave an entry in the DB and the entry should be removed.
            if (conf.get(config.SHUFFLE_SERVICE_DB_ENABLED) &&
                conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
              shuffleService.applicationRemoved(dir.getName)
            }
          }
        }(cleanupThreadExecutor)

        cleanupFuture.failed.foreach(e =>
          logError("App dir cleanup failed: " + e.getMessage, e)
        )(cleanupThreadExecutor)
      } catch {
        case _: RejectedExecutionException if cleanupThreadExecutor.isShutdown =>
          logWarning("Failed to cleanup work dir as executor pool was shutdown")
      }

    case MasterChanged(masterRef, masterWebUiUrl) =>
      logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
      changeMaster(masterRef, masterWebUiUrl, masterRef.address)

      val executorResponses = executors.values.map { e =>
        WorkerExecutorStateResponse(new ExecutorDescription(
          e.appId, e.execId, e.rpId, e.cores, e.memory, e.state), e.resources)
      }
      val driverResponses = drivers.keys.map { id =>
        WorkerDriverStateResponse(id, drivers(id).resources)}
      masterRef.send(WorkerSchedulerStateResponse(
        workerId, executorResponses.toList, driverResponses.toSeq))

    case ReconnectWorker(masterUrl) =>
      logInfo(s"Master with url $masterUrl requested this worker to reconnect.")
      registerWithMaster()

    case LaunchExecutor(masterUrl, appId, execId, rpId, appDesc, cores_, memory_, resources_) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else if (decommissioned) {
        logWarning("Asked to launch an executor while decommissioned. Not launching executor.")
      } else {
        try {
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))

          // Create the executor's working directory
          val executorDir = new File(workDir, appId + "/" + execId)
          if (!executorDir.mkdirs()) {
            throw new IOException("Failed to create directory " + executorDir)
          }

          // Create local dirs for the executor. These are passed to the executor via the
          // SPARK_EXECUTOR_DIRS environment variable, and deleted by the Worker when the
          // application finishes.
          val appLocalDirs = appDirectories.getOrElse(appId, {
            val localRootDirs = Utils.getOrCreateLocalRootDirs(conf)
            val dirs = localRootDirs.flatMap { dir =>
              try {
                val appDir = Utils.createDirectory(dir, namePrefix = "executor")
                Utils.chmod700(appDir)
                Some(appDir.getAbsolutePath())
              } catch {
                case e: IOException =>
                  logWarning(s"${e.getMessage}. Ignoring this directory.")
                  None
              }
            }.toSeq
            if (dirs.isEmpty) {
              throw new IOException("No subfolder can be created in " +
                s"${localRootDirs.mkString(",")}.")
            }
            dirs
          })
          appDirectories(appId) = appLocalDirs
          val manager = new ExecutorRunner(
            appId,
            execId,
            appDesc.copy(command = Worker.maybeUpdateSSLSettings(appDesc.command, conf)),
            cores_,
            memory_,
            self,
            workerId,
            webUi.scheme,
            host,
            webUi.boundPort,
            publicAddress,
            sparkHome,
            executorDir,
            workerUri,
            conf,
            appLocalDirs,
            ExecutorState.LAUNCHING,
            rpId,
            resources_)
          executors(appId + "/" + execId) = manager
          manager.start()
          coresUsed += cores_
          memoryUsed += memory_
          addResourcesUsed(resources_)
        } catch {
          case e: Exception =>
            logError(s"Failed to launch executor $appId/$execId for ${appDesc.name}.", e)
            if (executors.contains(appId + "/" + execId)) {
              executors(appId + "/" + execId).kill()
              executors -= appId + "/" + execId
            }
            syncExecutorStateWithMaster(ExecutorStateChanged(appId, execId, ExecutorState.FAILED,
              Some(e.toString), None))
        }
      }

    case executorStateChanged: ExecutorStateChanged =>
      handleExecutorStateChanged(executorStateChanged)

    case KillExecutor(masterUrl, appId, execId) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to kill executor " + execId)
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

    case LaunchDriver(driverId, driverDesc, resources_) =>
      logInfo(s"Asked to launch driver $driverId")
      val driver = new DriverRunner(
        conf,
        driverId,
        workDir,
        sparkHome,
        driverDesc.copy(command = Worker.maybeUpdateSSLSettings(driverDesc.command, conf)),
        self,
        workerUri,
        workerWebUiUrl,
        securityMgr,
        resources_)
      drivers(driverId) = driver
      driver.start()

      coresUsed += driverDesc.cores
      memoryUsed += driverDesc.mem
      addResourcesUsed(resources_)

    case KillDriver(driverId) =>
      logInfo(s"Asked to kill driver $driverId")
      drivers.get(driverId) match {
        case Some(runner) =>
          runner.kill()
        case None =>
          logError(s"Asked to kill unknown driver $driverId")
      }

    case driverStateChanged @ DriverStateChanged(driverId, state, exception) =>
      handleDriverStateChanged(driverStateChanged)

    case ReregisterWithMaster =>
      reregisterWithMaster()

    case ApplicationFinished(id) =>
      finishedApps += id
      maybeCleanupApplication(id)

    case DecommissionWorker =>
      decommissionSelf()

    case WorkerDecommissionSigReceived =>
      decommissionSelf()
      // Tell the Master that we are starting decommissioning
      // so it stops trying to launch executor/driver on us
      sendToMaster(WorkerDecommissioning(workerId, self))
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestWorkerState =>
      context.reply(WorkerStateResponse(host, port, workerId, executors.values.toList,
        finishedExecutors.values.toList, drivers.values.toList,
        finishedDrivers.values.toList, activeMasterUrl, cores, memory,
        coresUsed, memoryUsed, activeMasterWebUiUrl, resources,
        resourcesUsed.toMap.map { case (k, v) => (k, v.toResourceInformation)}))
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (master.exists(_.address == remoteAddress) ||
        masterAddressToConnect.contains(remoteAddress)) {
      logInfo(s"$remoteAddress Disassociated !")
      masterDisconnected()
    }
  }

  private def masterDisconnected(): Unit = {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  private def maybeCleanupApplication(id: String): Unit = {
    val shouldCleanup = finishedApps.contains(id) && !executors.values.exists(_.appId == id)
    if (shouldCleanup) {
      finishedApps -= id
      try {
        appDirectories.remove(id).foreach { dirList =>
          concurrent.Future {
            logInfo(s"Cleaning up local directories for application $id")
            dirList.foreach { dir =>
              Utils.deleteRecursively(new File(dir))
            }
          }(cleanupThreadExecutor).failed.foreach(e =>
            logError(s"Clean up app dir $dirList failed: ${e.getMessage}", e)
          )(cleanupThreadExecutor)
        }
      } catch {
        case _: RejectedExecutionException if cleanupThreadExecutor.isShutdown =>
          logWarning("Failed to cleanup application as executor pool was shutdown")
      }
      shuffleService.applicationRemoved(id)
    }
  }

  /**
   * Send a message to the current master. If we have not yet registered successfully with any
   * master, the message will be dropped.
   */
  private def sendToMaster(message: Any): Unit = {
    master match {
      case Some(masterRef) => masterRef.send(message)
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }

  /**
   * Send `ExecutorStateChanged` to the current master. Unlike `sendToMaster`, we use `askSync`
   * to send the message in order to ensure Master can receive the message.
   */
  private def syncExecutorStateWithMaster(newState: ExecutorStateChanged): Unit = {
    master match {
      case Some(masterRef) =>
        val fullId = s"${newState.appId}/${newState.execId}"
        // SPARK-34245: We used async `send` to send the state previously. In that case, the
        // finished executor can be leaked if Worker fails to send `ExecutorStateChanged`
        // message to Master due to some unexpected errors, e.g., temporary network error.
        // In the worst case, the application can get hang if the leaked executor is the only
        // or last executor for the application. Therefore, we switch to `ask` to ensure
        // the state is handled by Master.
        masterRef.ask[Boolean](newState).onComplete {
          case Success(_) =>
            executorStateSyncFailureAttempts.remove(fullId)

          case Failure(t) =>
            val failures = executorStateSyncFailureAttempts.getOrElse(fullId, 0) + 1
            if (failures < executorStateSyncMaxAttempts) {
              logError(s"Failed to send $newState to Master $masterRef, " +
                s"will retry ($failures/$executorStateSyncMaxAttempts).", t)
              executorStateSyncFailureAttempts(fullId) = failures
              // If the failure is not caused by TimeoutException, wait for a while before retry in
              // case the connection is temporarily unavailable.
              if (!t.isInstanceOf[TimeoutException]) {
                try {
                  Thread.sleep(defaultAskTimeout)
                } catch {
                  case _: InterruptedException => // Cancelled
                }
              }
              self.send(newState)
            } else {
              logError(s"Failed to send $newState to Master $masterRef for " +
                s"$executorStateSyncMaxAttempts times. Giving up.")
              System.exit(1)
            }
        }(executorStateSyncFailureHandler)

      case None =>
        logWarning(
          s"Dropping $newState because the connection to master has not yet been established")
    }
  }

  private def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  override def onStop(): Unit = {
    cleanupThreadExecutor.shutdownNow()
    metricsSystem.report()
    cancelLastRegistrationRetry()
    forwardMessageScheduler.shutdownNow()
    registerMasterThreadPool.shutdownNow()
    executors.values.foreach(_.kill())
    drivers.values.foreach(_.kill())
    shuffleService.stop()
    webUi.stop()
    metricsSystem.stop()
  }

  private def trimFinishedExecutorsIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedExecutors.size > retainedExecutors) {
      finishedExecutors.take(math.max(finishedExecutors.size / 10, 1)).foreach {
        case (executorId, _) => finishedExecutors.remove(executorId)
      }
    }
  }

  private def trimFinishedDriversIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedDrivers.size > retainedDrivers) {
      finishedDrivers.take(math.max(finishedDrivers.size / 10, 1)).foreach {
        case (driverId, _) => finishedDrivers.remove(driverId)
      }
    }
  }

  private[deploy] def decommissionSelf(): Unit = {
    if (conf.get(config.DECOMMISSION_ENABLED) && !decommissioned) {
      decommissioned = true
      logInfo(s"Decommission worker $workerId.")
    } else if (decommissioned) {
      logWarning(s"Worker $workerId already started decommissioning.")
    } else {
      logWarning(s"Receive decommission request, but decommission feature is disabled.")
    }
  }

  private[worker] def handleDriverStateChanged(driverStateChanged: DriverStateChanged): Unit = {
    val driverId = driverStateChanged.driverId
    val exception = driverStateChanged.exception
    val state = driverStateChanged.state
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
    sendToMaster(driverStateChanged)
    val driver = drivers.remove(driverId).get
    finishedDrivers(driverId) = driver
    trimFinishedDriversIfNecessary()
    memoryUsed -= driver.driverDesc.mem
    coresUsed -= driver.driverDesc.cores
    removeResourcesUsed(driver.resources)
  }

  private[worker] def handleExecutorStateChanged(executorStateChanged: ExecutorStateChanged):
    Unit = {
    syncExecutorStateWithMaster(executorStateChanged)
    val state = executorStateChanged.state
    if (ExecutorState.isFinished(state)) {
      val appId = executorStateChanged.appId
      val fullId = appId + "/" + executorStateChanged.execId
      val message = executorStateChanged.message
      val exitStatus = executorStateChanged.exitStatus
      executors.get(fullId) match {
        case Some(executor) =>
          logInfo("Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
          executors -= fullId
          finishedExecutors(fullId) = executor
          trimFinishedExecutorsIfNecessary()
          coresUsed -= executor.cores
          memoryUsed -= executor.memory
          removeResourcesUsed(executor.resources)

          if (CLEANUP_FILES_AFTER_EXECUTOR_EXIT) {
            shuffleService.executorRemoved(executorStateChanged.execId.toString, appId)
          }
        case None =>
          logInfo("Unknown Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
      }
      maybeCleanupApplication(appId)
    }
  }
}

private[deploy] object Worker extends Logging {
  val SYSTEM_NAME = "sparkWorker"
  val ENDPOINT_NAME = "Worker"
  private val SSL_NODE_LOCAL_CONFIG_PATTERN = """\-Dspark\.ssl\.useNodeLocalConf\=(.+)""".r

  def main(argStrings: Array[String]): Unit = {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new WorkerArguments(argStrings, conf)
    val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir, conf = conf,
      resourceFileOpt = conf.get(SPARK_WORKER_RESOURCE_FILE))
    // With external shuffle service enabled, if we request to launch multiple workers on one host,
    // we can only successfully launch the first worker and the rest fails, because with the port
    // bound, we may launch no more than one external shuffle service on each host.
    // When this happens, we should give explicit reason of failure instead of fail silently. For
    // more detail see SPARK-20989.
    val externalShuffleServiceEnabled = conf.get(config.SHUFFLE_SERVICE_ENABLED)
    val sparkWorkerInstances = scala.sys.env.getOrElse("SPARK_WORKER_INSTANCES", "1").toInt
    require(externalShuffleServiceEnabled == false || sparkWorkerInstances <= 1,
      "Starting multiple workers on one host is failed because we may launch no more than one " +
        "external shuffle service on each host, please set spark.shuffle.service.enabled to " +
        "false or set SPARK_WORKER_INSTANCES to 1 to resolve the conflict.")
    rpcEnv.awaitTermination()
  }

  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],
      workDir: String,
      workerNumber: Option[Int] = None,
      conf: SparkConf = new SparkConf,
      resourceFileOpt: Option[String] = None): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL)
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, webUiPort, cores, memory,
      masterAddresses, ENDPOINT_NAME, workDir, conf, securityMgr, resourceFileOpt))
    rpcEnv
  }

  def isUseLocalNodeSSLConfig(cmd: Command): Boolean = {
    val result = cmd.javaOpts.collectFirst {
      case SSL_NODE_LOCAL_CONFIG_PATTERN(_result) => _result.toBoolean
    }
    result.getOrElse(false)
  }

  def maybeUpdateSSLSettings(cmd: Command, conf: SparkConf): Command = {
    val prefix = "spark.ssl."
    val useNLC = "spark.ssl.useNodeLocalConf"
    if (isUseLocalNodeSSLConfig(cmd)) {
      val newJavaOpts = cmd.javaOpts
          .filter(opt => !opt.startsWith(s"-D$prefix")) ++
          conf.getAll.collect { case (key, value) if key.startsWith(prefix) => s"-D$key=$value" } :+
          s"-D$useNLC=true"
      cmd.copy(javaOpts = newJavaOpts)
    } else {
      cmd
    }
  }
}
