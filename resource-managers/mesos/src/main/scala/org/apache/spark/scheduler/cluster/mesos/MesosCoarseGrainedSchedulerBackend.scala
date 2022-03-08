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

package org.apache.spark.scheduler.cluster.mesos

import java.io.File
import java.util.{Collections, List => JList}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future

import org.apache.mesos.Protos.{SlaveID => AgentID, TaskInfo => MesosTaskInfo, _}
import org.apache.mesos.SchedulerDriver

import org.apache.spark.{SecurityManager, SparkConf, SparkContext, SparkException, TaskState}
import org.apache.spark.deploy.mesos.config._
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Network
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.mesos.MesosExternalBlockStoreClient
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.{ExecutorProcessLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

/**
 * A SchedulerBackend that runs tasks on Mesos, but uses "coarse-grained" tasks, where it holds
 * onto each Mesos node for the duration of the Spark job instead of relinquishing cores whenever
 * a task is done. It launches Spark tasks within the coarse-grained Mesos tasks using the
 * CoarseGrainedSchedulerBackend mechanism. This class is useful for lower and more predictable
 * latency.
 *
 * Unfortunately this has a bit of duplication from [[MesosFineGrainedSchedulerBackend]],
 * but it seems hard to remove this.
 */
private[spark] class MesosCoarseGrainedSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    master: String,
    securityManager: SecurityManager)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with MesosScheduler
  with MesosSchedulerUtils {

  // Exclude an agent after this many failures
  private val MAX_AGENT_FAILURES = 2

  private val maxCoresOption = conf.get(config.CORES_MAX)

  private val executorCoresOption = conf.getOption(config.EXECUTOR_CORES.key).map(_.toInt)

  private val minCoresPerExecutor = executorCoresOption.getOrElse(1)

  // Maximum number of cores to acquire
  private val maxCores = {
    val cores = maxCoresOption.getOrElse(Int.MaxValue)
    // Set maxCores to a multiple of smallest executor we can launch
    cores - (cores % minCoresPerExecutor)
  }

  private val useFetcherCache = conf.get(ENABLE_FETCHER_CACHE)

  private val maxGpus = conf.get(MAX_GPUS)

  private val taskLabels = conf.get(TASK_LABELS)

  private[this] val shutdownTimeoutMS = conf.get(COARSE_SHUTDOWN_TIMEOUT)

  // Synchronization protected by stateLock
  private[this] var stopCalled: Boolean = false

  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sc.conf

    override protected def onStopRequest(): Unit = {
      stopSchedulerBackend()
      setState(SparkAppHandle.State.KILLED)
    }
  }

  // If shuffle service is enabled, the Spark driver will register with the shuffle service.
  // This is for cleaning up shuffle files reliably.
  private val shuffleServiceEnabled = conf.get(config.SHUFFLE_SERVICE_ENABLED)

  // Cores we have acquired with each Mesos task ID
  private val coresByTaskId = new mutable.HashMap[String, Int]
  private val gpusByTaskId = new mutable.HashMap[String, Int]
  private var totalCoresAcquired = 0
  private var totalGpusAcquired = 0

  // The amount of time to wait for locality scheduling
  private val localityWaitNs = TimeUnit.MILLISECONDS.toNanos(conf.get(config.LOCALITY_WAIT))
  // The start of the waiting, for data local scheduling
  private var localityWaitStartTimeNs = System.nanoTime()
  // If true, the scheduler is in the process of launching executors to reach the requested
  // executor limit
  private var launchingExecutors = false

  // AgentID -> Agent
  // This map accumulates entries for the duration of the job.  Agents are never deleted, because
  // we need to maintain e.g. failure state and connection state.
  private val agents = new mutable.HashMap[String, Agent]

  /**
   * The total number of executors we aim to have. Undefined when not using dynamic allocation.
   * Initially set to 0 when using dynamic allocation, the executor allocation manager will send
   * the real initial limit later.
   */
  private var executorLimitOption: Option[Int] = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      Some(0)
    } else {
      None
    }
  }

  /**
   *  Return the current executor limit, which may be [[Int.MaxValue]]
   *  before properly initialized.
   */
  private[mesos] def executorLimit: Int = executorLimitOption.getOrElse(Int.MaxValue)

  // private lock object protecting mutable state above. Using the intrinsic lock
  // may lead to deadlocks since the superclass might also try to lock
  private val stateLock = new ReentrantLock

  private val extraCoresPerExecutor = conf.get(EXTRA_CORES_PER_EXECUTOR)

  // Offer constraints
  private val agentOfferConstraints =
    parseConstraintString(sc.conf.get(CONSTRAINTS))

  // Reject offers with mismatched constraints in seconds
  private val rejectOfferDurationForUnmetConstraints =
    getRejectOfferDurationForUnmetConstraints(sc.conf)

  // Reject offers when we reached the maximum number of cores for this framework
  private val rejectOfferDurationForReachedMaxCores =
    getRejectOfferDurationForReachedMaxCores(sc.conf)

  // A client for talking to the external shuffle service
  private val mesosExternalShuffleClient: Option[MesosExternalBlockStoreClient] = {
    if (shuffleServiceEnabled) {
      Some(getShuffleClient())
    } else {
      None
    }
  }

  // This method is factored out for testability
  protected def getShuffleClient(): MesosExternalBlockStoreClient = {
    new MesosExternalBlockStoreClient(
      SparkTransportConf.fromSparkConf(conf, "shuffle"),
      securityManager,
      securityManager.isAuthenticationEnabled(),
      conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT))
  }

  private var nextMesosTaskId = 0

  @volatile var appId: String = _

  private var schedulerDriver: SchedulerDriver = _

  private val defaultProfile = sc.resourceProfileManager.defaultResourceProfile


  def newMesosTaskId(): String = {
    val id = nextMesosTaskId
    nextMesosTaskId += 1
    id.toString
  }

  override def start(): Unit = {
    super.start()

    if (sc.deployMode == "client") {
      launcherBackend.connect()
    }
    val startedBefore = IdHelper.startedBefore.getAndSet(true)

    val suffix = if (startedBefore) {
      f"-${IdHelper.nextSCNumber.incrementAndGet()}%04d"
    } else {
      ""
    }

    val driver = createSchedulerDriver(
      master,
      MesosCoarseGrainedSchedulerBackend.this,
      sc.sparkUser,
      sc.appName,
      sc.conf,
      sc.conf.get(DRIVER_WEBUI_URL).orElse(sc.ui.map(_.webUrl)),
      None,
      Some(sc.conf.get(DRIVER_FAILOVER_TIMEOUT)),
      sc.conf.get(DRIVER_FRAMEWORK_ID).map(_ + suffix)
    )

    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    startScheduler(driver)
  }

  def createCommand(offer: Offer, numCores: Int, taskId: String): CommandInfo = {
    val environment = Environment.newBuilder()
    val extraClassPath = conf.get(config.EXECUTOR_CLASS_PATH)
    extraClassPath.foreach { cp =>
      environment.addVariables(
        Environment.Variable.newBuilder().setName("SPARK_EXECUTOR_CLASSPATH").setValue(cp).build())
    }
    val extraJavaOpts = conf.get(config.EXECUTOR_JAVA_OPTIONS).map {
      Utils.substituteAppNExecIds(_, appId, taskId)
    }.getOrElse("")

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    val prefixEnv = conf.get(config.EXECUTOR_LIBRARY_PATH).map { p =>
      Utils.libraryPathEnvPrefix(Seq(p))
    }.getOrElse("")

    environment.addVariables(
      Environment.Variable.newBuilder()
        .setName("SPARK_EXECUTOR_OPTS")
        .setValue(extraJavaOpts)
        .build())

    sc.executorEnvs.foreach { case (key, value) =>
      environment.addVariables(Environment.Variable.newBuilder()
        .setName(key)
        .setValue(value)
        .build())
    }

    MesosSchedulerBackendUtil.getSecretEnvVar(conf, executorSecretConfig).foreach { variable =>
      if (variable.getSecret.getReference.isInitialized) {
        logInfo(s"Setting reference secret ${variable.getSecret.getReference.getName} " +
          s"on file ${variable.getName}")
      } else {
        logInfo(s"Setting secret on environment variable name=${variable.getName}")
      }
      environment.addVariables(variable)
    }

    val command = CommandInfo.newBuilder()
      .setEnvironment(environment)

    val uri = conf.get(EXECUTOR_URI).orElse(Option(System.getenv("SPARK_EXECUTOR_URI")))

    if (uri.isEmpty) {
      val executorSparkHome = conf.get(EXECUTOR_HOME)
        .orElse(sc.getSparkHome())
        .getOrElse {
          throw new SparkException(s"Executor Spark home `$EXECUTOR_HOME` is not set!")
        }
      val executable = new File(executorSparkHome, "./bin/spark-class").getPath
      val runScript = "%s \"%s\" org.apache.spark.executor.CoarseGrainedExecutorBackend"
        .format(prefixEnv, executable)

      command.setValue(buildExecutorCommand(runScript, taskId, numCores, offer))
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = uri.get.split('/').last.split('.').head
      val runScript = s"cd $basename*; $prefixEnv " +
        "./bin/spark-class org.apache.spark.executor.CoarseGrainedExecutorBackend"

      command.setValue(buildExecutorCommand(runScript, taskId, numCores, offer))
      command.addUris(CommandInfo.URI.newBuilder().setValue(uri.get).setCache(useFetcherCache))
    }

    setupUris(conf.get(URIS_TO_DOWNLOAD), command, useFetcherCache)

    command.build()
  }

  private def buildExecutorCommand(
      runScript: String, taskId: String, numCores: Int, offer: Offer): String = {

    val sb = new StringBuilder()
      .append(runScript)
      .append(" --driver-url ")
      .append(driverURL)
      .append(" --executor-id ")
      .append(taskId)
      .append(" --cores ")
      .append(numCores)
      .append(" --app-id ")
      .append(appId)

    if (sc.conf.get(NETWORK_NAME).isEmpty) {
      sb.append(" --hostname ")
      sb.append(offer.getHostname)
    }

    sb.toString()
  }

  protected def driverURL: String = {
    if (conf.contains(IS_TESTING)) {
      "driverURL"
    } else {
      RpcEndpointAddress(
        conf.get(config.DRIVER_HOST_ADDRESS),
        conf.get(config.DRIVER_PORT),
        CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    }
  }

  override def offerRescinded(d: org.apache.mesos.SchedulerDriver, o: OfferID): Unit = {}

  override def registered(
      driver: org.apache.mesos.SchedulerDriver,
      frameworkId: FrameworkID,
      masterInfo: MasterInfo): Unit = {

    this.appId = frameworkId.getValue
    this.mesosExternalShuffleClient.foreach(_.init(appId))
    this.schedulerDriver = driver
    markRegistered()
    launcherBackend.setAppId(appId)
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get >= maxCoresOption.getOrElse(0) * minRegisteredRatio
  }

  override def disconnected(d: org.apache.mesos.SchedulerDriver): Unit = {
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
  }

  override def reregistered(d: org.apache.mesos.SchedulerDriver, masterInfo: MasterInfo): Unit = {
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  /**
   * Method called by Mesos to offer resources on agents. We respond by launching an executor,
   * unless we've already launched more than we wanted to.
   */
  override def resourceOffers(d: org.apache.mesos.SchedulerDriver, offers: JList[Offer]): Unit = {
    stateLock.synchronized {
      if (stopCalled) {
        logDebug("Ignoring offers during shutdown")
        // Driver should simply return a stopped status on race
        // condition between this.stop() and completing here
        offers.asScala.map(_.getId).foreach(d.declineOffer)
        return
      }

      if (numExecutors >= executorLimit) {
        logDebug("Executor limit reached. numExecutors: " + numExecutors +
          " executorLimit: " + executorLimit)
        offers.asScala.map(_.getId).foreach(d.declineOffer)
        launchingExecutors = false
        return
      } else {
        if (!launchingExecutors) {
          launchingExecutors = true
          localityWaitStartTimeNs = System.nanoTime()
        }
      }

      logDebug(s"Received ${offers.size} resource offers.")

      val (matchedOffers, unmatchedOffers) = offers.asScala.partition { offer =>
        val offerAttributes = toAttributeMap(offer.getAttributesList)
        matchesAttributeRequirements(agentOfferConstraints, offerAttributes)
      }

      declineUnmatchedOffers(d, unmatchedOffers)
      handleMatchedOffers(d, matchedOffers)
    }
  }

  private def declineUnmatchedOffers(
      driver: org.apache.mesos.SchedulerDriver, offers: mutable.Buffer[Offer]): Unit = {
    offers.foreach { offer =>
      declineOffer(
        driver,
        offer,
        Some("unmet constraints"),
        Some(rejectOfferDurationForUnmetConstraints))
    }
  }

  /**
   * Launches executors on accepted offers, and declines unused offers. Executors are launched
   * round-robin on offers.
   *
   * @param driver SchedulerDriver
   * @param offers Mesos offers that match attribute constraints
   */
  private def handleMatchedOffers(
      driver: org.apache.mesos.SchedulerDriver, offers: mutable.Buffer[Offer]): Unit = {
    val tasks = buildMesosTasks(offers)
    for (offer <- offers) {
      val offerAttributes = toAttributeMap(offer.getAttributesList)
      val offerMem = getResource(offer.getResourcesList, "mem")
      val offerCpus = getResource(offer.getResourcesList, "cpus")
      val offerPorts = getRangeResource(offer.getResourcesList, "ports")
      val offerReservationInfo = offer
        .getResourcesList
        .asScala
        .find { r => r.getReservation != null }
      val id = offer.getId.getValue

      if (tasks.contains(offer.getId)) { // accept
        val offerTasks = tasks(offer.getId)

        logDebug(s"Accepting offer: $id with attributes: $offerAttributes " +
          offerReservationInfo.map(resInfo =>
            s"reservation info: ${resInfo.getReservation.toString}").getOrElse("") +
          s"mem: $offerMem cpu: $offerCpus ports: $offerPorts " +
          s"resources: ${offer.getResourcesList.asScala.mkString(",")}." +
          s"  Launching ${offerTasks.size} Mesos tasks.")

        for (task <- offerTasks) {
          val taskId = task.getTaskId
          val mem = getResource(task.getResourcesList, "mem")
          val cpus = getResource(task.getResourcesList, "cpus")
          val ports = getRangeResource(task.getResourcesList, "ports").mkString(",")

          logDebug(s"Launching Mesos task: ${taskId.getValue} with mem: $mem cpu: $cpus" +
            s" ports: $ports" + s" on agent with agent id: ${task.getSlaveId.getValue} ")
        }

        driver.launchTasks(
          Collections.singleton(offer.getId),
          offerTasks.asJava)
      } else if (totalCoresAcquired >= maxCores) {
        // Reject an offer for a configurable amount of time to avoid starving other frameworks
        declineOffer(driver,
          offer,
          Some("reached spark.cores.max"),
          Some(rejectOfferDurationForReachedMaxCores))
      } else {
        declineOffer(
          driver,
          offer,
          Some("Offer was declined due to unmet task launch constraints."))
      }
    }
  }

  private def getContainerInfo(conf: SparkConf): ContainerInfo.Builder = {
    val containerInfo = MesosSchedulerBackendUtil.buildContainerInfo(conf)

    MesosSchedulerBackendUtil.getSecretVolume(conf, executorSecretConfig).foreach { volume =>
      if (volume.getSource.getSecret.getReference.isInitialized) {
        logInfo(s"Setting reference secret ${volume.getSource.getSecret.getReference.getName} " +
          s"on file ${volume.getContainerPath}")
      } else {
        logInfo(s"Setting secret on file name=${volume.getContainerPath}")
      }
      containerInfo.addVolumes(volume)
    }

    containerInfo
  }

  /**
   * Returns a map from OfferIDs to the tasks to launch on those offers.  In order to maximize
   * per-task memory and IO, tasks are round-robin assigned to offers.
   *
   * @param offers Mesos offers that match attribute constraints
   * @return A map from OfferID to a list of Mesos tasks to launch on that offer
   */
  private def buildMesosTasks(offers: mutable.Buffer[Offer]): Map[OfferID, List[MesosTaskInfo]] = {
    // offerID -> tasks
    val tasks = new mutable.HashMap[OfferID, List[MesosTaskInfo]].withDefaultValue(Nil)

    // offerID -> resources
    val remainingResources = mutable.Map[String, JList[Resource]]()
    remainingResources ++= offers.map(offer => (offer.getId.getValue, offer.getResourcesList))

    var launchTasks = true

    // TODO(mgummelt): combine offers for a single agent
    //
    // round-robin create executors on the available offers
    while (launchTasks) {
      launchTasks = false

      for (offer <- offers) {
        val agentId = offer.getSlaveId.getValue
        val offerId = offer.getId.getValue
        val resources = remainingResources(offerId)

        if (canLaunchTask(agentId, offer.getHostname, resources)) {
          // Create a task
          launchTasks = true
          val taskId = newMesosTaskId()
          val offerCPUs = getResource(resources, "cpus").toInt
          val taskGPUs = Math.min(
            Math.max(0, maxGpus - totalGpusAcquired), getResource(resources, "gpus").toInt)

          val taskCPUs = executorCores(offerCPUs)
          val taskMemory = executorMemory(sc)

          agents.getOrElseUpdate(agentId, new Agent(offer.getHostname)).taskIDs.add(taskId)

          val (resourcesLeft, resourcesToUse) =
            partitionTaskResources(resources, taskCPUs, taskMemory, taskGPUs)

          val taskBuilder = MesosTaskInfo.newBuilder()
            .setTaskId(TaskID.newBuilder().setValue(taskId).build())
            .setSlaveId(offer.getSlaveId)
            .setCommand(createCommand(offer, taskCPUs + extraCoresPerExecutor, taskId))
            .setName(s"${sc.appName} $taskId")
            .setLabels(MesosProtoUtils.mesosLabels(taskLabels))
            .addAllResources(resourcesToUse.asJava)
            .setContainer(getContainerInfo(sc.conf))

          tasks(offer.getId) ::= taskBuilder.build()
          remainingResources(offerId) = resourcesLeft.asJava
          totalCoresAcquired += taskCPUs
          coresByTaskId(taskId) = taskCPUs
          if (taskGPUs > 0) {
            totalGpusAcquired += taskGPUs
            gpusByTaskId(taskId) = taskGPUs
          }
        } else {
          logDebug(s"Cannot launch a task for offer with id: $offerId on agent " +
            s"with id: $agentId. Requirements were not met for this offer.")
        }
      }
    }
    tasks.toMap
  }

  /** Extracts task needed resources from a list of available resources. */
  private def partitionTaskResources(
      resources: JList[Resource],
      taskCPUs: Int,
      taskMemory: Int,
      taskGPUs: Int)
    : (List[Resource], List[Resource]) = {

    // partition cpus & mem
    val (afterCPUResources, cpuResourcesToUse) = partitionResources(resources, "cpus", taskCPUs)
    val (afterMemResources, memResourcesToUse) =
      partitionResources(afterCPUResources.asJava, "mem", taskMemory)
    val (afterGPUResources, gpuResourcesToUse) =
      partitionResources(afterMemResources.asJava, "gpus", taskGPUs)

    // If user specifies port numbers in SparkConfig then consecutive tasks will not be launched
    // on the same host. This essentially means one executor per host.
    // TODO: handle network isolator case
    val (nonPortResources, portResourcesToUse) =
      partitionPortResources(nonZeroPortValuesFromConfig(sc.conf), afterGPUResources)

    (nonPortResources,
      cpuResourcesToUse ++ memResourcesToUse ++ portResourcesToUse ++ gpuResourcesToUse)
  }

  private def canLaunchTask(agentId: String, offerHostname: String,
                            resources: JList[Resource]): Boolean = {
    val offerMem = getResource(resources, "mem")
    val offerCPUs = getResource(resources, "cpus").toInt
    val cpus = executorCores(offerCPUs)
    val mem = executorMemory(sc)
    val ports = getRangeResource(resources, "ports")
    val meetsPortRequirements = checkPorts(sc.conf, ports)

    cpus > 0 &&
      cpus <= offerCPUs &&
      cpus + totalCoresAcquired <= maxCores &&
      mem <= offerMem &&
      numExecutors < executorLimit &&
      agents.get(agentId).map(_.taskFailures).getOrElse(0) < MAX_AGENT_FAILURES &&
      meetsPortRequirements &&
      satisfiesLocality(offerHostname)
  }

  private def executorCores(offerCPUs: Int): Int = {
    executorCoresOption.getOrElse(
      math.min(offerCPUs, maxCores - totalCoresAcquired)
    )
  }

  private def satisfiesLocality(offerHostname: String): Boolean = {
    val hostToLocalTaskCount =
      rpHostToLocalTaskCount.getOrElse(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, Map.empty)
    if (!Utils.isDynamicAllocationEnabled(conf) || hostToLocalTaskCount.isEmpty) {
      return true
    }

    // Check the locality information
    val currentHosts = agents.values.filter(_.taskIDs.nonEmpty).map(_.hostname).toSet
    val allDesiredHosts = hostToLocalTaskCount.map { case (k, v) => k }.toSet

    // Try to match locality for hosts which do not have executors yet, to potentially
    // increase coverage.
    val remainingHosts = allDesiredHosts -- currentHosts
    if (!remainingHosts.contains(offerHostname) &&
      (System.nanoTime() - localityWaitStartTimeNs <= localityWaitNs)) {
      logDebug("Skipping host and waiting for locality. host: " + offerHostname)
      return false
    }
    return true
  }

  override def statusUpdate(d: org.apache.mesos.SchedulerDriver, status: TaskStatus): Unit = {
    val taskId = status.getTaskId.getValue
    val agentId = status.getSlaveId.getValue
    val state = mesosToTaskState(status.getState)

    logInfo(s"Mesos task $taskId is now ${status.getState}")

    stateLock.synchronized {
      val agent = agents(agentId)

      // If the shuffle service is enabled, have the driver register with each one of the
      // shuffle services. This allows the shuffle services to clean up state associated with
      // this application when the driver exits. There is currently not a great way to detect
      // this through Mesos, since the shuffle services are set up independently.
      if (state.equals(TaskState.RUNNING) &&
          shuffleServiceEnabled &&
          !agent.shuffleRegistered) {
        assume(mesosExternalShuffleClient.isDefined,
          "External shuffle client was not instantiated even though shuffle service is enabled.")
        // TODO: Remove this and allow the MesosExternalShuffleService to detect
        // framework termination when new Mesos Framework HTTP API is available.
        val externalShufflePort = conf.get(config.SHUFFLE_SERVICE_PORT)

        logDebug(s"Connecting to shuffle service on agent $agentId, " +
            s"host ${agent.hostname}, port $externalShufflePort for app ${conf.getAppId}")

        mesosExternalShuffleClient.get
          .registerDriverWithShuffleService(
            agent.hostname,
            externalShufflePort,
            sc.conf.get(
              config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT
            ).getOrElse(Utils.timeStringAsMs(s"${sc.conf.get(Network.NETWORK_TIMEOUT)}s")),
            sc.conf.get(config.EXECUTOR_HEARTBEAT_INTERVAL))
        agent.shuffleRegistered = true
      }

      if (TaskState.isFinished(state)) {
        // Remove the cores we have remembered for this task, if it's in the hashmap
        for (cores <- coresByTaskId.get(taskId)) {
          totalCoresAcquired -= cores
          coresByTaskId -= taskId
        }
        // Also remove the gpus we have remembered for this task, if it's in the hashmap
        for (gpus <- gpusByTaskId.get(taskId)) {
          totalGpusAcquired -= gpus
          gpusByTaskId -= taskId
        }
        // If it was a failure, mark the agent as failed for excluding purposes
        if (TaskState.isFailed(state)) {
          agent.taskFailures += 1

          if (agent.taskFailures >= MAX_AGENT_FAILURES) {
            logInfo(s"Excluding Mesos agent $agentId due to too many failures; " +
                "is Spark installed on it?")
          }
        }
        executorTerminated(agentId, taskId, s"Executor finished with state $state")
        // In case we'd rejected everything before but have now lost a node
        d.reviveOffers()
      }
    }
  }

  override def error(d: org.apache.mesos.SchedulerDriver, message: String): Unit = {
    logError(s"Mesos error: $message")
    scheduler.error(message)
  }

  override def stop(): Unit = {
    stopSchedulerBackend()
    launcherBackend.setState(SparkAppHandle.State.FINISHED)
    launcherBackend.close()
  }

  private def stopSchedulerBackend(): Unit = {
    // Make sure we're not launching tasks during shutdown
    stateLock.synchronized {
      if (stopCalled) {
        logWarning("Stop called multiple times, ignoring")
        return
      }
      stopCalled = true
      super.stop()
    }

    // Wait for executors to report done, or else mesosDriver.stop() will forcefully kill them.
    // See SPARK-12330
    val startTime = System.nanoTime()

    // agentIdsWithExecutors has no memory barrier, so this is eventually consistent
    while (numExecutors() > 0 &&
      System.nanoTime() - startTime < shutdownTimeoutMS * 1000L * 1000L) {
      Thread.sleep(100)
    }

    if (numExecutors() > 0) {
      logWarning(s"Timed out waiting for ${numExecutors()} remaining executors "
        + s"to terminate within $shutdownTimeoutMS ms. This may leave temporary files "
        + "on the mesos nodes.")
    }

    // Close the mesos external shuffle client if used
    mesosExternalShuffleClient.foreach(_.close())

    if (schedulerDriver != null) {
      schedulerDriver.stop()
    }
  }

  override def frameworkMessage(
      d: org.apache.mesos.SchedulerDriver, e: ExecutorID, s: AgentID, b: Array[Byte]): Unit = {}

  /**
   * Called when a agent is lost or a Mesos task finished. Updates local view on
   * what tasks are running. It also notifies the driver that an executor was removed.
   */
  private def executorTerminated(
      agentId: String,
      taskId: String,
      reason: String): Unit = {
    stateLock.synchronized {
      // Do not call removeExecutor() after this scheduler backend was stopped because
      // removeExecutor() internally will send a message to the driver endpoint but
      // the driver endpoint is not available now, otherwise an exception will be thrown.
      if (!stopCalled) {
        removeExecutor(taskId, ExecutorProcessLost(reason))
      }
      agents(agentId).taskIDs.remove(taskId)
    }
  }

  override def agentLost(d: org.apache.mesos.SchedulerDriver, agentId: AgentID): Unit = {
    logInfo(s"Mesos agent lost: ${agentId.getValue}")
  }

  override def executorLost(
      d: org.apache.mesos.SchedulerDriver, e: ExecutorID, s: AgentID, status: Int): Unit = {
    logInfo("Mesos executor lost: %s".format(e.getValue))
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]
  ): Future[Boolean] = Future.successful {
    // We don't truly know if we can fulfill the full amount of executors
    // since at coarse grain it depends on the amount of agents available.
    val numExecs = resourceProfileToTotalExecs.getOrElse(defaultProfile, 0)
    logInfo("Capping the total amount of executors to " + numExecs)
    executorLimitOption = Some(numExecs)
    // Update the locality wait start time to continue trying for locality.
    localityWaitStartTimeNs = System.nanoTime()
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future.successful {
    if (schedulerDriver == null) {
      logWarning("Asked to kill executors before the Mesos driver was started.")
      false
    } else {
      for (executorId <- executorIds) {
        val taskId = TaskID.newBuilder().setValue(executorId).build()
        schedulerDriver.killTask(taskId)
      }
      // no need to adjust `executorLimitOption` since the AllocationManager already communicated
      // the desired limit through a call to `doRequestTotalExecutors`.
      // See [[o.a.s.scheduler.cluster.CoarseGrainedSchedulerBackend.killExecutors]]
      true
    }
  }

  override protected def createTokenManager(): Option[HadoopDelegationTokenManager] = {
    Some(new HadoopDelegationTokenManager(conf, sc.hadoopConfiguration, driverEndpoint))
  }

  private def numExecutors(): Int = {
    agents.values.map(_.taskIDs.size).sum
  }
}

private class Agent(val hostname: String) {
  val taskIDs = new mutable.HashSet[String]()
  var taskFailures = 0
  var shuffleRegistered = false
}

object IdHelper {
  // Use atomic values since Spark contexts can be initialized in parallel
  private[mesos] val nextSCNumber = new AtomicLong(0)
  private[mesos] val startedBefore = new AtomicBoolean(false)
}
