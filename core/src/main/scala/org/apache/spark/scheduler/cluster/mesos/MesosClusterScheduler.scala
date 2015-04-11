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
import java.util.concurrent.locks.ReentrantLock
import java.util.{Collections, Date, List => JList}

import org.apache.mesos.Protos.Environment.Variable
import org.apache.mesos.Protos.TaskStatus.Reason
import org.apache.mesos.Protos.{TaskState => MesosTaskState, _}
import org.apache.mesos.{Scheduler, SchedulerDriver}
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.deploy.rest.{CreateSubmissionResponse, KillSubmissionResponse, SubmissionStatusResponse}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf, SparkException, TaskState}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
 * Tracks the current state of a Mesos Task that runs a Spark driver.
 * @param submission Submitted driver description from
 *                   [[org.apache.spark.deploy.rest.mesos.MesosRestServer]]
 * @param taskId Mesos TaskID generated for the task
 * @param slaveId Slave ID that the task is assigned to
 * @param taskState The last known task status update.
 * @param startDate The date the task was launched
 */
private[spark] class MesosClusterTaskState(
    val submission: MesosDriverDescription,
    val taskId: TaskID,
    val slaveId: SlaveID,
    var taskState: Option[TaskStatus],
    var startDate: Date)
  extends Serializable {

  def copy(): MesosClusterTaskState = {
    new MesosClusterTaskState(
      submission, taskId, slaveId, taskState, startDate)
  }
}

/**
 * Tracks the retry state of a driver, which includes the next time it should be scheduled
 * and necessary information to do exponential backoff.
 * This class is not thread-safe, and we expect the caller to handle synchronizing state.
 * @param lastFailureStatus Last Task status when it failed.
 * @param retries Number of times it has retried.
 * @param nextRetry Next retry time to be scheduled.
 * @param waitTime The amount of time driver is scheduled to wait until next retry.
 */
private[spark] class RetryState(
    val lastFailureStatus: TaskStatus,
    val retries: Int,
    val nextRetry: Date,
    val waitTime: Int) extends Serializable {
  def copy(): RetryState =
    new RetryState(lastFailureStatus, retries, nextRetry, waitTime)
}

/**
 * The full state of the cluster scheduler, currently being used for displaying
 * information on the UI.
 * @param frameworkId Mesos Framework id for the cluster scheduler.
 * @param masterUrl The Mesos master url
 * @param queuedDrivers All drivers queued to be launched
 * @param launchedDrivers All launched or running drivers
 * @param finishedDrivers All terminated drivers
 * @param retryList All drivers pending to be retried
 */
private[spark] class MesosClusterSchedulerState(
    val frameworkId: String,
    val masterUrl: Option[String],
    val queuedDrivers: Iterable[MesosDriverDescription],
    val launchedDrivers: Iterable[MesosClusterTaskState],
    val finishedDrivers: Iterable[MesosClusterTaskState],
    val retryList: Iterable[MesosDriverDescription])

/**
 * A Mesos scheduler that is responsible for launching submitted Spark drivers in cluster mode
 * as Mesos tasks in a Mesos cluster.
 * All drivers are launched asynchronously by the framework, which will eventually be launched
 * by one of the slaves in the cluster. The results of the driver will be stored in slave's task
 * sandbox which is accessible by visiting the Mesos UI.
 * This scheduler supports recovery by persisting all its state and performs task reconciliation
 * on recover, which gets all the latest state for all the drivers from Mesos master.
 */
private[spark] class MesosClusterScheduler(
    engineFactory: MesosClusterPersistenceEngineFactory,
    conf: SparkConf)
  extends Scheduler with MesosSchedulerUtils {
  var frameworkUrl: String = _

  private val metricsSystem =
    MetricsSystem.createMetricsSystem("mesos_cluster", conf, new SecurityManager(conf))
  private val master = conf.get("spark.master")
  private val appName = conf.get("spark.app.name")
  private val queuedCapacity = conf.getInt("spark.deploy.mesos.queuedDrivers", 200)
  private val retainedDrivers = conf.getInt("spark.deploy.retainedDrivers", 200)
  private val maxRetryWaitTime = conf.getInt("spark.mesos.cluster.retry.wait.max", 60) // 1 minute
  private val schedulerState = engineFactory.createEngine("scheduler")
  private val stateLock = new ReentrantLock()
  private val finishedDrivers = new mutable.ArrayBuffer[MesosClusterTaskState](retainedDrivers)
  private var frameworkId: String = null
  val launchedDrivers = new mutable.HashMap[String, MesosClusterTaskState]()
  // Holds the list of tasks that needs to reconcile with Mesos master.
  // All states that are loaded after failover are added here.
  private val pendingRecover = new mutable.HashMap[String, SlaveID]()
  // A queue that stores all the submitted drivers that hasn't been launched.
  val queuedDrivers = new ArrayBuffer[MesosDriverDescription]()
  // All supervised drivers that are waiting to retry after termination.
  val pendingRetryDrivers = new ArrayBuffer[MesosDriverDescription]()
  private val queuedDriversState = engineFactory.createEngine("driverQueue")
  private val launchedDriversState = engineFactory.createEngine("launchedDrivers")
  private val pendingRetryDriversState = engineFactory.createEngine("retryList")
  // Flag to mark if the scheduler is ready to be called, which is until the scheduler
  // is registered with Mesos master.
  protected var ready = false
  private var masterInfo: Option[MasterInfo] = None

  private def isQueueFull(): Boolean = launchedDrivers.size >= queuedCapacity

  def submitDriver(desc: MesosDriverDescription): CreateSubmissionResponse = {
    val c = new CreateSubmissionResponse
    if (!ready) {
      c.success = false
      c.message = "Scheduler is not ready to take requests"
      return c
    }

    stateLock.synchronized {
      if (isQueueFull()) {
        c.success = false
        c.message = "Already reached maximum submission size"
      }
      c.submissionId = desc.submissionId
      queuedDrivers += desc
      c.success = true
    }
    c
  }

  private def removeFromQueuedDrivers(id: String): Boolean = {
    val index = queuedDrivers.indexWhere(_.submissionId.equals(id))
    if (index != -1) {
      queuedDrivers.remove(index)
      queuedDriversState.expunge(id)
      true
    } else {
      false
    }
  }

  private def removeFromLaunchedDrivers(id: String): Boolean = {
    if (launchedDrivers.remove(id).isDefined) {
      launchedDriversState.expunge(id)
      true
    } else {
      false
    }
  }

  private def removeFromPendingRetryDrivers(id: String): Boolean = {
    val index = pendingRetryDrivers.indexWhere(_.submissionId.equals(id))
    if (index != -1) {
      pendingRetryDrivers.remove(index)
      pendingRetryDriversState.expunge(id)
      true
    } else {
      false
    }
  }

  def killDriver(submissionId: String): KillSubmissionResponse = {
    val k = new KillSubmissionResponse
    if (!ready) {
      k.success = false
      k.message = "Scheduler is not ready to take requests"
      return k
    }
    k.submissionId = submissionId
    stateLock.synchronized {
      // We look for the requested driver in the following places:
      // 1. Check if submission is running or launched.
      // 2. Check if it's still queued.
      // 3. Check if it's in the retry list.
      // 4. Check if it has already completed.
      if (launchedDrivers.contains(submissionId)) {
        val task = launchedDrivers(submissionId)
        driver.killTask(task.taskId)
        k.success = true
        k.message = "Killing running driver"
      } else if (removeFromQueuedDrivers(submissionId)) {
        k.success = true
        k.message = "Removed driver while it's still pending"
      } else if (removeFromPendingRetryDrivers(submissionId)) {
        k.success = true
        k.message = "Removed driver while it's retrying"
      } else if (finishedDrivers.exists(_.submission.submissionId.equals(submissionId))) {
        k.success = false
        k.message = "Driver already terminated"
      } else {
        k.success = false
        k.message = "Cannot find driver"
      }
    }
    k
  }

  /**
   * Recover scheduler state that is persisted.
   * We still need to do task reconciliation to be up to date of the latest task states
   * as it might have changed while the scheduler is failing over.
   */
  private def recoverState(): Unit = {
    stateLock.synchronized {
      launchedDriversState.fetchAll[MesosClusterTaskState]().foreach { state =>
        launchedDrivers(state.taskId.getValue) = state
        pendingRecover(state.taskId.getValue) = state.slaveId
      }
      queuedDriversState.fetchAll[MesosDriverDescription]().foreach(d => queuedDrivers += d)
      // There is potential timing issue where a queued driver might have been launched
      // but the scheduler shuts down before the queued driver was able to be removed
      // from the queue. We try to mitigate this issue by walking through all queued drivers
      // and remove if they're already launched.
      queuedDrivers
        .filter(d => launchedDrivers.contains(d.submissionId))
        .toSeq
        .foreach(d => removeFromQueuedDrivers(d.submissionId))
      pendingRetryDriversState.fetchAll[MesosDriverDescription]()
        .foreach(s => pendingRetryDrivers += s)
      // TODO: Consider storing finished drivers so we can show them on the UI after
      // failover. For now we clear the history on each recovery.
      finishedDrivers.clear()
    }
  }

  /**
   * Starts the cluster scheduler and wait until the scheduler is registered.
   * This also marks the scheduler to be ready for requests.
   */
  def start(): Unit = {
    // TODO: Implement leader election to make sure only one framework running in the cluster.
    val fwId = schedulerState.fetch[String]("frameworkId")
    val builder = FrameworkInfo.newBuilder()
      .setUser(Utils.getCurrentUserName())
      .setName(appName)
      .setWebuiUrl(frameworkUrl)
      .setCheckpoint(true)
      .setFailoverTimeout(Integer.MAX_VALUE) // Setting to max so tasks keep running on crash
    fwId.foreach { id =>
      builder.setId(FrameworkID.newBuilder().setValue(id).build())
      frameworkId = id
    }
    recoverState()
    metricsSystem.registerSource(new MesosClusterSchedulerSource(this))
    metricsSystem.start()
    startScheduler(
      "MesosClusterScheduler", master, MesosClusterScheduler.this, builder.build())
    ready = true
  }

  def stop(): Unit = {
    ready = false
    metricsSystem.report()
    metricsSystem.stop()
    driver.stop(true)
  }

  override def registered(
      driver: SchedulerDriver,
      newFrameworkId: FrameworkID,
      masterInfo: MasterInfo): Unit = {
    logInfo("Registered as framework ID " + newFrameworkId.getValue)
    if (newFrameworkId.getValue != frameworkId) {
      frameworkId = newFrameworkId.getValue
      schedulerState.persist("frameworkId", frameworkId)
    }
    markRegistered()

    stateLock.synchronized {
      this.masterInfo = Some(masterInfo)
      if (!pendingRecover.isEmpty) {
        // Start task reconciliation if we need to recover.
        val statuses = pendingRecover.collect {
          case (taskId, slaveId) =>
            val newStatus = TaskStatus.newBuilder()
              .setTaskId(TaskID.newBuilder().setValue(taskId).build())
              .setSlaveId(slaveId)
              .setState(MesosTaskState.TASK_STAGING)
              .build()
            launchedDrivers.get(taskId).map(_.taskState.getOrElse(newStatus))
              .getOrElse(newStatus)
        }
        // TODO: Page the status updates to avoid trying to reconcile
        // a large amount of tasks at once.
        driver.reconcileTasks(statuses)
      }
    }
  }

  private def buildDriverCommand(desc: MesosDriverDescription): CommandInfo = {
    val appJar = CommandInfo.URI.newBuilder()
      .setValue(desc.jarUrl.stripPrefix("file:").stripPrefix("local:")).build()
    val builder = CommandInfo.newBuilder().addUris(appJar)
    val entries =
      (conf.getOption("spark.executor.extraLibraryPath").toList ++
        desc.command.libraryPathEntries)
    val prefixEnv = if (!entries.isEmpty) {
      Utils.libraryPathEnvPrefix(entries)
    } else {
      ""
    }
    val envBuilder = Environment.newBuilder()
    desc.command.environment.foreach { case (k, v) =>
      envBuilder.addVariables(Variable.newBuilder().setName(k).setValue(v).build())
    }
    // Pass all spark properties to executor.
    val executorOpts = desc.schedulerProperties.map { case (k, v) => s"-D$k=$v" }.mkString(" ")
    envBuilder.addVariables(
      Variable.newBuilder().setName("SPARK_EXECUTOR_OPTS").setValue(executorOpts))
    val cmdOptions = generateCmdOption(desc)
    val executorUri = desc.schedulerProperties.get("spark.executor.uri")
      .orElse(desc.command.environment.get("SPARK_EXECUTOR_URI"))
    val appArguments = desc.command.arguments.mkString(" ")
    val cmd = if (executorUri.isDefined) {
      builder.addUris(CommandInfo.URI.newBuilder().setValue(executorUri.get).build())
      val folderBasename = executorUri.get.split('/').last.split('.').head
      val cmdExecutable = s"cd $folderBasename*; $prefixEnv bin/spark-submit"
      val cmdJar = s"../${desc.jarUrl.split("/").last}"
      s"$cmdExecutable ${cmdOptions.mkString(" ")} $cmdJar $appArguments"
    } else {
      val executorSparkHome = desc.schedulerProperties.get("spark.mesos.executor.home")
        .orElse(conf.getOption("spark.home"))
        .orElse(Option(System.getenv("SPARK_HOME")))
        .getOrElse {
          throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
        }
      val cmdExecutable = new File(executorSparkHome, "./bin/spark-submit").getCanonicalPath
      val cmdJar = desc.jarUrl.split("/").last
      s"$cmdExecutable ${cmdOptions.mkString(" ")} $cmdJar $appArguments"
    }
    builder.setValue(cmd)
    builder.setEnvironment(envBuilder.build())
    builder.build()
  }

  private def generateCmdOption(desc: MesosDriverDescription): Seq[String] = {
    var options = Seq(
      "--name", desc.schedulerProperties("spark.app.name"),
      "--class", desc.command.mainClass,
      "--master", s"mesos://${conf.get("spark.master")}",
      "--driver-cores", desc.cores.toString,
      "--driver-memory", s"${desc.mem}M")
    desc.schedulerProperties.get("spark.executor.memory").map { v =>
      options ++= Seq("--executor-memory", v)
    }
    desc.schedulerProperties.get("spark.cores.max").map { v =>
      options ++= Seq("--total-executor-cores", v)
    }
    options
  }

  private class ResourceOffer(val offer: Offer, var cpu: Double, var mem: Double) {
    override def toString(): String = {
      s"Offer id: ${offer.getId.getValue}, cpu: $cpu, mem: $mem"
    }
  }

  /**
   * This method takes all the possible candidates provided by the tasksFunc
   * and attempt to schedule them with Mesos offers.
   * Every time a new task is scheduled, the scheduledCallback is called to
   * perform post scheduled logic on each task.
   */
  private def scheduleTasks(
      candidates: Seq[MesosDriverDescription],
      scheduledCallback: (String) => Boolean,
      currentOffers: List[ResourceOffer],
      tasks: mutable.HashMap[OfferID, ArrayBuffer[TaskInfo]],
      currentTime: Date): Unit = {
    for (submission <- candidates) {
      val driverCpu = submission.cores
      val driverMem = submission.mem
      logTrace(s"Finding offer to launch driver with cpu: $driverCpu, mem: $driverMem")
      val offerOption = currentOffers.find { o =>
        o.cpu >= driverCpu && o.mem >= driverMem
      }
      if (offerOption.isEmpty) {
        logDebug(s"Unable to find offer to launch driver id: ${submission.submissionId}," +
          s"cpu: $driverCpu, mem: $driverMem")
      } else {
        val offer = offerOption.get
        offer.cpu -= driverCpu
        offer.mem -= driverMem
        val taskId = TaskID.newBuilder().setValue(submission.submissionId).build()
        val cpuResource = Resource.newBuilder()
          .setName("cpus").setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder().setValue(driverCpu)).build()
        val memResource = Resource.newBuilder()
          .setName("mem").setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder().setValue(driverMem)).build()
        val commandInfo = buildDriverCommand(submission)
        val appName = submission.schedulerProperties("spark.app.name")
        val taskInfo = TaskInfo.newBuilder()
          .setTaskId(taskId)
          .setName(s"Driver for $appName")
          .setSlaveId(offer.offer.getSlaveId)
          .setCommand(commandInfo)
          .addResources(cpuResource)
          .addResources(memResource)
          .build()
        val queuedTasks = if (!tasks.contains(offer.offer.getId)) {
          val buffer = new ArrayBuffer[TaskInfo]
          tasks(offer.offer.getId) = buffer
          buffer
        } else {
          tasks(offer.offer.getId)
        }
        queuedTasks += taskInfo
        logTrace(s"Using offer ${offer.offer.getId.getValue} to launch driver " +
          submission.submissionId)
        val newState = new MesosClusterTaskState(submission, taskId, offer.offer.getSlaveId,
          None, new Date())
        launchedDrivers(submission.submissionId) = newState
        launchedDriversState.persist(submission.submissionId, newState)
        scheduledCallback(submission.submissionId)
      }
    }
  }

  override def resourceOffers(driver: SchedulerDriver, offers: JList[Offer]): Unit = {
    /**
     * Prints the list of Mesos offers for logging purpose.
     */
    def printOffers(offers: Iterable[ResourceOffer]): String = {
      val builder = new StringBuilder()
      offers.foreach { o =>
        builder.append(o).append("\n")
      }
      builder.toString()
    }
    val currentOffers = offers.map { o =>
      new ResourceOffer(
        o, getResource(o.getResourcesList, "cpus"), getResource(o.getResourcesList, "mem"))
    }.toList
    logTrace(s"Received offers from Mesos: \n${printOffers(currentOffers)}")
    val tasks = new mutable.HashMap[OfferID, ArrayBuffer[TaskInfo]]()
    val currentTime = new Date()

    stateLock.synchronized {
      // We first schedule all the supervised drivers that are ready to retry.
      // This list will be empty if none of the drivers are marked as supervise.
      scheduleTasks(pendingRetryDrivers
        .filter(d => d.retryState.get.nextRetry.before(currentTime)),
        removeFromPendingRetryDrivers,
        currentOffers, tasks, currentTime)
      // Then we walk through the queued drivers and try to schedule them.
      scheduleTasks(queuedDrivers,
        removeFromQueuedDrivers,
        currentOffers, tasks, currentTime)
    }
    tasks.foreach { case (offerId, tasks) =>
      driver.launchTasks(Collections.singleton(offerId), tasks)
    }
    offers
      .filter(o => !tasks.keySet.contains(o.getId))
      .foreach(o => driver.declineOffer(o.getId))
  }

  def getState(): MesosClusterSchedulerState = {
    def copyBuffer(
      buffer: ArrayBuffer[MesosDriverDescription]): ArrayBuffer[MesosDriverDescription] = {
      val newBuffer = new ArrayBuffer[MesosDriverDescription](buffer.size)
      buffer.copyToBuffer(newBuffer)
      newBuffer
    }
    stateLock.synchronized {
      new MesosClusterSchedulerState(
        frameworkId,
        masterInfo.map(m => s"http://${m.getIp}:${m.getPort}"),
        copyBuffer(queuedDrivers),
        launchedDrivers.values.map(_.copy).toList,
        finishedDrivers.map(_.copy).toList,
        copyBuffer(pendingRetryDrivers))
    }
  }

  def getStatus(submissionId: String): SubmissionStatusResponse = {
    val s = new SubmissionStatusResponse
    if (!ready) {
      s.success = false
      s.message = "Scheduler is not ready to take requests"
      return s
    }
    s.submissionId = submissionId
    stateLock.synchronized {
      if (queuedDrivers.exists(_.submissionId.equals(submissionId))) {
        s.success = true
        s.driverState = "Driver is queued for launch"
      } else if (launchedDrivers.contains(submissionId)) {
        s.success = true
        s.driverState = "Driver is running"
        launchedDrivers(submissionId).taskState.foreach(state => s.message = state.toString)
      } else if (finishedDrivers.exists(s => s.submission.submissionId.equals(submissionId))) {
        s.success = true
        s.driverState = "Driver already finished"
        finishedDrivers.find(d => d.submission.submissionId.equals(submissionId)).get.taskState
          .foreach(state => s.message = state.toString)
      } else if (pendingRetryDrivers.exists(_.submissionId.equals(submissionId))) {
        val status = pendingRetryDrivers.find(_.submissionId.equals(submissionId))
          .get.retryState.get.lastFailureStatus
        s.success = true
        s.driverState = "Driver failed and retrying"
        s.message = status.toString
      } else {
        s.success = false
        s.driverState = "Driver not found"
      }
    }
    s
  }

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {}
  override def disconnected(driver: SchedulerDriver): Unit = {}
  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = {
    logInfo(s"Framework re-registered with master ${masterInfo.getId}")
  }
  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = {}
  override def error(driver: SchedulerDriver, error: String): Unit = {}

  /**
   * Check if the task state is a recoverable state that we can relaunch the task.
   * Task state like TASK_ERROR are not relaunchable state since it wasn't able
   * to be validated by Mesos.
   */
  def shouldRelaunch(state: MesosTaskState): Boolean = {
    state == MesosTaskState.TASK_FAILED ||
      state == MesosTaskState.TASK_KILLED ||
      state == MesosTaskState.TASK_LOST
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    val taskId = status.getTaskId.getValue
    stateLock.synchronized {
      if (launchedDrivers.contains(taskId)) {
        if (status.getReason == Reason.REASON_RECONCILIATION &&
          !pendingRecover.contains(taskId)) {
          // Task has already received update and no longer requires reconciliation.
          return
        }
        val state = launchedDrivers(taskId)
        // Check if the driver is supervise enabled and can be relaunched.
        if (state.submission.supervise && shouldRelaunch(status.getState)) {
          removeFromLaunchedDrivers(taskId)
          val retryState: Option[RetryState] = state.submission.retryState
          val (retries, waitTimeSec) = if (retryState.isDefined) {
            (retryState.get.retries + 1,
              Math.min(maxRetryWaitTime, retryState.get.waitTime * 2))
          } else {
            (1, 1)
          }
          val nextRetry = new Date(new Date().getTime + waitTimeSec * 1000L)

          val newDriverDescription = state.submission.copy(
            retryState = Some(new RetryState(status, retries, nextRetry, waitTimeSec)))
          pendingRetryDrivers += newDriverDescription
          pendingRetryDriversState.persist(taskId, newDriverDescription)
        } else if (TaskState.isFinished(TaskState.fromMesos(status.getState))) {
          removeFromLaunchedDrivers(taskId)
          if (finishedDrivers.size >= retainedDrivers) {
            val toRemove = math.max(retainedDrivers / 10, 1)
            finishedDrivers.trimStart(toRemove)
          }
          finishedDrivers += state
        }
        state.taskState = Option(status)
      } else {
        logError(s"Unable to find driver $taskId in status update")
      }
    }
  }

  override def frameworkMessage(
      driver: SchedulerDriver,
      executorId: ExecutorID,
      slaveId: SlaveID,
      message: Array[Byte]): Unit = {}

  override def executorLost(
      driver: SchedulerDriver,
      executorId: ExecutorID,
      slaveId: SlaveID,
      status: Int): Unit = {}
}
