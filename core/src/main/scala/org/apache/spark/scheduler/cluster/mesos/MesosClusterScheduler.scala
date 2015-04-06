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
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.{Collections, Date, List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.mesos.Protos.Environment.Variable
import org.apache.mesos.Protos.TaskStatus.Reason
import org.apache.mesos.Protos.{TaskState => MesosTaskState, _}
import org.apache.mesos.{Scheduler, SchedulerDriver}

import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.deploy.rest.{CreateSubmissionResponse, KillSubmissionResponse, SubmissionStatusResponse}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkException, SecurityManager, TaskState}


/**
 * Tracks the current state of a Mesos Task that runs a Spark driver.
 * @param submission Submitted driver description from
 *                   [[org.apache.spark.deploy.rest.mesos.MesosRestServer]]
 * @param taskId Mesos TaskID generated for the task
 * @param slaveId Slave ID that the task is assigned to
 * @param taskState The last known task status update.
 * @param startDate The date the task was launched
 * @param retryState Retry state for this task (only applicable to supervised drivers)
 */
private[spark] class MesosClusterTaskState(
    val submission: MesosDriverDescription,
    val taskId: TaskID,
    val slaveId: SlaveID,
    var taskState: Option[TaskStatus],
    var startDate: Date,
    val retryState: Option[RetryState] = None)
  extends Serializable {

  def copy(): MesosClusterTaskState = {
    new MesosClusterTaskState(
      submission, taskId, slaveId, taskState, startDate, retryState)
  }
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
    val retryList: Iterable[RetryState])

/**
 * Mesos cluster scheduler for running, killing and requesting
 * status of Spark drivers that will be launched in a Mesos cluster.
 * This interface is mainly for [[org.apache.spark.deploy.rest.mesos.MesosRestServer]] and
 * [[org.apache.spark.deploy.mesos.ui.MesosClusterPage]] to interact with
 * [[MesosClusterSchedulerDriver]] and hide all the Mesos specific methods that it doesn't
 * need to care about.
 */
private[spark] trait MesosClusterScheduler {
  def submitDriver(desc: MesosDriverDescription): CreateSubmissionResponse
  def killDriver(submissionId: String): KillSubmissionResponse
  def getStatus(submissionId: String): SubmissionStatusResponse
  def getState(): MesosClusterSchedulerState
}

/**
 * A Mesos scheduler that is responsible for launching submitted Spark drivers in cluster mode
 * as Mesos tasks in a Mesos cluster.
 * All drivers are launched asynchronously by the framework, which will eventually be launched
 * by one of the slaves in the cluster. The results of the driver will be stored in slave's task
 * sandbox which is accessible by visiting the Mesos UI.
 * This scheduler supports recovery by persisting all its state and performs task reconciliation
 * on recover, which gets all the latest state for all the drivers from Mesos master.
 */
private[spark] class MesosClusterSchedulerDriver(
    engineFactory: MesosClusterPersistenceEngineFactory,
    conf: SparkConf)
  extends Scheduler with MesosSchedulerUtils with MesosClusterScheduler {

  var frameworkUrl: String = _

  private val metricsSystem =
    MetricsSystem.createMetricsSystem("mesos_cluster", conf, new SecurityManager(conf))
  private val master = conf.get("spark.master")
  private val appName = conf.get("spark.app.name")
  private val queuedCapacity = conf.getInt("spark.deploy.mesos.queuedDrivers", 200)
  private val retainedDrivers = conf.getInt("spark.deploy.retainedDrivers", 200)
  private val maxRetryWaitTime = conf.getInt("spark.mesos.cluster.retry.wait.max", 60) // 1 minute
  private val state = engineFactory.createEngine("scheduler")
  private val stateLock = new ReentrantLock()
  private val finishedDrivers = new mutable.ArrayBuffer[MesosClusterTaskState](retainedDrivers)
  private val nextDriverNumber: AtomicLong = new AtomicLong(0)
  private var frameworkId: String = null

  // Stores all the launched and running drivers' states.
  var launchedDrivers: LaunchedDrivers = _

  // A queue that stores all the submitted drivers that hasn't been launched.
  var queue: DriverQueue = _

  // All supervised drivers that are waiting to retry after termination.
  var superviseRetryList: SuperviseRetryList = _

  private var masterInfo: Option[MasterInfo] = None

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")  // For application IDs
  private def newDriverId(submitDate: Date): String = {
    "driver-%s-%04d".format(
        createDateFormat.format(submitDate), nextDriverNumber.incrementAndGet())
  }

  def submitDriver(desc: MesosDriverDescription): CreateSubmissionResponse = {
    val c = new CreateSubmissionResponse
    val submitDate = new Date()
    desc.submissionId = Some(newDriverId(submitDate))
    desc.submissionDate = Some(submitDate)

    stateLock.synchronized {
      if (queue.isFull) {
        c.success = false
        c.message = "Already reached maximum submission size"
      }
      c.submissionId = desc.submissionId.get
      queue.offer(desc)
      c.success = true
    }
    c
  }

  def killDriver(submissionId: String): KillSubmissionResponse = {
    val k = new KillSubmissionResponse
    k.submissionId = submissionId
    stateLock.synchronized {
      // We look for the requested driver in the following places:
      // 1. Check if submission is running or launched.
      // 2. Check if it's still queued.
      // 3. Check if it's in the retry list.
      // 4. Check if it has already completed.
      if (launchedDrivers.contains(submissionId)) {
        val task = launchedDrivers.get(submissionId)
        driver.killTask(task.taskId)
        k.success = true
        k.message = "Killing running driver"
      } else if (queue.remove(submissionId)) {
        k.success = true
        k.message = "Removed driver while it's still pending"
      } else if (superviseRetryList.remove(submissionId)) {
        k.success = true
        k.message = "Removed driver while it's retrying"
      } else if (finishedDrivers.exists(s => s.submission.submissionId.equals(submissionId))) {
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
  def recoverState: Unit = {
    stateLock.synchronized {
      queue = new DriverQueue(engineFactory.createEngine("driverQueue"), queuedCapacity)
      launchedDrivers = new LaunchedDrivers(engineFactory.createEngine("launchedDrivers"))
      // There is potential timing issue where a queued driver might have been launched
      // but the scheduler shuts down before the queued driver was able to be removed
      // from the queue. We try to mitigate this issue by walking through all queued drivers
      // and remove if they're already launched.
      queue.drivers.map(_.submissionId.get).filter(launchedDrivers.contains).foreach(queue.remove)

      superviseRetryList = new SuperviseRetryList(engineFactory.createEngine("retryList"))
      // TODO: Consider storing finished drivers so we can show them on the UI after
      // failover. For now we clear the history on each recovery.
      finishedDrivers.clear()
    }
  }

  def start(): Unit = {
    // TODO: Implement leader election to make sure only one framework running in the cluster.
    val fwId = state.fetch[String]("frameworkId")
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
    recoverState
    metricsSystem.registerSource(new MesosClusterSchedulerSource(this))
    metricsSystem.start()
    startScheduler(
      "MesosClusterScheduler", master, MesosClusterSchedulerDriver.this, builder.build())
  }

  def stop(): Unit = {
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
      state.persist("frameworkId", frameworkId)
    }
    markRegistered()

    stateLock.synchronized {
      this.masterInfo = Some(masterInfo)
      if (!launchedDrivers.pendingRecover.isEmpty) {
        // Start task reconciliation if we need to recover.
        val statuses = launchedDrivers.pendingRecover.collect {
          case (taskId, slaveId) =>
            launchedDrivers.get(taskId).taskState.getOrElse(
              TaskStatus.newBuilder()
                .setTaskId(TaskID.newBuilder().setValue(taskId).build())
                .setSlaveId(slaveId)
                .setState(MesosTaskState.TASK_STAGING)
                .build())
        }
        // TODO: Page the status updates to avoid trying to reconcile
        // a large amount of tasks at once.
        driver.reconcileTasks(statuses)
      }
    }
  }

  private def buildCommand(desc: MesosDriverDescription): CommandInfo = {
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
    val executorOpts = desc.schedulerProperties.map { case (k, v) => s"-D$k=$v"}.mkString(" ")
    // Pass all spark properties to executor.
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
    builder.build
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

  override def resourceOffers(driver: SchedulerDriver, offers: JList[Offer]): Unit = {
    def printOffers(offers: Iterable[ResourceOffer]): String = {
      val builder = new StringBuilder()
      offers.foreach { o =>
        builder.append(o).append("\n")
      }
      builder.toString()
    }

    val currentOffers = offers.map { o =>
      new ResourceOffer(
        o,
        getResource(o.getResourcesList, "cpus"),
        getResource(o.getResourcesList, "mem"))
    }
    logTrace(s"Received offers from Mesos: \n${printOffers(currentOffers)}")
    val tasks = new mutable.HashMap[OfferID, ArrayBuffer[TaskInfo]]()
    val currentTime = new Date()
    def scheduleTasks(
        tasksFunc: () => Seq[(MesosDriverDescription, Option[RetryState])],
        scheduledCallback: (String) => Unit): Unit = {
      val candidates = tasksFunc()
      for ((submission, retryState) <- candidates) {
        val driverCpu = submission.cores
        val driverMem = submission.mem
        logTrace(s"Finding offer to launch driver with cpu: $driverCpu, mem: $driverMem")
        val offerOption = currentOffers.find { o =>
          o.cpu >= driverCpu && o.mem >= driverMem
        }

        if (offerOption.isEmpty) {
          logDebug(s"Unable to find offer to launch driver id: ${submission.submissionId.get}," +
              s"cpu: $driverCpu, mem: $driverMem")
        } else {
          val offer = offerOption.get
          offer.cpu -= driverCpu
          offer.mem -= driverMem
          val taskId = TaskID.newBuilder().setValue(submission.submissionId.get).build()
          val cpuResource = Resource.newBuilder()
            .setName("cpus").setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(driverCpu)).build()
          val memResource = Resource.newBuilder()
            .setName("mem").setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(driverMem)).build()
          val commandInfo = buildCommand(submission)
          val appName = submission.schedulerProperties("spark.app.name")
          val taskInfo = TaskInfo.newBuilder()
            .setTaskId(taskId)
            .setName(s"Driver for $appName")
            .setSlaveId(offer.offer.getSlaveId)
            .setCommand(commandInfo)
            .addResources(cpuResource)
            .addResources(memResource)
            .build
          val queuedTasks = if (!tasks.contains(offer.offer.getId)) {
            val buffer = new ArrayBuffer[TaskInfo]
            tasks(offer.offer.getId) = buffer
            buffer
          } else {
            tasks(offer.offer.getId)
          }
          queuedTasks += taskInfo
          logTrace(s"Using offer ${offer.offer.getId.getValue} to launch driver " +
            submission.submissionId.get)

          launchedDrivers.set(
            submission.submissionId.get,
            new MesosClusterTaskState(submission, taskId, offer.offer.getSlaveId,
              None, new Date(), retryState))
          scheduledCallback(submission.submissionId.get)
        }
      }
    }

    stateLock.synchronized {
      scheduleTasks(() => {
        superviseRetryList.getNextRetries(currentTime)
      }, (id: String) => {
        superviseRetryList.remove(id)
      })
      scheduleTasks(() => queue.drivers.map(d => (d, None)), (id) => queue.remove(id))
    }

    tasks.foreach { case (offerId, tasks) =>
      driver.launchTasks(Collections.singleton(offerId), tasks)
    }

    offers
      .filter(o => !tasks.keySet.contains(o.getId))
      .foreach(o => driver.declineOffer(o.getId))
  }

  def getState(): MesosClusterSchedulerState = {
    stateLock.synchronized {
      new MesosClusterSchedulerState(
        frameworkId,
        masterInfo.map(m => s"http://${m.getIp}:${m.getPort}"),
        queue.copyDrivers,
        launchedDrivers.states,
        finishedDrivers.collect { case s => s.copy() },
        superviseRetryList.retries)
    }
  }

  def getStatus(submissionId: String): SubmissionStatusResponse = {
    val s = new SubmissionStatusResponse
    s.submissionId = submissionId
    stateLock.synchronized {
      if (queue.contains(submissionId)) {
        s.success = true
        s.driverState = "Driver is queued for launch"
      } else if (launchedDrivers.contains(submissionId)) {
        s.success = true
        s.driverState = "Driver is running"
        launchedDrivers.get(submissionId).taskState.foreach(state => s.message = state.toString)
      } else if (finishedDrivers.exists(s => s.submission.submissionId.equals(submissionId))) {
        s.success = true
        s.driverState = "Driver already finished"
        finishedDrivers.find(d => d.submission.submissionId.equals(submissionId)).get.taskState
          .foreach(state => s.message = state.toString)
      } else if (superviseRetryList.contains(submissionId)) {
        val status = superviseRetryList.get(submissionId).get.lastFailureStatus
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
          !launchedDrivers.pendingRecover.contains(taskId)) {
          // Task has already received update and no longer requires reconciliation.
          return
        }
        val state = launchedDrivers.get(taskId)
        // Check if the driver is supervise enabled and can be relaunched.
        if (state.submission.supervise && shouldRelaunch(status.getState)) {
          val (retries, waitTimeSec) = if (state.retryState.isDefined) {
            (state.retryState.get.retries + 1,
              Math.min(maxRetryWaitTime, state.retryState.get.waitTime * 2))
          } else {
            (1, 1)
          }
          val nextRetry = new Date(new Date().getTime + waitTimeSec * 1000L)
          superviseRetryList.add(
            RetryState(state.submission, status, retries, nextRetry, waitTimeSec))
        }
        if (TaskState.isFinished(TaskState.fromMesos(status.getState))) {
          launchedDrivers.remove(taskId)
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
