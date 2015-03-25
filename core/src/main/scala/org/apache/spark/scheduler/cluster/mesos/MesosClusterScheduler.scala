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
import java.util.{Date, List => JList}

import org.apache.mesos.{SchedulerDriver, Scheduler}
import org.apache.mesos.Protos._
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import scala.concurrent.duration.Duration
import org.apache.mesos.Protos.Environment.Variable
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.mesos.Protos.TaskStatus.Reason

private[spark] class DriverSubmission(
    val submissionId: String,
    val desc: MesosDriverDescription,
    val submitDate: Date) extends Serializable {

  def canEqual(other: Any): Boolean = other.isInstanceOf[DriverSubmission]

  override def equals(other: Any): Boolean = other match {
    case that: DriverSubmission =>
      (that canEqual this) &&
        submissionId == that.submissionId
    case _ => false
  }
}

private [spark] case class ClusterTaskState(
    val submission: DriverSubmission,
    val taskId: TaskID,
    val slaveId: SlaveID,
    var taskState: Option[TaskStatus],
    var driverState: DriverState,
    var startDate: Date,
    val lastRetry: Option[RetryState] = None) extends Serializable {

  def copy(): ClusterTaskState = {
    ClusterTaskState(submission, taskId, slaveId, taskState, driverState, startDate, lastRetry)
  }
}

private[spark] case class SubmitResponse(id: String, success: Boolean, message: String)

private[spark] case class StatusResponse(
    id: String,
    success: Boolean,
    state: String,
    status: Option[TaskStatus] = None)

private[spark] case class KillResponse(id: String, success: Boolean, message: String)

private[spark] case class ClusterSchedulerState(
    appId: String,
    queuedDrivers: Iterable[DriverSubmission],
    launchedDrivers: Iterable[ClusterTaskState],
    finishedDrivers: Iterable[ClusterTaskState],
    retryList: Iterable[RetryState])

private[spark] trait ClusterScheduler {
  def submitDriver(desc: MesosDriverDescription): SubmitResponse

  def killDriver(submissionId: String): KillResponse

  def getStatus(submissionId: String): StatusResponse

  def getState(): ClusterSchedulerState
}

private[spark] class MesosClusterScheduler(
    engineFactory: ClusterPersistenceEngineFactory,
    conf: SparkConf) extends Scheduler with MesosSchedulerHelper with ClusterScheduler {

  var frameworkUrl: String = _

  val master = conf.get("spark.master")
  val appName = conf.get("spark.app.name")
  val queuedCapacity = conf.getInt("spark.deploy.mesos.queuedDrivers", 200)
  val retainedDrivers = conf.getInt("spark.deploy.retainedDrivers", 200)
  val maxRetryWaitTime = conf.getInt("spark.mesos.cluster.retry.wait.max", 60) // 1 minute
  val state = engineFactory.createEngine("scheduler")
  val stateTimeout =
    Duration.create(conf.getLong("spark.mesos.cluster.recover.timeout", 30), "seconds")

  val stateLock = new ReentrantLock()

  val finishedDrivers = new mutable.ArrayBuffer[ClusterTaskState](retainedDrivers)

  val nextDriverNumber: AtomicLong = new AtomicLong(0)
  var appId: String = null

  private var launchedDrivers: LaunchedDrivers = _

  private var queue: DriverQueue = _

  def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")  // For application IDs

  private var superviseRetryList: SuperviseRetryList = _

  private def newDriverId(submitDate: Date): String = {
    "driver-%s-%04d".format(
        createDateFormat.format(submitDate), nextDriverNumber.incrementAndGet())
  }

  def submitDriver(desc: MesosDriverDescription): SubmitResponse = {
    stateLock.synchronized {
      if (queue.isFull) {
        return SubmitResponse("", false, "Already reached maximum submission size")
      }

      val submitDate: Date = new Date()
      val submissionId: String = newDriverId(submitDate)
      val submission = new DriverSubmission(submissionId, desc, submitDate)
      queue.offer(submission)
      SubmitResponse(submissionId, true, "")
    }
  }

  def killDriver(submissionId: String): KillResponse = {
    stateLock.synchronized {
      // We look for the requested driver in the following places:
      // 1. Check if submission is running or launched.
      // 2. Check if it's still queued.
      // 3. Check if it's in the retry list.
      if (launchedDrivers.contains(submissionId)) {
        val task = launchedDrivers.get(submissionId)
        driver.killTask(task.taskId)
        return KillResponse(submissionId, true, "Killing running driver")
      } else if (queue.remove(submissionId)) {
        return KillResponse(submissionId, true, "Removed driver while it's still pending")
      } else if (superviseRetryList.remove(submissionId)) {
        return KillResponse(submissionId, true, "Removed driver while it's retrying")
      } else {
        return KillResponse(submissionId, false, "Cannot find driver")
      }
    }
  }

  def recoverState {
    stateLock.synchronized {
      queue = new DriverQueue(engineFactory.createEngine("driverQueue"), queuedCapacity)

      launchedDrivers = new LaunchedDrivers(engineFactory.createEngine("launchedDrivers"))

      // There is potential timing issue where a queued driver might have been launched
      // but the scheduler shuts down before the queued driver was able to be removed
      // from the queue. We try to mitigate this issue by walking through all queued drivers
      // and remove if they're already launched.
      queue.drivers.foreach {
        d => if (launchedDrivers.contains(d.submissionId)) {
          queue.remove(d.submissionId)
        }
      }

      superviseRetryList = new SuperviseRetryList(engineFactory.createEngine("retryList"))

      // TODO: Consider storing finished drivers so we can show them on the UI after
      // failover. For now we clear the history on each recovery.
      finishedDrivers.clear()
    }
  }

  def start() {
    // TODO: Implement leader election to make sure only one framework running in the cluster.
    val fwId = state.fetch[String]("frameworkId")

    val builder = FrameworkInfo.newBuilder()
      .setUser(Utils.getCurrentUserName())
      .setName(appName)
      .setWebuiUrl(frameworkUrl)
      .setCheckpoint(true)
      .setFailoverTimeout(Integer.MAX_VALUE) // Setting to max for tasks keep running until recovery

    fwId.foreach { id =>
      builder.setId(FrameworkID.newBuilder().setValue(id).build())
      appId = id
    }

    // Recover scheduler state that is persisted.
    // We still need to do task reconciliation to be up to date of the latest task states
    // as it might have changed while the scheduler is failing over.
    recoverState
    startScheduler("MesosClusterScheduler", master, MesosClusterScheduler.this, builder.build())
  }

  def stop() {
    driver.stop(true)
  }

  override def registered(
      driver: SchedulerDriver,
      frameworkId: FrameworkID,
      masterInfo: MasterInfo): Unit = {
    logInfo("Registered as framework ID " + frameworkId.getValue)
    if (frameworkId.getValue != appId) {
      appId = frameworkId.getValue
      state.persist("frameworkId", appId)
    }
    markRegistered()

    stateLock.synchronized {
      if (!launchedDrivers.pendingRecover.isEmpty) {
        // Start task reconciliation if we need to recover.
        val statuses = launchedDrivers.pendingRecover.collect {
          case (taskId, slaveId) =>
            launchedDrivers.get(taskId).taskState.getOrElse(
              TaskStatus.newBuilder()
                .setTaskId(TaskID.newBuilder().setValue(taskId).build())
                .setSlaveId(slaveId)
                .setState(TaskState.TASK_STAGING)
                .build)
        }

        // TODO: Page the status updates to avoid trying to reconcile
        // a large amount of tasks at once.
        driver.reconcileTasks(statuses)
      }
    }
  }

  private def buildCommand(req: DriverSubmission): CommandInfo = {
    val desc = req.desc

    val appJar = CommandInfo.URI.newBuilder()
      .setValue(desc.desc.jarUrl.stripPrefix("file:").stripPrefix("local:")).build()

    val builder = CommandInfo.newBuilder()
      .addUris(appJar)

    val entries =
      (conf.getOption("spark.executor.extraLibraryPath").toList ++
        desc.desc.command.libraryPathEntries)

    val prefixEnv = if (!entries.isEmpty) {
      Utils.libraryPathEnvPrefix(entries)
    } else {
      ""
    }

    val envBuilder = Environment.newBuilder()
    desc.desc.command.environment.foreach {
      case (k, v) =>
        envBuilder.addVariables(
          Variable.newBuilder().setName(k).setValue(v).build())
    }

    builder.setEnvironment(envBuilder.build())

    val cmdOptions = generateCmdOption(req)

    val executorUri = req.desc.schedulerProperties.get("spark.executor.uri")
      .orElse(req.desc.desc.command.environment.get("SPARK_EXECUTOR_URI"))

    val cmd = if (executorUri.isDefined) {
      builder.addUris(CommandInfo.URI.newBuilder().setValue(executorUri.get).build())

      val folderBasename = executorUri.get.split('/').last.split('.').head

      val cmdExecutable = s"cd $folderBasename*; $prefixEnv bin/spark-submit"

      val cmdJar = s"../${desc.desc.jarUrl.split("/").last}"

      val appArguments = desc.desc.command.arguments.mkString(" ")

      s"$cmdExecutable ${cmdOptions.mkString(" ")} $cmdJar $appArguments"
    } else {
      val executorSparkHome = req.desc.schedulerProperties.get("spark.mesos.executor.home")
        .orElse(conf.getOption("spark.home"))
        .orElse(Option(System.getenv("SPARK_HOME")))
        .getOrElse {
          throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
        }

      val cmdExecutable = new File(executorSparkHome, "./bin/spark-submit").getCanonicalPath

      val cmdJar = desc.desc.jarUrl.split("/").last

      s"$cmdExecutable ${cmdOptions.mkString(" ")} $cmdJar"
    }

    builder.setValue(cmd)

    builder.build
  }

  private def generateCmdOption(req: DriverSubmission): Seq[String] = {
    var options = Seq(
        "--name", req.desc.schedulerProperties("spark.app.name"),
        "--class", req.desc.desc.command.mainClass,
        "--master", s"mesos://${conf.get("spark.master")}",
        "--driver-cores", req.desc.desc.cores.toString,
        "--driver-memory", s"${req.desc.desc.mem}M")

    req.desc.schedulerProperties.get("spark.executor.memory").map { v =>
      options ++= Seq("--executor-memory", v)
    }

    req.desc.schedulerProperties.get("spark.cores.max").map { v =>
      options ++= Seq("--total-executor-cores", v)
    }

    options
  }

  private [spark] case class ResourceOffer(val offer: Offer, var cpu: Double, var mem: Double)

  override def resourceOffers(driver: SchedulerDriver, offers: JList[Offer]): Unit = {
    val currentOffers = offers.map {
      o =>
        ResourceOffer(
          o,
          getResource(o.getResourcesList, "cpus"),
          getResource(o.getResourcesList, "mem"))
    }

    val tasks = new mutable.HashMap[OfferID, ArrayBuffer[TaskInfo]]()

    val currentTime = new Date()

    def scheduleTasks(
        taskFunc: () => (Option[DriverSubmission], Option[RetryState]),
        scheduledCallback: (String) => Unit) {
      var nextItem = taskFunc()
      // TODO: We should not stop scheduling at the very first task
      // that cannot be scheduled. Instead we should exhaust the
      // candidate list and remove drivers that cannot scheduled
      // over a configurable period of time.
      while (nextItem._1.isDefined) {
        val (submission, retryState) = (nextItem._1.get, nextItem._2)

        val driverCpu = submission.desc.desc.cores
        val driverMem = submission.desc.desc.mem

        val offerOption = currentOffers.find { o =>
          o.cpu >= driverCpu && o.mem >= driverMem
        }

        if (offerOption.isEmpty) {
          return
        }

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

        val commandInfo = buildCommand(submission)

        val taskInfo = TaskInfo.newBuilder()
          .setTaskId(taskId)
          .setName(s"driver for ${submission.desc.desc.command.mainClass}")
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

        launchedDrivers.set(
          submission.submissionId,
          ClusterTaskState(submission, taskId, offer.offer.getSlaveId,
            None, DriverState.SUBMITTED, new Date(), retryState))

        scheduledCallback(submission.submissionId)

        nextItem = taskFunc()
      }
    }

    stateLock.synchronized {
      scheduleTasks(() => {
        superviseRetryList.getNextRetry(currentTime)
      }, (id: String) => {
        superviseRetryList.remove(id)
      })
      scheduleTasks(() => (queue.peek, None), (_) => queue.poll)
    }

    tasks.foreach {
      case (offerId, tasks) => driver.launchTasks(offerId, tasks)
    }

    offers
      .filter(o => !tasks.keySet.contains(o.getId))
      .foreach(o => driver.declineOffer(o.getId))
  }

  def getState(): ClusterSchedulerState = {
    stateLock.synchronized {
      ClusterSchedulerState(
        appId,
        queue.drivers,
        launchedDrivers.states,
        finishedDrivers.collect { case s => s.copy() },
        superviseRetryList.retries)
    }
  }

  def getStatus(submissionId: String): StatusResponse = {
    stateLock.synchronized {
      if (queue.contains(submissionId)) {
        return StatusResponse(submissionId, true, "Driver is queued for launch")
      } else if (launchedDrivers.contains(submissionId)) {
        val status = launchedDrivers.get(submissionId).taskState
        return StatusResponse(submissionId, true, "Driver is running", status)
      } else if (finishedDrivers.contains(submissionId)) {
        val status =
          finishedDrivers.find(d => d.submission.submissionId.equals(submissionId)).get.taskState
        return StatusResponse(submissionId, true, "Driver already finished", status)
      } else if (superviseRetryList.contains(submissionId)) {
        val status = superviseRetryList.get(submissionId).get.lastFailureStatus
        return StatusResponse(submissionId, true, "Driver failed and retrying", Some(status))
      } else {
        return StatusResponse(submissionId, false, "Driver not found")
      }
    }
  }

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {}

  override def disconnected(driver: SchedulerDriver): Unit = {}

  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = {
    logInfo(s"Framework re-registered with master ${masterInfo.getId}")
  }

  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = {}

  override def error(driver: SchedulerDriver, error: String): Unit = {}

  def getDriverState(state: TaskState): DriverState = {
    state match {
      case TaskState.TASK_FAILED => DriverState.FAILED
      case TaskState.TASK_ERROR => DriverState.ERROR
      case TaskState.TASK_FINISHED => DriverState.FINISHED
      case TaskState.TASK_KILLED => DriverState.KILLED
      case TaskState.TASK_LOST => DriverState.ERROR
      case TaskState.TASK_RUNNING => DriverState.RUNNING
      case TaskState.TASK_STARTING | TaskState.TASK_STAGING => DriverState.SUBMITTED
      case _ => DriverState.UNKNOWN
    }
  }

  def shouldRelaunch(state: TaskState): Boolean = {
    state == TaskState.TASK_FAILED ||
      state == TaskState.TASK_KILLED ||
      state == TaskState.TASK_LOST
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

        if (state.submission.desc.desc.supervise && shouldRelaunch(status.getState)) {
          val (retries, waitTimeSec) = if (state.lastRetry.isDefined) {
            (state.lastRetry.get.retries + 1,
              Math.min(maxRetryWaitTime, state.lastRetry.get.waitTime * 2))
          } else {
            (1, 1)
          }
          val nextRetry = new Date(new Date().getTime + waitTimeSec * 1000L)
          superviseRetryList.add(
            RetryState(state.submission, status, retries, nextRetry, waitTimeSec))
        }

        val driverState = getDriverState(status.getState)
        if (isFinished(status.getState)) {
          launchedDrivers.remove(taskId)
          if (finishedDrivers.size >= retainedDrivers) {
            val toRemove = math.max(retainedDrivers / 10, 1)
            finishedDrivers.trimStart(toRemove)
          }

          finishedDrivers += state
        }

        state.taskState = Option(status)
        state.driverState = driverState
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
