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

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.{List => JList}
import java.util.{Collections, Date}
import org.apache.mesos.{SchedulerDriver, Scheduler}
import org.apache.mesos.Protos._
import org.apache.spark.deploy.DriverDescription
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils
import scala.collection.mutable
import scala.collection.JavaConversions._
import org.apache.mesos.Protos.Environment.Variable
import org.apache.spark.SparkException
import java.io.File

case class DriverRequest(desc: DriverDescription, conf: SparkConf)

private[spark] class DriverSubmission(
    val submissionId: String,
    val req: DriverRequest,
    val submitDate: Date) {

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
    var startDate: Date) {
  def copy(): ClusterTaskState = {
    ClusterTaskState(submission, taskId, slaveId, taskState, driverState, startDate)
  }
}

private[spark] case class SubmitResponse(id: String, success: Boolean, message: Option[String])
private[spark] case class StatusResponse(id: String, success: Boolean, message: Option[String])
private[spark] case class KillResponse(id: String, success: Boolean, message: Option[String])

private[spark] case class ClusterSchedulerState(
    queuedDrivers: Iterable[DriverSubmission],
    launchedDrivers: Iterable[ClusterTaskState],
    finishedDrivers: Iterable[ClusterTaskState])

private[spark] trait ClusterScheduler {
  def submitDriver(desc: DriverRequest): SubmitResponse

  def killDriver(submissionId: String): KillResponse

  def getStatus(submissionId: String): StatusResponse

  def getState(): ClusterSchedulerState
}

private[spark] class MesosClusterScheduler(conf: SparkConf)
  extends Scheduler with MesosSchedulerHelper with ClusterScheduler {

  var frameworkUrl: String = _
  val master = conf.get("spark.master")
  val appName = conf.get("spark.app.name")
  val queuedCapacity = conf.getInt("spark.deploy.mesos.queuedDrivers", 200)
  val retainedDrivers = conf.getInt("spark.deploy.retainedDrivers", 200)

  val stateLock = new Object
  val launchedDrivers = new mutable.HashMap[String, ClusterTaskState]()

  val finishedDrivers = new mutable.ArrayBuffer[ClusterTaskState](retainedDrivers)
  val nextDriverNumber: AtomicLong = new AtomicLong(0)
  var appId: String = _

  private val queue = new LinkedBlockingQueue[DriverSubmission](queuedCapacity)

  def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")  // For application IDs

  private def newDriverId(submitDate: Date): String = {
    "driver-%s-%04d".format(
        createDateFormat.format(submitDate), nextDriverNumber.incrementAndGet())
  }

  def submitDriver(req: DriverRequest): SubmitResponse = {
    val submitDate: Date = new Date()
    val submissionId: String = newDriverId(submitDate)
    val submission = new DriverSubmission(submissionId, req, submitDate)
    if (queue.offer(submission)) {
      SubmitResponse(submissionId, true, None)
    } else {
      SubmitResponse(submissionId, false, Option("Already reached maximum submission size"))
    }
  }

  def killDriver(submissionId: String): KillResponse = {
    stateLock.synchronized {
      if (launchedDrivers.contains(submissionId)) {
        // Check if submission is running
        val task = launchedDrivers(submissionId)
        driver.killTask(task.taskId)
        Some(KillResponse(submissionId, true, Option("Killing running driver")))
      } else {
        None
      }
    }.orElse {
      // Check if submission is queued
      if (queue.remove(new DriverSubmission(submissionId, null, null))) {
        Some(KillResponse(submissionId, true, Option("Removed driver while it's still pending")))
      } else {
        None
      }
    }.getOrElse{
      KillResponse(submissionId, false, Option("Cannot find driver"))
    }
  }

  def start() {
    val fwInfo = FrameworkInfo.newBuilder()
      .setUser(Utils.getCurrentUserName())
      .setName(appName)
      .setWebuiUrl(frameworkUrl)
      .setCheckpoint(true)
      .build()
    startScheduler("MesosClusterScheduler", master, MesosClusterScheduler.this, fwInfo)
  }

  override def registered(
      driver: SchedulerDriver,
      frameworkId: FrameworkID,
      masterInfo: MasterInfo): Unit = {
    appId = frameworkId.getValue
    logInfo("Registered as framework ID " + appId)
    markRegistered()
  }

  private def buildCommand(req: DriverRequest): CommandInfo = {

    val desc = req.desc

    val cleanedJarUrl = desc.jarUrl.stripPrefix("file:")

    logInfo(s"jarUrl: $cleanedJarUrl")

    val builder = CommandInfo.newBuilder()
      .addUris(CommandInfo.URI.newBuilder().setValue(cleanedJarUrl).build())

    val entries =
      (conf.getOption("spark.executor.extraLibraryPath").toList ++ desc.command.libraryPathEntries)

    val prefixEnv = if (!entries.isEmpty) {
      Utils.libraryPathEnvPrefix(entries)
    } else {
      ""
    }

    val envBuilder = Environment.newBuilder()
    desc.command.environment.foreach {
      case (k, v) =>
        envBuilder.addVariables(
          Variable.newBuilder().setName(k).setValue(v).build())
    }

    builder.setEnvironment(envBuilder.build())

    val cmdOptions = generateCmdOption(req)

    val executorUri = req.conf.getOption("spark.executor.uri")
      .orElse(req.desc.command.environment.get("SPARK_EXECUTOR_URI"))

    val cmd = if (executorUri.isDefined) {
      builder.addUris(CommandInfo.URI.newBuilder().setValue(executorUri.get).build())

      val folderBasename = executorUri.get.split('/').last.split('.').head

      val cmdExecutable = s"cd $folderBasename*; $prefixEnv bin/spark-submit"

      val cmdJar = s"../${desc.jarUrl.split("/").last}"

      s"$cmdExecutable ${cmdOptions.mkString(" ")} $cmdJar"
    } else {
      val executorSparkHome = req.conf.getOption("spark.mesos.executor.home")
        .orElse(conf.getOption("spark.home"))
        .orElse(Option(System.getenv("SPARK_HOME")))
        .getOrElse {
          throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
        }

      val cmdExecutable = new File(executorSparkHome, "./bin/spark-submit").getCanonicalPath

      val cmdJar = desc.jarUrl.split("/").last

      s"$cmdExecutable ${cmdOptions.mkString(" ")} $cmdJar"
    }

    builder.setValue(cmd)

    builder.build
  }

  private def generateCmdOption(req: DriverRequest): Seq[String] = {
    Seq(
        "--name", req.conf.get("spark.app.name"),
        "--class", req.desc.command.mainClass,
        "--master", s"mesos://${conf.get("spark.master")}",
        "--driver-cores", req.desc.cores.toString,
        "--driver-memory", s"${req.desc.mem}M",
        "--executor-memory", req.conf.get("spark.executor.memory"),
        "--total-executor-cores", req.conf.get("spark.cores.max")
        )
  }

  private [spark] case class ResourceOffer(val offer: Offer, var cpu: Double, var mem: Double)

  override def resourceOffers(driver: SchedulerDriver, offers: JList[Offer]): Unit = {
    var submission = queue.peek

    val usedOffers = new mutable.HashSet[OfferID]

    val currentOffers = offers.map {
      o =>
        ResourceOffer(
          o,
          getResource(o.getResourcesList, "cpus"),
          getResource(o.getResourcesList, "mem"))
    }

    var canSchedule = true

    while (canSchedule && submission != null) {
      val driverCpu = submission.req.desc.cores
      val driverMem = submission.req.desc.mem

      val offerOption = currentOffers.find { o =>
        o.cpu >= driverCpu && o.mem >= driverMem
      }

      if (offerOption.isDefined) {
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

        val commandInfo = buildCommand(submission.req)

        val taskInfo = TaskInfo.newBuilder()
          .setTaskId(taskId)
          .setName(s"driver for ${submission.req.desc.command.mainClass}")
          .setSlaveId(offer.offer.getSlaveId)
          .setCommand(commandInfo)
          .addResources(cpuResource)
          .addResources(memResource)
          .build

        logDebug(s"Launching task ${taskInfo}, with offer: ${offer.offer}")
        driver.launchTasks(Collections.singleton(offer.offer.getId), Collections.singleton(taskInfo))

        stateLock.synchronized {
          launchedDrivers(submission.submissionId) =
            ClusterTaskState(submission, taskId, offer.offer.getSlaveId,
              None, DriverState.SUBMITTED, new Date())
        }

        usedOffers += offer.offer.getId

        // remove driver from queue.
        queue.poll(0, TimeUnit.SECONDS)

        submission = queue.peek
      } else {
        // We can stop at very first driver that we cannot schedule on.
        // TODO: We should remove the top driver that cannot be scheduled
        // over a configurable time period.
        canSchedule = false
      }
    }

    offers
      .filter(o => !usedOffers.contains(o.getId))
      .foreach(o => driver.declineOffer(o.getId))
  }

  def getState(): ClusterSchedulerState = {
    def copyDriverStates(states: Iterable[ClusterTaskState]): Iterable[ClusterTaskState] = {
      states.collect { case s => s.copy() }
    }

    stateLock.synchronized {
      val queueCopy = new Array[DriverSubmission](queue.size())
      queue.copyToArray(queueCopy)
      ClusterSchedulerState(
        queueCopy,
        copyDriverStates(launchedDrivers.values),
        copyDriverStates(finishedDrivers))
    }
  }

  def getStatus(submissionId: String): StatusResponse = {
    stateLock.synchronized {
      if (queue.contains(new DriverSubmission(submissionId, null, null))) {
        return StatusResponse(submissionId, true, Option("Driver is queued for launch"))
      } else if (launchedDrivers.contains(submissionId)) {
        return StatusResponse(submissionId, true, Option("Driver is running"))
      } else if (finishedDrivers.contains(submissionId)) {
        return StatusResponse(submissionId, true, Option("Driver already finished"))
      } else {
        return StatusResponse(submissionId, false, None)
      }
    }
  }

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {}

  override def disconnected(driver: SchedulerDriver): Unit = {}

  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = {}

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

  def canRelaunch(state: TaskState): Boolean = {
    state == TaskState.TASK_FAILED ||
      state == TaskState.TASK_KILLED ||
      state == TaskState.TASK_LOST
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    val taskId = status.getTaskId.getValue
    stateLock.synchronized {
      if (launchedDrivers.contains(taskId)) {
        if (canRelaunch(status.getState)) {
          // TODO: We should try to relaunch if supervise is turned on.
          // Also check how many times we've retried.
        }

        val driverState = getDriverState(status.getState)
        val state = if (isFinished(status.getState)) {
          val launchedState = launchedDrivers.remove(taskId).get
          if (finishedDrivers.size >= retainedDrivers) {
            val toRemove = math.max(retainedDrivers / 10, 1)
            finishedDrivers.trimStart(toRemove)
          }

          finishedDrivers += launchedState

          launchedState
        } else {
          launchedDrivers(taskId)
        }

        state.taskState = Option(status)
        state.driverState = driverState
      } else {
        logError("Unable to find driver " + taskId + " in status update")
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
