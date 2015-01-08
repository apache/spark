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
import java.util.{List => JList}
import java.util.Collections

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet}

import org.apache.mesos.{Scheduler => MScheduler}
import org.apache.mesos._
import org.apache.mesos.Protos.{TaskInfo => MesosTaskInfo, TaskState => MesosTaskState, _}

import org.apache.spark.{Logging, SparkContext, SparkEnv, SparkException}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils
import com.google.common.collect.{BiMap, HashBiMap}
import org.apache.mesos.protobuf.ByteString
import java.util.concurrent.locks.ReentrantLock

case class SlaveStatus(var executorRunning: Boolean, var taskRunning: Boolean) {
  def finished = !taskRunning && !executorRunning
}

/**
 * A SchedulerBackend that runs tasks on Mesos, but uses "coarse-grained" tasks, where it holds
 * onto each Mesos node for the duration of the Spark job instead of relinquishing cores whenever
 * a task is done. It launches Spark tasks within the coarse-grained Mesos tasks using the
 * CoarseGrainedSchedulerBackend mechanism. This class is useful for lower and more predictable
 * latency.
 *
 * Unfortunately this has a bit of duplication from MesosSchedulerBackend, but it seems hard to
 * remove this.
 */
private[spark] class CoarseMesosSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    master: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
  with MScheduler
  with Logging {

  val MAX_SLAVE_FAILURES = 2 // Blacklist a slave after this many failures

  // Lock used to wait for scheduler to be registered
  var isRegistered = false
  val registeredLock = new Object()

  // Driver for talking to Mesos
  var driver: SchedulerDriver = null

  // Maximum number of cores to acquire (TODO: we'll need more flexible controls here)
  val maxCores = conf.get("spark.cores.max", Int.MaxValue.toString).toInt

  // Cores we have acquired with each Mesos task ID
  val coresByTaskId = new HashMap[Int, Int]
  var totalCoresAcquired = 0

  val slaveStatuses = new HashMap[String, SlaveStatus]

  val taskIdToSlaveId = HashBiMap.create[Int, String]()

  val failuresBySlaveId = new HashMap[String, Int] // How many times tasks on each slave failed

  val pendingRemovedSlaveIds = new HashSet[String]

  val extraCoresPerSlave = conf.getInt("spark.mesos.extra.cores", 0)

  var nextMesosTaskId = 0

  var executorLimit: Option[Int] = None

  // Introducing a new lock for protecting above shared state. We avoid locking on the class object
  // as it can result in deadlock when synchronized method in this class calls a parent's method
  // that is also trying to lock on the class object.
  var stateLock = new ReentrantLock()

  @volatile var appId: String = _

  def newMesosTaskId(): Int = {
    val id = nextMesosTaskId
    nextMesosTaskId += 1
    id
  }

  override def start() {
    super.start()

    synchronized {
      new Thread("CoarseMesosSchedulerBackend driver") {
        setDaemon(true)

        override def run() {
          val scheduler = CoarseMesosSchedulerBackend.this
          val fwInfo = FrameworkInfo.newBuilder().setUser(sc.sparkUser).setName(sc.appName).build()
          driver = new MesosSchedulerDriver(scheduler, fwInfo, master)
          try { {
            val ret = driver.run()
            logInfo("driver.run() returned with code " + ret)
          }
          } catch {
            case e: Exception => logError("driver.run() failed", e)
          }
        }
      }.start()

      waitForRegister()
    }
  }

  // Set the environment variable through a command prefix
  // to append to the existing value of the variable
  lazy val prefixEnv = conf.getOption("spark.executor.extraLibraryPath").map { p =>
    Utils.libraryPathEnvPrefix(Seq(p))
  }.getOrElse("")

  lazy val driverUrl = "akka.tcp://%s@%s:%s/user/%s".format(
    SparkEnv.driverActorSystemName,
    conf.get("spark.driver.host"),
    conf.get("spark.driver.port"),
    CoarseGrainedSchedulerBackend.ACTOR_NAME)

  lazy val executorUri = conf.get("spark.executor.uri", null)

  lazy val executorSparkHome = conf.getOption("spark.mesos.executor.home")
    .orElse(sc.getSparkHome())
    .getOrElse {
    throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
  }

  def createBaseCommand(offer: Offer): CommandInfo.Builder = {
    val environment = Environment.newBuilder()
    val extraClassPath = conf.getOption("spark.executor.extraClassPath")
    extraClassPath.foreach { cp =>
      environment.addVariables(
        Environment.Variable.newBuilder().setName("SPARK_CLASSPATH").setValue(cp).build())
    }
    val extraJavaOpts = conf.get("spark.executor.extraJavaOptions", "")

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
    val command = CommandInfo.newBuilder()
      .setEnvironment(environment)

    if (executorUri != null) {
      command.addUris(CommandInfo.URI.newBuilder().setValue(executorUri))
    }

    command
  }

  def sparkExecutorId(slaveId: String, taskId: String) = "%s/%s".format(slaveId, taskId)

  def createTaskCommandString(
      offer: Offer,
      numCores: Int,
      taskId: String,
      executorUri: String = executorUri,
      sparkHome: String = null): String = {
    val sparkClassCommand = if (executorUri == null && sparkHome == null) {
      "./bin/spark-class "
    } else if (executorUri == null) {
      val runScript = new File(executorSparkHome, "./bin/spark-class").getCanonicalPath
      "%s \"%s\" ".format(prefixEnv, runScript)
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = executorUri.split('/').last.split('.').head
      "cd %s*; %s ./bin/spark-class ".format(basename, prefixEnv)
    }
    sparkClassCommand +
      "org.apache.spark.executor.CoarseGrainedExecutorBackend " +
      "%s %s %s %d %s".format(
        driverUrl, sparkExecutorId(offer.getSlaveId.getValue, taskId),
        offer.getHostname, numCores, appId)
  }

  def createTaskCommand(offer: Offer, numCores: Int, taskId: String): CommandInfo = {
    val command = createBaseCommand(offer)
    command.setValue(createTaskCommandString(offer, numCores, taskId)).build
  }

  def createExecutorCommand(offer: Offer): CommandInfo = {
    val command = createBaseCommand(offer)

    if (executorUri == null) {
      val runScript = new File(executorSparkHome, "./bin/spark-class").getCanonicalPath
      command.setValue(
        ("%s \"%s\" org.apache.spark.executor.CoarseGrainedMesosExecutorBackend").format(
          prefixEnv, runScript))
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = executorUri.split('/').last.split('.').head
      command.setValue(
        ("cd %s*; %s " +
          "./bin/spark-class org.apache.spark.executor.CoarseGrainedMesosExecutorBackend ")
          .format(basename, prefixEnv))
    }

    command.build()
  }

  override def offerRescinded(d: SchedulerDriver, o: OfferID) {}

  override def registered(d: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {
    appId = frameworkId.getValue
    logInfo("Registered as framework ID " + appId)
    registeredLock.synchronized {
      isRegistered = true
      registeredLock.notifyAll()
    }
  }

  def waitForRegister() {
    registeredLock.synchronized {
      while (!isRegistered) {
        registeredLock.wait()
      }
    }
  }

  override def disconnected(d: SchedulerDriver) {}

  override def reregistered(d: SchedulerDriver, masterInfo: MasterInfo) {}

  lazy val shuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false)

  lazy val shuffleServiceMem = if (shuffleServiceEnabled) {
    conf.getInt("spark.mesos.shuffle.service.mem", 1024)
  } else {
    0
  }

  lazy val shuffleServiceCpu = if (shuffleServiceEnabled) {
    conf.getInt("spark.mesos.shuffle.service.cpu", 1)
  } else {
    0
  }

  /**
   * Method called by Mesos to offer resources on slaves. We respond by launching an executor,
   * unless we've already launched more than we wanted to.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    stateLock.synchronized {
      val filters = Filters.newBuilder().setRefuseSeconds(-1).build()

      for (offer <- offers) {
        val slaveId = offer.getSlaveId.getValue
        val mem = getResource(offer.getResourcesList, "mem")
        val cpus = getResource(offer.getResourcesList, "cpus").toInt
        logTrace("Received offer id: " + offer.getId.getValue + ", cpu: " + cpus.toString +
          ", mem: " + mem.toString)

        var minCpuRequired = 1
        var minMemRequired = MemoryUtils.calculateTotalMemory(sc)

        if (!slaveStatuses.contains(slaveId) || !slaveStatuses(slaveId).executorRunning) {
          minCpuRequired += shuffleServiceCpu
          minMemRequired += shuffleServiceMem
        }

        if (taskIdToSlaveId.size < executorLimit.getOrElse(Int.MaxValue) &&
          totalCoresAcquired < maxCores &&
          failuresBySlaveId.getOrElse(slaveId, 0) < MAX_SLAVE_FAILURES &&
          (!slaveStatuses.contains(slaveId) || !slaveStatuses(slaveId).taskRunning) &&
          mem >= minMemRequired &&
          cpus >= minCpuRequired) {

          // Launch an executor on the slave
          val cpusToUse = math.min(cpus, maxCores - totalCoresAcquired)
          totalCoresAcquired += cpusToUse
          val taskId = newMesosTaskId()
          taskIdToSlaveId(taskId) = slaveId
          coresByTaskId(taskId) = cpusToUse
          val taskIdString: String = taskId.toString
          val builder = MesosTaskInfo.newBuilder()
            .setTaskId(TaskID.newBuilder().setValue(taskIdString).build())
            .setSlaveId(offer.getSlaveId)
            .setName("Task " + taskId)

          if (!shuffleServiceEnabled) {
            slaveStatuses(slaveId) = SlaveStatus(false, true)
            builder
              .setCommand(createTaskCommand(offer, cpusToUse + extraCoresPerSlave, taskIdString))
              .addResources(createResource("cpus", cpusToUse))
              .addResources(createResource("mem",
              MemoryUtils.calculateTotalMemory(sc)))
          } else {
            slaveStatuses(slaveId) = SlaveStatus(true, true)
            val taskCpus = cpusToUse - shuffleServiceCpu
            val taskMem = mem - shuffleServiceMem
            builder.setExecutor(
              ExecutorInfo.newBuilder()
                .setExecutorId(ExecutorID.newBuilder().setValue(slaveId).build)
                .addResources(createResource("cpus", shuffleServiceCpu))
                .addResources(createResource("mem", shuffleServiceMem))
                .setCommand(createExecutorCommand(offer)).build())
              .addResources(createResource("cpus", taskCpus))
              .addResources(createResource("mem", taskMem))

            builder.setData(
              ByteString.copyFrom(
                Utils.serialize(
                  createTaskCommandString(offer, taskCpus, taskIdString, null, null))))
          }

          logTrace("Launching task with offer id: " + offer.getId.getValue +
            ", task: " + builder.build())

          val status = slaveStatuses(slaveId)
          status.taskRunning = true
          status.executorRunning = true

          d.launchTasks(
            Collections.singleton(offer.getId), Collections.singletonList(builder.build()), filters)
        } else {
          d.declineOffer(offer.getId)
        }
      }
    }
  }

  /** Helper function to pull out a resource from a Mesos Resources protobuf */
  private def getResource(res: JList[Resource], name: String): Double = {
    for (r <- res if r.getName == name) {
      return r.getScalar.getValue
    }
    0
  }

  /** Build a Mesos resource protobuf object */
  private def createResource(resourceName: String, quantity: Double): Protos.Resource = {
    Resource.newBuilder()
      .setName(resourceName)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(quantity).build())
      .build()
  }

  /** Check whether a Mesos task state represents a finished task */
  private def isFinished(state: MesosTaskState) = {
    state == MesosTaskState.TASK_FINISHED ||
      state == MesosTaskState.TASK_FAILED ||
      state == MesosTaskState.TASK_KILLED ||
      state == MesosTaskState.TASK_LOST
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus) {
    val taskId = status.getTaskId.getValue.toInt
    val state = status.getState
    stateLock.synchronized {
      if (!taskIdToSlaveId.containsKey(taskId)) {
        return
      }

      logInfo("Mesos task " + taskId + " is now " + state)

      if (isFinished(state)) {
        val slaveId = taskIdToSlaveId(taskId)
        // Remove the cores we have remembered for this task, if it's in the hashmap
        for (cores <- coresByTaskId.get(taskId)) {
          totalCoresAcquired -= cores
          coresByTaskId -= taskId
        }
        // If it was a failure, mark the slave as failed for blacklisting purposes
        if (state == MesosTaskState.TASK_FAILED || state == MesosTaskState.TASK_LOST) {
          failuresBySlaveId(slaveId) = failuresBySlaveId.getOrElse(slaveId, 0) + 1
          if (failuresBySlaveId(slaveId) >= MAX_SLAVE_FAILURES) {
            logInfo("Blacklisting Mesos slave " + slaveId + " due to too many failures; " +
              "is Spark installed on it?")
          }
        }

        executorTerminated(d, status.getSlaveId.getValue, "Executor finished with state: " + state)
        driver.reviveOffers() // In case we'd rejected everything before but have now lost a node
      }
    }
  }

  override def error(d: SchedulerDriver, message: String) {
    logError("Mesos error: " + message)
    scheduler.error(message)
  }

  override def stop() {
    super.stop()
    if (driver != null) {
      driver.stop()
    }
  }

  override def frameworkMessage(d: SchedulerDriver, e: ExecutorID, s: SlaveID, b: Array[Byte]) {}

  def executorTerminated(d: SchedulerDriver, slaveId: String, reason: String) {
    stateLock.synchronized {
      if (slaveStatuses.contains(slaveId)) {
        val slaveIdToTaskId: BiMap[String, Int] = taskIdToSlaveId.inverse()
        if (slaveIdToTaskId.contains(slaveId)) {
          val taskId: Int = slaveIdToTaskId.get(slaveId)
          taskIdToSlaveId.remove(taskId)
          removeExecutor(sparkExecutorId(slaveId, taskId.toString), reason)
        }
        pendingRemovedSlaveIds -= slaveId
        val status = slaveStatuses(slaveId)
        status.taskRunning = false
        if (status.finished) {
          slaveStatuses -= slaveId
        }
      }
    }
  }

  override def slaveLost(d: SchedulerDriver, slaveId: SlaveID) {
    logInfo("Mesos slave lost: " + slaveId.getValue)
    executorTerminated(d, slaveId.getValue, "Mesos slave lost: " + slaveId.getValue)
    slaveStatuses -= slaveId.getValue
  }

  override def executorLost(d: SchedulerDriver, e: ExecutorID, s: SlaveID, status: Int) {
    logInfo("Executor lost: %s, marking slave %s as lost".format(e.getValue, s.getValue))
    slaveLost(d, s)
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  override def doRequestTotalExecutors(requestedTotal: Int): Boolean = stateLock.synchronized {
    // We don't truly know if we can fulfill the full amount of executors
    // since at coarse grain it depends on the amount of slaves available.
    logInfo("Capping the total amount of executors to " + requestedTotal)
    executorLimit = Option(requestedTotal)
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Boolean = stateLock.synchronized {
    if (driver == null) {
      logWarning("Asked to kill executors before the executor was started.")
      return false
    }

    val slaveIdToTaskId = taskIdToSlaveId.inverse()
    for (executorId <- executorIds) {
      val slaveId = executorId.split("/")(0)
      if (slaveIdToTaskId.contains(slaveId)) {
        driver.killTask(
          TaskID.newBuilder().setValue(slaveIdToTaskId.get(slaveId).toString).build)
        pendingRemovedSlaveIds += slaveId
      } else {
        logWarning("Unable to find executor Id '" + executorId + "' in Mesos scheduler")
      }
    }

    assert(pendingRemovedSlaveIds.size <= taskIdToSlaveId.size)

    // We cannot simply decrement from the existing executor limit as we may not able to
    // launch as much executors as the limit. But we assume if we are notified to kill
    // executors, that means the scheduler wants to set the limit that is less than
    // the amount of the executors that has been launched. Therefore, we take the existing
    // amount of executors launched and deduct the executors killed as the new limit.
    executorLimit = Option(taskIdToSlaveId.size - pendingRemovedSlaveIds.size)
    true
  }
}
