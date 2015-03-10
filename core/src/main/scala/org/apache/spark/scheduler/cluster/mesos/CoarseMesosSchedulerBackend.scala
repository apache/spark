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
import java.util.concurrent.locks.ReentrantLock

import com.google.common.collect.HashBiMap

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet}

import org.apache.mesos.{Scheduler => MScheduler}
import org.apache.mesos._
import org.apache.mesos.Protos.{TaskInfo => MesosTaskInfo, TaskState => MesosTaskState, _}

import org.apache.spark.{Logging, SparkContext, SparkEnv, SparkException}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{Utils, AkkaUtils}

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

  val MAX_SLAVE_FAILURES = 2     // Blacklist a slave after this many failures

  // Lock used to wait for scheduler to be registered
  var isRegistered = false
  val registeredLock = new Object()

  // Driver for talking to Mesos
  var driver: SchedulerDriver = null

  // Maximum number of cores to acquire (TODO: we'll need more flexible controls here)
  val maxCores = conf.get("spark.cores.max",  Int.MaxValue.toString).toInt

  // Cores we have acquired with each Mesos task ID
  val coresByTaskId = new HashMap[Int, Int]
  var totalCoresAcquired = 0

  val slaveIdsWithExecutors = new HashSet[String]

  val taskIdToSlaveId: HashBiMap[Int, String] = HashBiMap.create[Int, String]
  val failuresBySlaveId: HashMap[String, Int] = new HashMap[String, Int] // How many times tasks on each slave failed


  // the total number of executors we aim to have
  private var executorLimit: Option[Int] = None
  private val pendingRemovedSlaveIds = new HashSet[String]

  // private lock object protecting mutable state above. Using the intrinsic lock
  // may lead to deadlocks since the superclass might also try to lock
  private val stateLock = new ReentrantLock

  val extraCoresPerSlave = conf.getInt("spark.mesos.extra.cores", 0)

  var nextMesosTaskId = 0

  @volatile var appId: String = _

  def newMesosTaskId(): Int = {
    val id = nextMesosTaskId
    nextMesosTaskId += 1
    id
  }

  override def start() {
    super.start()

    stateLock.synchronized {
      new Thread("CoarseMesosSchedulerBackend driver") {
        setDaemon(true)
        override def run() {
          val scheduler = CoarseMesosSchedulerBackend.this
          val fwInfo = FrameworkInfo.newBuilder().setUser(sc.sparkUser).setName(sc.appName).build()
          driver = new MesosSchedulerDriver(scheduler, fwInfo, master)
          try {
            val ret = driver.run()
            logInfo("driver.run() returned with code " + ret)
          } catch {
            case e: Exception => logError("driver.run() failed", e)
          }
        }
      }.start()

      waitForRegister()
    }
  }

  def createCommand(offer: Offer, numCores: Int): CommandInfo = {
    val executorSparkHome = conf.getOption("spark.mesos.executor.home")
      .orElse(sc.getSparkHome())
      .getOrElse {
        throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
      }
    val environment = Environment.newBuilder()
    val extraClassPath = conf.getOption("spark.executor.extraClassPath")
    extraClassPath.foreach { cp =>
      environment.addVariables(
        Environment.Variable.newBuilder().setName("SPARK_CLASSPATH").setValue(cp).build())
    }
    val extraJavaOpts = conf.get("spark.executor.extraJavaOptions", "")

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    val prefixEnv = conf.getOption("spark.executor.extraLibraryPath").map { p =>
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
    val command = CommandInfo.newBuilder()
      .setEnvironment(environment)
    val driverUrl = AkkaUtils.address(
      AkkaUtils.protocol(sc.env.actorSystem),
      SparkEnv.driverActorSystemName,
      conf.get("spark.driver.host"),
      conf.get("spark.driver.port"),
      CoarseGrainedSchedulerBackend.ACTOR_NAME)

    val uri = conf.get("spark.executor.uri", null)
    if (uri == null) {
      val runScript = new File(executorSparkHome, "./bin/spark-class").getCanonicalPath
      command.setValue(
        "%s \"%s\" org.apache.spark.executor.CoarseGrainedExecutorBackend"
          .format(prefixEnv, runScript) +
        s" --driver-url $driverUrl" +
        s" --executor-id ${offer.getSlaveId.getValue}" +
        s" --hostname ${offer.getHostname}" +
        s" --cores $numCores" +
        s" --app-id $appId")
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = uri.split('/').last.split('.').head
      command.setValue(
        s"cd $basename*; $prefixEnv " +
         "./bin/spark-class org.apache.spark.executor.CoarseGrainedExecutorBackend" +
        s" --driver-url $driverUrl" +
        s" --executor-id ${offer.getSlaveId.getValue}" +
        s" --hostname ${offer.getHostname}" +
        s" --cores $numCores" +
        s" --app-id $appId")
      command.addUris(CommandInfo.URI.newBuilder().setValue(uri))
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

  /**
   * Method called by Mesos to offer resources on slaves. We respond by launching an executor,
   * unless we've already launched more than we wanted to.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    stateLock.synchronized {
      val filters = Filters.newBuilder().setRefuseSeconds(-1).build()

      for (offer <- offers) {
        val slaveId = offer.getSlaveId.toString
        val mem = getResource(offer.getResourcesList, "mem")
        val cpus = getResource(offer.getResourcesList, "cpus").toInt
        if (totalCoresAcquired < maxCores &&
            mem >= MemoryUtils.calculateTotalMemory(sc) &&
            cpus >= 1 &&
            failuresBySlaveId.getOrElse(slaveId, 0) < MAX_SLAVE_FAILURES &&
            !slaveIdsWithExecutors.contains(slaveId)) {
          // Launch an executor on the slave
          val cpusToUse = math.min(cpus, maxCores - totalCoresAcquired)
          totalCoresAcquired += cpusToUse
          val taskId = newMesosTaskId()
          taskIdToSlaveId(taskId) = slaveId
          slaveIdsWithExecutors += slaveId
          coresByTaskId(taskId) = cpusToUse
          val task = MesosTaskInfo.newBuilder()
            .setTaskId(TaskID.newBuilder().setValue(taskId.toString).build())
            .setSlaveId(offer.getSlaveId)
            .setCommand(createCommand(offer, cpusToUse + extraCoresPerSlave))
            .setName("Task " + taskId)
            .addResources(createResource("cpus", cpusToUse))
            .addResources(createResource("mem",
              MemoryUtils.calculateTotalMemory(sc)))
            .build()
          d.launchTasks(
            Collections.singleton(offer.getId),  Collections.singletonList(task), filters)
        } else {
          // Filter it out
          d.launchTasks(
            Collections.singleton(offer.getId), Collections.emptyList[MesosTaskInfo](), filters)
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
    logInfo("Mesos task " + taskId + " is now " + state)
    stateLock.synchronized {
      if (isFinished(state)) {
        val slaveId = taskIdToSlaveId(taskId)
        slaveIdsWithExecutors -= slaveId
        taskIdToSlaveId -= taskId
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
        executorTerminated(d, slaveId, s"Executor finished with state $state")
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

  /** Called when a slave is lost or a Mesos task finished. Update local view on
   *  what tasks are running and remove the terminated slave from the list of pending
   *  slave IDs that we might have asked to be killed. It also notifies the driver
   *  that an executor was removed.
   */
  private def executorTerminated(d: SchedulerDriver, slaveId: String, reason: String) {
    stateLock.synchronized {
      if (slaveIdsWithExecutors.contains(slaveId)) {
        val slaveIdToTaskId = taskIdToSlaveId.inverse()
        if (slaveIdToTaskId.contains(slaveId)) {
          val taskId: Int = slaveIdToTaskId.get(slaveId)
          taskIdToSlaveId.remove(taskId)
          removeExecutor(sparkExecutorId(slaveId, taskId.toString), reason)
        }
        pendingRemovedSlaveIds -= slaveId
        slaveIdsWithExecutors -= slaveId
      }
    }
  }

  private def sparkExecutorId(slaveId: String, taskId: String) = "%s/%s".format(slaveId, taskId)

  override def slaveLost(d: SchedulerDriver, slaveId: SlaveID) {
    logInfo("Mesos slave lost: " + slaveId.getValue)
    executorTerminated(d, slaveId.getValue, "Mesos slave lost: " + slaveId.getValue)
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

  override def doRequestTotalExecutors(requestedTotal: Int): Boolean = {
    // We don't truly know if we can fulfill the full amount of executors
    // since at coarse grain it depends on the amount of slaves available.
    logInfo("Capping the total amount of executors to " + requestedTotal)
    executorLimit = Option(requestedTotal)
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Boolean = {
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
