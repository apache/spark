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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.{SparkEnv, Logging, SparkException, TaskState}
import org.apache.spark.scheduler.{SchedulerBackend, SlaveLost, TaskDescription, TaskSchedulerImpl, WorkerOffer}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ActorLogReceive, SerializableBuffer, AkkaUtils, Utils}
import org.apache.spark.ui.JettyUtils

/**
 * A scheduler backend that waits for coarse grained executors to connect to it through Akka.
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 */
private[spark]
class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, actorSystem: ActorSystem)
  extends SchedulerBackend with Logging
{
  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)
  var totalRegisteredExecutors = new AtomicInteger(0)
  val conf = scheduler.sc.conf
  private val timeout = AkkaUtils.askTimeout(conf)
  private val akkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)
  // Submit tasks only after (registered resources / total expected resources)
  // is equal to at least this value, that is double between 0 and 1.
  var minRegisteredRatio =
    math.min(1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0))
  // Submit tasks after maxRegisteredWaitingTime milliseconds
  // if minRegisteredRatio has not yet been reached
  val maxRegisteredWaitingTime =
    conf.getInt("spark.scheduler.maxRegisteredResourcesWaitingTime", 30000)
  val createTime = System.currentTimeMillis()

  class DriverActor(sparkProperties: Seq[(String, String)]) extends Actor with ActorLogReceive {
    override protected def log = CoarseGrainedSchedulerBackend.this.log
    private val addressToExecutorId = new HashMap[Address, String]
    private val executorDataMap = new HashMap[String, ExecutorData]

    override def preStart() {
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

      // Periodically revive offers to allow delay scheduling to work
      val reviveInterval = conf.getLong("spark.scheduler.revive.interval", 1000)
      import context.dispatcher
      context.system.scheduler.schedule(0.millis, reviveInterval.millis, self, ReviveOffers)
    }

    def receiveWithLogging = {
      case RegisterExecutor(executorId, hostPort, cores) =>
        Utils.checkHostPort(hostPort, "Host port expected " + hostPort)
        if (executorDataMap.contains(executorId)) {
          sender ! RegisterExecutorFailed("Duplicate executor ID: " + executorId)
        } else {
          logInfo("Registered executor: " + sender + " with ID " + executorId)
          sender ! RegisteredExecutor
          executorDataMap.put(executorId, new ExecutorData(sender, sender.path.address,
            Utils.parseHostPort(hostPort)._1, cores, cores))

          addressToExecutorId(sender.path.address) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          makeOffers()
        }

      case StatusUpdate(executorId, taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              executorInfo.freeCores += scheduler.CPUS_PER_TASK
              makeOffers(executorId)
            case None =>
              // Ignoring the update since we don't know about the executor.
              logWarning(s"Ignored task status update ($taskId state $state) " +
                "from unknown executor $sender with ID $executorId")
          }
        }

      case ReviveOffers =>
        makeOffers()

      case KillTask(taskId, executorId, interruptThread) =>
        executorDataMap(executorId).executorActor ! KillTask(taskId, executorId, interruptThread)

      case StopDriver =>
        sender ! true
        context.stop(self)

      case StopExecutors =>
        logInfo("Asking each executor to shut down")
        for ((_, executorData) <- executorDataMap) {
          executorData.executorActor ! StopExecutor
        }
        sender ! true

      case RemoveExecutor(executorId, reason) =>
        removeExecutor(executorId, reason)
        sender ! true

      case AddWebUIFilter(filterName, filterParams, proxyBase) =>
        addWebUIFilter(filterName, filterParams, proxyBase)
        sender ! true

      case DisassociatedEvent(_, address, _) =>
        addressToExecutorId.get(address).foreach(removeExecutor(_,
          "remote Akka client disassociated"))

      case RetrieveSparkProps =>
        sender ! sparkProperties
    }

    // Make fake resource offers on all executors
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(executorDataMap.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toSeq))
    }

    // Make fake resource offers on just one executor
    def makeOffers(executorId: String) {
      val executorData = executorDataMap(executorId)
      launchTasks(scheduler.resourceOffers(
        Seq(new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))))
    }

    // Launch tasks returned by a set of resource offers
    def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val serializedTask = ser.serialize(task)
        if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
          val taskSetId = scheduler.taskIdToTaskSetId(task.taskId)
          scheduler.activeTaskSets.get(taskSetId).foreach { taskSet =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
                "spark.akka.frameSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize,
                AkkaUtils.reservedSizeBytes)
              taskSet.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else {
          val executorData = executorDataMap(task.executorId)
          executorData.freeCores -= scheduler.CPUS_PER_TASK
          executorData.executorActor ! LaunchTask(new SerializableBuffer(serializedTask))
        }
      }
    }

    // Remove a disconnected slave from the cluster
    def removeExecutor(executorId: String, reason: String) {
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          executorDataMap -= executorId
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          scheduler.executorLost(executorId, SlaveLost(reason))
        case None => logError(s"Asked to remove non existant executor $executorId")
      }
    }
  }

  var driverActor: ActorRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }
    // TODO (prashant) send conf instead of properties
    driverActor = actorSystem.actorOf(
      Props(new DriverActor(properties)), name = CoarseGrainedSchedulerBackend.ACTOR_NAME)
  }

  def stopExecutors() {
    try {
      if (driverActor != null) {
        logInfo("Shutting down all executors")
        val future = driverActor.ask(StopExecutors)(timeout)
        Await.ready(future, timeout)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error asking standalone scheduler to shut down executors", e)
    }
  }

  override def stop() {
    stopExecutors()
    try {
      if (driverActor != null) {
        val future = driverActor.ask(StopDriver)(timeout)
        Await.ready(future, timeout)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver actor", e)
    }
  }

  override def reviveOffers() {
    driverActor ! ReviveOffers
  }

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    driverActor ! KillTask(taskId, executorId, interruptThread)
  }

  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  // Called by subclasses when notified of a lost worker
  def removeExecutor(executorId: String, reason: String) {
    try {
      val future = driverActor.ask(RemoveExecutor(executorId, reason))(timeout)
      Await.ready(future, timeout)
    } catch {
      case e: Exception =>
        throw new SparkException("Error notifying standalone scheduler's driver actor", e)
    }
  }

  def sufficientResourcesRegistered(): Boolean = true

  override def isReady(): Boolean = {
    if (sufficientResourcesRegistered) {
      logInfo("SchedulerBackend is ready for scheduling beginning after " +
        s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
      return true
    }
    if ((System.currentTimeMillis() - createTime) >= maxRegisteredWaitingTime) {
      logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
        s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTime(ms)")
      return true
    }
    false
  }

  // Add filters to the SparkUI
  def addWebUIFilter(filterName: String, filterParams: String, proxyBase: String) {
    if (proxyBase != null && proxyBase.nonEmpty) {
      System.setProperty("spark.ui.proxyBase", proxyBase)
    }

    if (Seq(filterName, filterParams).forall(t => t != null && t.nonEmpty)) {
      logInfo(s"Add WebUI Filter. $filterName, $filterParams, $proxyBase")
      conf.set("spark.ui.filters", filterName)
      conf.set(s"spark.$filterName.params", filterParams)
      scheduler.sc.ui.foreach { ui => JettyUtils.addFilters(ui.getHandlers, conf) }
    }
  }
}

private[spark] object CoarseGrainedSchedulerBackend {
  val ACTOR_NAME = "CoarseGrainedScheduler"
}
