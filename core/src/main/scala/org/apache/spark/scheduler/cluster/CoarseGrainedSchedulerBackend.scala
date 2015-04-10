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

import java.util.concurrent.{TimeUnit, Executors}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark.rpc._
import org.apache.spark.{ExecutorAllocationClient, Logging, SparkEnv, SparkException, TaskState}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{SerializableBuffer, AkkaUtils, Utils}

/**
 * A scheduler backend that waits for coarse grained executors to connect to it through Akka.
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 */
private[spark]
class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv)
  extends ExecutorAllocationClient with SchedulerBackend with Logging
{
  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)
  // Total number of executors that are currently registered
  var totalRegisteredExecutors = new AtomicInteger(0)
  val conf = scheduler.sc.conf
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

  private val executorDataMap = new HashMap[String, ExecutorData]

  // Number of executors requested from the cluster manager that have not registered yet
  private var numPendingExecutors = 0

  private val listenerBus = scheduler.sc.listenerBus

  // Executors we have requested the cluster manager to kill that have not died yet
  private val executorsPendingToRemove = new HashSet[String]

  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {
    override protected def log = CoarseGrainedSchedulerBackend.this.log

    private val addressToExecutorId = new HashMap[RpcAddress, String]

    private val reviveThread =
      Executors.newSingleThreadScheduledExecutor(Utils.namedThreadFactory("driver-revive-thread"))

    override def onStart() {
      // Periodically revive offers to allow delay scheduling to work
      val reviveInterval = conf.getLong("spark.scheduler.revive.interval", 1000)
      reviveThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          Option(self).foreach(_.send(ReviveOffers))
        }
      }, 0, reviveInterval, TimeUnit.MILLISECONDS)
    }

    override def receive: PartialFunction[Any, Unit] = {
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
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            executorInfo.executorEndpoint.send(KillTask(taskId, executorId, interruptThread))
          case None =>
            // Ignoring the task kill since the executor is not registered.
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId, executorRef, hostPort, cores, logUrls) =>
        Utils.checkHostPort(hostPort, "Host port expected " + hostPort)
        if (executorDataMap.contains(executorId)) {
          context.reply(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
        } else {
          logInfo("Registered executor: " + executorRef + " with ID " + executorId)
          context.reply(RegisteredExecutor)

          addressToExecutorId(executorRef.address) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          val (host, _) = Utils.parseHostPort(hostPort)
          val data = new ExecutorData(executorRef, executorRef.address, host, cores, cores, logUrls)
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          CoarseGrainedSchedulerBackend.this.synchronized {
            executorDataMap.put(executorId, data)
            if (numPendingExecutors > 0) {
              numPendingExecutors -= 1
              logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
            }
          }
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
          makeOffers()
        }

      case StopDriver =>
        context.reply(true)
        stop()

      case StopExecutors =>
        logInfo("Asking each executor to shut down")
        for ((_, executorData) <- executorDataMap) {
          executorData.executorEndpoint.send(StopExecutor)
        }
        context.reply(true)

      case RemoveExecutor(executorId, reason) =>
        removeExecutor(executorId, reason)
        context.reply(true)

      case RetrieveSparkProps =>
        context.reply(sparkProperties)
    }

    // Make fake resource offers on all executors
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(executorDataMap.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toSeq))
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId.get(remoteAddress).foreach(removeExecutor(_,
        "remote Rpc client disassociated"))
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
          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    // Remove a disconnected slave from the cluster
    def removeExecutor(executorId: String, reason: String): Unit = {
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          CoarseGrainedSchedulerBackend.this.synchronized {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
            executorsPendingToRemove -= executorId
          }
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          totalRegisteredExecutors.addAndGet(-1)
          scheduler.executorLost(executorId, SlaveLost(reason))
          listenerBus.post(
            SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason))
        case None => logError(s"Asked to remove non-existent executor $executorId")
      }
    }

    override def onStop() {
      reviveThread.shutdownNow()
    }
  }

  var driverEndpoint: RpcEndpointRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }
    // TODO (prashant) send conf instead of properties
    driverEndpoint = rpcEnv.setupEndpoint(
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME, new DriverEndpoint(rpcEnv, properties))
  }

  def stopExecutors() {
    try {
      if (driverEndpoint != null) {
        logInfo("Shutting down all executors")
        driverEndpoint.askWithReply[Boolean](StopExecutors)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error asking standalone scheduler to shut down executors", e)
    }
  }

  override def stop() {
    stopExecutors()
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askWithReply[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver endpoint", e)
    }
  }

  override def reviveOffers() {
    driverEndpoint.send(ReviveOffers)
  }

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread))
  }

  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  // Called by subclasses when notified of a lost worker
  def removeExecutor(executorId: String, reason: String) {
    try {
      driverEndpoint.askWithReply[Boolean](RemoveExecutor(executorId, reason))
    } catch {
      case e: Exception =>
        throw new SparkException("Error notifying standalone scheduler's driver endpoint", e)
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

  /**
   * Return the number of executors currently registered with this backend.
   */
  def numExistingExecutors: Int = executorDataMap.size

  /**
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is acknowledged.
   */
  final override def requestExecutors(numAdditionalExecutors: Int): Boolean = synchronized {
    if (numAdditionalExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of additional executor(s) " +
        s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
    }
    logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")
    logDebug(s"Number of pending executors is now $numPendingExecutors")
    numPendingExecutors += numAdditionalExecutors
    // Account for executors pending to be added or removed
    val newTotal = numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size
    doRequestTotalExecutors(newTotal)
  }

  /**
   * Express a preference to the cluster manager for a given total number of executors. This can
   * result in canceling pending requests or filing additional requests.
   * @return whether the request is acknowledged.
   */
  final override def requestTotalExecutors(numExecutors: Int): Boolean = synchronized {
    if (numExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of executor(s) " +
          s"$numExecutors from the cluster manager. Please specify a positive number!")
    }
    numPendingExecutors =
      math.max(numExecutors - numExistingExecutors + executorsPendingToRemove.size, 0)
    doRequestTotalExecutors(numExecutors)
  }

  /**
   * Request executors from the cluster manager by specifying the total number desired,
   * including existing pending and running executors.
   *
   * The semantics here guarantee that we do not over-allocate executors for this application,
   * since a later request overrides the value of any prior request. The alternative interface
   * of requesting a delta of executors risks double counting new executors when there are
   * insufficient resources to satisfy the first request. We make the assumption here that the
   * cluster manager will eventually fulfill all requests when resources free up.
   *
   * @return whether the request is acknowledged.
   */
  protected def doRequestTotalExecutors(requestedTotal: Int): Boolean = false

  /**
   * Request that the cluster manager kill the specified executors.
   * Return whether the kill request is acknowledged.
   */
  final override def killExecutors(executorIds: Seq[String]): Boolean = synchronized {
    logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")
    val filteredExecutorIds = new ArrayBuffer[String]
    executorIds.foreach { id =>
      if (executorDataMap.contains(id)) {
        filteredExecutorIds += id
      } else {
        logWarning(s"Executor to kill $id does not exist!")
      }
    }
    // Killing executors means effectively that we want less executors than before, so also update
    // the target number of executors to avoid having the backend allocate new ones.
    val newTotal = (numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size
      - filteredExecutorIds.size)
    doRequestTotalExecutors(newTotal)

    executorsPendingToRemove ++= filteredExecutorIds
    doKillExecutors(filteredExecutorIds)
  }

  /**
   * Kill the given list of executors through the cluster manager.
   * Return whether the kill request is acknowledged.
   */
  protected def doKillExecutors(executorIds: Seq[String]): Boolean = false

}

private[spark] object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}
