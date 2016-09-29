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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.apache.spark.{ExecutorAllocationClient, SparkEnv, SparkException, TaskState}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend.ENDPOINT_NAME
import org.apache.spark.util.{RpcUtils, SerializableBuffer, ThreadUtils, Utils}

/**
 * A scheduler backend that waits for coarse-grained executors to connect.
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
  protected val totalCoreCount = new AtomicInteger(0)
  // Total number of executors that are currently registered
  protected val totalRegisteredExecutors = new AtomicInteger(0)
  protected val conf = scheduler.sc.conf
  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
  private val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)
  // Submit tasks only after (registered resources / total expected resources)
  // is equal to at least this value, that is double between 0 and 1.
  private val _minRegisteredRatio =
    math.min(1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0))
  // Submit tasks after maxRegisteredWaitingTime milliseconds
  // if minRegisteredRatio has not yet been reached
  private val maxRegisteredWaitingTimeMs =
    conf.getTimeAsMs("spark.scheduler.maxRegisteredResourcesWaitingTime", "30s")
  private val createTime = System.currentTimeMillis()

  // Accessing `executorDataMap` in `DriverEndpoint.receive/receiveAndReply` doesn't need any
  // protection. But accessing `executorDataMap` out of `DriverEndpoint.receive/receiveAndReply`
  // must be protected by `CoarseGrainedSchedulerBackend.this`. Besides, `executorDataMap` should
  // only be modified in `DriverEndpoint.receive/receiveAndReply` with protection by
  // `CoarseGrainedSchedulerBackend.this`.
  private val executorDataMap = new HashMap[String, ExecutorData]

  // Number of executors requested from the cluster manager that have not registered yet
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private var numPendingExecutors = 0

  private val listenerBus = scheduler.sc.listenerBus

  // Executors we have requested the cluster manager to kill that have not died yet; maps
  // the executor ID to whether it was explicitly killed by the driver (and thus shouldn't
  // be considered an app-related failure).
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private val executorsPendingToRemove = new HashMap[String, Boolean]

  // A map to store hostname with its possible task number running on it
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  protected var hostToLocalTaskCount: Map[String, Int] = Map.empty

  // The number of pending tasks which is locality required
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  protected var localityAwareTasks = 0

  // The num of current max ExecutorId used to re-register appMaster
  @volatile protected var currentExecutorIdCounter = 0

  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {

    // Executors that have been lost, but for which we don't yet know the real exit reason.
    protected val executorsPendingLossReason = new HashSet[String]

    // If this DriverEndpoint is changed to support multiple threads,
    // then this may need to be changed so that we don't share the serializer
    // instance across threads
    private val ser = SparkEnv.get.closureSerializer.newInstance()

    protected val addressToExecutorId = new HashMap[RpcAddress, String]

    private val reviveThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-revive-thread")

    override def onStart() {
      // Periodically revive offers to allow delay scheduling to work
      val reviveIntervalMs = conf.getTimeAsMs("spark.scheduler.revive.interval", "1s")

      reviveThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          Option(self).foreach(_.send(ReviveOffers))
        }
      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)
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
                s"from unknown executor with ID $executorId")
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

      case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls) =>
        if (executorDataMap.contains(executorId)) {
          executorRef.send(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
          context.reply(true)
        } else {
          // If the executor's rpc env is not listening for incoming connections, `hostPort`
          // will be null, and the client connection should be used to contact the executor.
          val executorAddress = if (executorRef.address != null) {
              executorRef.address
            } else {
              context.senderAddress
            }
          logInfo(s"Registered executor $executorRef ($executorAddress) with ID $executorId")
          addressToExecutorId(executorAddress) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          val data = new ExecutorData(executorRef, executorRef.address, hostname,
            cores, cores, logUrls)
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          CoarseGrainedSchedulerBackend.this.synchronized {
            executorDataMap.put(executorId, data)
            if (currentExecutorIdCounter < executorId.toInt) {
              currentExecutorIdCounter = executorId.toInt
            }
            if (numPendingExecutors > 0) {
              numPendingExecutors -= 1
              logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
            }
          }
          executorRef.send(RegisteredExecutor)
          // Note: some tests expect the reply to come after we put the executor in the map
          context.reply(true)
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
        // We will remove the executor's state and cannot restore it. However, the connection
        // between the driver and the executor may be still alive so that the executor won't exit
        // automatically, so try to tell the executor to stop itself. See SPARK-13519.
        executorDataMap.get(executorId).foreach(_.executorEndpoint.send(StopExecutor))
        removeExecutor(executorId, reason)
        context.reply(true)

      case RetrieveSparkProps =>
        context.reply(sparkProperties)
    }

    // Make fake resource offers on all executors
    private def makeOffers() {
      // Filter out executors under killing
      val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
      val workOffers = activeExecutors.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toSeq
      launchTasks(scheduler.resourceOffers(workOffers))
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId
        .get(remoteAddress)
        .foreach(removeExecutor(_, SlaveLost("Remote RPC client disassociated. Likely due to " +
          "containers exceeding thresholds, or network issues. Check driver logs for WARN " +
          "messages.")))
    }

    // Make fake resource offers on just one executor
    private def makeOffers(executorId: String) {
      // Filter out executors under killing
      if (executorIsAlive(executorId)) {
        val executorData = executorDataMap(executorId)
        val workOffers = Seq(
          new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))
        launchTasks(scheduler.resourceOffers(workOffers))
      }
    }

    private def executorIsAlive(executorId: String): Boolean = synchronized {
      !executorsPendingToRemove.contains(executorId) &&
        !executorsPendingLossReason.contains(executorId)
    }

    // Launch tasks returned by a set of resource offers
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        val serializedTask = ser.serialize(task)
        if (serializedTask.limit >= maxRpcMessageSize) {
          scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.rpc.message.maxSize (%d bytes). Consider increasing " +
                "spark.rpc.message.maxSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, maxRpcMessageSize)
              taskSetMgr.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else {
          val executorData = executorDataMap(task.executorId)
          executorData.freeCores -= scheduler.CPUS_PER_TASK

          logDebug(s"Launching task ${task.taskId} on executor id: ${task.executorId} hostname: " +
            s"${executorData.executorHost}.")

          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    // Remove a disconnected slave from the cluster
    private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
      logDebug(s"Asked to remove executor $executorId with reason $reason")
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          val killed = CoarseGrainedSchedulerBackend.this.synchronized {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
            executorsPendingLossReason -= executorId
            executorsPendingToRemove.remove(executorId).getOrElse(false)
          }
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          totalRegisteredExecutors.addAndGet(-1)
          scheduler.executorLost(executorId, if (killed) ExecutorKilled else reason)
          listenerBus.post(
            SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason.toString))
        case None =>
          // SPARK-15262: If an executor is still alive even after the scheduler has removed
          // its metadata, we may receive a heartbeat from that executor and tell its block
          // manager to reregister itself. If that happens, the block manager master will know
          // about the executor, but the scheduler will not. Therefore, we should remove the
          // executor from the block manager when we hit this case.
          scheduler.sc.env.blockManager.master.removeExecutorAsync(executorId)
          logInfo(s"Asked to remove non-existent executor $executorId")
      }
    }

    /**
     * Stop making resource offers for the given executor. The executor is marked as lost with
     * the loss reason still pending.
     *
     * @return Whether executor should be disabled
     */
    protected def disableExecutor(executorId: String): Boolean = {
      val shouldDisable = CoarseGrainedSchedulerBackend.this.synchronized {
        if (executorIsAlive(executorId)) {
          executorsPendingLossReason += executorId
          true
        } else {
          // Returns true for explicitly killed executors, we also need to get pending loss reasons;
          // For others return false.
          executorsPendingToRemove.contains(executorId)
        }
      }

      if (shouldDisable) {
        logInfo(s"Disabling executor $executorId.")
        scheduler.executorLost(executorId, LossReasonPending)
      }

      shouldDisable
    }

    override def onStop() {
      reviveThread.shutdownNow()
    }
  }

  var driverEndpoint: RpcEndpointRef = null

  protected def minRegisteredRatio: Double = _minRegisteredRatio

  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }

    // TODO (prashant) send conf instead of properties
    driverEndpoint = createDriverEndpointRef(properties)
  }

  protected def createDriverEndpointRef(
      properties: ArrayBuffer[(String, String)]): RpcEndpointRef = {
    rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint(properties))
  }

  protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new DriverEndpoint(rpcEnv, properties)
  }

  def stopExecutors() {
    try {
      if (driverEndpoint != null) {
        logInfo("Shutting down all executors")
        driverEndpoint.askWithRetry[Boolean](StopExecutors)
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
        driverEndpoint.askWithRetry[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver endpoint", e)
    }
  }

  /**
   * Reset the state of CoarseGrainedSchedulerBackend to the initial state. Currently it will only
   * be called in the yarn-client mode when AM re-registers after a failure.
   * */
  protected def reset(): Unit = synchronized {
    numPendingExecutors = 0
    executorsPendingToRemove.clear()

    // Remove all the lingering executors that should be removed but not yet. The reason might be
    // because (1) disconnected event is not yet received; (2) executors die silently.
    executorDataMap.toMap.foreach { case (eid, _) =>
      driverEndpoint.askWithRetry[Boolean](
        RemoveExecutor(eid, SlaveLost("Stale executor after cluster manager re-registered.")))
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

  /**
   * Called by subclasses when notified of a lost worker. It just fires the message and returns
   * at once.
   */
  protected def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    // Only log the failure since we don't care about the result.
    driverEndpoint.ask[Boolean](RemoveExecutor(executorId, reason)).onFailure { case t =>
      logError(t.getMessage, t)
    }(ThreadUtils.sameThread)
  }

  def sufficientResourcesRegistered(): Boolean = true

  override def isReady(): Boolean = {
    if (sufficientResourcesRegistered) {
      logInfo("SchedulerBackend is ready for scheduling beginning after " +
        s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
      return true
    }
    if ((System.currentTimeMillis() - createTime) >= maxRegisteredWaitingTimeMs) {
      logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
        s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTimeMs(ms)")
      return true
    }
    false
  }

  /**
   * Return the number of executors currently registered with this backend.
   */
  private def numExistingExecutors: Int = executorDataMap.size

  override def getExecutorIds(): Seq[String] = {
    executorDataMap.keySet.toSeq
  }

  /**
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is acknowledged.
   */
  final override def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    if (numAdditionalExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of additional executor(s) " +
        s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
    }
    logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")

    val response = synchronized {
      numPendingExecutors += numAdditionalExecutors
      logDebug(s"Number of pending executors is now $numPendingExecutors")

      // Account for executors pending to be added or removed
      doRequestTotalExecutors(
        numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size)
    }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  final override def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]
    ): Boolean = {
    if (numExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of executor(s) " +
          s"$numExecutors from the cluster manager. Please specify a positive number!")
    }

    val response = synchronized {
      this.localityAwareTasks = localityAwareTasks
      this.hostToLocalTaskCount = hostToLocalTaskCount

      numPendingExecutors =
        math.max(numExecutors - numExistingExecutors + executorsPendingToRemove.size, 0)

      doRequestTotalExecutors(numExecutors)
    }

    defaultAskTimeout.awaitResult(response)
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
   * @return a future whose evaluation indicates whether the request is acknowledged.
   */
  protected def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] =
    Future.successful(false)

  /**
   * Request that the cluster manager kill the specified executors.
   * @return whether the kill request is acknowledged. If list to kill is empty, it will return
   *         false.
   */
  final override def killExecutors(executorIds: Seq[String]): Seq[String] = {
    killExecutors(executorIds, replace = false, force = false)
  }

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * When asking the executor to be replaced, the executor loss is considered a failure, and
   * killed tasks that are running on the executor will count towards the failure limits. If no
   * replacement is being requested, then the tasks will not count towards the limit.
   *
   * @param executorIds identifiers of executors to kill
   * @param replace whether to replace the killed executors with new ones
   * @param force whether to force kill busy executors
   * @return whether the kill request is acknowledged. If list to kill is empty, it will return
   *         false.
   */
  final def killExecutors(
      executorIds: Seq[String],
      replace: Boolean,
      force: Boolean): Seq[String] = {
    logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")

    val response = synchronized {
      val (knownExecutors, unknownExecutors) = executorIds.partition(executorDataMap.contains)
      unknownExecutors.foreach { id =>
        logWarning(s"Executor to kill $id does not exist!")
      }

      // If an executor is already pending to be removed, do not kill it again (SPARK-9795)
      // If this executor is busy, do not kill it unless we are told to force kill it (SPARK-9552)
      val executorsToKill = knownExecutors
        .filter { id => !executorsPendingToRemove.contains(id) }
        .filter { id => force || !scheduler.isExecutorBusy(id) }
      executorsToKill.foreach { id => executorsPendingToRemove(id) = !replace }

      logInfo(s"Actual list of executor(s) to be killed is ${executorsToKill.mkString(", ")}")

      // If we do not wish to replace the executors we kill, sync the target number of executors
      // with the cluster manager to avoid allocating new ones. When computing the new target,
      // take into account executors that are pending to be added or removed.
      val adjustTotalExecutors =
        if (!replace) {
          doRequestTotalExecutors(
            numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size)
        } else {
          numPendingExecutors += knownExecutors.size
          Future.successful(true)
        }

      val killExecutors: Boolean => Future[Boolean] =
        if (!executorsToKill.isEmpty) {
          _ => doKillExecutors(executorsToKill)
        } else {
          _ => Future.successful(false)
        }

      val killResponse = adjustTotalExecutors.flatMap(killExecutors)(ThreadUtils.sameThread)

      killResponse.flatMap(killSuccessful =>
        Future.successful (if (killSuccessful) executorsToKill else Seq.empty[String])
      )(ThreadUtils.sameThread)
    }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Kill the given list of executors through the cluster manager.
   * @return whether the kill request is acknowledged.
   */
  protected def doKillExecutors(executorIds: Seq[String]): Future[Boolean] =
    Future.successful(false)
}

private[spark] object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}
