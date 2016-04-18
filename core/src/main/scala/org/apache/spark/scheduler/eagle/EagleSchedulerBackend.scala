/*
 * EAGLE 
 *
 * Copyright 2016 Operating Systems Laboratory EPFL
 *
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

package org.apache.spark.scheduler.eagle

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.deploy.client.AppClient
import org.apache.spark.scheduler.cluster.ExecutorData

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark._
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend.ENDPOINT_NAME
import org.apache.spark.util.{RpcUtils, ThreadUtils, Utils}
import org.apache.spark.internal.Logging

/**
  * A scheduler backend that waits for eagle executors to connect.
  */
private[spark]
class EagleSchedulerBackend(scheduler: EagleScheduler)
  extends ExecutorAllocationClient with SchedulerBackend with Logging {
  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)
  // Total number of executors that are currently registered
  var totalRegisteredExecutors = new AtomicInteger(0)
  val sc = scheduler.sc
  val conf = sc.conf
  val rpcEnv: RpcEnv = sc.env.rpcEnv
  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
  // Submit tasks only after (registered resources / total expected resources)
  // is equal to at least this value, that is double between 0 and 1.
  var minRegisteredRatio =
    math.min(1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0))
  // Submit tasks after maxRegisteredWaitingTime milliseconds
  // if minRegisteredRatio has not yet been reached
  val maxRegisteredWaitingTimeMs =
    conf.getTimeAsMs("spark.scheduler.maxRegisteredResourcesWaitingTime", "30s")
  val createTime = System.currentTimeMillis()

  private val executorDataMap = new HashMap[String, ExecutorData]

  // Number of executors requested from the cluster manager that have not registered yet
  private var numPendingExecutors = 0

  private val listenerBus = scheduler.sc.listenerBus

  // Executors we have requested the cluster manager to kill that have not died yet; maps
  // the executor ID to whether it was explicitly killed by the driver (and thus shouldn't
  // be considered an app-related failure).
  private val executorsPendingToRemove = new HashMap[String, Boolean]

  // A map to store hostname with its possible task number running on it
  protected var hostToLocalTaskCount: Map[String, Int] = Map.empty

  // The number of pending tasks which is locality required
  protected var localityAwareTasks = 0

  // Executors that have been lost, but for which we don't yet know the real exit reason.
  protected val executorsPendingLossReason = new HashSet[String]

  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {

    // If this DriverEndpoint is changed to support multiple threads,
    // then this may need to be changed so that we don't share the serializer
    // instance across threads
    private val ser = SparkEnv.get.closureSerializer.newInstance()

    override protected def log = EagleSchedulerBackend.this.log

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
        logInfo(s"For now we ignore updates $executorId, $taskId, $state")

      case ReviveOffers =>
        logTrace("revive offers.")

      case KillTask(taskId, executorId, interruptThread) =>
        logInfo(s"Killtask not implemented. $taskId, $executorId")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

      case RegisterExecutor(executorId, executorRef, cores, logUrls) =>
        logInfo(s"Registered executor request $executorRef with ID $executorId")
        if (executorDataMap.contains(executorId)) {
          context.reply(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
        } else {
          // If the executor's rpc env is not listening for incoming connections, `hostPort`
          // will be null, and the client connection should be used to contact the executor.

          logInfo(s"Registered executor $executorRef with ID $executorId")
          //addressToExecutorId(executorAddress) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          val data = new ExecutorData(executorRef, executorRef.address, executorId,
            cores, cores, logUrls)
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          EagleSchedulerBackend.this.synchronized {
            executorDataMap.put(executorId, data)
          }
          // Note: some tests expect the reply to come after we put the executor in the map
          context.reply(RegisteredExecutor(executorId))
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
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

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId
        .get(remoteAddress)
        .foreach(removeExecutor(_, SlaveLost("Remote RPC client disassociated. Likely due to " +
          "containers exceeding thresholds, or network issues. Check driver logs for WARN " +
          "messages.")))
    }

    private def executorIsAlive(executorId: String): Boolean = synchronized {
      !executorsPendingToRemove.contains(executorId) &&
        !executorsPendingLossReason.contains(executorId)
    }

    // Remove a disconnected slave from the cluster
    def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          val killed = EagleSchedulerBackend.this.synchronized {
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
        case None => logInfo(s"Asked to remove non-existent executor $executorId")
      }
    }

    override def onStop() {
      reviveThread.shutdownNow()
    }
  }

  var driverEndpoint: RpcEndpointRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  @volatile private var appId: String = _

  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }

    driverEndpoint = createDriverEndpointRef(properties)
  }

  protected def createDriverEndpointRef(
      properties: ArrayBuffer[(String, String)]): RpcEndpointRef = {
    rpcEnv.setupEndpoint(EagleSchedulerBackend.ENDPOINT_NAME, createDriverEndpoint(properties))
  }

  protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new DriverEndpoint(sc.env.rpcEnv, properties)
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
  def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    try {
      driverEndpoint.askWithRetry[Boolean](RemoveExecutor(executorId, reason))
    } catch {
      case e: Exception =>
        throw new SparkException("Error notifying standalone scheduler's driver endpoint", e)
    }
  }

  def sufficientResourcesRegistered(): Boolean = true

  override def getExecutorIds(): Seq[String] = {
    executorDataMap.keySet.toSeq
  }

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
  def numExistingExecutors: Int = executorDataMap.size

  /**
    * Update the cluster manager on our scheduling needs. Three bits of information are included
    * to help it make decisions.
    *
    * @param numExecutors         The total number of executors we'd like to have. The cluster manager
    *                             shouldn't kill any running executor to reach this number, but,
    *                             if all existing executors were to die, this is the number of executors
    *                             we'd want to be allocated.
    * @param localityAwareTasks   The number of tasks in all active stages that have a locality
    *                             preferences. This includes running, pending, and completed tasks.
    * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
    *                             that would like to like to run on that host.
    *                             This includes running, pending, and completed tasks.
    * @return whether the request is acknowledged by the cluster manager.
    */
  override private[spark] def requestTotalExecutors(numExecutors: Int,
      localityAwareTasks: Int, hostToLocalTaskCount: Map[String, Int]): Boolean = false

  /**
    * Request an additional number of executors from the cluster manager.
    *
    * @return whether the request is acknowledged by the cluster manager.
    */
  override def requestExecutors(numAdditionalExecutors: Int): Boolean = false

  /**
    * Request that the cluster manager kill the specified executors.
    *
    * @return whether the request is acknowledged by the cluster manager.
    */
  override def killExecutors(executorIds: Seq[String]): Boolean = false
}

private[spark] object EagleSchedulerBackend {
  val ENDPOINT_NAME = "EagleSchedulerBackend"
}
