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

import scala.collection.mutable.HashMap

import org.apache.spark.{SparkEnv, TaskState}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{RpcUtils, SerializableBuffer, ThreadUtils, Utils}

private[spark]
class DriverEndpoint(backend: CoarseGrainedSchedulerBackend,
                     scheduler: TaskSchedulerImpl,
                     override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
  extends ThreadSafeRpcEndpoint with Logging {

  protected val conf = scheduler.sc.conf
  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)
  // If this DriverEndpoint is changed to support multiple threads,
  // then this may need to be changed so that we don't share the serializer
  // instance across threads
  private val ser = SparkEnv.get.closureSerializer.newInstance()

  protected val addressToExecutorId = new HashMap[RpcAddress, String]

  private val reviveThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-revive-thread")

  private val executorDataMap = new HashMap[String, ExecutorData]

  private val listenerBus = scheduler.sc.listenerBus

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

        val data = new ExecutorData(executorRef, executorRef.address, hostname,
          cores, cores, logUrls)
        executorDataMap.put(executorId, data)
        backend.handleExecutorRegistered(executorId, cores)
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
    val activeExecutors = executorDataMap.filterKeys(backend.executorIsAlive)
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
    if (backend.executorIsAlive(executorId)) {
      val executorData = executorDataMap(executorId)
      val workOffers = Seq(
        new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))
      launchTasks(scheduler.resourceOffers(workOffers))
    }
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

        logInfo(s"Launching task ${task.taskId} on executor id: ${task.executorId} hostname: " +
          s"${executorData.executorHost}.")

        executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
      }
    }
  }

  // Remove a disconnected slave from the cluster
  private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    executorDataMap.get(executorId) match {
      case Some(executorInfo) =>

        val killed = backend.handleRemoveExecutor(executorId, executorInfo.totalCores)
        addressToExecutorId -= executorInfo.executorAddress
        executorDataMap -= executorId
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
    val shouldDisable = backend.handleDisableExecutor(executorId)
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
