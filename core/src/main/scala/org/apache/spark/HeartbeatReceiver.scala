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

package org.apache.spark

import java.util.concurrent.{ScheduledFuture, TimeUnit, Executors}

import scala.collection.mutable

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rpc.{ThreadSafeRpcEndpoint, RpcEnv, RpcCallContext}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.scheduler.{SlaveLost, TaskScheduler}
import org.apache.spark.util.Utils

/**
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks. It will also 
 * expire the hosts that have not heartbeated for more than spark.network.timeout.
 */
private[spark] case class Heartbeat(
    executorId: String,
    taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics
    blockManagerId: BlockManagerId)

/**
 * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
 * created.
 */
private[spark] case object TaskSchedulerIsSet

private[spark] case object ExpireDeadHosts 
    
private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * Lives in the driver to receive heartbeats from executors..
 */
private[spark] class HeartbeatReceiver(sc: SparkContext)
  extends ThreadSafeRpcEndpoint with Logging {

  override val rpcEnv: RpcEnv = sc.env.rpcEnv

  private[spark] var scheduler: TaskScheduler = null

  // executor ID -> timestamp of when the last heartbeat from this executor was received
  private val executorLastSeen = new mutable.HashMap[String, Long]

  // "spark.network.timeout" uses "seconds", while `spark.storage.blockManagerSlaveTimeoutMs` uses
  // "milliseconds"
  private val slaveTimeoutMs = 
    sc.conf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs", "120s")
  private val executorTimeoutMs = 
    sc.conf.getTimeAsSeconds("spark.network.timeout", s"${slaveTimeoutMs}ms") * 1000
  
  // "spark.network.timeoutInterval" uses "seconds", while
  // "spark.storage.blockManagerTimeoutIntervalMs" uses "milliseconds"
  private val timeoutIntervalMs = 
    sc.conf.getTimeAsMs("spark.storage.blockManagerTimeoutIntervalMs", "60s")
  private val checkTimeoutIntervalMs = 
    sc.conf.getTimeAsSeconds("spark.network.timeoutInterval", s"${timeoutIntervalMs}ms") * 1000
  
  private var timeoutCheckingTask: ScheduledFuture[_] = null

  private val timeoutCheckingThread = Executors.newSingleThreadScheduledExecutor(
    Utils.namedThreadFactory("heartbeat-timeout-checking-thread"))

  private val killExecutorThread = Executors.newSingleThreadExecutor(
    Utils.namedThreadFactory("kill-executor-thread"))

  override def onStart(): Unit = {
    timeoutCheckingTask = timeoutCheckingThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        Option(self).foreach(_.send(ExpireDeadHosts))
      }
    }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case ExpireDeadHosts =>
      expireDeadHosts()
    case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case heartbeat @ Heartbeat(executorId, taskMetrics, blockManagerId) =>
      if (scheduler != null) {
        val unknownExecutor = !scheduler.executorHeartbeatReceived(
          executorId, taskMetrics, blockManagerId)
        val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
        executorLastSeen(executorId) = System.currentTimeMillis()
        context.reply(response)
      } else {
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }
  }

  private def expireDeadHosts(): Unit = {
    logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
    val now = System.currentTimeMillis()
    for ((executorId, lastSeenMs) <- executorLastSeen) {
      if (now - lastSeenMs > executorTimeoutMs) {
        logWarning(s"Removing executor $executorId with no recent heartbeats: " +
          s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")
        scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +
          s"timed out after ${now - lastSeenMs} ms"))
        if (sc.supportDynamicAllocation) {
          // Asynchronously kill the executor to avoid blocking the current thread
          killExecutorThread.submit(new Runnable {
            override def run(): Unit = sc.killExecutor(executorId)
          })
        }
        executorLastSeen.remove(executorId)
      }
    }
  }
  
  override def onStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    timeoutCheckingThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}

object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}
