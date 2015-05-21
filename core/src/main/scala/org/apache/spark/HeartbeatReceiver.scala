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

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rpc.{ThreadSafeRpcEndpoint, RpcEnv, RpcCallContext}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.scheduler._
import org.apache.spark.util.{ThreadUtils, Utils}

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

private[spark] case class RegisterExecutor(executorId: String)

private[spark] case class RemoveExecutor(executorId: String)

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * Lives in the driver to receive heartbeats from executors..
 */
private[spark] class HeartbeatReceiver(sc: SparkContext)
  extends ThreadSafeRpcEndpoint with SparkListener with Logging {

  sc.addSparkListener(this)

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

  // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
  // block the thread for a long time.
  private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")

  private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")

  override def onStart(): Unit = {
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        Option(self).foreach(_.send(ExpireDeadHosts))
      }
    }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisterExecutor(executorId) =>
      executorLastSeen(executorId) = System.currentTimeMillis()
    case RemoveExecutor(executorId) =>
      executorLastSeen.remove(executorId)
    case ExpireDeadHosts =>
      expireDeadHosts()
    case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case heartbeat @ Heartbeat(executorId, taskMetrics, blockManagerId) =>
      if (scheduler != null) {
        if (executorLastSeen.contains(executorId)) {
          executorLastSeen(executorId) = System.currentTimeMillis()
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, taskMetrics, blockManagerId)
              val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
              context.reply(response)
            }
          })
        } else {
          // This may happen if we get an executor's in-flight heartbeat immediately
          // after we just removed it. It's not really an error condition so we should
          // not log warning here.
          logDebug(s"Received heartbeat from unknown executor $executorId")
        }
      } else {
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor registrations.
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    if (self != null) {
      self.send(RegisterExecutor(executorAdded.executorId))
    }
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
   * log superfluous errors.
   *
   * Note that we must do this after the executor is actually removed to guard against the
   * following race condition: if we remove an executor's metadata from our data structure
   * prematurely, we may get an in-flight heartbeat from the executor before the executor is
   * actually removed, in which case we will still mark the executor as a dead host later
   * and expire it with loud error messages.
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    if (self != null) {
      self.send(RemoveExecutor(executorRemoved.executorId))
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
            override def run(): Unit = Utils.tryLogNonFatalError {
              sc.killExecutor(executorId)
            }
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
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}

object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}
