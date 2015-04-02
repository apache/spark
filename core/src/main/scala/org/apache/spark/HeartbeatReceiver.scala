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

import scala.concurrent.duration._
import scala.collection.mutable

import akka.actor.{Actor, Cancellable}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.scheduler.{SlaveLost, TaskScheduler}
import org.apache.spark.util.ActorLogReceive

/**
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks. It will also 
 * expire the hosts that have not heartbeated for more than spark.network.timeout.
 */
private[spark] case class Heartbeat(
    executorId: String,
    taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics
    blockManagerId: BlockManagerId)

private[spark] case object ExpireDeadHosts 
    
private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * Lives in the driver to receive heartbeats from executors..
 */
private[spark] class HeartbeatReceiver(sc: SparkContext, scheduler: TaskScheduler)
  extends Actor with ActorLogReceive with Logging {

  // executor ID -> timestamp of when the last heartbeat from this executor was received
  private val executorLastSeen = new mutable.HashMap[String, Long]

  // "spark.network.timeout" uses "seconds", while `spark.storage.blockManagerSlaveTimeoutMs` uses
  // "milliseconds"
  private val executorTimeoutMs = sc.conf.getOption("spark.network.timeout").map(_.toLong * 1000).
    getOrElse(sc.conf.getLong("spark.storage.blockManagerSlaveTimeoutMs", 120000))

  // "spark.network.timeoutInterval" uses "seconds", while
  // "spark.storage.blockManagerTimeoutIntervalMs" uses "milliseconds"
  private val checkTimeoutIntervalMs =
    sc.conf.getOption("spark.network.timeoutInterval").map(_.toLong * 1000).
      getOrElse(sc.conf.getLong("spark.storage.blockManagerTimeoutIntervalMs", 60000))
  
  private var timeoutCheckingTask: Cancellable = null
  
  override def preStart(): Unit = {
    import context.dispatcher
    timeoutCheckingTask = context.system.scheduler.schedule(0.seconds,
      checkTimeoutIntervalMs.milliseconds, self, ExpireDeadHosts)
    super.preStart()
  }
  
  override def receiveWithLogging: PartialFunction[Any, Unit] = {
    case Heartbeat(executorId, taskMetrics, blockManagerId) =>
      val unknownExecutor = !scheduler.executorHeartbeatReceived(
        executorId, taskMetrics, blockManagerId)
      val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
      executorLastSeen(executorId) = System.currentTimeMillis()
      sender ! response
    case ExpireDeadHosts =>
      expireDeadHosts()
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
          sc.killExecutor(executorId)
        }
        executorLastSeen.remove(executorId)
      }
    }
  }
  
  override def postStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel()
    }
    super.postStop()
  }
}
