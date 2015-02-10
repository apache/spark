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
 * components to convey liveness or execution information for in-progress tasks.
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

  val executorLastSeen = new mutable.HashMap[String, Long]

  val slaveTimeout = sc.conf.getLong("spark.storage.blockManagerSlaveTimeoutMs",
    math.max(sc.conf.getInt("spark.executor.heartbeatInterval", 10000) * 3, 120000))

  val checkTimeoutInterval = sc.conf.getLong("spark.storage.blockManagerTimeoutIntervalMs", 60000)

  var timeoutCheckingTask: Cancellable = null

  override def preStart() {
    import context.dispatcher
    timeoutCheckingTask = context.system.scheduler.schedule(0.seconds,
      checkTimeoutInterval.milliseconds, self, ExpireDeadHosts)
    super.preStart()
  }

  override def receiveWithLogging = {
    case Heartbeat(executorId, taskMetrics, blockManagerId) =>
      heartbeatReceived(executorId)
      val response = HeartbeatResponse(
        !scheduler.executorHeartbeatReceived(executorId, taskMetrics, blockManagerId))
      sender ! response
    case ExpireDeadHosts =>
      expireDeadHosts()
  }

  private def heartbeatReceived(executorId: String) = {
    executorLastSeen(executorId) = System.currentTimeMillis()
  }

  private def expireDeadHosts() {
    logTrace("Checking for hosts with no recent heart beats in HeartbeatReceiver.")
    val now = System.currentTimeMillis()
    val minSeenTime = now - slaveTimeout
    for ((executorId, lastSeenMs) <- executorLastSeen) {
      if (lastSeenMs < minSeenTime) {
        logWarning("Removing Executor " + executorId + " with no recent heart beats: "
          + (now - lastSeenMs) + " ms exceeds " + slaveTimeout + "ms")
        scheduler.executorLost(executorId, SlaveLost())
        sc.killExecutor(executorId)
        executorLastSeen.remove(executorId)
      }
    }
  }

  override def postStop() {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel()
    }
  }
}
