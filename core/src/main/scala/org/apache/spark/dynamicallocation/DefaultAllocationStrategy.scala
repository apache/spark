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
package org.apache.spark.dynamicallocation

import scala.collection.mutable

import org.apache.spark.{ExecutorAllocationClient, ExecutorAllocationManager, SparkConf, SparkEnv}
import org.apache.spark.dynamicallocation.DefaultAllocationStrategy.NOT_SET
import org.apache.spark.internal.Logging

/**
 * The default strategy maintains a moving target number of executors which is periodically
 * synced to the cluster manager. The target starts at a configured initial value and changes with
 * the number of pending and running tasks.
 *
 * Decreasing the target number of executors happens when the current target is more than needed to
 * handle the current load. The target number of executors is always truncated to the number of
 * executors that could run all current running and pending tasks at once.
 *
 * Increasing the target number of executors happens in response to backlogged tasks waiting to be
 * scheduled. If the scheduler queue is not drained in N seconds, then new executors are added. If
 * the queue persists for another M seconds, then more executors are added and so on. The number
 * added in each round increases exponentially from the previous round until an upper bound has been
 * reached. The upper bound is based both on a configured property and on the current number of
 * running and pending tasks, as described above.
 *
 * The rationale for the exponential increase is twofold: (1) Executors should be added slowly
 * in the beginning in case the number of extra executors needed turns out to be small. Otherwise,
 * we may add more executors than we need just to remove them later. (2) Executors should be added
 * quickly over time in case the maximum number of executors is very high. Otherwise, it will take
 * a long time to ramp up under heavy workloads.
 *
 * The remove policy is simpler: If an executor has been idle for K seconds, meaning it has not
 * been scheduled to run any tasks, then it is removed.
 *
 * There is no retry logic in either case because we make the assumption that the cluster manager
 * will eventually fulfill all requests it receives asynchronously.
 *
 * The relevant Spark properties include the following:
 *
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *
 *   spark.dynamicAllocation.initialExecutors - Number of executors to start with
 *
 *   spark.dynamicAllocation.schedulerBacklogTimeout (M) -
 *     If there are backlogged tasks for this duration, add new executors
 *
 *   spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N) -
 *     If the backlog is sustained for this duration, add more executors
 *     This is used only after the initial backlog timeout is exceeded
 *
 *   spark.dynamicAllocation.executorIdleTimeout (K) -
 *     If an executor has been idle for this duration, remove it
 */
class DefaultAllocationStrategy(
    conf: SparkConf,
    allocationManager: ExecutorAllocationManager,
    client: ExecutorAllocationClient)
  extends AllocationStrategy(conf)
  with Logging {
  // Number of executors to add in the next round
  private var numExecutorsToAdd = 1

  // The desired number of executors at this moment in time. If all our executors were to die, this
  // is the number of executors we would immediately want from the cluster manager.
  private var numExecutorsTarget = initialNumExecutors

  // Executors that have been requested to be removed but have not been killed yet
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  private var addTime: Long = NOT_SET

  // A timestamp for each executor of when the executor should be removed, indexed by the ID
  // This is set when an executor is no longer running a task, or when it first registers
  private val removeTimes = new mutable.HashMap[String, Long]

  /**
   * The maximum number of executors we would need under the current load to satisfy all running
   * and pending tasks, rounded up.
   */
  private def maxNumExecutorsNeeded(): Int = {
    val numRunningOrPendingTasks = allocationManager.listener.totalPendingTasks +
      allocationManager.listener.totalRunningTasks
    (numRunningOrPendingTasks + tasksPerExecutor - 1) / tasksPerExecutor
  }

  /**
   * Updates our target number of executors and syncs the result with the cluster manager.
   *
   * Check to see whether our existing allocation and the requests we've made previously exceed our
   * current needs. If so, truncate our target and let the cluster manager know so that it can
   * cancel pending requests that are unneeded.
   *
   * If not, and the add time has expired, see if we can request new executors and refresh the add
   * time.
   *
   * @return the delta in the target number of executors.
   */
  private def updateAndSyncNumExecutorsTarget(now: Long): Int = synchronized {
    val maxNeeded = maxNumExecutorsNeeded

    if (allocationManager.initializing) {
      // Do not change our target while we are still initializing,
      // Otherwise the first job may have to ramp up unnecessarily
      0
    } else if (maxNeeded < numExecutorsTarget) {
      // The target number exceeds the number we actually need, so stop adding new
      // executors and inform the cluster manager to cancel the extra pending requests
      val oldNumExecutorsTarget = numExecutorsTarget
      numExecutorsTarget = math.max(maxNeeded, minNumExecutors)
      numExecutorsToAdd = 1

      // If the new target has not changed, avoid sending a message to the cluster manager
      if (numExecutorsTarget < oldNumExecutorsTarget) {
        client.requestTotalExecutors(numExecutorsTarget, allocationManager.localityAwareTasks,
          allocationManager.hostToLocalTaskCount)
        logDebug(s"Lowering target number of executors to $numExecutorsTarget (previously " +
          s"$oldNumExecutorsTarget) because not all requested executors are actually needed")
      }
      numExecutorsTarget - oldNumExecutorsTarget
    } else if (addTime != NOT_SET && now >= addTime) {
      val delta = addExecutors(maxNeeded)
      logDebug(s"Starting timer to add more executors (to " +
        s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
      addTime += sustainedSchedulerBacklogTimeoutS * 1000
      delta
    } else {
      0
    }
  }

  /**
   * Request a number of executors from the cluster manager.
   * If the cap on the number of executors is reached, give up and reset the
   * number of executors to add next round instead of continuing to double it.
   *
   * @param maxNumExecutorsNeeded the maximum number of executors all currently running or pending
   *                              tasks could fill
   * @return the number of additional executors actually requested.
   */
  private def addExecutors(maxNumExecutorsNeeded: Int): Int = {
    // Do not request more executors if it would put our target over the upper bound
    if (numExecutorsTarget >= maxNumExecutors) {
      logDebug(s"Not adding executors because our current target total " +
        s"is already $numExecutorsTarget (limit $maxNumExecutors)")
      numExecutorsToAdd = 1
      return 0
    }

    val oldNumExecutorsTarget = numExecutorsTarget
    // There's no point in wasting time ramping up to the number of executors we already have, so
    // make sure our target is at least as much as our current allocation:
    numExecutorsTarget = math.max(numExecutorsTarget, allocationManager.executorIds.size)
    // Boost our target with the number to add for this round:
    numExecutorsTarget += numExecutorsToAdd
    // Ensure that our target doesn't exceed what we need at the present moment:
    numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
    // Ensure that our target fits within configured bounds:
    numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors), minNumExecutors)

    val delta = numExecutorsTarget - oldNumExecutorsTarget

    // If our target has not changed, do not send a message
    // to the cluster manager and reset our exponential growth
    if (delta == 0) {
      numExecutorsToAdd = 1
      return 0
    }

    val addRequestAcknowledged = testing ||
      client.requestTotalExecutors(numExecutorsTarget, allocationManager.localityAwareTasks,
        allocationManager.hostToLocalTaskCount)
    if (addRequestAcknowledged) {
      val executorsString = "executor" + { if (delta > 1) "s" else "" }
      logInfo(s"Requesting $delta new $executorsString because tasks are backlogged" +
        s" (new desired total will be $numExecutorsTarget)")
      numExecutorsToAdd = if (delta == numExecutorsToAdd) {
        numExecutorsToAdd * 2
      } else {
        1
      }
      delta
    } else {
      logWarning(
        s"Unable to reach the cluster manager to request $numExecutorsTarget total executors!")
      numExecutorsTarget = oldNumExecutorsTarget
      0
    }
  }

  /**
   * Request the cluster manager to remove the given executor.
   * Return whether the request is received.
   */
  private def removeExecutor(executorId: String): Boolean = synchronized {
    // Do not kill the executor if we are not aware of it (should never happen)
    if (!allocationManager.executorIds.contains(executorId)) {
      logWarning(s"Attempted to remove unknown executor $executorId!")
      return false
    }

    // Do not kill the executor again if it is already pending to be killed (should never happen)
    if (executorsPendingToRemove.contains(executorId)) {
      logWarning(s"Attempted to remove executor $executorId " +
        s"when it is already pending to be removed!")
      return false
    }

    // Do not kill the executor if we have already reached the lower bound
    val numExistingExecutors = allocationManager.executorIds.size - executorsPendingToRemove.size
    if (numExistingExecutors - 1 < minNumExecutors) {
      logDebug(s"Not removing idle executor $executorId because there are only " +
        s"$numExistingExecutors executor(s) left (limit $minNumExecutors)")
      return false
    }

    // Send a request to the backend to kill this executor
    val removeRequestAcknowledged = testing || client.killExecutor(executorId)
    if (removeRequestAcknowledged) {
      logInfo(s"Removing executor $executorId because it has been idle for " +
        s"$executorIdleTimeoutS seconds (new desired total will be ${numExistingExecutors - 1})")
      executorsPendingToRemove.add(executorId)
      true
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor $executorId," +
        s"or no executor eligible to kill!")
      false
    }
  }

  def onExecutorAdded(executorId: String): Unit = {}

  def onExecutorRemoved(executorId: String): Unit = {
    removeTimes.remove(executorId)
    if (executorsPendingToRemove.contains(executorId)) {
      executorsPendingToRemove.remove(executorId)
      logDebug(s"Executor $executorId is no longer pending to " +
        s"be removed (${executorsPendingToRemove.size} left)")
    }
  }

  def onSchedulerBacklogged(): Unit = {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeoutS seconds)")
      addTime = clock.getTimeMillis + schedulerBacklogTimeoutS * 1000
    }
  }

  def onSchedulerQueueEmpty(): Unit = {
    logDebug("Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAdd = 1
  }

  def onExecutorIdle(executorId: String): Unit = {
    if (!removeTimes.contains(executorId) && !executorsPendingToRemove.contains(executorId)) {
      // Note that it is not necessary to query the executors since all the cached
      // blocks we are concerned with are reported to the driver. Note that this
      // does not include broadcast blocks.
      val hasCachedBlocks = SparkEnv.get.blockManager.master.hasCachedBlocks(executorId)
      val now = clock.getTimeMillis()
      val timeout = {
        if (hasCachedBlocks) {
          // Use a different timeout if the executor has cached blocks.
          now + cachedExecutorIdleTimeoutS * 1000
        } else {
          now + executorIdleTimeoutS * 1000
        }
      }
      val realTimeout = if (timeout <= 0) Long.MaxValue else timeout // overflow
      removeTimes(executorId) = realTimeout
      logDebug(s"Starting idle timer for $executorId because there are no more tasks " +
        s"scheduled to run on the executor (to expire in ${(realTimeout - now)/1000} seconds)")
    }
  }

  def onExecutorBusy(executorId: String): Unit = {
    logDebug(s"Clearing idle timer for $executorId because it is now running a task")
    removeTimes.remove(executorId)
  }

  override def allocate(now: Long): Unit = {
    updateAndSyncNumExecutorsTarget(now)
  }

  override def release(now: Long): Boolean = {
    var removedExecutor = false
    removeTimes.retain { case (executorId, expireTime) =>
      val expired = now >= expireTime
      if (expired) {
        removedExecutor = true
        removeExecutor(executorId)
      }
      !expired
    }
    removedExecutor
  }

  def reset(): Unit = {
    numExecutorsTarget = initialNumExecutors
    numExecutorsToAdd = 1
    executorsPendingToRemove.clear()
    removeTimes.clear()
  }

  def getMetricsToRegister(): Seq[Metric[Any]] = {
    Seq(
      new Metric("numberExecutorsToAdd", numExecutorsToAdd, 0),
      new Metric("numberExecutorsPendingToRemove", executorsPendingToRemove.size, 0),
      new Metric("numberTargetExecutors", numExecutorsTarget, 0),
      new Metric("numberMaxNeededExecutors", maxNumExecutorsNeeded(), 0)
    )
  }
}

object DefaultAllocationStrategy {
  val NOT_SET = Long.MaxValue
}
