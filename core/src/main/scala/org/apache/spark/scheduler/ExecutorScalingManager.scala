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

package org.apache.spark.scheduler

import java.util.{Timer, TimerTask}

import scala.collection.mutable

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

/**
 * An agent that dynamically scales the number of executors based on the workload.
 *
 * The add policy depends on the number of pending tasks. If the queue of pending tasks has not
 * been drained for N seconds, then new executors are added. If the queue persists for another N
 * seconds, then more executors are added. The number added in each round increases exponentially
 * from the previous round until an upper bound on the number of executors has been reached.
 *
 * The remove policy is similar, but depends on the number of idle executors. If there have been
 * idle executors for more than M seconds, then a subset of these executors are removed. The
 * number removed in each round increases exponentially from the previous round until a lower
 * bound has been reached. An executor is marked as idle if it has not been assigned a task for
 * more than K seconds.
 *
 * The relevant Spark properties include the following:
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *   spark.dynamicAllocation.addExecutorInterval - How often to add executors (N)
 *   spark.dynamicAllocation.removeExecutorInterval - How often to remove executors (M)
 *   spark.dynamicAllocation.executorIdleThreshold - How long before an executor is marked idle (K)
 *
 * Note: This is part of a larger implementation (SPARK-3174) and currently does not actually
 * request to add or remove executors. The mechanism to actually do this will be added separately,
 * e.g. in SPARK-3822 for Yarn.
 */
private[scheduler] class ExecutorScalingManager(scheduler: TaskSchedulerImpl) extends Logging {
  private val conf = scheduler.conf

  // Lower and upper bounds on the number of executors
  private val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", -1)
  private val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", -1)

  if (minNumExecutors < 0 || maxNumExecutors < 0) {
    throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be set!")
  }

  // How frequently to add and remove executors
  private val addExecutorIntervalMs =
    conf.getLong("spark.dynamicAllocation.addExecutorInterval", 60) * 1000
  private val removeExecutorIntervalMs =
    conf.getLong("spark.dynamicAllocation.removeExecutorInterval", 300) * 1000

  // Timers that keep track of when to add and remove executors
  private var addExecutorTimer: Option[Timer] = None
  private var removeExecutorTimer: Option[Timer] = None

  // The number of pending executors that have not actually been added/removed yet
  private var numExecutorsPendingToAdd = 0
  private var numExecutorsPendingToRemove = 0

  // Keep track of all executors here to decouple us from the logic in TaskSchedulerImpl
  private val executorIds = new mutable.HashSet[String] ++= scheduler.executorIdToHost.keys

  // TODO: deal with synchronization

  /**
   * Start a timer to add new executors (if there is not already one).
   * This is called when a new pending task is added. Each addition is triggered
   * when there have been pending tasks for more than `addExecutorIntervalMs`.
   */
  def startAddExecutorTimer(): Unit = {
    if (addExecutorTimer.isEmpty) {
      logInfo(s"Starting add executor timer (to expire in $addExecutorIntervalMs ms)")
      addExecutorTimer = Some(new Timer)
      addExecutorTimer.get.schedule(
        new AddExecutorTimerTask, addExecutorIntervalMs, addExecutorIntervalMs)
    }
  }

  /**
   * Start a timer to remove executors (if there is not already one).
   * This is called when a new executor is marked as idle, meaning it has not been scheduled
   * a task for a certain duration. Each removal is triggered when there have been idle
   * executors for more than `removeExecutorIntervalMs`.
   */
  def startRemoveExecutorTimer(): Unit = {
    if (removeExecutorTimer.isEmpty) {
      logInfo(s"Starting remove executor timer (to expire in $removeExecutorIntervalMs ms)")
      removeExecutorTimer = Some(new Timer)
      removeExecutorTimer.get.schedule(
        new RemoveExecutorTimerTask, removeExecutorIntervalMs, removeExecutorIntervalMs)
    }
  }

  /**
   * Cancel any existing timer that adds executors.
   * This is called when the pending task queue is drained.
   */
  def cancelAddExecutorTimer(): Unit = {
    addExecutorTimer.foreach { timer =>
      logInfo("Canceling add executor timer because task queue is drained!")
      timer.cancel()
    }
    addExecutorTimer = None
  }

  /**
   * Cancel any existing timer that removes executors.
   * This is called when there are no more idle executors.
   */
  def cancelRemoveExecutorTimer(): Unit = {
    removeExecutorTimer.foreach { timer =>
      logInfo("Canceling remove executor timer because there are no more idle executors!")
      timer.cancel()
    }
    removeExecutorTimer = None
  }

  /**
   * Negotiate with the scheduler backend to add new executors.
   * This ensures the resulting number of executors is correctly constrained by the upper bound.
   * Return whether executors are actually added.
   */
  private def addExecutors(numExecutorsRequested: Int): Int = {
    val numExistingExecutors = executorIds.size + numExecutorsPendingToAdd
    val numExecutorsToAdd =
      if (numExistingExecutors + numExecutorsRequested <= maxNumExecutors) {
        numExecutorsRequested
      } else {
        // Add just enough to reach `maxNumExecutors`
        maxNumExecutors - numExistingExecutors
      }
    val newNumExecutors = numExistingExecutors + numExecutorsToAdd

    if (numExecutorsToAdd > 0) {
      getCoarseGrainedBackend.foreach { backend =>
        logInfo(s"Pending tasks are building up! " +
          s"Adding $numExecutorsToAdd new executors (new total is $newNumExecutors).")
        backend.requestExecutors(numExecutorsToAdd)
        numExecutorsPendingToAdd += numExecutorsToAdd
        return numExecutorsToAdd
      }
    } else {
      logDebug(s"Not adding executors because there are already $maxNumExecutors executors, " +
        s"which is the limit.")
    }
    0
  }

  /**
   * Negotiate with the scheduler backend to remove existing executors.
   * This ensures the resulting number of executors is correctly constrained by the lower bound.
   * Return whether executors are actually removed.
   */
  private def removeExecutors(numExecutorsRequestedToRemove: Int): Int = {
    val numExistingExecutors = executorIds.size - numExecutorsPendingToRemove
    val numExecutorsToRemove =
      if (numExistingExecutors - numExecutorsRequestedToRemove >= minNumExecutors) {
        numExecutorsRequestedToRemove
      } else {
        // Remove just enough to reach `minNumExecutors`
        numExistingExecutors - minNumExecutors
      }
    val newNumExecutors = numExistingExecutors - numExecutorsToRemove

    if (numExecutorsToRemove > 0) {
      getCoarseGrainedBackend.foreach { backend =>
        logInfo(s"Removing $numExecutorsToRemove idle executors! (new total is $newNumExecutors).")
        // backend.killExecutors(...)
        numExecutorsPendingToRemove += numExecutorsToRemove
        return numExecutorsToRemove
      }
    } else {
      logDebug(s"Not removing idle executors because there are only $minNumExecutors executors " +
        s"left, which is the limit.")
    }
    0
  }

  /**
   * Callback for the scheduler to signal that the given executor has been added.
   */
  def executorAdded(executorId: String): Unit = {
    if (!executorIds.contains(executorId)) {
      logDebug(s"New executor $executorId has registered.")
      if (numExecutorsPendingToAdd > 0) {
        numExecutorsPendingToAdd -= 1
        logDebug(s"Decrementing pending executors to add (now at $numExecutorsPendingToAdd).")
      }
      executorIds += executorId
    }
  }

  /**
   * Callback for the scheduler to signal that the given executor has been removed.
   */
  def executorRemoved(executorId: String): Unit = {
    if (executorIds.contains(executorId)) {
      logDebug(s"Existing executor $executorId has been removed.")
      executorIds -= executorId
      if (numExecutorsPendingToRemove > 0) {
        numExecutorsPendingToRemove -= 1
        logDebug(s"Decrementing pending executors to remove (now at $numExecutorsPendingToRemove).")
      }
    }
  }

  /**
   * Return the backend as a CoarseGrainedSchedulerBackend if possible.
   *
   * Otherwise, guard against the use of this feature either before the backend has initialized,
   * or because the scheduler is running in fine-grained mode. In the latter case, the executors
   * are already dynamically allocated by definition, so an appropriate exception is thrown.
   */
  private def getCoarseGrainedBackend: Option[CoarseGrainedSchedulerBackend] = {
    scheduler.backend match {
      case b: CoarseGrainedSchedulerBackend => Some(b)
      case null =>
        logWarning("Scheduler backend not initialized yet for dynamically scaling executors!")
        None
      case _ =>
        throw new SparkException("Dynamic allocation of executors is not applicable to " +
          "fine-grained schedulers. Please set spark.dynamicAllocation.enabled to false.")
    }
  }

  /** A timer task that adds executors in rounds. */
  private class AddExecutorTimerTask extends ExecutorScalingTimerTask(addExecutors)

  /** A timer task that removes executors in rounds. */
  private class RemoveExecutorTimerTask extends ExecutorScalingTimerTask(removeExecutors)

  /**
   * A timer task that scales the number of executors up/down using the provided scaling method.
   * This method takes in a requested number of executors to add/remove, and returns the actual
   * number of executors added/removed as constrained by the lower and upper bounds.
   */
  private abstract class ExecutorScalingTimerTask(scaleExecutors: (Int => Int)) extends TimerTask {

    // How many executors to scale up/down in the next round
    private var numExecutorsToScale = 1

    /**
     * Request to add/remove a given number of executors. The delta increases exponentially from
     * the previous round unless the one of the bounds is reached, in which case it is reset to 1.
     */
    override def run(): Unit = {
      val numExecutorsScaled = scaleExecutors(numExecutorsToScale)
      val limitReached = numExecutorsScaled < numExecutorsToScale
      numExecutorsToScale = if (limitReached) 1 else { numExecutorsToScale * 2 }
    }
  }
}
