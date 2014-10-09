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
 * Synchronization: Because the schedulers in Spark are single-threaded, multiple tasks cannot
 * be scheduled at the same time unless the application itself runs multiple jobs concurrently.
 * This means, under normal circumstances, concurrent accesses are only expected when executors
 * are added or removed. Thus, it is inexpensive to synchronize all methods on this class itself,
 * assuming thread biased locking is enabled in the JVM (on by default for Java 6+). Tighter
 * locks are also used where possible.
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
  private val addExecutorInterval =
    conf.getLong("spark.dynamicAllocation.addExecutorInterval", 60) // s
  private val removeExecutorInterval =
    conf.getLong("spark.dynamicAllocation.removeExecutorInterval", 300) // s
  private val executorIdleThreshold =
    conf.getLong("spark.dynamicAllocation.executorIdleThreshold", removeExecutorInterval) // s

  // Timers that keep track of when to add and remove executors
  private var addExecutorTimer: Option[Timer] = None
  private var removeExecutorTimer: Option[Timer] = None
  private val executorIdleTimers: mutable.Map[String, Timer] = new mutable.HashMap[String, Timer]

  // The number of pending executors that have not actually been added/removed yet
  private var numExecutorsPendingToAdd = 0
  private var numExecutorsPendingToRemove = 0

  // Keep track of all executors here to decouple us from the logic in TaskSchedulerImpl
  private val executorIds = new mutable.HashSet[String] ++= scheduler.executorIdToHost.keys

  // Keep track of idle executors to remove them later
  private val idleExecutorIds = new mutable.HashSet[String]

  // Start idle timer for all new executors
  synchronized { executorIds.foreach(startExecutorIdleTimer) }

  /**
   * Start a timer to add new executors (if there is not already one).
   * This is called when a new pending task is added. Each addition is triggered
   * when there have been pending tasks for more than `addExecutorIntervalMs`.
   */
  def startAddExecutorTimer(): Unit = addExecutorTimer.synchronized {
    if (addExecutorTimer.isEmpty) {
      logInfo(s"Starting add executor timer (to expire in $addExecutorInterval seconds)")
      val addExecutorIntervalMs = addExecutorInterval * 1000
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
  def startRemoveExecutorTimer(): Unit = removeExecutorTimer.synchronized {
    if (removeExecutorTimer.isEmpty) {
      logInfo(s"Starting remove executor timer (to expire in $removeExecutorInterval seconds)")
      val removeExecutorIntervalMs = removeExecutorInterval * 1000
      removeExecutorTimer = Some(new Timer)
      removeExecutorTimer.get.schedule(
        new RemoveExecutorTimerTask, removeExecutorIntervalMs, removeExecutorIntervalMs)
    }
  }

  /**
   * Start a timer on the given executor to mark it idle.
   * This is called when an existing executor has not been scheduled a task for
   * `executorIdleThresholdMs`. When the task is triggered (only once), it marks
   * the given executor as idle and starts the remove executor timer.
   */
  def startExecutorIdleTimer(executorId: String): Unit = synchronized {
    if (!executorIdleTimers.contains(executorId)) {
      logDebug(s"Starting idle timer for executor $executorId " +
        s"(to expire in $executorIdleThreshold seconds)")
      executorIdleTimers(executorId) = new Timer
      executorIdleTimers(executorId).schedule(
        new ExecutorIdleTimerTask(executorId), executorIdleThreshold * 1000)
    }
    if (!executorIds.contains(executorId)) {
      logWarning(s"Started idle timer for unknown executor $executorId.")
      executorIds.add(executorId)
    }
  }

  /**
   *
   * Cancel any existing timer that adds executors.
   * This is called when the pending task queue is drained.
   */
  def cancelAddExecutorTimer(): Unit = addExecutorTimer.synchronized {
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
  def cancelRemoveExecutorTimer(): Unit = removeExecutorTimer.synchronized {
    removeExecutorTimer.foreach { timer =>
      logInfo("Canceling remove executor timer because there are no more idle executors!")
      timer.cancel()
    }
    removeExecutorTimer = None
  }

  /**
   * Cancel any existing idle timer on the given executor and un-mark it as idle if needed.
   * This is called when the executor is scheduled a new task to run.
   * If there are no more idle executors, cancel the remove executor timer if it exists.
   */
  def cancelExecutorIdleTimer(executorId: String): Unit = synchronized {
    if (executorIdleTimers.contains(executorId)) {
      logDebug(s"Canceling idle timer for executor $executorId.")
      executorIdleTimers(executorId).cancel()
      executorIdleTimers.remove(executorId)
    }
    if (idleExecutorIds.contains(executorId)) {
      logDebug(s"Executor $executorId is no longer marked as idle.")
      idleExecutorIds.remove(executorId)
    }
    if (idleExecutorIds.isEmpty) {
      cancelRemoveExecutorTimer()
    }
  }

  /**
   * Negotiate with the scheduler backend to add new executors.
   * This ensures the resulting number of executors is correctly constrained by the upper bound.
   * Return whether executors are actually added.
   */
  private def addExecutors(numExecutorsRequested: Int): Int = synchronized {
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
        numExecutorsPendingToAdd += numExecutorsToAdd
        backend.requestExecutors(numExecutorsToAdd)
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
  private def removeExecutors(numExecutorsRequestedToRemove: Int): Int = synchronized {
    val numExistingExecutors = executorIds.size - numExecutorsPendingToRemove
    val numExecutorsToRemove =
      if (numExistingExecutors - numExecutorsRequestedToRemove >= minNumExecutors) {
        numExecutorsRequestedToRemove
      } else {
        // Remove just enough to reach `minNumExecutors`
        numExistingExecutors - minNumExecutors
      }

    if (numExecutorsToRemove > 0) {
      getCoarseGrainedBackend.foreach { backend =>
        val executorsToRemove = idleExecutorIds.take(numExecutorsToRemove)
        val newNumExecutors = numExistingExecutors - executorsToRemove.size
        logInfo(s"Removing ${executorsToRemove.size} idle executors(s): " +
          s"${executorsToRemove.mkString(", ")} (new total is $newNumExecutors).")
        idleExecutorIds --= executorsToRemove
        numExecutorsPendingToRemove += executorsToRemove.size
        backend.killExecutors(executorsToRemove.toSeq)
        return executorsToRemove.size
      }
    } else {
      logDebug(s"Not removing idle executors because there are only $minNumExecutors " +
        "executor(s) left, which is the limit.")
    }
    0
  }

  /**
   * Callback for the scheduler to signal that the given executor has been added.
   */
  def executorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      logDebug(s"New executor $executorId has registered.")
      if (numExecutorsPendingToAdd > 0) {
        numExecutorsPendingToAdd -= 1
        logDebug(s"Decrementing pending executors to add (now at $numExecutorsPendingToAdd).")
      }
      executorIds += executorId
      startExecutorIdleTimer(executorId)
    }
  }

  /**
   * Callback for the scheduler to signal that the given executor has been removed.
   */
  def executorRemoved(executorId: String): Unit = synchronized {
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

  /** A timer task that marks an executor as idle. */
  private class ExecutorIdleTimerTask(executorId: String) extends TimerTask {
    override def run(): Unit = {
      logInfo(s"Marking executor $executorId as idle because it has not been scheduled " +
        s"a task for $executorIdleThreshold seconds.")
      executorIdleTimers.remove(executorId)
      idleExecutorIds.add(executorId)
      startRemoveExecutorTimer()
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
