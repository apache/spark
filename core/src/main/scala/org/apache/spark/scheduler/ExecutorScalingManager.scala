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
 * been drained for N seconds, then new executors are added. If the queue persists for another M
 * seconds, then more executors are added and so on. The number added in each round increases
 * exponentially from the previous round until an upper bound on the number of executors has
 * been reached.
 *
 * The rationale for the exponential increase is twofold: (1) Executors should be added slowly
 * in the beginning in case the number of extra executors needed turns out to be small. Otherwise,
 * we may add more executors than we need just to remove them later. (2) Executors should be added
 * quickly over time in case the maximum number of executors is very high. Otherwise, it will take
 * a long time to ramp up under heavy workloads.
 *
 * The remove policy is simpler: If an executor has been idle, meaning it has not been scheduled
 * to run any tasks, for K seconds, then it is removed. This requires starting a timer on each
 * executor instead of just starting a global one as in the add case.
 *
 * The relevant Spark properties include the following:
 *
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *
 *   spark.dynamicAllocation.addExecutorThreshold - How long before new executors are added (N)
 *   spark.dynamicAllocation.addExecutorInterval - How often to add new executors (M)
 *   spark.dynamicAllocation.removeExecutorThreshold - How long before an executor is removed (K)
 *
 *   spark.dynamicAllocation.retryAddExecutorInterval - How often to retry adding executors
 *   spark.dynamicAllocation.retryRemoveExecutorInterval - How often to retry removing executors
 *   spark.dynamicAllocation.maxRetryAddExecutorAttempts - Max retries in re-adding executors
 *   spark.dynamicAllocation.maxRetryRemoveExecutorAttempts - Max retries in re-removing executors
 *
 * Synchronization: Because the schedulers in Spark are single-threaded, contention only arises
 * if the application itself runs multiple jobs concurrently. Under normal circumstances, however,
 * synchronizing each method on this class should not be expensive assuming biased locking is
 * enabled in the JVM (on by default for Java 6+).
 *
 * Note: This is part of a larger implementation (SPARK-3174) and currently does not actually
 * request to add or remove executors. The mechanism to actually do this will be added separately,
 * e.g. in SPARK-3822 for Yarn.
 */
private[scheduler] class ExecutorScalingManager(scheduler: TaskSchedulerImpl) extends Logging {
  private val conf = scheduler.conf

  // Lower and upper bounds on the number of executors. These are required.
  private val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", -1)
  private val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", -1)
  if (minNumExecutors < 0 || maxNumExecutors < 0) {
    throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be set!")
  }

  // How frequently to add and remove executors (seconds)
  private val addExecutorThreshold =
    conf.getLong("spark.dynamicAllocation.addExecutorThreshold", 60)
  private val addExecutorInterval =
    conf.getLong("spark.dynamicAllocation.addExecutorInterval", addExecutorThreshold)
  private val removeExecutorThreshold =
    conf.getLong("spark.dynamicAllocation.removeExecutorThreshold", 600)
  private val retryAddExecutorInterval =
    conf.getLong("spark.dynamicAllocation.retryAddExecutorInterval", 300)
  private val retryRemoveExecutorInterval =
    conf.getLong("spark.dynamicAllocation.retryRemoveExecutorInterval", 300)

  // Timers that keep track of when to add and remove executors
  private var addExecutorTimer: Option[Timer] = None
  private val removeExecutorTimers: mutable.Map[String, Timer] = new mutable.HashMap[String, Timer]
  private var retryAddExecutorTimer: Option[Timer] = None
  private var retryRemoveExecutorTimer: Option[Timer] = None

  // Number of executors to add in the next round
  private var numExecutorsToAdd = 1

  // Pending executors that have not actually been added/removed yet
  private var numExecutorsPendingToAdd = 0
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // Retry attempts
  private var retryAddExecutorAttempts = 0
  private var retryRemoveExecutorAttempts = 0
  private val maxRetryAddExecutorAttempts =
    conf.getInt("spark.dynamicAllocation.maxRetryAddExecutorAttempts", 10)
  private val maxRetryRemoveExecutorAttempts =
    conf.getInt("spark.dynamicAllocation.maxRetryRemoveExecutorAttempts", 10)

  // Keep track of all executors here to decouple us from the logic in TaskSchedulerImpl
  private val executorIds = new mutable.HashSet[String]

  // Initialize with existing known executors
  scheduler.executorIdToHost.keys.foreach(executorAdded)

  /**
   * Start the add executor timer if it does not already exist.
   * This is called when a new pending task is added. The add is then triggered
   * if the pending tasks queue is not drained in `addExecutorThreshold` seconds.
   */
  def startAddExecutorTimer(): Unit = startAddExecutorTimer(addExecutorThreshold)

  /**
   * Restart the add executor timer.
   * This is called when the previous add executor timer has expired but not canceled. The add
   * is then triggered again if all pending executors from the previous round have registered,
   * and the pending tasks queue is still not drained in `addExecutorInterval` seconds.
   */
  private def restartAddExecutorTimer(): Unit = startAddExecutorTimer(addExecutorInterval)

  /**
   * Start the add executor timer using the given delay if the timer does not already exist.
   */
  private def startAddExecutorTimer(timerDelaySeconds: Long): Unit = synchronized {
    if (addExecutorTimer.isEmpty) {
      logDebug(s"Starting add executor timer (to expire in $timerDelaySeconds seconds)")
      addExecutorTimer = Some(new Timer)
      addExecutorTimer.get.schedule(new AddExecutorTask, timerDelaySeconds * 1000)
    }
  }

  /**
   * Start the retry add executor timer if it does not already exist.
   * This is called when an add executor timer has expired, but there are still pending executor
   * requests in flight. If the number of attempts has exceeded the limit, cancel the timer and
   * return immediately.
   */
  private def startRetryAddExecutorTimer(): Unit = synchronized {
    if (retryAddExecutorTimer.isEmpty) {
      retryAddExecutorAttempts += 1
      if (retryAddExecutorAttempts <= maxRetryAddExecutorAttempts) {
        logDebug(s"Starting retry add executor timer " +
          s"[retry attempt $retryAddExecutorAttempts/$maxRetryAddExecutorAttempts] " +
          s"(to expire in $retryAddExecutorInterval seconds)")
        retryAddExecutorTimer = Some(new Timer)
        retryAddExecutorTimer.get.schedule(
          new RetryAddExecutorTask, retryAddExecutorInterval * 1000)
      } else {
        logInfo(s"Max retry add executor attempts of $maxRetryAddExecutorAttempts exceeded! " +
          s"Giving up on adding $numExecutorsPendingToAdd executor(s).")
        numExecutorsPendingToAdd = 0
        cancelRetryAddExecutorTimer()
      }
    }
  }

  /**
   * Start a timer to remove the given executor if the timer does not already exist.
   * This is called when the executor initially registers with the driver or finishes running
   * a task. The removal is then triggered if the executor stays idle (i.e. not running a task)
   * for `removeExecutorThreshold` seconds.
   */
  def startRemoveExecutorTimer(executorId: String): Unit = synchronized {
    if (!removeExecutorTimers.contains(executorId)) {
      logDebug(s"Starting idle timer for executor $executorId " +
        s"(to expire in $removeExecutorThreshold seconds)")
      removeExecutorTimers(executorId) = new Timer
      removeExecutorTimers(executorId).schedule(
        new RemoveExecutorTask(executorId), removeExecutorThreshold * 1000)
    }
    // If we do not already know about this executor, keep track of it (should never happen)
    if (!executorIds.contains(executorId)) {
      logWarning(s"Started idle timer for unknown executor $executorId.")
      executorIds.add(executorId)
    }
  }

  /**
   * Start the retry remove executor timer if it does not already exist.
   * This is refreshed whenever an executor is removed. If the number of attempts has exceeded
   * the limit, cancel the timer and return immediately.
   */
  private def startRetryRemoveExecutorTimer(): Unit = synchronized {
    if (retryRemoveExecutorTimer.isEmpty) {
      retryRemoveExecutorAttempts += 1
      if (retryRemoveExecutorAttempts <= maxRetryRemoveExecutorAttempts) {
        logDebug(s"Starting retry remove executor timer " +
          s"[retry attempt $retryRemoveExecutorAttempts/$maxRetryRemoveExecutorAttempts] " +
          s"(to expire in $retryRemoveExecutorInterval seconds)")
        retryRemoveExecutorTimer = Some(new Timer)
        retryRemoveExecutorTimer.get.schedule(
          new RetryRemoveExecutorTask, retryRemoveExecutorInterval * 1000)
      } else {
        logInfo(
          s"Max retry remove executor attempts of $maxRetryRemoveExecutorAttempts exceeded! " +
          s"Giving up on removing executor(s) ${executorsPendingToRemove.mkString(", ")}.")
        executorsPendingToRemove.clear()
        cancelRetryRemoveExecutorTimer()
      }
    }
  }

  /**
   * Cancel any existing timer that adds executors.
   * This is called when the pending task queue is drained.
   */
  def cancelAddExecutorTimer(): Unit = synchronized {
    addExecutorTimer.foreach { timer =>
      logDebug("Canceling add executor timer because task queue is drained!")
      timer.cancel()
    }
    addExecutorTimer = None
    numExecutorsToAdd = 1
    numExecutorsPendingToAdd = 0
    cancelRetryAddExecutorTimer()
  }

  /**
   * Cancel any existing retry add executor timer.
   * This is called when all pending executors have registered,
   * or if the original add timer is canceled.
   */
  private def cancelRetryAddExecutorTimer(): Unit = synchronized {
    retryAddExecutorTimer.foreach { timer =>
      logDebug("Canceling retry add executor timer because there are no more pending requests!")
      timer.cancel()
    }
    retryAddExecutorTimer = None
    retryAddExecutorAttempts = 0
  }

  /**
   * Cancel any existing timer that removes the given executor.
   * This is called when the executor is no longer idle.
   */
  def cancelRemoveExecutorTimer(executorId: String): Unit = synchronized {
    if (removeExecutorTimers.contains(executorId)) {
      logDebug(s"Canceling idle timer for executor $executorId.")
      removeExecutorTimers(executorId).cancel()
    }
    removeExecutorTimers.remove(executorId)
    executorsPendingToRemove.remove(executorId)
    if (executorsPendingToRemove.isEmpty) {
      cancelRetryRemoveExecutorTimer()
    }
  }

  /**
   * Cancel any existing retry remove executor timer.
   * This is called when all executors pending to be removed have been removed,
   * or if all of the original remove timers are canceled.
   */
  private def cancelRetryRemoveExecutorTimer(refreshing: Boolean = false): Unit = synchronized {
    retryRemoveExecutorTimer.foreach { timer =>
      // If we are simply refreshing the timer, do not log this message because it's not true
      if (!refreshing) {
        logDebug("Canceling retry remove executor timer because " +
          "there are no more pending executors to remove!")
      }
      timer.cancel()
    }
    retryRemoveExecutorTimer = None
    retryRemoveExecutorAttempts = 0
  }

  /**
   * Negotiate with the scheduler backend to add new executors.
   * This ensures the resulting number of executors is correctly constrained by the upper bound.
   * Return the number of executors actually requested.
   */
  private def addExecutors(): Int = synchronized {
    val numExistingExecutors = executorIds.size + numExecutorsPendingToAdd
    val actualNumExecutorsToAdd =
      if (numExistingExecutors + numExecutorsToAdd <= maxNumExecutors) {
        numExecutorsToAdd
      } else {
        // Add just enough to reach `maxNumExecutors`
        maxNumExecutors - numExistingExecutors
      }
    val newNumExecutors = numExistingExecutors + actualNumExecutorsToAdd

    if (actualNumExecutorsToAdd > 0) {
      getCoarseGrainedBackend.foreach { backend =>
        logInfo(s"Pending tasks are building up! " +
          s"Adding $actualNumExecutorsToAdd new executor(s) (new total is $newNumExecutors).")
        numExecutorsPendingToAdd += actualNumExecutorsToAdd
        backend.requestExecutors(actualNumExecutorsToAdd)
        return actualNumExecutorsToAdd
      }
    } else {
      logDebug(s"Not adding executors because there are already $maxNumExecutors executor(s), " +
        s"which is the limit.")
    }
    0
  }

  /**
   * Negotiate with the scheduler backend to retry adding any pending executors that have been
   * requested previously but have not registered yet.
   */
  private def retryAddExecutors(): Unit = synchronized {
    if (numExecutorsPendingToAdd > 0) {
      getCoarseGrainedBackend.foreach { backend =>
        logInfo(s"Previously requested executors have not all registered yet. " +
          s"Retrying to add $numExecutorsPendingToAdd executor(s).")
        backend.requestExecutors(numExecutorsPendingToAdd)
      }
    }
  }

  /**
   * Negotiate with the scheduler backend to remove existing executors.
   * This ensures the resulting number of executors is correctly constrained by the lower bound.
   * Return whether the request to remove the executor is actually sent.
   */
  private def removeExecutor(executorId: String): Boolean = synchronized {
    val numExistingExecutors = executorIds.size - executorsPendingToRemove.size
    if (numExistingExecutors - 1 >= minNumExecutors) {
      getCoarseGrainedBackend.foreach { backend =>
        logInfo(s"Removing executor $executorId because it has been idle for " +
          s"$removeExecutorThreshold seconds (new total is ${numExistingExecutors - 1}).")
        executorsPendingToRemove.add(executorId)
        backend.killExecutor(executorId)
        return true
      }
    } else {
      logDebug(s"Not removing idle executor $executorId because there are only $minNumExecutors " +
        "executor(s) left, which is the limit.")
      // If the executor is not removed, do not forget that it's idle
      startRemoveExecutorTimer(executorId)
    }
    false
  }

  /**
   * Negotiate with the scheduler backend to retry removing
   * any executors pending to be removed but are still around.
   */
  private def retryRemoveExecutors(): Unit = synchronized {
    if (executorsPendingToRemove.nonEmpty) {
      getCoarseGrainedBackend.foreach { backend =>
        logInfo(s"Previous requests to remove executors have not been fulfilled yet. " +
          s"Retrying to remove executor(s) ${executorsPendingToRemove.mkString(", ")}.")
        executorsPendingToRemove.foreach(backend.killExecutor)
      }
    }
  }

  /**
   * Callback for the scheduler to signal that the given executor has been added.
   */
  def executorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      logDebug(s"New executor $executorId has registered.")
      if (numExecutorsPendingToAdd > 0) {
        numExecutorsPendingToAdd -= 1
        logDebug(s"Decremented pending executors to add ($numExecutorsPendingToAdd left).")
        if (numExecutorsPendingToAdd == 0) {
          cancelRetryAddExecutorTimer()
        }
      }
      executorIds.add(executorId)
      startRemoveExecutorTimer(executorId)
    }
  }

  /**
   * Callback for the scheduler to signal that the given executor has been removed.
   */
  def executorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      logDebug(s"Existing executor $executorId has been removed.")
      executorIds.remove(executorId)
      if (executorsPendingToRemove.contains(executorId)) {
        executorsPendingToRemove.remove(executorId)
        logDebug(s"Removing executor $executorId from pending executors to remove " +
          s"(${executorsPendingToRemove.size} left)")
        if (executorsPendingToRemove.isEmpty) {
          cancelRetryRemoveExecutorTimer()
        }
      }
    } else {
      logWarning(s"Not removing unknown executor $executorId")
    }
  }

  /**
   * Return the backend as a CoarseGrainedSchedulerBackend if possible.
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

  /**
   * A timer task that adds the given number of executors.
   * This task does not request new executors until the ones pending from the previous round have
   * all registered. Then, if the number of executors requested is as expected (i.e. the upper
   * bound is not reached), the number to request next round increases exponentially. Finally,
   * after requesting executors, this restarts the add executor timer unless the timer is canceled.
   */
  private class AddExecutorTask extends TimerTask {
    override def run(): Unit = ExecutorScalingManager.this.synchronized {
      if (addExecutorTimer.isEmpty) {
        logDebug("Add executor timer was canceled but the timer is triggered!")
        return
      }

      // Request new executors unless previous requests are still in flight
      // If no new requests are made or the upper bound is reached, reset the delta
      if (numExecutorsPendingToAdd == 0) {
        val numExecutorsAdded = addExecutors()
        val limitReached = numExecutorsAdded < numExecutorsToAdd
        numExecutorsToAdd = if (limitReached) 1 else { numExecutorsToAdd * 2 }
      } else {
        logInfo(s"Not adding new executors until all $numExecutorsPendingToAdd " +
          s"pending executor(s) have registered.")
        numExecutorsToAdd = 1
        startRetryAddExecutorTimer()
      }

      // Restart the timer
      addExecutorTimer = None
      restartAddExecutorTimer()
    }
  }

  /**
   * A timer task that removes the given executor.
   */
  private class RemoveExecutorTask(executorId: String) extends TimerTask {
    override def run(): Unit = ExecutorScalingManager.this.synchronized {
      if (!removeExecutorTimers.contains(executorId)) {
        logDebug(s"Idle timer for $executorId was canceled but the timer is triggered!")
        return
      }
      removeExecutorTimers.remove(executorId)

      if (removeExecutor(executorId)) {
        // Reuse any existing retry timer to avoid starting a separate one for each executor
        // If there is an existing timer, refresh it to avoid retrying any new removals eagerly
        cancelRetryRemoveExecutorTimer(refreshing = true)
        startRetryRemoveExecutorTimer()
      }
    }
  }

  /**
   * A timer task that retries adding pending executors.
   * This task keeps retrying until all pending executors have registered.
   */
  private class RetryAddExecutorTask extends TimerTask {
    override def run(): Unit = ExecutorScalingManager.this.synchronized {
      if (retryAddExecutorTimer.isEmpty) {
        logDebug("Retry add executor timer was canceled but the timer is triggered!")
        return
      }

      // If there are pending executors to re-add, do it,
      // and restart the timer in case this add attempt also fails
      if (numExecutorsPendingToAdd > 0) {
        retryAddExecutors()
        retryAddExecutorTimer = None
        startRetryAddExecutorTimer()
      } else {
        logDebug("No executors pending to re-add.")
        cancelRetryAddExecutorTimer()
      }
    }
  }

  /**
   * A timer task that retries removing executors pending to be removed.
   * This task keeps retrying until all executors pending to be removed are removed.
   */
  private class RetryRemoveExecutorTask extends TimerTask {
    override def run(): Unit = ExecutorScalingManager.this.synchronized {
      if (retryRemoveExecutorTimer.isEmpty) {
        logDebug("Retry remove executor timer was canceled but the timer is triggered!")
        return
      }

      // If there are executors pending to be removed, remove them,
      // and restart the timer in case this removal attempt also fails
      if (executorsPendingToRemove.nonEmpty) {
        retryRemoveExecutors()
        retryRemoveExecutorTimer = None
        startRetryRemoveExecutorTimer()
      } else {
        logDebug("No more executors pending to re-remove.")
        cancelRetryRemoveExecutorTimer()
      }
    }
  }

}
