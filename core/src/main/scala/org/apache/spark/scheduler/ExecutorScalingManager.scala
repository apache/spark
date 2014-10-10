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
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *   spark.dynamicAllocation.addExecutorThreshold - How long before new executors are added (N)
 *   spark.dynamicAllocation.addExecutorInterval - How often to add new executors (M)
 *   spark.dynamicAllocation.removeExecutorThreshold - How long before an executor is removed (K)
 *
 * Synchronization: Because the schedulers in Spark are single-threaded, contention only arises
 * if the application itself runs multiple jobs concurrently. Under normal circumstances, however,
 * synchronizing each method on this class should not be expensive assuming biased locking is
 * enabled in the JVM (on by default for Java 6+). Tighter locks are also used where possible.
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

  // How frequently to add and remove executors
  private val addExecutorThreshold =
    conf.getLong("spark.dynamicAllocation.addExecutorThreshold", 60) // s
  private val addExecutorInterval =
    conf.getLong("spark.dynamicAllocation.addExecutorInterval", addExecutorThreshold) // s
  private val removeExecutorThreshold =
    conf.getLong("spark.dynamicAllocation.removeExecutorThreshold", 300) // s

  // Timers that keep track of when to add and remove executors
  private var addExecutorTimer: Option[Timer] = None
  private val removeExecutorTimers: mutable.Map[String, Timer] = new mutable.HashMap[String, Timer]

  // Number of executors to add in the next round
  private var numExecutorsToAdd = 1

  // The number of pending executors that have not actually been added/removed yet
  private var numExecutorsPendingToAdd = 0
  private var numExecutorsPendingToRemove = 0

  // Keep track of all executors here to decouple us from the logic in TaskSchedulerImpl
  private val executorIds = new mutable.HashSet[String] ++= scheduler.executorIdToHost.keys

  // Start idle timer for all new executors
  synchronized { executorIds.foreach(startRemoveExecutorTimer) }

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
  private def startAddExecutorTimer(timerDelaySeconds: Long): Unit = {
    addExecutorTimer.synchronized {
      if (addExecutorTimer.isEmpty) {
        logDebug(s"Starting add executor timer (to expire in $timerDelaySeconds seconds)")
        addExecutorTimer = Some(new Timer)
        addExecutorTimer.get.schedule(
          new AddExecutorTimerTask(numExecutorsToAdd), timerDelaySeconds * 1000)
      }
    }
  }

  /**
   * Start a timer to remove the given executor if the timer does not already exist.
   * This is called when the executor initially registers with the driver or finishes running
   * a task. The removal is then triggered if the executor stays idle (i.e. not running a task)
   * for `removeExecutorThreshold` seconds.
   */
  def startRemoveExecutorTimer(executorId: String): Unit = {
    removeExecutorTimers.synchronized {
      if (!removeExecutorTimers.contains(executorId)) {
        logDebug(s"Starting idle timer for executor $executorId " +
          s"(to expire in $removeExecutorThreshold seconds)")
        removeExecutorTimers(executorId) = new Timer
        removeExecutorTimers(executorId).schedule(
          new RemoveExecutorTimerTask(executorId), removeExecutorThreshold * 1000)
      }
    }
    // Acquire a more general lock here because we might mutate `executorId`
    synchronized {
      if (!executorIds.contains(executorId)) {
        logWarning(s"Started idle timer for unknown executor $executorId.")
        executorIds.add(executorId)
      }
    }
  }

  /**
   * Cancel any existing timer that adds executors.
   * This is called when the pending task queue is drained.
   */
  def cancelAddExecutorTimer(): Unit = addExecutorTimer.synchronized {
    addExecutorTimer.foreach { timer =>
      logDebug("Canceling add executor timer because task queue is drained!")
      timer.cancel()
      numExecutorsToAdd = 1
      addExecutorTimer = None
    }
  }

  /**
   * Cancel any existing timer that removes the given executor.
   * This is called when the executor is no longer idle.
   */
  def cancelRemoveExecutorTimer(executorId: String): Unit = removeExecutorTimers.synchronized {
    if (removeExecutorTimers.contains(executorId)) {
      logDebug(s"Canceling idle timer for executor $executorId.")
      removeExecutorTimers(executorId).cancel()
      removeExecutorTimers.remove(executorId)
    }
  }

  /**
   * Negotiate with the scheduler backend to add new executors.
   * This ensures the resulting number of executors is correctly constrained by the upper bound.
   * Return the number of executors actually requested.
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
          s"Adding $numExecutorsToAdd new executor(s) (new total is $newNumExecutors).")
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
   * Return whether the request to remove the executor is actually sent.
   */
  private def removeExecutor(executorId: String): Boolean = synchronized {
    val numExistingExecutors = executorIds.size - numExecutorsPendingToRemove
    if (numExistingExecutors - 1 >= minNumExecutors) {
      getCoarseGrainedBackend.foreach { backend =>
        logInfo(s"Removing executor $executorId because it has been idle for " +
          s"$removeExecutorThreshold seconds (new total is ${numExistingExecutors - 1}).")
        numExecutorsPendingToRemove += 1
        backend.killExecutor(executorId)
        return true
      }
    } else {
      logDebug(s"Not removing idle executor $executorId because there are only $minNumExecutors " +
        "executor(s) left, which is the limit.")
    }
    false
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
      if (numExecutorsPendingToRemove > 0) {
        numExecutorsPendingToRemove -= 1
        logDebug(s"Decrementing pending executors to remove (now at $numExecutorsPendingToRemove).")
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
   *
   * This task does not request new executors until the ones pending from the previous round have
   * all registered. Then, if the number of executors requested is as expected (i.e. the upper
   * bound is not reached), the number to request next round increases exponentially. Finally,
   * after requesting executors, this restarts the add executor timer unless the timer is canceled.
   */
  private class AddExecutorTimerTask(_numExecutorsToAdd: Int) extends TimerTask {
    override def run(): Unit = {
      // Whether we have successfully requested the expected number of executors
      var success = false

      synchronized {
        // Do not add executors until those requested in the previous round have registered
        if (numExecutorsPendingToAdd == 0) {
          val numExecutorsAdded = addExecutors(_numExecutorsToAdd)
          success = numExecutorsAdded == _numExecutorsToAdd
        } else {
          logInfo(s"Not adding new executors until all $numExecutorsPendingToAdd pending " +
            "executor(s) have registered.")
        }
      }

      addExecutorTimer.synchronized {
        // Do this check in case the timer has been canceled in the mean time
        if (addExecutorTimer.isDefined) {
          numExecutorsToAdd = if (success) { _numExecutorsToAdd * 2 } else 1
          addExecutorTimer = None
          restartAddExecutorTimer()
        }
      }
    }
  }

  /**
   * A timer task that removes the given executor.
   */
  private class RemoveExecutorTimerTask(executorId: String) extends TimerTask {
    override def run(): Unit = {
      removeExecutor(executorId)
      cancelRemoveExecutorTimer(executorId)
    }
  }

}
