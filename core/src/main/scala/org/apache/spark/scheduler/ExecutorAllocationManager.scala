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

import scala.collection.mutable

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The add policy depends on the number of pending tasks. If the queue of pending tasks is not
 * drained in N seconds, then new executors are added. If the queue persists for another M
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
 * The remove policy is simpler: If an executor has been idle for K seconds (meaning it has not
 * been scheduled to run any tasks), then it is removed. This requires starting a timer on each
 * executor instead of just starting a global one as in the add case.
 *
 * Both add and remove attempts are retried on failure up to a maximum number of times.
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
 *   spark.dynamicAllocation.addExecutorRetryInterval - How often to retry adding executors
 *   spark.dynamicAllocation.removeExecutorRetryInterval - How often to retry removing executors
 *   spark.dynamicAllocation.maxAddExecutorRetryAttempts - Max retries in re-adding executors
 *   spark.dynamicAllocation.maxRemoveExecutorRetryAttempts - Max retries in re-removing executors
 *
 * Synchronization: Because the schedulers in Spark are single-threaded, contention should only
 * arise when new executors register or when existing executors have been removed, both of which
 * are relatively rare events with respect to task scheduling. Thus, synchronizing each method on
 * the same lock should not be expensive assuming biased locking is enabled in the JVM (on by
 * default for Java 6+). This may not be true, however, if the application itself runs multiple
 * jobs concurrently.
 *
 * Note: This is part of a larger implementation (SPARK-3174) and currently does not actually
 * request to add or remove executors. The mechanism to actually do this will be added separately,
 * e.g. in SPARK-3822 for Yarn.
 */
private[scheduler] class ExecutorAllocationManager(scheduler: TaskSchedulerImpl) extends Logging {
  private val conf = scheduler.conf

  // Lower and upper bounds on the number of executors. These are required.
  private val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", -1)
  private val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", -1)
  if (minNumExecutors < 0 || maxNumExecutors < 0) {
    throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be set!")
  }

  // How frequently to add and remove executors (seconds)
  private val addThreshold =
    conf.getLong("spark.dynamicAllocation.addExecutorThreshold", 60)
  private val addInterval =
    conf.getLong("spark.dynamicAllocation.addExecutorInterval", addThreshold)
  private val addRetryInterval =
    conf.getLong("spark.dynamicAllocation.addExecutorRetryInterval", addInterval)
  private val removeThreshold =
    conf.getLong("spark.dynamicAllocation.removeExecutorThreshold", 600)
  private val removeRetryInterval =
    conf.getLong("spark.dynamicAllocation.removeExecutorRetryInterval", 300)

  // Number of executors to add in the next round
  private var numExecutorsToAdd = 1

  // Number of executors that have been requested but have not registered yet
  private var numExecutorsPendingToAdd = 0

  // Executors that have been requested to be removed but have not been killed yet
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // Retry attempts
  private var addRetryAttempts = 0
  private val removeRetryAttempts = new mutable.HashMap[String, Int]
  private val maxAddRetryAttempts =
    conf.getInt("spark.dynamicAllocation.maxAddExecutorRetryAttempts", 10)
  private val maxRemoveRetryAttempts =
    conf.getInt("spark.dynamicAllocation.maxRemoveExecutorRetryAttempts", 10)

  // Keep track of all executors here to decouple us from the logic in TaskSchedulerImpl
  private val executorIds = new mutable.HashSet[String]

  // A counter in milliseconds of how long the timer to add new executors has been started for,
  // or -1 if the timer is not started. This timer is started when there are pending tasks built
  // up, and canceled when there are no more pending tasks.
  private var addTimer = -1

  // A counter in milliseconds of how long the timer to retry adding new executors has been
  // started for, or -1 if the timer is not started. This timer is started when an attempt to add
  // new executors is made, and canceled when all executors pending to be added have registered.
  private var addRetryTimer = -1

  // A counter in milliseconds for each executor of how long the executor has been idle for.
  // This timer is started when the executor first registers and when it finishes running a task,
  // and canceled when the executor is scheduled to run a new task.
  private val removeTimers = new mutable.HashMap[String, Long]

  // A counter in milliseconds for each executor of how long the timer to retry removing the
  // executor has been started for. This timer is started when an attempt to remove the executor
  // is made, and canceled when the executor is actually removed.
  private val removeRetryTimers = new mutable.HashMap[String, Long]

  // Whether the add timer will expire on `addInterval` instead of `addThreshold`
  private var addThresholdCrossed = false

  // Loop interval (ms)
  private val intervalMillis = 100

  // Scheduler backend through which requests to add/remove executors are made
  // Note that this assumes the backend has already initialized when this is first used
  // Otherwise, an appropriate exception is thrown
  private lazy val backend = scheduler.backend match {
    case b: CoarseGrainedSchedulerBackend => b
    case null =>
      throw new SparkException("Scheduler backend not initialized yet!")
    case _ =>
      throw new SparkException(
        "Dynamic allocation of executors is not applicable to fine-grained schedulers. " +
        "Please set spark.dynamicAllocation.enabled to false.")
  }

  initialize()

  /**
   * Start the main polling thread that keeps track of when to add and remove executors.
   * During each interval, this thread checks if any of the timers have expired, and, if
   * so, triggers the relevant timer actions.
   */
  def initialize(): Unit = {
    val thread = new Thread {
      override def run() {
        while (true) {
          try {
            if (addTimer > 0) {
              val threshold = if (addThresholdCrossed) addInterval else addThreshold
              if (addTimer > threshold * 1000) {
                addThresholdCrossed = true
                addExecutors()
              }
            }

            if (addRetryTimer > 0) {
              if (addRetryTimer > addRetryInterval * 1000) {
                retryAddExecutors()
              }
            }

            removeTimers.foreach { case (id, t) =>
              if (t > removeThreshold * 1000) {
                removeExecutor(id)
              }
            }

            removeRetryTimers.foreach { case (id, t) =>
              if (t > removeRetryInterval * 1000) {
                retryRemoveExecutors(id)
              }
            }
          } catch {
            case e: Exception =>
              logError("Exception encountered in dynamic executor allocation thread!", e)
          } finally {
            // Advance all timers that are enabled
            Thread.sleep(intervalMillis)
            if (addTimer > 0) {
              addTimer += intervalMillis
            }
            if (addRetryTimer > 0) {
              addRetryTimer += intervalMillis
            }
            removeTimers.foreach { case (id, _) =>
              removeTimers(id) += intervalMillis
            }
            removeRetryTimers.foreach { case (id, _) =>
              removeRetryTimers(id) += intervalMillis
            }
          }
        }
      }
    }
    thread.setName("spark-dynamic-executor-allocation")
    thread.setDaemon(true)
    thread.start()
  }

  /**
   * Request a number of executors from the scheduler backend.
   * This automatically restarts the add timer unless it is explicitly canceled.
   */
  private def addExecutors(): Unit = synchronized {
    // Restart add timer because there are still pending tasks
    startAddTimer()

    // Wait until the previous round of executors have registered
    if (numExecutorsPendingToAdd > 0) {
      logInfo(s"Not adding executors because there are still " +
        s"$numExecutorsPendingToAdd request(s) in flight")
      return
    }

    // Do not request more executors if we have already reached the upper bound
    val numExistingExecutors = executorIds.size + numExecutorsPendingToAdd
    if (numExistingExecutors >= maxNumExecutors) {
      logInfo(s"Not adding executors because there are already " +
        s"$maxNumExecutors executor(s), which is the limit")
      numExecutorsToAdd = 1
      return
    }

    // Request executors with respect to the upper bound
    // Start the retry timer in case this addition fails
    val actualNumExecutorsToAdd =
      math.min(numExistingExecutors + numExecutorsToAdd, maxNumExecutors) - numExistingExecutors
    val newTotalExecutors = numExistingExecutors + actualNumExecutorsToAdd
    logInfo(s"Pending tasks are building up! Adding $actualNumExecutorsToAdd " +
      s"new executor(s) (new total is $newTotalExecutors)")
    numExecutorsToAdd *= 2
    numExecutorsPendingToAdd = actualNumExecutorsToAdd
    backend.requestExecutors(actualNumExecutorsToAdd)
    startAddRetryTimer()
  }

  /**
   * Retry a previous executor request that has not been fulfilled.
   * This restarts the retry timer to keep trying up to a maximum number of attempts.
   */
  private def retryAddExecutors(): Unit = synchronized {
    // Do not retry if there are no executors pending to be added (should never happen)
    if (numExecutorsPendingToAdd == 0) {
      logWarning("Attempted to retry adding executors when there are none pending to be added")
      cancelAddRetryTimer()
      return
    }

    // Do not retry if we have already exceeded the maximum number of attempts
    addRetryAttempts += 1
    if (addRetryAttempts > maxAddRetryAttempts) {
      logInfo(s"Giving up on adding $numExecutorsPendingToAdd executor(s) " +
        s"after $maxAddRetryAttempts failed attempts")
      numExecutorsPendingToAdd = 0
      // Also cancel original add timer because the cluster is not granting us new executors
      cancelAddTimer()
      return
    }

    // Retry a previous request, then restart the retry timer in case this retry also fails
    logInfo(s"Previously requested executors have not all registered yet. " +
      s"Retrying to add $numExecutorsPendingToAdd executor(s) " +
      s"[attempt $addRetryAttempts/$maxAddRetryAttempts]")
    backend.requestExecutors(numExecutorsPendingToAdd)
    startAddRetryTimer()
  }

  /**
   * Request the scheduler backend to decommission the given executor.
   * This expires the remove timer unless the executor is kept alive intentionally.
   */
  private def removeExecutor(executorId: String): Unit = synchronized {
    // Do not kill the executor if we are not aware of it (should never happen)
    if (!executorIds.contains(executorId)) {
      logWarning(s"Attempted to remove unknown executor $executorId")
      cancelRemoveTimer(executorId)
      return
    }

    // Do not kill the executor again if it is already pending to be killed (should never happen)
    if (executorsPendingToRemove.contains(executorId) ||
        removeRetryAttempts.contains(executorId) ||
        removeRetryTimers.contains(executorId)) {
      logWarning(s"Executor $executorId is already pending to be removed!")
      cancelRemoveTimer(executorId)
      return
    }

    // Do not kill the executor if we have already reached the lower bound
    val numExistingExecutors = executorIds.size - executorsPendingToRemove.size
    if (numExistingExecutors - 1 < minNumExecutors) {
      logDebug(s"Not removing idle executor $executorId because there are only $minNumExecutors " +
        "executor(s) left, which is the limit")
      // Restart the remove timer to keep the executor marked as idle
      // Otherwise we won't be able to remove this executor even after new executors have joined
      startRemoveTimer(executorId)
      return
    }

    // Send a kill request to the backend for this executor
    // Start the retry timer in case this removal fails
    logInfo(s"Removing executor $executorId because it has been idle for " +
      s"$removeThreshold seconds (new total is ${numExistingExecutors - 1})")
    executorsPendingToRemove.add(executorId)
    backend.killExecutor(executorId)
    cancelRemoveTimer(executorId)
    startRemoveRetryTimer(executorId)
  }

  /**
   * Retry a previous attempt to decommission the given executor.
   * This restarts the retry timer to keep trying up to a maximum number of attempts.
   */
  private def retryRemoveExecutors(executorId: String): Unit = synchronized {
    // Do not kill the executor if we are not aware of it (should never happen)
    if (!executorIds.contains(executorId)) {
      logWarning(s"Attempted to retry removing unknown executor $executorId")
      cancelRemoveRetryTimer(executorId)
      return
    }

    // Do not retry if this executor is not pending to be killed (should never happen)
    if (!executorsPendingToRemove.contains(executorId)) {
      logDebug(s"Attempted to retry removing executor $executorId when it's not to be removed!")
      cancelRemoveRetryTimer(executorId)
      return
    }

    // Do not retry if we have already exceeded the maximum number of attempts
    removeRetryAttempts(executorId) =
      removeRetryAttempts.getOrElse(executorId, 0) + 1
    if (removeRetryAttempts(executorId) > maxRemoveRetryAttempts) {
      logInfo(s"Giving up on removing executor $executorId after " +
        s"$maxRemoveRetryAttempts failed attempts!")
      executorsPendingToRemove.remove(executorId)
      cancelRemoveRetryTimer(executorId)
      return
    }

    // Retry a previous kill request for this executor
    // Restart the retry timer in case this retry also fails
    logInfo(s"Retrying previous attempt to remove $executorId " +
      s"[attempt ${removeRetryAttempts(executorId)}/$maxRemoveRetryAttempts]")
    backend.killExecutor(executorId)
    startRemoveRetryTimer(executorId)
  }

  /**
   * Callback for the scheduler to signal that the given executor has been added.
   */
  def executorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      logDebug(s"New executor $executorId has registered")
      if (numExecutorsPendingToAdd > 0) {
        numExecutorsPendingToAdd -= 1
        logDebug(s"Decremented pending executors to add ($numExecutorsPendingToAdd left)")
        if (numExecutorsPendingToAdd == 0) {
          logDebug("All previously pending executors have registered!")
          cancelAddRetryTimer()
        }
      }
      executorIds.add(executorId)
      startRemoveTimer(executorId)
    }
  }

  /**
   * Callback for the scheduler to signal that the given executor has been removed.
   */
  def executorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      logDebug(s"Existing executor $executorId has been removed")
      executorIds.remove(executorId)
      if (executorsPendingToRemove.contains(executorId)) {
        executorsPendingToRemove.remove(executorId)
        logDebug(s"Removing executor $executorId from pending executors to remove " +
          s"(${executorsPendingToRemove.size} left)")
        cancelRemoveRetryTimer(executorId)
      }
    } else {
      logWarning(s"Unknown executor $executorId has been removed!")
    }
  }

  /**
   * Return whether the add timer is already running.
   */
  def isAddTimerRunning: Boolean = addTimer > 0 || addRetryTimer > 0

  /**
   * Return whether the remove timer for the given executor is already running.
   */
  def isRemoveTimerRunning(executorId: String): Boolean = {
    removeTimers.contains(executorId) || removeRetryTimers.contains(executorId)
  }

  /**
   * Start a timer to add executors, to expire in `addThreshold` seconds in the first
   * round, and `addInterval` seconds in every round thereafter. This is called when
   * the scheduler receives new pending tasks and the timer is not already started. This resets
   * the value of any existing add timer.
   */
  def startAddTimer(): Unit = {
    val threshold = if (addThresholdCrossed) addInterval else addThreshold
    logDebug(s"Starting add executor timer (to expire in $threshold seconds)")
    addTimer = 0
  }

  /**
   * Start a timer to remove the given executor, to expire in `removeThreshold` seconds.
   * This is called when an executor registers or finishes running tasks, and the timer is not
   * already started. This resets the value of any existing timer to remove this executor.
   */
  def startRemoveTimer(executorId: String): Unit = {
    logDebug(s"Starting remove executor timer for $executorId " +
      s"(to expire in $removeThreshold seconds)")
    removeTimers(executorId) = 0
  }

  /**
   * Start a retry timer to add executors, to expire in `addRetryInterval` seconds. This
   * is called when an add timer or another retry timer is triggered. This resets the value of
   * any existing retry timer.
   */
  private def startAddRetryTimer(): Unit = {
    logDebug(s"Starting add executor retry timer (to expire in $addRetryInterval seconds)")
    addRetryTimer = 0
  }

  /**
   * Start a retry timer to remove the given executor, to expire in `removeRetryInterval`
   * seconds. This is called when the remove timer or another retry timer for this executor is
   * triggered. This resets the value of any existing retry timer to remove this executor.
   */
  private def startRemoveRetryTimer(executorId: String): Unit = {
    logDebug(s"Starting remove executor retry timer for $executorId " +
      s"(to expire in $removeRetryInterval seconds)")
    removeRetryTimers(executorId) = 0
  }

  /**
   * Cancel any existing add timer. This is called when there are no longer pending tasks left.
   */
  def cancelAddTimer(): Unit = {
    logDebug(s"Canceling add executor timer")
    addTimer = -1
    addThresholdCrossed = false
    cancelAddRetryTimer()
  }

  /**
   * Cancel any existing remove timer for the given executor.
   * This is called when this executor is scheduled a new task.
   */
  def cancelRemoveTimer(executorId: String): Unit = {
    logDebug(s"Canceling remove executor timer for $executorId")
    removeTimers.remove(executorId)
    cancelRemoveRetryTimer(executorId)
  }

  /**
   * Cancel any existing add retry timer. This is called when all previously requested
   * executors have registered, or when there is no longer a need to add executors.
   */
  private def cancelAddRetryTimer(): Unit = {
    logDebug(s"Canceling add executor retry timer")
    addRetryTimer = -1
    addRetryAttempts = 0
  }

  /**
   * Cancel any existing remove retry timer for the given executor. This is called when the
   * executor pending to be removed has been removed, or when there is no longer a need to
   * remove this executor.
   */
  private def cancelRemoveRetryTimer(executorId: String): Unit = {
    logDebug(s"Canceling remove executor retry timer for $executorId")
    removeRetryAttempts.remove(executorId)
    removeRetryTimers.remove(executorId)
  }

}
