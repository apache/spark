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
 * There is no retry logic in either case. Because the requests to the cluster manager are
 * asynchronous, this class does not know whether a request has been granted until later. For
 * this reason, both add and remove are treated as best-effort only.
 *
 * The relevant Spark properties include the following:
 *
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *
 *   spark.dynamicAllocation.addExecutorThresholdSeconds - How long before new executors are added
 *   spark.dynamicAllocation.addExecutorIntervalSeconds - How often to add new executors
 *   spark.dynamicAllocation.removeExecutorThresholdSeconds - How long before an executor is removed
 *
 * Synchronization: Because the schedulers in Spark are single-threaded, contention should only
 * arise when new executors register or when existing executors are removed, both of which are
 * relatively rare events with respect to task scheduling. Thus, synchronizing each method on the
 * same lock should not be expensive assuming biased locking is enabled in the JVM (on by default
 * for Java 6+). This may not be true, however, if the application itself runs multiple jobs
 * concurrently.
 *
 * Note: This is part of a larger implementation (SPARK-3174) and currently does not actually
 * request to add or remove executors. The mechanism to actually do this will be added separately,
 * e.g. in SPARK-3822 for Yarn.
 */
private[scheduler] class ExecutorAllocationManager(scheduler: TaskSchedulerImpl) extends Logging {
  import ExecutorAllocationManager._

  private val conf = scheduler.conf

  // Lower and upper bounds on the number of executors. These are required.
  private val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", -1)
  private val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", -1)
  if (minNumExecutors < 0 || maxNumExecutors < 0) {
    throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be set!")
  }

  // How frequently to add and remove executors (seconds)
  private val addThresholdSeconds =
    conf.getLong("spark.dynamicAllocation.addExecutorThresholdSeconds", 60)
  private val addIntervalSeconds =
    conf.getLong("spark.dynamicAllocation.addExecutorIntervalSeconds", addThresholdSeconds)
  private val removeThresholdSeconds =
    conf.getLong("spark.dynamicAllocation.removeExecutorThresholdSeconds", 600)

  // Number of executors to add in the next round
  private var numExecutorsToAdd = 1

  // Number of executors that have been requested but have not registered yet
  private var numExecutorsPending = 0

  // Executors that have been requested to be removed but have not been killed yet
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // Keep track of all executors here to decouple us from the logic in TaskSchedulerImpl
  private val executorIds = new mutable.HashSet[String]

  // A timestamp of when the add timer should be triggered, or NOT_STARTED if the timer is not
  // started. This timer is started when there are pending tasks built up, and canceled when
  // there are no more pending tasks.
  private var addTime = NOT_STARTED

  // A timestamp for each executor of when the remove timer for that executor should be triggered.
  // Each remove timer is started when the executor first registers or when the executor finishes
  // running a task, and canceled when the executor is scheduled to run a new task.
  private val removeTimes = new mutable.HashMap[String, Long]

  // Polling loop interval (ms)
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
   * During each loop interval, this thread checks if any of the timers have timed out, and,
   * if so, triggers the relevant timer actions.
   */
  def initialize(): Unit = {
    val thread = new Thread {
      override def run(): Unit = {
        while (true) {
          ExecutorAllocationManager.this.synchronized {
            val now = System.currentTimeMillis
            try {
              // If the add timer has timed out, add executors and refresh the timer
              if (addTime != NOT_STARTED && now >= addTime) {
                addExecutors()
                logDebug(s"Restarting add executor timer " +
                  s"(to be triggered in $addIntervalSeconds seconds)")
                addTime += addIntervalSeconds * 1000
              }

              // If a remove timer has timed out, remove the executor and cancel the timer
              removeTimes.foreach { case (executorId, triggerTime) =>
                if (now > triggerTime) {
                  removeExecutor(executorId)
                  cancelRemoveTimer(executorId)
                }
              }
            } catch {
              case e: Exception => logError("Exception in dynamic executor allocation thread!", e)
            }
          }
          Thread.sleep(intervalMillis)
        }
      }
    }
    thread.setName("spark-dynamic-executor-allocation")
    thread.setDaemon(true)
    thread.start()
  }

  /**
   * Request a number of executors from the scheduler backend.
   * If the cap on the number of executors is reached, give up and reset the
   * number of executors to add next round instead of continuing to double it.
   */
  private def addExecutors(): Unit = synchronized {
    // Do not request more executors if we have already reached the upper bound
    val numExistingExecutors = executorIds.size + numExecutorsPending
    if (numExistingExecutors >= maxNumExecutors) {
      logDebug(s"Not adding executors because there are already " +
        s"$maxNumExecutors executor(s), which is the limit")
      numExecutorsToAdd = 1
      return
    }

    // Request executors with respect to the upper bound
    val actualNumExecutorsToAdd =
      math.min(numExistingExecutors + numExecutorsToAdd, maxNumExecutors) - numExistingExecutors
    val newTotalExecutors = numExistingExecutors + actualNumExecutorsToAdd
    // TODO: Actually request executors once SPARK-3822 goes in
    val addRequestAcknowledged = true // backend.requestExecutors(actualNumbersToAdd)
    if (addRequestAcknowledged) {
      logInfo(s"Pending tasks are building up! Adding $actualNumExecutorsToAdd " +
        s"new executor(s) (new total will be $newTotalExecutors)")
      numExecutorsToAdd *= 2
      numExecutorsPending += actualNumExecutorsToAdd
    } else {
      logWarning(s"Unable to reach the cluster manager " +
        s"to request $actualNumExecutorsToAdd executors!")
    }
  }

  /**
   * Request the scheduler backend to decommission the given executor.
   */
  private def removeExecutor(executorId: String): Unit = synchronized {
    // Do not kill the executor if we are not aware of it (should never happen)
    if (!executorIds.contains(executorId)) {
      logWarning(s"Attempted to remove unknown executor $executorId")
      return
    }

    // Do not kill the executor again if it is already pending to be killed (should never happen)
    if (executorsPendingToRemove.contains(executorId)) {
      logWarning(s"Executor $executorId is already pending to be removed!")
      return
    }

    // Do not kill the executor if we have already reached the lower bound
    val numExistingExecutors = executorIds.size - executorsPendingToRemove.size
    if (numExistingExecutors - 1 < minNumExecutors) {
      logInfo(s"Not removing idle executor $executorId because there are " +
        s"only $minNumExecutors executor(s) left, which is the limit")
      return
    }

    // Send a request to the backend to kill this executor
    // TODO: Actually kill the executor once SPARK-3822 goes in
    val removeRequestAcknowledged = true // backend.killExecutor(executorId)
    if (removeRequestAcknowledged) {
      logInfo(s"Removing executor $executorId because it has been idle for " +
        s"$removeThresholdSeconds seconds (new total will be ${numExistingExecutors - 1})")
      executorsPendingToRemove.add(executorId)
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor $executorId!")
    }
  }

  /**
   * Callback for the scheduler to signal that the given executor has been added.
   */
  def executorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      executorIds.add(executorId)
      executorIds.foreach(startRemoveTimer)
      logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
      if (numExecutorsPending > 0) {
        numExecutorsPending -= 1
        logDebug(s"Decremented pending executors to add ($numExecutorsPending left)")
      }
    }
  }

  /**
   * Callback for the scheduler to signal that the given executor has been removed.
   */
  def executorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      executorIds.remove(executorId)
      logInfo(s"Existing executor $executorId has been removed (new total is ${executorIds.size})")
      if (executorsPendingToRemove.contains(executorId)) {
        executorsPendingToRemove.remove(executorId)
        logDebug(s"Removing executor $executorId from pending executors to remove " +
          s"(${executorsPendingToRemove.size} left)")
      }
    } else {
      logWarning(s"Unknown executor $executorId has been removed!")
    }
  }

  /**
   * Start a timer to add executors if it is not already started. This timer is to be triggered
   * in `addThresholdSeconds` in the first round, and `addIntervalSeconds` in every round
   * thereafter. This is called when the scheduler receives new pending tasks.
   */
  def startAddTimer(): Unit = synchronized {
    if (addTime == NOT_STARTED) {
      logDebug(s"Starting add executor timer because pending tasks " +
        s"are building up (to be triggered in $addThresholdSeconds seconds)")
      addTime = System.currentTimeMillis + addThresholdSeconds * 1000
    }
  }

  /**
   * Start a timer to remove the given executor in `removeThresholdSeconds` if the timer is
   * not already started. This is called when an executor registers or finishes running a task.
   */
  def startRemoveTimer(executorId: String): Unit = synchronized {
    if (!removeTimes.contains(executorId)) {
      logDebug(s"Starting remove timer for $executorId because there are no tasks " +
        s"scheduled to run on the executor (to be triggered in $removeThresholdSeconds seconds)")
      removeTimes(executorId) = System.currentTimeMillis + removeThresholdSeconds * 1000
    }
  }

  /**
   * Cancel any existing add timer.
   * This is called when there are no longer pending tasks left.
   */
  def cancelAddTimer(): Unit = synchronized {
    logDebug(s"Canceling add executor timer")
    addTime = NOT_STARTED
    numExecutorsToAdd = 1
  }

  /**
   * Cancel any existing remove timer for the given executor.
   * This is called when this executor is scheduled a new task.
   */
  def cancelRemoveTimer(executorId: String): Unit = synchronized {
    logDebug(s"Canceling remove executor timer for $executorId")
    removeTimes.remove(executorId)
  }

}

private object ExecutorAllocationManager {
  private val NOT_STARTED = -1L
}
