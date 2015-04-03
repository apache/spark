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

import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.mutable

import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The add policy depends on whether there are backlogged tasks waiting to be scheduled. If
 * the scheduler queue is not drained in N seconds, then new executors are added. If the queue
 * persists for another M seconds, then more executors are added and so on. The number added
 * in each round increases exponentially from the previous round until an upper bound on the
 * number of executors has been reached. The upper bound is based both on a configured property
 * and on the number of tasks pending: the policy will never increase the number of executor
 * requests past the number needed to handle all pending tasks.
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
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
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
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    conf: SparkConf)
  extends Logging {

  allocationManager =>

  import ExecutorAllocationManager._

  // Lower and upper bounds on the number of executors.
  private val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)
  private val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors",
    Integer.MAX_VALUE)

  // How long there must be backlogged tasks for before an addition is triggered (seconds)
  private val schedulerBacklogTimeout = conf.getLong(
    "spark.dynamicAllocation.schedulerBacklogTimeout", 5)

  // Same as above, but used only after `schedulerBacklogTimeout` is exceeded
  private val sustainedSchedulerBacklogTimeout = conf.getLong(
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", schedulerBacklogTimeout)

  // How long an executor must be idle for before it is removed (seconds)
  private val executorIdleTimeout = conf.getLong(
    "spark.dynamicAllocation.executorIdleTimeout", 600)

  // During testing, the methods to actually kill and add executors are mocked out
  private val testing = conf.getBoolean("spark.dynamicAllocation.testing", false)

  // TODO: The default value of 1 for spark.executor.cores works right now because dynamic
  // allocation is only supported for YARN and the default number of cores per executor in YARN is
  // 1, but it might need to be attained differently for different cluster managers
  private val tasksPerExecutor =
    conf.getInt("spark.executor.cores", 1) / conf.getInt("spark.task.cpus", 1)

  validateSettings()

  // Number of executors to add in the next round
  private var numExecutorsToAdd = 1

  // Number of executors that have been requested but have not registered yet
  private var numExecutorsPending = 0

  // Executors that have been requested to be removed but have not been killed yet
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // All known executors
  private val executorIds = new mutable.HashSet[String]

  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  private var addTime: Long = NOT_SET

  // A timestamp for each executor of when the executor should be removed, indexed by the ID
  // This is set when an executor is no longer running a task, or when it first registers
  private val removeTimes = new mutable.HashMap[String, Long]

  // Polling loop interval (ms)
  private val intervalMillis: Long = 100

  // Clock used to schedule when executors should be added and removed
  private var clock: Clock = new SystemClock()

  // Listener for Spark events that impact the allocation policy
  private val listener = new ExecutorAllocationListener

  // Executor that handles the scheduling task.
  private val executor = Executors.newSingleThreadScheduledExecutor(
    Utils.namedThreadFactory("spark-dynamic-executor-allocation"))

  /**
   * Verify that the settings specified through the config are valid.
   * If not, throw an appropriate exception.
   */
  private def validateSettings(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be positive!")
    }
    if (maxNumExecutors == 0) {
      throw new SparkException("spark.dynamicAllocation.maxExecutors cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"spark.dynamicAllocation.minExecutors ($minNumExecutors) must " +
        s"be less than or equal to spark.dynamicAllocation.maxExecutors ($maxNumExecutors)!")
    }
    if (schedulerBacklogTimeout <= 0) {
      throw new SparkException("spark.dynamicAllocation.schedulerBacklogTimeout must be > 0!")
    }
    if (sustainedSchedulerBacklogTimeout <= 0) {
      throw new SparkException(
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout must be > 0!")
    }
    if (executorIdleTimeout <= 0) {
      throw new SparkException("spark.dynamicAllocation.executorIdleTimeout must be > 0!")
    }
    // Require external shuffle service for dynamic allocation
    // Otherwise, we may lose shuffle files when killing executors
    if (!conf.getBoolean("spark.shuffle.service.enabled", false) && !testing) {
      throw new SparkException("Dynamic allocation of executors requires the external " +
        "shuffle service. You may enable this through spark.shuffle.service.enabled.")
    }
    if (tasksPerExecutor == 0) {
      throw new SparkException("spark.executor.cores must not be less than spark.task.cpus.")
    }
  }

  /**
   * Use a different clock for this allocation manager. This is mainly used for testing.
   */
  def setClock(newClock: Clock): Unit = {
    clock = newClock
  }

  /**
   * Register for scheduler callbacks to decide when to add and remove executors, and start
   * the scheduling task.
   */
  def start(): Unit = {
    listenerBus.addListener(listener)

    val scheduleTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(schedule())
    }
    executor.scheduleAtFixedRate(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)
  }

  /**
   * Stop the allocation manager.
   */
  def stop(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  /**
   * The number of executors we would have if the cluster manager were to fulfill all our existing
   * requests.
   */
  private def targetNumExecutors(): Int =
    numExecutorsPending + executorIds.size - executorsPendingToRemove.size

  /**
   * The maximum number of executors we would need under the current load to satisfy all running
   * and pending tasks, rounded up.
   */
  private def maxNumExecutorsNeeded(): Int = {
    val numRunningOrPendingTasks = listener.totalPendingTasks + listener.totalRunningTasks
    (numRunningOrPendingTasks + tasksPerExecutor - 1) / tasksPerExecutor
  }

  /**
   * This is called at a fixed interval to regulate the number of pending executor requests
   * and number of executors running.
   *
   * First, adjust our requested executors based on the add time and our current needs.
   * Then, if the remove time for an existing executor has expired, kill the executor.
   *
   * This is factored out into its own method for testing.
   */
  private def schedule(): Unit = synchronized {
    val now = clock.getTimeMillis

    addOrCancelExecutorRequests(now)

    removeTimes.retain { case (executorId, expireTime) =>
      val expired = now >= expireTime
      if (expired) {
        removeExecutor(executorId)
      }
      !expired
    }
  }

  /**
   * Check to see whether our existing allocation and the requests we've made previously exceed our
   * current needs. If so, let the cluster manager know so that it can cancel pending requests that
   * are unneeded.
   *
   * If not, and the add time has expired, see if we can request new executors and refresh the add
   * time.
   *
   * @return the delta in the target number of executors.
   */
  private def addOrCancelExecutorRequests(now: Long): Int = synchronized {
    val currentTarget = targetNumExecutors
    val maxNeeded = maxNumExecutorsNeeded

    if (maxNeeded < currentTarget) {
      // The target number exceeds the number we actually need, so stop adding new
      // executors and inform the cluster manager to cancel the extra pending requests.
      val newTotalExecutors = math.max(maxNeeded, minNumExecutors)
      client.requestTotalExecutors(newTotalExecutors)
      numExecutorsToAdd = 1
      updateNumExecutorsPending(newTotalExecutors)
    } else if (addTime != NOT_SET && now >= addTime) {
      val delta = addExecutors(maxNeeded)
      logDebug(s"Starting timer to add more executors (to " +
        s"expire in $sustainedSchedulerBacklogTimeout seconds)")
      addTime += sustainedSchedulerBacklogTimeout * 1000
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
    val currentTarget = targetNumExecutors
    if (currentTarget >= maxNumExecutors) {
      logDebug(s"Not adding executors because there are already ${executorIds.size} " +
        s"registered and $numExecutorsPending pending executor(s) (limit $maxNumExecutors)")
      numExecutorsToAdd = 1
      return 0
    }

    val actualMaxNumExecutors = math.min(maxNumExecutors, maxNumExecutorsNeeded)
    val newTotalExecutors = math.min(currentTarget + numExecutorsToAdd, actualMaxNumExecutors)
    val addRequestAcknowledged = testing || client.requestTotalExecutors(newTotalExecutors)
    if (addRequestAcknowledged) {
      val delta = updateNumExecutorsPending(newTotalExecutors)
      logInfo(s"Requesting $delta new executor(s) because tasks are backlogged" +
        s" (new desired total will be $newTotalExecutors)")
      numExecutorsToAdd = if (delta == numExecutorsToAdd) {
        numExecutorsToAdd * 2
      } else {
        1
      }
      delta
    } else {
      logWarning(
        s"Unable to reach the cluster manager to request $newTotalExecutors total executors!")
      0
    }
  }

  /**
   * Given the new target number of executors, update the number of pending executor requests,
   * and return the delta from the old number of pending requests.
   */
  private def updateNumExecutorsPending(newTotalExecutors: Int): Int = {
    val newNumExecutorsPending =
      newTotalExecutors - executorIds.size + executorsPendingToRemove.size
    val delta = newNumExecutorsPending - numExecutorsPending
    numExecutorsPending = newNumExecutorsPending
    delta
  }

  /**
   * Request the cluster manager to remove the given executor.
   * Return whether the request is received.
   */
  private def removeExecutor(executorId: String): Boolean = synchronized {
    // Do not kill the executor if we are not aware of it (should never happen)
    if (!executorIds.contains(executorId)) {
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
    val numExistingExecutors = executorIds.size - executorsPendingToRemove.size
    if (numExistingExecutors - 1 < minNumExecutors) {
      logDebug(s"Not removing idle executor $executorId because there are only " +
        s"$numExistingExecutors executor(s) left (limit $minNumExecutors)")
      return false
    }

    // Send a request to the backend to kill this executor
    val removeRequestAcknowledged = testing || client.killExecutor(executorId)
    if (removeRequestAcknowledged) {
      logInfo(s"Removing executor $executorId because it has been idle for " +
        s"$executorIdleTimeout seconds (new desired total will be ${numExistingExecutors - 1})")
      executorsPendingToRemove.add(executorId)
      true
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor $executorId!")
      false
    }
  }

  /**
   * Callback invoked when the specified executor has been added.
   */
  private def onExecutorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      executorIds.add(executorId)
      // If an executor (call this executor X) is not removed because the lower bound
      // has been reached, it will no longer be marked as idle. When new executors join,
      // however, we are no longer at the lower bound, and so we must mark executor X
      // as idle again so as not to forget that it is a candidate for removal. (see SPARK-4951)
      executorIds.filter(listener.isExecutorIdle).foreach(onExecutorIdle)
      logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
      if (numExecutorsPending > 0) {
        numExecutorsPending -= 1
        logDebug(s"Decremented number of pending executors ($numExecutorsPending left)")
      }
    } else {
      logWarning(s"Duplicate executor $executorId has registered")
    }
  }

  /**
   * Callback invoked when the specified executor has been removed.
   */
  private def onExecutorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      executorIds.remove(executorId)
      removeTimes.remove(executorId)
      logInfo(s"Existing executor $executorId has been removed (new total is ${executorIds.size})")
      if (executorsPendingToRemove.contains(executorId)) {
        executorsPendingToRemove.remove(executorId)
        logDebug(s"Executor $executorId is no longer pending to " +
          s"be removed (${executorsPendingToRemove.size} left)")
      }
    } else {
      logWarning(s"Unknown executor $executorId has been removed!")
    }
  }

  /**
   * Callback invoked when the scheduler receives new pending tasks.
   * This sets a time in the future that decides when executors should be added
   * if it is not already set.
   */
  private def onSchedulerBacklogged(): Unit = synchronized {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeout seconds)")
      addTime = clock.getTimeMillis + schedulerBacklogTimeout * 1000
    }
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors.
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug(s"Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAdd = 1
  }

  /**
   * Callback invoked when the specified executor is no longer running any tasks.
   * This sets a time in the future that decides when this executor should be removed if
   * the executor is not already marked as idle.
   */
  private def onExecutorIdle(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      if (!removeTimes.contains(executorId) && !executorsPendingToRemove.contains(executorId)) {
        logDebug(s"Starting idle timer for $executorId because there are no more tasks " +
          s"scheduled to run on the executor (to expire in $executorIdleTimeout seconds)")
        removeTimes(executorId) = clock.getTimeMillis + executorIdleTimeout * 1000
      }
    } else {
      logWarning(s"Attempted to mark unknown executor $executorId idle")
    }
  }

  /**
   * Callback invoked when the specified executor is now running a task.
   * This resets all variables used for removing this executor.
   */
  private def onExecutorBusy(executorId: String): Unit = synchronized {
    logDebug(s"Clearing idle timer for $executorId because it is now running a task")
    removeTimes.remove(executorId)
  }

  /**
   * A listener that notifies the given allocation manager of when to add and remove executors.
   *
   * This class is intentionally conservative in its assumptions about the relative ordering
   * and consistency of events returned by the listener. For simplicity, it does not account
   * for speculated tasks.
   */
  private class ExecutorAllocationListener extends SparkListener {

    private val stageIdToNumTasks = new mutable.HashMap[Int, Int]
    private val stageIdToTaskIndices = new mutable.HashMap[Int, mutable.HashSet[Int]]
    private val executorIdToTaskIds = new mutable.HashMap[String, mutable.HashSet[Long]]
    // Number of tasks currently running on the cluster.  Should be 0 when no stages are active.
    private var numRunningTasks: Int = _

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      val stageId = stageSubmitted.stageInfo.stageId
      val numTasks = stageSubmitted.stageInfo.numTasks
      allocationManager.synchronized {
        stageIdToNumTasks(stageId) = numTasks
        allocationManager.onSchedulerBacklogged()
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      allocationManager.synchronized {
        stageIdToNumTasks -= stageId
        stageIdToTaskIndices -= stageId

        // If this is the last stage with pending tasks, mark the scheduler queue as empty
        // This is needed in case the stage is aborted for any reason
        if (stageIdToNumTasks.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
          if (numRunningTasks != 0) {
            logWarning("No stages are running, but numRunningTasks != 0")
            numRunningTasks = 0
          }
        }
      }
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val stageId = taskStart.stageId
      val taskId = taskStart.taskInfo.taskId
      val taskIndex = taskStart.taskInfo.index
      val executorId = taskStart.taskInfo.executorId

      allocationManager.synchronized {
        numRunningTasks += 1
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }

        // If this is the last pending task, mark the scheduler queue as empty
        stageIdToTaskIndices.getOrElseUpdate(stageId, new mutable.HashSet[Int]) += taskIndex
        val numTasksScheduled = stageIdToTaskIndices(stageId).size
        val numTasksTotal = stageIdToNumTasks.getOrElse(stageId, -1)
        if (numTasksScheduled == numTasksTotal) {
          // No more pending tasks for this stage
          stageIdToNumTasks -= stageId
          if (stageIdToNumTasks.isEmpty) {
            allocationManager.onSchedulerQueueEmpty()
          }
        }

        // Mark the executor on which this task is scheduled as busy
        executorIdToTaskIds.getOrElseUpdate(executorId, new mutable.HashSet[Long]) += taskId
        allocationManager.onExecutorBusy(executorId)
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val executorId = taskEnd.taskInfo.executorId
      val taskId = taskEnd.taskInfo.taskId
      allocationManager.synchronized {
        numRunningTasks -= 1
        // If the executor is no longer running any scheduled tasks, mark it as idle
        if (executorIdToTaskIds.contains(executorId)) {
          executorIdToTaskIds(executorId) -= taskId
          if (executorIdToTaskIds(executorId).isEmpty) {
            executorIdToTaskIds -= executorId
            allocationManager.onExecutorIdle(executorId)
          }
        }
      }
    }

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
      val executorId = executorAdded.executorId
      if (executorId != SparkContext.DRIVER_IDENTIFIER) {
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }
      }
    }

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
      allocationManager.onExecutorRemoved(executorRemoved.executorId)
    }

    /**
     * An estimate of the total number of pending tasks remaining for currently running stages. Does
     * not account for tasks which may have failed and been resubmitted.
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     */
    def totalPendingTasks(): Int = {
      stageIdToNumTasks.map { case (stageId, numTasks) =>
        numTasks - stageIdToTaskIndices.get(stageId).map(_.size).getOrElse(0)
      }.sum
    }

    /**
     * The number of tasks currently running across all stages.
     */
    def totalRunningTasks(): Int = numRunningTasks

    /**
     * Return true if an executor is not currently running a task, and false otherwise.
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     */
    def isExecutorIdle(executorId: String): Boolean = {
      !executorIdToTaskIds.contains(executorId)
    }
  }

}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue
}
