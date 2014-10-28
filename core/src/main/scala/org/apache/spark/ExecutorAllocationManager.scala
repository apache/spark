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

import scala.collection.mutable

import org.apache.spark.scheduler._

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The add policy depends on whether there are backlogged tasks waiting to be scheduled. If
 * the scheduler queue is not drained in N seconds, then new executors are added. If the queue
 * persists for another M seconds, then more executors are added and so on. The number added
 * in each round increases exponentially from the previous round until an upper bound on the
 * number of executors has been reached.
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
private[spark] class ExecutorAllocationManager(sc: SparkContext) extends Logging {
  import ExecutorAllocationManager._

  private val conf = sc.conf

  // Lower and upper bounds on the number of executors. These are required.
  private val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", -1)
  private val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", -1)
  if (minNumExecutors < 0 || maxNumExecutors < 0) {
    throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be set!")
  }
  if (minNumExecutors > maxNumExecutors) {
    throw new SparkException("spark.dynamicAllocation.minExecutors must " +
      "be less than or equal to spark.dynamicAllocation.maxExecutors!")
  }

  // How long there must be backlogged tasks for before an addition is triggered
  private val schedulerBacklogTimeout = conf.getLong(
    "spark.dynamicAllocation.schedulerBacklogTimeout", 60)

  // Same as above, but used only after `schedulerBacklogTimeout` is exceeded
  private val sustainedSchedulerBacklogTimeout = conf.getLong(
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", schedulerBacklogTimeout)

  // How long an executor must be idle for before it is removed
  private val removeThresholdSeconds = conf.getLong(
    "spark.dynamicAllocation.executorIdleTimeout", 600)

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

  /**
   * Register for scheduler callbacks to decide when to add and remove executors.
   */
  def start(): Unit = {
    val listener = new ExecutorAllocationListener(this)
    sc.addSparkListener(listener)
    startPolling()
  }

  /**
   * Start the main polling thread that keeps track of when to add and remove executors.
   * During each loop interval, this thread checks if the time then has exceeded any of the
   * add and remove times that are set. If so, it triggers the corresponding action.
   */
  private def startPolling(): Unit = {
    val t = new Thread {
      override def run(): Unit = {
        while (true) {
          ExecutorAllocationManager.this.synchronized {
            val now = System.currentTimeMillis
            try {
              // If the add time has expired, add executors and refresh the add time
              if (addTime != NOT_SET && now >= addTime) {
                addExecutors()
                logDebug(s"Starting timer to add more executors (to " +
                  s"expire in $sustainedSchedulerBacklogTimeout seconds)")
                addTime += sustainedSchedulerBacklogTimeout * 1000
              }

              // If any remove time has expired, remove the corresponding executor
              removeTimes.foreach { case (executorId, expireTime) =>
                if (now > expireTime) {
                  removeExecutor(executorId)
                  removeTimes.remove(executorId)
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
    t.setName("spark-dynamic-executor-allocation")
    t.setDaemon(true)
    t.start()
  }

  /**
   * Request a number of executors from the cluster manager.
   * If the cap on the number of executors is reached, give up and reset the
   * number of executors to add next round instead of continuing to double it.
   * Return the number actually requested. Exposed for testing.
   */
  def addExecutors(): Int = synchronized {
    // Do not request more executors if we have already reached the upper bound
    val numExistingExecutors = executorIds.size + numExecutorsPending
    if (numExistingExecutors >= maxNumExecutors) {
      logDebug(s"Not adding executors because there are already " +
        s"$maxNumExecutors executor(s), which is the limit")
      numExecutorsToAdd = 1
      return 0
    }

    // Request executors with respect to the upper bound
    val actualNumExecutorsToAdd =
      if (numExistingExecutors + numExecutorsToAdd <= maxNumExecutors) {
        numExecutorsToAdd
      } else {
        maxNumExecutors - numExistingExecutors
      }
    val newTotalExecutors = numExistingExecutors + actualNumExecutorsToAdd
    // TODO: Actually request executors once SPARK-3822 goes in
    val addRequestAcknowledged = true // sc.requestExecutors(actualNumbersToAdd)
    if (addRequestAcknowledged) {
      logInfo(s"Adding $actualNumExecutorsToAdd new executor(s) because " +
        s"tasks are backlogged (new total will be $newTotalExecutors)")
      numExecutorsToAdd =
        if (actualNumExecutorsToAdd == numExecutorsToAdd) numExecutorsToAdd * 2 else 1
      numExecutorsPending += actualNumExecutorsToAdd
      actualNumExecutorsToAdd
    } else {
      logWarning(s"Unable to reach the cluster manager " +
        s"to request $actualNumExecutorsToAdd executors!")
      0
    }
  }

  /**
   * Request the cluster manager to remove the given executor.
   * Return whether the request is received. Exposed for testing.
   */
  def removeExecutor(executorId: String): Boolean = synchronized {
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
      logInfo(s"Not removing idle executor $executorId because there are only " +
        s"$numExistingExecutors executor(s) left (limit $minNumExecutors)")
      return false
    }

    // Send a request to the backend to kill this executor
    // TODO: Actually kill the executor once SPARK-3822 goes in
    val removeRequestAcknowledged = true // sc.killExecutor(executorId)
    if (removeRequestAcknowledged) {
      logInfo(s"Removing executor $executorId because it has been idle for " +
        s"$removeThresholdSeconds seconds (new total will be ${numExistingExecutors - 1})")
      executorsPendingToRemove.add(executorId)
      true
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor $executorId!")
      false
    }
  }

  /**
   * Callback invoked when the specified executor has been added. Exposed for testing.
   */
  def onExecutorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      executorIds.add(executorId)
      executorIds.foreach(onExecutorIdle)
      logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
      if (numExecutorsPending > 0) {
        numExecutorsPending -= 1
        logDebug(s"Decremented number of pending executors ($numExecutorsPending left)")
      }
    }
  }

  /**
   * Callback invoked when the specified executor has been removed. Exposed for testing.
   */
  def onExecutorRemoved(executorId: String): Unit = synchronized {
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
   * if it is not already set. Exposed for testing.
   */
  def onSchedulerBacklogged(): Unit = synchronized {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeout seconds)")
      addTime = System.currentTimeMillis + schedulerBacklogTimeout * 1000
    }
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors. Exposed for testing.
   */
  def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug(s"Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAdd = 1
  }

  /**
   * Callback invoked when the specified executor is no longer running any tasks.
   * This sets a time in the future that decides when this executor should be removed if
   * the executor is not already marked as idle. Exposed for testing.
   */
  def onExecutorIdle(executorId: String): Unit = synchronized {
    if (!removeTimes.contains(executorId)) {
      logDebug(s"Starting idle timer for $executorId because there are no more tasks " +
        s"scheduled to run on the executor (to expire in $removeThresholdSeconds seconds)")
      removeTimes(executorId) = System.currentTimeMillis + removeThresholdSeconds * 1000
    }
  }

  /**
   * Callback invoked when the specified executor is now running a task.
   * This resets all variables used for removing this executor. Exposed for testing.
   */
  def onExecutorBusy(executorId: String): Unit = synchronized {
    logDebug(s"Clearing idle timer for $executorId because it is now running a task")
    removeTimes.remove(executorId)
  }

  /* --------------------------- *
   | Getters exposed for testing |
   * --------------------------- */

  def getNumExecutorsToAdd: Int = numExecutorsToAdd
  def getNumExecutorsPending: Int = numExecutorsPending
  def getExecutorsPendingToRemove: collection.Set[String] = executorsPendingToRemove
  def getExecutorIds: collection.Set[String] = executorIds
  def getAddTime: Long = addTime
  def getRemoveTimes: collection.Map[String, Long] = removeTimes

}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue
}

/**
 * A listener that notifies the given allocation manager of when to add and remove executors.
 */
private class ExecutorAllocationListener(allocationManager: ExecutorAllocationManager)
  extends SparkListener {

  private val stageIdToPendingTaskIndices = new mutable.HashMap[Int, mutable.HashSet[Int]]
  private val executorIdToTaskIds = new mutable.HashMap[String, mutable.HashSet[Long]]

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    val stageId = stageSubmitted.stageInfo.stageId
    val numTasks = stageSubmitted.stageInfo.numTasks
    // Start the add timer because there are new pending tasks
    stageIdToPendingTaskIndices.getOrElseUpdate(
      stageId, new mutable.HashSet[Int]) ++= (0 to numTasks - 1)
    allocationManager.onSchedulerBacklogged()
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val stageId = taskStart.stageId
    val taskId = taskStart.taskInfo.taskId
    val taskIndex = taskStart.taskInfo.index
    val executorId = taskStart.taskInfo.executorId

    // If there are no more pending tasks, cancel the add timer
    if (stageIdToPendingTaskIndices.contains(stageId)) {
      stageIdToPendingTaskIndices(stageId) -= taskIndex
      if (stageIdToPendingTaskIndices(stageId).isEmpty) {
        stageIdToPendingTaskIndices -= stageId
      }
    }
    if (stageIdToPendingTaskIndices.isEmpty) {
      allocationManager.onSchedulerQueueEmpty()
    }

    // Cancel the remove timer because the executor is now running a task
    executorIdToTaskIds.getOrElseUpdate(executorId, new mutable.HashSet[Long]) += taskId
    allocationManager.onExecutorBusy(executorId)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val executorId = taskEnd.taskInfo.executorId
    val taskId = taskEnd.taskInfo.taskId
    if (executorIdToTaskIds.contains(executorId)) {
      executorIdToTaskIds(executorId) -= taskId
      if (executorIdToTaskIds(executorId).isEmpty) {
        executorIdToTaskIds -= executorId
      }
    }
    // If there are no more tasks running on this executor, start the remove timer
    if (!executorIdToTaskIds.contains(executorId)) {
      allocationManager.onExecutorIdle(executorId)
    }
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    val executorId = blockManagerAdded.blockManagerId.executorId
    if (executorId != "<driver>") {
      allocationManager.onExecutorAdded(executorId)
    }
  }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    allocationManager.onExecutorRemoved(blockManagerRemoved.blockManagerId.executorId)
  }
}
