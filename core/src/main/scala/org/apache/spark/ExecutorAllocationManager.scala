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

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.{ControlThrowable, NonFatal}

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Tests.TEST_SCHEDULE_INTERVAL
import org.apache.spark.metrics.source.Source
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.dynalloc.ExecutorMonitor
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The ExecutorAllocationManager maintains a moving target number of executors which is periodically
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
 * been scheduled to run any tasks, then it is removed. Note that an executor caching any data
 * blocks will be removed if it has been idle for more than L seconds.
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
 *   spark.dynamicAllocation.executorAllocationRatio -
 *     This is used to reduce the parallelism of the dynamic allocation that can waste
 *     resources when tasks are small
 *
 *   spark.dynamicAllocation.schedulerBacklogTimeout (M) -
 *     If there are backlogged tasks for this duration, add new executors
 *
 *   spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N) -
 *     If the backlog is sustained for this duration, add more executors
 *     This is used only after the initial backlog timeout is exceeded
 *
 *   spark.dynamicAllocation.executorIdleTimeout (K) -
 *     If an executor without caching any data blocks has been idle for this duration, remove it
 *
 *   spark.dynamicAllocation.cachedExecutorIdleTimeout (L) -
 *     If an executor with caching data blocks has been idle for more than this duration,
 *     the executor will be removed
 *
 */
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    conf: SparkConf,
    clock: Clock = new SystemClock())
  extends Logging {

  allocationManager =>

  import ExecutorAllocationManager._

  // Lower and upper bounds on the number of executors.
  private val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
  private val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
  private val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)

  // How long there must be backlogged tasks for before an addition is triggered (seconds)
  private val schedulerBacklogTimeoutS = conf.get(DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT)

  // Same as above, but used only after `schedulerBacklogTimeoutS` is exceeded
  private val sustainedSchedulerBacklogTimeoutS =
    conf.get(DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT)

  // During testing, the methods to actually kill and add executors are mocked out
  private val testing = conf.get(DYN_ALLOCATION_TESTING)

  // TODO: The default value of 1 for spark.executor.cores works right now because dynamic
  // allocation is only supported for YARN and the default number of cores per executor in YARN is
  // 1, but it might need to be attained differently for different cluster managers
  private val tasksPerExecutorForFullParallelism =
    conf.get(EXECUTOR_CORES) / conf.get(CPUS_PER_TASK)

  private val executorAllocationRatio =
    conf.get(DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO)

  validateSettings()

  // Number of executors to add in the next round
  private var numExecutorsToAdd = 1

  // The desired number of executors at this moment in time. If all our executors were to die, this
  // is the number of executors we would immediately want from the cluster manager.
  private var numExecutorsTarget = initialNumExecutors

  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  private var addTime: Long = NOT_SET

  // Polling loop interval (ms)
  private val intervalMillis: Long = if (Utils.isTesting) {
      conf.get(TEST_SCHEDULE_INTERVAL)
    } else {
      100
    }

  // Listener for Spark events that impact the allocation policy
  val listener = new ExecutorAllocationListener

  val executorMonitor = new ExecutorMonitor(conf, client, clock)

  // Executor that handles the scheduling task.
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-dynamic-executor-allocation")

  // Metric source for ExecutorAllocationManager to expose internal status to MetricsSystem.
  val executorAllocationManagerSource = new ExecutorAllocationManagerSource

  // Whether we are still waiting for the initial set of executors to be allocated.
  // While this is true, we will not cancel outstanding executor requests. This is
  // set to false when:
  //   (1) a stage is submitted, or
  //   (2) an executor idle timeout has elapsed.
  @volatile private var initializing: Boolean = true

  // Number of locality aware tasks, used for executor placement.
  private var localityAwareTasks = 0

  // Host to possible task running on it, used for executor placement.
  private var hostToLocalTaskCount: Map[String, Int] = Map.empty

  /**
   * Verify that the settings specified through the config are valid.
   * If not, throw an appropriate exception.
   */
  private def validateSettings(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException(
        s"${DYN_ALLOCATION_MIN_EXECUTORS.key} and ${DYN_ALLOCATION_MAX_EXECUTORS.key} must be " +
          "positive!")
    }
    if (maxNumExecutors == 0) {
      throw new SparkException(s"${DYN_ALLOCATION_MAX_EXECUTORS.key} cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"${DYN_ALLOCATION_MIN_EXECUTORS.key} ($minNumExecutors) must " +
        s"be less than or equal to ${DYN_ALLOCATION_MAX_EXECUTORS.key} ($maxNumExecutors)!")
    }
    if (schedulerBacklogTimeoutS <= 0) {
      throw new SparkException(s"${DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT.key} must be > 0!")
    }
    if (sustainedSchedulerBacklogTimeoutS <= 0) {
      throw new SparkException(
        s"s${DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT.key} must be > 0!")
    }
    // Require external shuffle service for dynamic allocation
    // Otherwise, we may lose shuffle files when killing executors
    if (!conf.get(config.SHUFFLE_SERVICE_ENABLED) && !testing) {
      throw new SparkException("Dynamic allocation of executors requires the external " +
        "shuffle service. You may enable this through spark.shuffle.service.enabled.")
    }

    if (executorAllocationRatio > 1.0 || executorAllocationRatio <= 0.0) {
      throw new SparkException(
        s"${DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO.key} must be > 0 and <= 1.0")
    }
  }

  /**
   * Register for scheduler callbacks to decide when to add and remove executors, and start
   * the scheduling task.
   */
  def start(): Unit = {
    listenerBus.addToManagementQueue(listener)
    listenerBus.addToManagementQueue(executorMonitor)

    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        try {
          schedule()
        } catch {
          case ct: ControlThrowable =>
            throw ct
          case t: Throwable =>
            logWarning(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        }
      }
    }
    executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)

    client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
  }

  /**
   * Stop the allocation manager.
   */
  def stop(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  /**
   * Reset the allocation manager when the cluster manager loses track of the driver's state.
   * This is currently only done in YARN client mode, when the AM is restarted.
   *
   * This method forgets about any state about existing executors, and forces the scheduler to
   * re-evaluate the number of needed executors the next time it's run.
   */
  def reset(): Unit = synchronized {
    addTime = 0L
    numExecutorsTarget = initialNumExecutors
    executorMonitor.reset()
  }

  /**
   * The maximum number of executors we would need under the current load to satisfy all running
   * and pending tasks, rounded up.
   */
  private def maxNumExecutorsNeeded(): Int = {
    val numRunningOrPendingTasks = listener.totalPendingTasks + listener.totalRunningTasks
    math.ceil(numRunningOrPendingTasks * executorAllocationRatio /
              tasksPerExecutorForFullParallelism)
      .toInt
  }

  private def totalRunningTasks(): Int = synchronized {
    listener.totalRunningTasks
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
    val executorIdsToBeRemoved = executorMonitor.timedOutExecutors()
    if (executorIdsToBeRemoved.nonEmpty) {
      initializing = false
    }

    // Update executor target number only after initializing flag is unset
    updateAndSyncNumExecutorsTarget(clock.getTimeMillis())
    if (executorIdsToBeRemoved.nonEmpty) {
      removeExecutors(executorIdsToBeRemoved)
    }
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

    if (initializing) {
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
        // We lower the target number of executors but don't actively kill any yet.  Killing is
        // controlled separately by an idle timeout.  It's still helpful to reduce the target number
        // in case an executor just happens to get lost (eg., bad hardware, or the cluster manager
        // preempts it) -- in that case, there is no point in trying to immediately  get a new
        // executor, since we wouldn't even use it yet.
        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
        logDebug(s"Lowering target number of executors to $numExecutorsTarget (previously " +
          s"$oldNumExecutorsTarget) because not all requested executors are actually needed")
      }
      numExecutorsTarget - oldNumExecutorsTarget
    } else if (addTime != NOT_SET && now >= addTime) {
      val delta = addExecutors(maxNeeded)
      logDebug(s"Starting timer to add more executors (to " +
        s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
      addTime = now + (sustainedSchedulerBacklogTimeoutS * 1000)
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
    numExecutorsTarget = math.max(numExecutorsTarget, executorMonitor.executorCount)
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
      // Check if there is any speculative jobs pending
      if (listener.pendingTasks == 0 && listener.pendingSpeculativeTasks > 0) {
        numExecutorsTarget =
          math.max(math.min(maxNumExecutorsNeeded + 1, maxNumExecutors), minNumExecutors)
      } else {
        numExecutorsToAdd = 1
        return 0
      }
    }

    val addRequestAcknowledged = try {
      testing ||
        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
    } catch {
      case NonFatal(e) =>
        // Use INFO level so the error it doesn't show up by default in shells. Errors here are more
        // commonly caused by YARN AM restarts, which is a recoverable issue, and generate a lot of
        // noisy output.
        logInfo("Error reaching cluster manager.", e)
        false
    }
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
   * Request the cluster manager to remove the given executors.
   * Returns the list of executors which are removed.
   */
  private def removeExecutors(executors: Seq[String]): Seq[String] = synchronized {
    val executorIdsToBeRemoved = new ArrayBuffer[String]

    logInfo("Request to remove executorIds: " + executors.mkString(", "))
    val numExistingExecutors = executorMonitor.executorCount - executorMonitor.pendingRemovalCount

    var newExecutorTotal = numExistingExecutors
    executors.foreach { executorIdToBeRemoved =>
      if (newExecutorTotal - 1 < minNumExecutors) {
        logDebug(s"Not removing idle executor $executorIdToBeRemoved because there are only " +
          s"$newExecutorTotal executor(s) left (minimum number of executor limit $minNumExecutors)")
      } else if (newExecutorTotal - 1 < numExecutorsTarget) {
        logDebug(s"Not removing idle executor $executorIdToBeRemoved because there are only " +
          s"$newExecutorTotal executor(s) left (number of executor target $numExecutorsTarget)")
      } else {
        executorIdsToBeRemoved += executorIdToBeRemoved
        newExecutorTotal -= 1
      }
    }

    if (executorIdsToBeRemoved.isEmpty) {
      return Seq.empty[String]
    }

    // Send a request to the backend to kill this executor(s)
    val executorsRemoved = if (testing) {
      executorIdsToBeRemoved
    } else {
      // We don't want to change our target number of executors, because we already did that
      // when the task backlog decreased.
      client.killExecutors(executorIdsToBeRemoved, adjustTargetNumExecutors = false,
        countFailures = false, force = false)
    }

    // [SPARK-21834] killExecutors api reduces the target number of executors.
    // So we need to update the target with desired value.
    client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
    // reset the newExecutorTotal to the existing number of executors
    newExecutorTotal = numExistingExecutors
    if (testing || executorsRemoved.nonEmpty) {
      newExecutorTotal -= executorsRemoved.size
      executorMonitor.executorsKilled(executorsRemoved)
      logInfo(s"Executors ${executorsRemoved.mkString(",")} removed due to idle timeout." +
        s"(new desired total will be $newExecutorTotal)")
      executorsRemoved
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor/s " +
        s"${executorIdsToBeRemoved.mkString(",")} or no executor eligible to kill!")
      Seq.empty[String]
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
        s"are building up (to expire in $schedulerBacklogTimeoutS seconds)")
      addTime = clock.getTimeMillis + schedulerBacklogTimeoutS * 1000
    }
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors.
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug("Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAdd = 1
  }

  private case class StageAttempt(stageId: Int, stageAttemptId: Int) {
    override def toString: String = s"Stage $stageId (Attempt $stageAttemptId)"
  }

  /**
   * A listener that notifies the given allocation manager of when to add and remove executors.
   *
   * This class is intentionally conservative in its assumptions about the relative ordering
   * and consistency of events returned by the listener.
   */
  private[spark] class ExecutorAllocationListener extends SparkListener {

    private val stageAttemptToNumTasks = new mutable.HashMap[StageAttempt, Int]
    // Number of running tasks per stageAttempt including speculative tasks.
    // Should be 0 when no stages are active.
    private val stageAttemptToNumRunningTask = new mutable.HashMap[StageAttempt, Int]
    private val stageAttemptToTaskIndices = new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]
    // Number of speculative tasks to be scheduled in each stageAttempt
    private val stageAttemptToNumSpeculativeTasks = new mutable.HashMap[StageAttempt, Int]
    // The speculative tasks started in each stageAttempt
    private val stageAttemptToSpeculativeTaskIndices =
      new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]

    // stageAttempt to tuple (the number of task with locality preferences, a map where each pair
    // is a node and the number of tasks that would like to be scheduled on that node) map,
    // maintain the executor placement hints for each stageAttempt used by resource framework
    // to better place the executors.
    private val stageAttemptToExecutorPlacementHints =
      new mutable.HashMap[StageAttempt, (Int, Map[String, Int])]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      initializing = false
      val stageId = stageSubmitted.stageInfo.stageId
      val stageAttemptId = stageSubmitted.stageInfo.attemptNumber()
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val numTasks = stageSubmitted.stageInfo.numTasks
      allocationManager.synchronized {
        stageAttemptToNumTasks(stageAttempt) = numTasks
        allocationManager.onSchedulerBacklogged()

        // Compute the number of tasks requested by the stage on each host
        var numTasksPending = 0
        val hostToLocalTaskCountPerStage = new mutable.HashMap[String, Int]()
        stageSubmitted.stageInfo.taskLocalityPreferences.foreach { locality =>
          if (!locality.isEmpty) {
            numTasksPending += 1
            locality.foreach { location =>
              val count = hostToLocalTaskCountPerStage.getOrElse(location.host, 0) + 1
              hostToLocalTaskCountPerStage(location.host) = count
            }
          }
        }
        stageAttemptToExecutorPlacementHints.put(stageAttempt,
          (numTasksPending, hostToLocalTaskCountPerStage.toMap))

        // Update the executor placement hints
        updateExecutorPlacementHints()
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      val stageAttemptId = stageCompleted.stageInfo.attemptNumber()
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        // do NOT remove stageAttempt from stageAttemptToNumRunningTasks,
        // because the attempt may still have running tasks,
        // even after another attempt for the stage is submitted.
        stageAttemptToNumTasks -= stageAttempt
        stageAttemptToNumSpeculativeTasks -= stageAttempt
        stageAttemptToTaskIndices -= stageAttempt
        stageAttemptToSpeculativeTaskIndices -= stageAttempt
        stageAttemptToExecutorPlacementHints -= stageAttempt

        // Update the executor placement hints
        updateExecutorPlacementHints()

        // If this is the last stage with pending tasks, mark the scheduler queue as empty
        // This is needed in case the stage is aborted for any reason
        if (stageAttemptToNumTasks.isEmpty && stageAttemptToNumSpeculativeTasks.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
        }
      }
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val stageId = taskStart.stageId
      val stageAttemptId = taskStart.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val taskIndex = taskStart.taskInfo.index
      allocationManager.synchronized {
        stageAttemptToNumRunningTask(stageAttempt) =
          stageAttemptToNumRunningTask.getOrElse(stageAttempt, 0) + 1
        // If this is the last pending task, mark the scheduler queue as empty
        if (taskStart.taskInfo.speculative) {
          stageAttemptToSpeculativeTaskIndices.getOrElseUpdate(stageAttempt,
            new mutable.HashSet[Int]) += taskIndex
        } else {
          stageAttemptToTaskIndices.getOrElseUpdate(stageAttempt,
            new mutable.HashSet[Int]) += taskIndex
        }
        if (totalPendingTasks() == 0) {
          allocationManager.onSchedulerQueueEmpty()
        }
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val stageId = taskEnd.stageId
      val stageAttemptId = taskEnd.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val taskIndex = taskEnd.taskInfo.index
      allocationManager.synchronized {
        if (stageAttemptToNumRunningTask.contains(stageAttempt)) {
          stageAttemptToNumRunningTask(stageAttempt) -= 1
          if (stageAttemptToNumRunningTask(stageAttempt) == 0) {
            stageAttemptToNumRunningTask -= stageAttempt
          }
        }
        // If the task failed, we expect it to be resubmitted later. To ensure we have
        // enough resources to run the resubmitted task, we need to mark the scheduler
        // as backlogged again if it's not already marked as such (SPARK-8366)
        if (taskEnd.reason != Success) {
          if (totalPendingTasks() == 0) {
            allocationManager.onSchedulerBacklogged()
          }
          if (taskEnd.taskInfo.speculative) {
            stageAttemptToSpeculativeTaskIndices.get(stageAttempt).foreach {_.remove(taskIndex)}
          } else {
            stageAttemptToTaskIndices.get(stageAttempt).foreach {_.remove(taskIndex)}
          }
        }
      }
    }

    override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted)
      : Unit = {
      val stageId = speculativeTask.stageId
      val stageAttemptId = speculativeTask.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        stageAttemptToNumSpeculativeTasks(stageAttempt) =
          stageAttemptToNumSpeculativeTasks.getOrElse(stageAttempt, 0) + 1
        allocationManager.onSchedulerBacklogged()
      }
    }

    /**
     * An estimate of the total number of pending tasks remaining for currently running stages. Does
     * not account for tasks which may have failed and been resubmitted.
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     */
    def pendingTasks(): Int = {
      stageAttemptToNumTasks.map { case (stageAttempt, numTasks) =>
        numTasks - stageAttemptToTaskIndices.get(stageAttempt).map(_.size).getOrElse(0)
      }.sum
    }

    def pendingSpeculativeTasks(): Int = {
      stageAttemptToNumSpeculativeTasks.map { case (stageAttempt, numTasks) =>
        numTasks - stageAttemptToSpeculativeTaskIndices.get(stageAttempt).map(_.size).getOrElse(0)
      }.sum
    }

    def totalPendingTasks(): Int = {
      pendingTasks + pendingSpeculativeTasks
    }

    /**
     * The number of tasks currently running across all stages.
     * Include running-but-zombie stage attempts
     */
    def totalRunningTasks(): Int = {
      stageAttemptToNumRunningTask.values.sum
    }

    /**
     * Update the Executor placement hints (the number of tasks with locality preferences,
     * a map where each pair is a node and the number of tasks that would like to be scheduled
     * on that node).
     *
     * These hints are updated when stages arrive and complete, so are not up-to-date at task
     * granularity within stages.
     */
    def updateExecutorPlacementHints(): Unit = {
      var localityAwareTasks = 0
      val localityToCount = new mutable.HashMap[String, Int]()
      stageAttemptToExecutorPlacementHints.values.foreach { case (numTasksPending, localities) =>
        localityAwareTasks += numTasksPending
        localities.foreach { case (hostname, count) =>
          val updatedCount = localityToCount.getOrElse(hostname, 0) + count
          localityToCount(hostname) = updatedCount
        }
      }

      allocationManager.localityAwareTasks = localityAwareTasks
      allocationManager.hostToLocalTaskCount = localityToCount.toMap
    }
  }

  /**
   * Metric source for ExecutorAllocationManager to expose its internal executor allocation
   * status to MetricsSystem.
   * Note: These metrics heavily rely on the internal implementation of
   * ExecutorAllocationManager, metrics or value of metrics will be changed when internal
   * implementation is changed, so these metrics are not stable across Spark version.
   */
  private[spark] class ExecutorAllocationManagerSource extends Source {
    val sourceName = "ExecutorAllocationManager"
    val metricRegistry = new MetricRegistry()

    private def registerGauge[T](name: String, value: => T, defaultValue: T): Unit = {
      metricRegistry.register(MetricRegistry.name("executors", name), new Gauge[T] {
        override def getValue: T = synchronized { Option(value).getOrElse(defaultValue) }
      })
    }

    registerGauge("numberExecutorsToAdd", numExecutorsToAdd, 0)
    registerGauge("numberExecutorsPendingToRemove", executorMonitor.pendingRemovalCount, 0)
    registerGauge("numberAllExecutors", executorMonitor.executorCount, 0)
    registerGauge("numberTargetExecutors", numExecutorsTarget, 0)
    registerGauge("numberMaxNeededExecutors", maxNumExecutorsNeeded(), 0)
  }
}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue
}
