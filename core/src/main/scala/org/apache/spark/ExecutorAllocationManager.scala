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
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.Tests.TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceProfile.UNKNOWN_RESOURCE_PROFILE_ID
import org.apache.spark.resource.ResourceProfileManager
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.dynalloc.ExecutorMonitor
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The ExecutorAllocationManager maintains a moving target number of executors, for each
 * ResourceProfile, which is periodically synced to the cluster manager. The target starts
 * at a configured initial value and changes with the number of pending and running tasks.
 *
 * Decreasing the target number of executors happens when the current target is more than needed to
 * handle the current load. The target number of executors is always truncated to the number of
 * executors that could run all current running and pending tasks at once.
 *
 * Increasing the target number of executors happens in response to backlogged tasks waiting to be
 * scheduled. If the scheduler queue is not drained in M seconds, then new executors are added. If
 * the queue persists for another N seconds, then more executors are added and so on. The number
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
 * The remove policy is simpler and is applied on each ResourceProfile separately. If an executor
 * for that ResourceProfile has been idle for K seconds and the number of executors is more
 * then what is needed for that ResourceProfile, meaning there are not enough tasks that could use
 * the executor, then it is removed. Note that an executor caching any data
 * blocks will be removed if it has been idle for more than L seconds.
 *
 * There is no retry logic in either case because we make the assumption that the cluster manager
 * will eventually fulfill all requests it receives asynchronously.
 *
 * The relevant Spark properties are below. Each of these properties applies separately to
 * every ResourceProfile. So if you set a minimum number of executors, that is a minimum
 * for each ResourceProfile.
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
    cleaner: Option[ContextCleaner] = None,
    clock: Clock = new SystemClock(),
    resourceProfileManager: ResourceProfileManager)
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

  private val executorAllocationRatio =
    conf.get(DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO)

  private val decommissionEnabled = conf.get(DECOMMISSION_ENABLED)

  private val defaultProfileId = resourceProfileManager.defaultResourceProfile.id

  validateSettings()

  // Number of executors to add for each ResourceProfile in the next round
  private val numExecutorsToAddPerResourceProfileId = new mutable.HashMap[Int, Int]
  numExecutorsToAddPerResourceProfileId(defaultProfileId) = 1

  // The desired number of executors at this moment in time. If all our executors were to die, this
  // is the number of executors we would immediately want from the cluster manager.
  // Note every profile will be allowed to have initial number,
  // we may want to make this configurable per Profile in the future
  private val numExecutorsTargetPerResourceProfileId = new mutable.HashMap[Int, Int]
  numExecutorsTargetPerResourceProfileId(defaultProfileId) = initialNumExecutors

  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  private var addTime: Long = NOT_SET

  // Polling loop interval (ms)
  private val intervalMillis: Long = 100

  // Listener for Spark events that impact the allocation policy
  val listener = new ExecutorAllocationListener

  val executorMonitor = new ExecutorMonitor(conf, client, listenerBus, clock)

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

  // Number of locality aware tasks for each ResourceProfile, used for executor placement.
  private var numLocalityAwareTasksPerResourceProfileId = new mutable.HashMap[Int, Int]
  numLocalityAwareTasksPerResourceProfileId(defaultProfileId) = 0

  // ResourceProfile id to Host to possible task running on it, used for executor placement.
  private var rpIdToHostToLocalTaskCount: Map[Int, Map[String, Int]] = Map.empty

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
    if (!conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
      // If dynamic allocation shuffle tracking or worker decommissioning along with
      // storage shuffle decommissioning is enabled we have *experimental* support for
      // decommissioning without a shuffle service.
      if (conf.get(config.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED) ||
          (decommissionEnabled &&
            conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED))) {
        logWarning("Dynamic allocation without a shuffle service is an experimental feature.")
      } else if (!testing) {
        throw new SparkException("Dynamic allocation of executors requires the external " +
          "shuffle service. You may enable this through spark.shuffle.service.enabled.")
      }
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
    cleaner.foreach(_.attachListener(executorMonitor))

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

    if (!testing || conf.get(TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED)) {
      executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)
    }

    // copy the maps inside synchronize to ensure not being modified
    val (numExecutorsTarget, numLocalityAware) = synchronized {
      val numTarget = numExecutorsTargetPerResourceProfileId.toMap
      val numLocality = numLocalityAwareTasksPerResourceProfileId.toMap
      (numTarget, numLocality)
    }

    client.requestTotalExecutors(numExecutorsTarget, numLocalityAware, rpIdToHostToLocalTaskCount)
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
    numExecutorsTargetPerResourceProfileId.keys.foreach { rpId =>
      numExecutorsTargetPerResourceProfileId(rpId) = initialNumExecutors
    }
    numExecutorsToAddPerResourceProfileId.keys.foreach { rpId =>
      numExecutorsToAddPerResourceProfileId(rpId) = 1
    }
    executorMonitor.reset()
  }

  /**
   * The maximum number of executors, for the ResourceProfile id passed in, that we would need
   * under the current load to satisfy all running and pending tasks, rounded up.
   */
  private def maxNumExecutorsNeededPerResourceProfile(rpId: Int): Int = {
    val pending = listener.totalPendingTasksPerResourceProfile(rpId)
    val pendingSpeculative = listener.pendingSpeculativeTasksPerResourceProfile(rpId)
    val unschedulableTaskSets = listener.pendingUnschedulableTaskSetsPerResourceProfile(rpId)
    val running = listener.totalRunningTasksPerResourceProfile(rpId)
    val numRunningOrPendingTasks = pending + running
    val rp = resourceProfileManager.resourceProfileFromId(rpId)
    val tasksPerExecutor = rp.maxTasksPerExecutor(conf)
    logDebug(s"max needed for rpId: $rpId numpending: $numRunningOrPendingTasks," +
      s" tasksperexecutor: $tasksPerExecutor")
    val maxNeeded = math.ceil(numRunningOrPendingTasks * executorAllocationRatio /
      tasksPerExecutor).toInt

    val maxNeededWithSpeculationLocalityOffset =
      if (tasksPerExecutor > 1 && maxNeeded == 1 && pendingSpeculative > 0) {
      // If we have pending speculative tasks and only need a single executor, allocate one more
      // to satisfy the locality requirements of speculation
      maxNeeded + 1
    } else {
      maxNeeded
    }

    if (unschedulableTaskSets > 0) {
      // Request additional executors to account for task sets having tasks that are unschedulable
      // due to executors excluded for failures when the active executor count has already reached
      // the max needed which we would normally get.
      val maxNeededForUnschedulables = math.ceil(unschedulableTaskSets * executorAllocationRatio /
        tasksPerExecutor).toInt
      math.max(maxNeededWithSpeculationLocalityOffset,
        executorMonitor.executorCountWithResourceProfile(rpId) + maxNeededForUnschedulables)
    } else {
      maxNeededWithSpeculationLocalityOffset
    }
  }

  private def totalRunningTasksPerResourceProfile(id: Int): Int = synchronized {
    listener.totalRunningTasksPerResourceProfile(id)
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
    updateAndSyncNumExecutorsTarget(clock.nanoTime())
    if (executorIdsToBeRemoved.nonEmpty) {
      removeExecutors(executorIdsToBeRemoved)
    }
  }

  /**
   * Updates our target number of executors for each ResourceProfile and then syncs the result
   * with the cluster manager.
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
    if (initializing) {
      // Do not change our target while we are still initializing,
      // Otherwise the first job may have to ramp up unnecessarily
      0
    } else {
      val updatesNeeded = new mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]

      // Update targets for all ResourceProfiles then do a single request to the cluster manager
      numExecutorsTargetPerResourceProfileId.foreach { case (rpId, targetExecs) =>
        val maxNeeded = maxNumExecutorsNeededPerResourceProfile(rpId)
        if (maxNeeded < targetExecs) {
          // The target number exceeds the number we actually need, so stop adding new
          // executors and inform the cluster manager to cancel the extra pending requests

          // We lower the target number of executors but don't actively kill any yet.  Killing is
          // controlled separately by an idle timeout.  It's still helpful to reduce
          // the target number in case an executor just happens to get lost (e.g., bad hardware,
          // or the cluster manager preempts it) -- in that case, there is no point in trying
          // to immediately  get a new executor, since we wouldn't even use it yet.
          decrementExecutorsFromTarget(maxNeeded, rpId, updatesNeeded)
        } else if (addTime != NOT_SET && now >= addTime) {
          addExecutorsToTarget(maxNeeded, rpId, updatesNeeded)
        }
      }
      doUpdateRequest(updatesNeeded.toMap, now)
    }
  }

  private def addExecutorsToTarget(
      maxNeeded: Int,
      rpId: Int,
      updatesNeeded: mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]): Int = {
    updateTargetExecs(addExecutors, maxNeeded, rpId, updatesNeeded)
  }

  private def decrementExecutorsFromTarget(
      maxNeeded: Int,
      rpId: Int,
      updatesNeeded: mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]): Int = {
    updateTargetExecs(decrementExecutors, maxNeeded, rpId, updatesNeeded)
  }

  private def updateTargetExecs(
      updateTargetFn: (Int, Int) => Int,
      maxNeeded: Int,
      rpId: Int,
      updatesNeeded: mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]): Int = {
    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
    // update the target number (add or remove)
    val delta = updateTargetFn(maxNeeded, rpId)
    if (delta != 0) {
      updatesNeeded(rpId) = ExecutorAllocationManager.TargetNumUpdates(delta, oldNumExecutorsTarget)
    }
    delta
  }

  private def doUpdateRequest(
      updates: Map[Int, ExecutorAllocationManager.TargetNumUpdates],
      now: Long): Int = {
    // Only call cluster manager if target has changed.
    if (updates.size > 0) {
      val requestAcknowledged = try {
        logDebug("requesting updates: " + updates)
        testing ||
          client.requestTotalExecutors(
            numExecutorsTargetPerResourceProfileId.toMap,
            numLocalityAwareTasksPerResourceProfileId.toMap,
            rpIdToHostToLocalTaskCount)
      } catch {
        case NonFatal(e) =>
          // Use INFO level so the error it doesn't show up by default in shells.
          // Errors here are more commonly caused by YARN AM restarts, which is a recoverable
          // issue, and generate a lot of noisy output.
          logInfo("Error reaching cluster manager.", e)
          false
      }
      if (requestAcknowledged) {
        // have to go through all resource profiles that changed
        var totalDelta = 0
        updates.foreach { case (rpId, targetNum) =>
          val delta = targetNum.delta
          totalDelta += delta
          if (delta > 0) {
            val executorsString = "executor" + { if (delta > 1) "s" else "" }
            logInfo(s"Requesting $delta new $executorsString because tasks are backlogged " +
              s"(new desired total will be ${numExecutorsTargetPerResourceProfileId(rpId)} " +
              s"for resource profile id: ${rpId})")
            numExecutorsToAddPerResourceProfileId(rpId) =
              if (delta == numExecutorsToAddPerResourceProfileId(rpId)) {
                numExecutorsToAddPerResourceProfileId(rpId) * 2
              } else {
                1
              }
            logDebug(s"Starting timer to add more executors (to " +
              s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
            addTime = now + TimeUnit.SECONDS.toNanos(sustainedSchedulerBacklogTimeoutS)
          } else {
            logDebug(s"Lowering target number of executors to" +
              s" ${numExecutorsTargetPerResourceProfileId(rpId)} (previously " +
              s"${targetNum.oldNumExecutorsTarget} for resource profile id: ${rpId}) " +
              "because not all requested executors " +
              "are actually needed")
          }
        }
        totalDelta
      } else {
        // request was for all profiles so we have to go through all to reset to old num
        updates.foreach { case (rpId, targetNum) =>
          logWarning("Unable to reach the cluster manager to request more executors!")
          numExecutorsTargetPerResourceProfileId(rpId) = targetNum.oldNumExecutorsTarget
        }
        0
      }
    } else {
      logDebug("No change in number of executors")
      0
    }
  }

  private def decrementExecutors(maxNeeded: Int, rpId: Int): Int = {
    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
    numExecutorsTargetPerResourceProfileId(rpId) = math.max(maxNeeded, minNumExecutors)
    numExecutorsToAddPerResourceProfileId(rpId) = 1
    numExecutorsTargetPerResourceProfileId(rpId) - oldNumExecutorsTarget
  }

  /**
   * Update the target number of executors and figure out how many to add.
   * If the cap on the number of executors is reached, give up and reset the
   * number of executors to add next round instead of continuing to double it.
   *
   * @param maxNumExecutorsNeeded the maximum number of executors all currently running or pending
   *                              tasks could fill
   * @param rpId                  the ResourceProfile id of the executors
   * @return the number of additional executors actually requested.
   */
  private def addExecutors(maxNumExecutorsNeeded: Int, rpId: Int): Int = {
    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
    // Do not request more executors if it would put our target over the upper bound
    // this is doing a max check per ResourceProfile
    if (oldNumExecutorsTarget >= maxNumExecutors) {
      logDebug("Not adding executors because our current target total " +
        s"is already ${oldNumExecutorsTarget} (limit $maxNumExecutors)")
      numExecutorsToAddPerResourceProfileId(rpId) = 1
      return 0
    }
    // There's no point in wasting time ramping up to the number of executors we already have, so
    // make sure our target is at least as much as our current allocation:
    var numExecutorsTarget = math.max(numExecutorsTargetPerResourceProfileId(rpId),
        executorMonitor.executorCountWithResourceProfile(rpId))
    // Boost our target with the number to add for this round:
    numExecutorsTarget += numExecutorsToAddPerResourceProfileId(rpId)
    // Ensure that our target doesn't exceed what we need at the present moment:
    numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
    // Ensure that our target fits within configured bounds:
    numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors), minNumExecutors)
    val delta = numExecutorsTarget - oldNumExecutorsTarget
    numExecutorsTargetPerResourceProfileId(rpId) = numExecutorsTarget

    // If our target has not changed, do not send a message
    // to the cluster manager and reset our exponential growth
    if (delta == 0) {
      numExecutorsToAddPerResourceProfileId(rpId) = 1
    }
    delta
  }

  /**
   * Request the cluster manager to remove the given executors.
   * Returns the list of executors which are removed.
   */
  private def removeExecutors(executors: Seq[(String, Int)]): Seq[String] = synchronized {
    val executorIdsToBeRemoved = new ArrayBuffer[String]
    logDebug(s"Request to remove executorIds: ${executors.mkString(", ")}")
    val numExecutorsTotalPerRpId = mutable.Map[Int, Int]()
    executors.foreach { case (executorIdToBeRemoved, rpId) =>
      if (rpId == UNKNOWN_RESOURCE_PROFILE_ID) {
        if (testing) {
          throw new SparkException("ResourceProfile Id was UNKNOWN, this is not expected")
        }
        logWarning(s"Not removing executor $executorIdToBeRemoved because the " +
          "ResourceProfile was UNKNOWN!")
      } else {
        // get the running total as we remove or initialize it to the count - pendingRemoval
        val newExecutorTotal = numExecutorsTotalPerRpId.getOrElseUpdate(rpId,
          (executorMonitor.executorCountWithResourceProfile(rpId) -
            executorMonitor.pendingRemovalCountPerResourceProfileId(rpId) -
            executorMonitor.decommissioningPerResourceProfileId(rpId)
          ))
        if (newExecutorTotal - 1 < minNumExecutors) {
          logDebug(s"Not removing idle executor $executorIdToBeRemoved because there " +
            s"are only $newExecutorTotal executor(s) left (minimum number of executor limit " +
            s"$minNumExecutors)")
        } else if (newExecutorTotal - 1 < numExecutorsTargetPerResourceProfileId(rpId)) {
          logDebug(s"Not removing idle executor $executorIdToBeRemoved because there " +
            s"are only $newExecutorTotal executor(s) left (number of executor " +
            s"target ${numExecutorsTargetPerResourceProfileId(rpId)})")
        } else {
          executorIdsToBeRemoved += executorIdToBeRemoved
          numExecutorsTotalPerRpId(rpId) -= 1
        }
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
      if (decommissionEnabled) {
        val executorIdsWithoutHostLoss = executorIdsToBeRemoved.toSeq.map(
          id => (id, ExecutorDecommissionInfo("spark scale down"))).toArray
        client.decommissionExecutors(
          executorIdsWithoutHostLoss,
          adjustTargetNumExecutors = false,
          triggeredByExecutor = false)
      } else {
        client.killExecutors(executorIdsToBeRemoved.toSeq, adjustTargetNumExecutors = false,
          countFailures = false, force = false)
      }
    }

    // [SPARK-21834] killExecutors api reduces the target number of executors.
    // So we need to update the target with desired value.
    client.requestTotalExecutors(
      numExecutorsTargetPerResourceProfileId.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap,
      rpIdToHostToLocalTaskCount)

    // reset the newExecutorTotal to the existing number of executors
    if (testing || executorsRemoved.nonEmpty) {
      if (decommissionEnabled) {
        executorMonitor.executorsDecommissioned(executorsRemoved.toSeq)
      } else {
        executorMonitor.executorsKilled(executorsRemoved.toSeq)
      }
      logInfo(s"Executors ${executorsRemoved.mkString(",")} removed due to idle timeout.")
      executorsRemoved.toSeq
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
      addTime = clock.nanoTime() + TimeUnit.SECONDS.toNanos(schedulerBacklogTimeoutS)
    }
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors.
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug("Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAddPerResourceProfileId.transform { case (_, _) => 1 }
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
    // Number of speculative tasks pending/running in each stageAttempt
    private val stageAttemptToNumSpeculativeTasks = new mutable.HashMap[StageAttempt, Int]
    // The speculative tasks started in each stageAttempt
    private val stageAttemptToSpeculativeTaskIndices =
      new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]

    private val resourceProfileIdToStageAttempt =
      new mutable.HashMap[Int, mutable.Set[StageAttempt]]

    // Keep track of unschedulable task sets because of executor/node exclusions from too many task
    // failures. This is a Set of StageAttempt's because we'll only take the last unschedulable task
    // in a taskset although there can be more. This is done in order to avoid costly loops in the
    // scheduling. Check TaskSetManager#getCompletelyExcludedTaskIfAny for more details.
    private val unschedulableTaskSets = new mutable.HashSet[StageAttempt]

    // stageAttempt to tuple (the number of task with locality preferences, a map where each pair
    // is a node and the number of tasks that would like to be scheduled on that node, and
    // the resource profile id) map,
    // maintain the executor placement hints for each stageAttempt used by resource framework
    // to better place the executors.
    private val stageAttemptToExecutorPlacementHints =
      new mutable.HashMap[StageAttempt, (Int, Map[String, Int], Int)]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      initializing = false
      val stageId = stageSubmitted.stageInfo.stageId
      val stageAttemptId = stageSubmitted.stageInfo.attemptNumber()
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val numTasks = stageSubmitted.stageInfo.numTasks
      allocationManager.synchronized {
        stageAttemptToNumTasks(stageAttempt) = numTasks
        allocationManager.onSchedulerBacklogged()
        // need to keep stage task requirements to ask for the right containers
        val profId = stageSubmitted.stageInfo.resourceProfileId
        logDebug(s"Stage resource profile id is: $profId with numTasks: $numTasks")
        resourceProfileIdToStageAttempt.getOrElseUpdate(
          profId, new mutable.HashSet[StageAttempt]) += stageAttempt
        numExecutorsToAddPerResourceProfileId.getOrElseUpdate(profId, 1)

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
          (numTasksPending, hostToLocalTaskCountPerStage.toMap, profId))
        // Update the executor placement hints
        updateExecutorPlacementHints()

        if (!numExecutorsTargetPerResourceProfileId.contains(profId)) {
          numExecutorsTargetPerResourceProfileId.put(profId, initialNumExecutors)
          if (initialNumExecutors > 0) {
            logDebug(s"requesting executors, rpId: $profId, initial number is $initialNumExecutors")
            // we need to trigger a schedule since we add an initial number here.
            client.requestTotalExecutors(
              numExecutorsTargetPerResourceProfileId.toMap,
              numLocalityAwareTasksPerResourceProfileId.toMap,
              rpIdToHostToLocalTaskCount)
          }
        }
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      val stageAttemptId = stageCompleted.stageInfo.attemptNumber()
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        // do NOT remove stageAttempt from stageAttemptToNumRunningTask
        // because the attempt may still have running tasks,
        // even after another attempt for the stage is submitted.
        stageAttemptToNumTasks -= stageAttempt
        stageAttemptToNumSpeculativeTasks -= stageAttempt
        stageAttemptToTaskIndices -= stageAttempt
        stageAttemptToSpeculativeTaskIndices -= stageAttempt
        stageAttemptToExecutorPlacementHints -= stageAttempt
        removeStageFromResourceProfileIfUnused(stageAttempt)

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
        if (!hasPendingTasks) {
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
            removeStageFromResourceProfileIfUnused(stageAttempt)
          }
        }
        if (taskEnd.taskInfo.speculative) {
          stageAttemptToSpeculativeTaskIndices.get(stageAttempt).foreach {_.remove{taskIndex}}
          // If the previous task attempt succeeded first and it was the last task in a stage,
          // the stage may have been removed before handing this speculative TaskEnd event.
          if (stageAttemptToNumSpeculativeTasks.contains(stageAttempt)) {
            stageAttemptToNumSpeculativeTasks(stageAttempt) -= 1
          }
        }

        taskEnd.reason match {
          case Success | _: TaskKilled =>
          case _ =>
            if (!hasPendingTasks) {
              // If the task failed (not intentionally killed), we expect it to be resubmitted
              // later. To ensure we have enough resources to run the resubmitted task, we need to
              // mark the scheduler as backlogged again if it's not already marked as such
              // (SPARK-8366)
              allocationManager.onSchedulerBacklogged()
            }
            if (!taskEnd.taskInfo.speculative) {
              // If a non-speculative task is intentionally killed, it means the speculative task
              // has succeeded, and no further task of this task index will be resubmitted. In this
              // case, the task index is completed and we shouldn't remove it from
              // stageAttemptToTaskIndices. Otherwise, we will have a pending non-speculative task
              // for the task index (SPARK-30511)
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

    override def onUnschedulableTaskSetAdded(
        unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit = {
      val stageId = unschedulableTaskSetAdded.stageId
      val stageAttemptId = unschedulableTaskSetAdded.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        unschedulableTaskSets.add(stageAttempt)
        allocationManager.onSchedulerBacklogged()
      }
    }

    override def onUnschedulableTaskSetRemoved(
        unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit = {
      val stageId = unschedulableTaskSetRemoved.stageId
      val stageAttemptId = unschedulableTaskSetRemoved.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        // Clear unschedulableTaskSets since atleast one task becomes schedulable now
        unschedulableTaskSets.remove(stageAttempt)
        removeStageFromResourceProfileIfUnused(stageAttempt)
      }
    }

    def removeStageFromResourceProfileIfUnused(stageAttempt: StageAttempt): Unit = {
      if (!stageAttemptToNumRunningTask.contains(stageAttempt) &&
          !stageAttemptToNumTasks.contains(stageAttempt) &&
          !stageAttemptToNumSpeculativeTasks.contains(stageAttempt) &&
          !stageAttemptToTaskIndices.contains(stageAttempt) &&
          !stageAttemptToSpeculativeTaskIndices.contains(stageAttempt)
      ) {
        val rpForStage = resourceProfileIdToStageAttempt.filter { case (k, v) =>
          v.contains(stageAttempt)
        }.keys
        if (rpForStage.size == 1) {
          // be careful about the removal from here due to late tasks, make sure stage is
          // really complete and no tasks left
          resourceProfileIdToStageAttempt(rpForStage.head) -= stageAttempt
        } else {
          logWarning(s"Should have exactly one resource profile for stage $stageAttempt," +
              s" but have $rpForStage")
        }
      }
    }

    /**
     * An estimate of the total number of pending tasks remaining for currently running stages. Does
     * not account for tasks which may have failed and been resubmitted.
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     */
    def pendingTasksPerResourceProfile(rpId: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rpId, Set.empty).toSeq
      attempts.map(attempt => getPendingTaskSum(attempt)).sum
    }

    def hasPendingRegularTasks: Boolean = {
      val attemptSets = resourceProfileIdToStageAttempt.values
      attemptSets.exists(attempts => attempts.exists(getPendingTaskSum(_) > 0))
    }

    private def getPendingTaskSum(attempt: StageAttempt): Int = {
      val numTotalTasks = stageAttemptToNumTasks.getOrElse(attempt, 0)
      val numRunning = stageAttemptToTaskIndices.get(attempt).map(_.size).getOrElse(0)
      numTotalTasks - numRunning
    }

    def pendingSpeculativeTasksPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty).toSeq
      attempts.map(attempt => getPendingSpeculativeTaskSum(attempt)).sum
    }

    def hasPendingSpeculativeTasks: Boolean = {
      val attemptSets = resourceProfileIdToStageAttempt.values
      attemptSets.exists { attempts =>
        attempts.exists(getPendingSpeculativeTaskSum(_) > 0)
      }
    }

    private def getPendingSpeculativeTaskSum(attempt: StageAttempt): Int = {
      val numTotalTasks = stageAttemptToNumSpeculativeTasks.getOrElse(attempt, 0)
      val numRunning = stageAttemptToSpeculativeTaskIndices.get(attempt).map(_.size).getOrElse(0)
      numTotalTasks - numRunning
    }

    /**
     * Currently we only know when a task set has an unschedulable task, we don't know
     * the exact number and since the allocation manager isn't tied closely with the scheduler,
     * we use the number of tasks sets that are unschedulable as a heuristic to add more executors.
     */
    def pendingUnschedulableTaskSetsPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty).toSeq
      attempts.count(attempt => unschedulableTaskSets.contains(attempt))
    }

    def hasPendingTasks: Boolean = {
      hasPendingSpeculativeTasks || hasPendingRegularTasks
    }

    def totalPendingTasksPerResourceProfile(rp: Int): Int = {
      pendingTasksPerResourceProfile(rp) + pendingSpeculativeTasksPerResourceProfile(rp)
    }

    /**
     * The number of tasks currently running across all stages.
     * Include running-but-zombie stage attempts
     */
    def totalRunningTasks(): Int = {
      stageAttemptToNumRunningTask.values.sum
    }

    def totalRunningTasksPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty).toSeq
      // attempts is a Set, change to Seq so we keep all values
      attempts.map { attempt =>
        stageAttemptToNumRunningTask.getOrElse(attempt, 0)
      }.sum
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
      val localityAwareTasksPerResourceProfileId = new mutable.HashMap[Int, Int]

      // ResourceProfile id => map[host, count]
      val rplocalityToCount = new mutable.HashMap[Int, mutable.HashMap[String, Int]]()
      stageAttemptToExecutorPlacementHints.values.foreach {
        case (numTasksPending, localities, rpId) =>
          val rpNumPending =
            localityAwareTasksPerResourceProfileId.getOrElse(rpId, 0)
          localityAwareTasksPerResourceProfileId(rpId) = rpNumPending + numTasksPending
          localities.foreach { case (hostname, count) =>
            val rpBasedHostToCount =
              rplocalityToCount.getOrElseUpdate(rpId, new mutable.HashMap[String, Int])
            val newUpdated = rpBasedHostToCount.getOrElse(hostname, 0) + count
            rpBasedHostToCount(hostname) = newUpdated
          }
      }

      allocationManager.numLocalityAwareTasksPerResourceProfileId =
        localityAwareTasksPerResourceProfileId
      allocationManager.rpIdToHostToLocalTaskCount =
        rplocalityToCount.map { case (k, v) => (k, v.toMap)}.toMap
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

    // The metrics are going to return the sum for all the different ResourceProfiles.
    registerGauge("numberExecutorsToAdd",
      numExecutorsToAddPerResourceProfileId.values.sum, 0)
    registerGauge("numberExecutorsPendingToRemove", executorMonitor.pendingRemovalCount, 0)
    registerGauge("numberAllExecutors", executorMonitor.executorCount, 0)
    registerGauge("numberTargetExecutors",
      numExecutorsTargetPerResourceProfileId.values.sum, 0)
    registerGauge("numberMaxNeededExecutors", numExecutorsTargetPerResourceProfileId.keys
        .map(maxNumExecutorsNeededPerResourceProfile(_)).sum, 0)
  }
}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue

  // helper case class for requesting executors, here to be visible for testing
  private[spark] case class TargetNumUpdates(delta: Int, oldNumExecutorsTarget: Int)

}
