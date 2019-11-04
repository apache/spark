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
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.resource.ResourceProfile.UNKNOWN_RESOURCE_PROFILE_ID
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
 * The remove policy is simpler: If an executor has been idle for K seconds and the number of
 * executors is more then what is needed, meaning there are not enough tasks that could use
 * the executor, then it is removed. Note that an executor caching any data
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
    cleaner: Option[ContextCleaner] = None,
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

  // TODO - make this configurable by ResourceProfile in the future
  private val executorAllocationRatio =
    conf.get(DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO)

  private val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(conf)

  validateSettings()

  // Number of executors to add for each ResourceProfile in the next round
  private val numExecutorsToAddPerResourceProfileId = new mutable.HashMap[Int, Int]
  numExecutorsToAddPerResourceProfileId(defaultProfile.id) = 1

  // The desired number of executors at this moment in time. If all our executors were to die, this
  // is the number of executors we would immediately want from the cluster manager.
  // private var numExecutorsTarget = initialNumExecutors
  private val numExecutorsTargetPerResourceProfile = new mutable.HashMap[ResourceProfile, Int]
  numExecutorsTargetPerResourceProfile(defaultProfile) = initialNumExecutors

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
  private var localityAwareTasks = 0
  private var numLocalityAwareTasksPerResourceProfileId = new mutable.HashMap[Int, Int]
  numLocalityAwareTasksPerResourceProfileId(defaultProfile.id) = 0

  // Host and ResourceProfile to possible task running on it, used for executor placement.
  // private var hostToLocalTaskCount: Map[String, Int] = Map.empty
  // TODO - create case class for (host, resourceprofile)?
  private var hostToLocalTaskCount: Map[(String, ResourceProfile), Int] = Map.empty

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
      if (conf.get(config.DYN_ALLOCATION_SHUFFLE_TRACKING)) {
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
    executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)

    client.requestTotalExecutors(0, numLocalityAwareTasksPerResourceProfileId.toMap,
      hostToLocalTaskCount, Some(numExecutorsTargetPerResourceProfile.toMap))
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
    numExecutorsTargetPerResourceProfile.keys.foreach { rp =>
      // Note this means every profile will be allowed to have initial number
      // we may want to make this configurable per Profile in the future
      numExecutorsTargetPerResourceProfile(rp) = initialNumExecutors
    }
    executorMonitor.reset()
  }

  /**
   * The maximum number of executors we would need under the current load to satisfy all running
   * and pending tasks, rounded up.
   */
  private def maxNumExecutorsNeededPerResourceProfile(rp: Int): Int = {
    val numRunningOrPendingTasks = listener.totalPendingTasksPerResourceProfile(rp) +
      listener.totalRunningTasksPerResourceProfile(rp)
    math.ceil(numRunningOrPendingTasks * executorAllocationRatio /
      tasksPerExecutorForFullParallelism).toInt
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
  private def updateAndSyncNumExecutorsTarget(now: Long): Unit = synchronized {

    if (initializing) {
      // Do not change our target while we are still initializing,
      // Otherwise the first job may have to ramp up unnecessarily
      0
    } else {
      // TOOD - what if no resource profiles and start with some and remove before stage started?
      val updatesNeeded = new mutable.HashMap[ResourceProfile, TargetNumUpdates]

      listener.execResourceProfIdToNumTasks.foreach { case (rProfId, needed) =>
        val maxNeeded = maxNumExecutorsNeededPerResourceProfile(rProfId)
        logInfo("max needed for rp: " + rProfId + " is: " + maxNeeded)
        val resourceProfile = listener.resourceProfileIdToResourceProfile(rProfId)
        val targetExecs =
          numExecutorsTargetPerResourceProfile.getOrElseUpdate(resourceProfile, initialNumExecutors)
        if (maxNeeded < targetExecs) {
          // The target number exceeds the number we actually need, so stop adding new
          // executors and inform the cluster manager to cancel the extra pending requests

          // We lower the target number of executors but don't actively kill any yet.  Killing is
          // controlled separately by an idle timeout.  It's still helpful to reduce
          // the target number in case an executor just happens to get lost (eg., bad hardware,
          // or the cluster manager preempts it) -- in that case, there is no point in trying
          // to immediately  get a new executor, since we wouldn't even use it yet.
          updateTarget(decrementExecutors, maxNeeded, resourceProfile, updatesNeeded)
        } else if (addTime != NOT_SET && now >= addTime) {
          updateTarget(addExecutors, maxNeeded, resourceProfile, updatesNeeded)
        }
      }
      doRequest(updatesNeeded.toMap, now)

      /* if (updateTotalExecutors) {
         // TODO - not passing numExecutors since in resource profile map
         logInfo("updating total executors")
         client.requestTotalExecutors(0, localityAwareTasks,
           hostToLocalTaskCount, Some(numExecutorsTargetPerResourceProfile.toMap))
       } */
    }
  }

  private case class TargetNumUpdates(delta: Int, oldNumExecutorsTarget: Int)

  private def updateTarget(
      updateTargetFn: (Int, ResourceProfile) => Int,
      maxNeeded: Int,
      rp: ResourceProfile,
      updatesNeeded: mutable.HashMap[ResourceProfile, TargetNumUpdates]): Int = {

    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfile(rp)
    // update the target number (add or remove)
    val delta = updateTargetFn(maxNeeded, rp)

    if (delta != 0) {
      updatesNeeded(rp) = TargetNumUpdates(delta, oldNumExecutorsTarget)
    }
    delta
  }

  private def doRequest(updates: Map[ResourceProfile, TargetNumUpdates], now: Long): Unit = {
    // Only call cluster manager if target has changed.
    logWarning("in do Request update: " + updates)
    if (updates.size > 0) {
      val requestAcknowledged = try {
        logInfo("requesting updates: " + updates)
        testing ||
          client.requestTotalExecutors(0,
            numLocalityAwareTasksPerResourceProfileId.toMap, hostToLocalTaskCount,
            Some(numExecutorsTargetPerResourceProfile.toMap))
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
        updates.foreach { case (rp, targetNum) =>
          val delta = targetNum.delta
          if (delta > 0) {
            val executorsString = "executor" + {
              if (delta > 1) "s" else ""
            }
            logInfo(s"Requesting $delta new $executorsString because tasks are backlogged " +
              s"(new desired total will be ${numExecutorsTargetPerResourceProfile(rp)} " +
              s"for resource profile id: ${rp.id})")
            numExecutorsToAddPerResourceProfileId(rp.id) =
              if (delta == numExecutorsToAddPerResourceProfileId(rp.id)) {
                numExecutorsToAddPerResourceProfileId(rp.id) * 2
              } else {
                1
              }
            logDebug(s"Starting timer to add more executors (to " +
              s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
            addTime = now + (sustainedSchedulerBacklogTimeoutS * 1000)
          } else {
            logDebug(s"Lowering target number of executors to" +
              s" ${numExecutorsTargetPerResourceProfile(rp)} (previously " +
              s"$targetNum.oldNumExecutorsTarget for resource profile id: ${rp.id}) " +
              "because not all requested executors " +
              "are actually needed")
          }
        }
      } else {
        // request for all stages so we have to go through all to reset to old num
        updates.foreach { case (rp, targetNum) =>
          logWarning(
            s"Unable to reach the cluster manager to request more executors!")
          numExecutorsTargetPerResourceProfile(rp) = targetNum.oldNumExecutorsTarget
        }
      }
    } else {
      logDebug("No change in number of executors")
    }
  }

    private def decrementExecutors(maxNeeded: Int, rp: ResourceProfile): Int = {
      val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfile(rp)
      numExecutorsTargetPerResourceProfile(rp) = math.max(maxNeeded, minNumExecutors)
      numExecutorsToAddPerResourceProfileId(rp.id) = 1
      numExecutorsTargetPerResourceProfile(rp) - oldNumExecutorsTarget
    }

    /**
     * Update the target number of executors and figure out how many to add.
     * If the cap on the number of executors is reached, give up and reset the
     * number of executors to add next round instead of continuing to double it.
     *
     * @param maxNumExecutorsNeeded the maximum number of executors all currently running or pending
     *                              tasks could fill
     * @param rp                    the ResourceProfile of the executors
     * @return the number of additional executors actually requested.
     */
  private def addExecutors(maxNumExecutorsNeeded: Int, rp: ResourceProfile): Int = {

    logWarning("in add executors max: " + maxNumExecutorsNeeded)
    val oldNumExecutorsTarget =
      numExecutorsTargetPerResourceProfile.getOrElseUpdate(rp, 0)
    // Do not request more executors if it would put our target over the upper bound
    // this is doing a max check per ResourceProfile
    if (oldNumExecutorsTarget >= maxNumExecutors) {
      logDebug(s"Not adding executors because our current target total " +
        s"is already ${oldNumExecutorsTarget} (limit $maxNumExecutors)")
      numExecutorsToAddPerResourceProfileId(rp.id) = 1
      return 0
    }

    // There's no point in wasting time ramping up to the number of executors we already have, so
    // make sure our target is at least as much as our current allocation:
    numExecutorsTargetPerResourceProfile(rp) =
      math.max(numExecutorsTargetPerResourceProfile(rp),
        executorMonitor.executorCountWithResourceProfile(rp.id))

    // Boost our target with the number to add for this round:
    numExecutorsTargetPerResourceProfile(rp) +=
      numExecutorsToAddPerResourceProfileId.getOrElseUpdate(rp.id, 0)
    // Ensure that our target doesn't exceed what we need at the present moment:
    numExecutorsTargetPerResourceProfile(rp) =
      math.min(numExecutorsTargetPerResourceProfile(rp), maxNumExecutorsNeeded)
    // Ensure that our target fits within configured bounds:
    numExecutorsTargetPerResourceProfile(rp) = math.max(
      math.min(numExecutorsTargetPerResourceProfile(rp), maxNumExecutors), minNumExecutors)

    val delta = numExecutorsTargetPerResourceProfile(rp) - oldNumExecutorsTarget

    // If our target has not changed, do not send a message
    // to the cluster manager and reset our exponential growth
    if (delta == 0) {
      numExecutorsToAddPerResourceProfileId(rp.id) = 1
    }
    delta
  }

  private def getResourceProfileIdOfExecutor(executorId: String): Int = {
    executorMonitor.getResourceProfileId(executorId)
  }

  /**
   * Request the cluster manager to remove the given executors.
   * Returns the list of executors which are removed.
   */
  private def removeExecutors(executors: Seq[String]): Seq[String] = synchronized {
    val executorIdsToBeRemoved = new ArrayBuffer[String]

    logInfo("Request to remove executorIds: " + executors.mkString(", "))
    val numExecutorsTotalPerRpId = mutable.Map[Int, Int]()

    executors.foreach { executorIdToBeRemoved =>
      val rpId = getResourceProfileIdOfExecutor(executorIdToBeRemoved)
      if (rpId == UNKNOWN_RESOURCE_PROFILE_ID) {
        logWarning(s"Not removing executor $executorIdsToBeRemoved because couldn't find " +
          "ResourceProfile for it!")
      }

      val newExecutorTotal = numExecutorsTotalPerRpId.getOrElseUpdate(rpId,
        (executorMonitor.executorCountWithResourceProfile(rpId) -
          executorMonitor.pendingRemovalCountPerResourceProfileId(rpId)))
      val rp = listener.resourceProfileIdToResourceProfile(rpId)

      // TODO - ideally min num per stage (or ResourceProfile), otherwise min * num stages executors
      if (newExecutorTotal - 1 < minNumExecutors) {
        logDebug(s"Not removing idle executor $executorIdToBeRemoved because there are only " +
          s"$newExecutorTotal executor(s) left (minimum number of executor limit $minNumExecutors)")
      } else if (newExecutorTotal - 1 < numExecutorsTargetPerResourceProfile(rp)) {
        logDebug(s"Not removing idle executor $executorIdToBeRemoved because there are only " +
          s"$newExecutorTotal executor(s) left (number of executor " +
          s"target ${numExecutorsTargetPerResourceProfile(rp)})")
      } else {
        executorIdsToBeRemoved += executorIdToBeRemoved
        numExecutorsTotalPerRpId(rpId) -= 1
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
    client.requestTotalExecutors(0, numLocalityAwareTasksPerResourceProfileId.toMap,
        hostToLocalTaskCount, Some(numExecutorsTargetPerResourceProfile.toMap))

    // reset the newExecutorTotal to the existing number of executors
    if (testing || executorsRemoved.nonEmpty) {
      executorMonitor.executorsKilled(executorsRemoved)
      logInfo(s"Executors ${executorsRemoved.mkString(",")} removed due to idle timeout.")
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
    numExecutorsToAddPerResourceProfileId.keys.foreach(numExecutorsToAddPerResourceProfileId(_) = 1)
  }

    // TOOD - change to not be private
    case class StageAttempt(stageId: Int, stageAttemptId: Int) {
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

    private val stageAttemptToResourceProfile =
      new mutable.HashMap[StageAttempt, ResourceProfile]
    // TODO - make private - ResourceProfileId to num tasks
    val execResourceProfIdToNumTasks = new mutable.HashMap[Int, Int]
    val resourceProfileIdToStageAttempt =
      new mutable.HashMap[Int, mutable.Set[StageAttempt]]
    // val resourceProfileIdToExecutor = new mutable.HashMap[Int, mutable.Set[String]]
    val resourceProfileIdToResourceProfile = new mutable.HashMap[Int, ResourceProfile]
    resourceProfileIdToResourceProfile(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) = defaultProfile
    execResourceProfIdToNumTasks(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) = 0

    // stageAttempt to tuple (the number of task with locality preferences, a map where each pair
    // is a node and the number of tasks that would like to be scheduled on that node, and
    // the resource profile id) map,
    // maintain the executor placement hints for each stageAttempt used by resource framework
    // to better place the executors.
    private val stageAttemptToExecutorPlacementHints =
      new mutable.HashMap[StageAttempt, (Int, Map[String, Int], ResourceProfile)]

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
        val stageResourceProf = stageSubmitted.stageInfo.resourceProfile.getOrElse(defaultProfile)
        logInfo("stage reosurce profile is: " + stageResourceProf)
        stageAttemptToResourceProfile(stageAttempt) = stageResourceProf
        val profId = stageResourceProf.id
        resourceProfileIdToStageAttempt.getOrElseUpdate(
          profId, new mutable.HashSet[StageAttempt]) += stageAttempt
        logInfo("adding to execResourceReqsToNumTasks: " + numTasks)
        execResourceProfIdToNumTasks(stageResourceProf.id) =
          execResourceProfIdToNumTasks.getOrElse(stageResourceProf.id, 0) + numTasks
        numExecutorsToAddPerResourceProfileId.getOrElseUpdate(profId, 1)
        // TODO - currently never remove, we could remove is all executors using a profile exit
        resourceProfileIdToResourceProfile.getOrElseUpdate(profId, stageResourceProf)
        numExecutorsTargetPerResourceProfile.getOrElseUpdate(stageResourceProf, 0)
        logInfo("value to execResourceReqsToNumTasks: " +
          execResourceProfIdToNumTasks(profId))

        // Compute the number of tasks requested by the stage on each host
        var numTasksPending = 0
        val hostToLocalTaskCountPerStage = new mutable.HashMap[String, Int]()
        // TODO - what if locality preference and resourceprofile conflict?? Do we want to change
        // this logic?
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
          (numTasksPending, hostToLocalTaskCountPerStage.toMap,
            stageSubmitted.stageInfo.resourceProfile.getOrElse(defaultProfile)))

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
        val numTasks = stageAttemptToNumTasks(stageAttempt)
        stageAttemptToNumTasks -= stageAttempt
        stageAttemptToNumSpeculativeTasks -= stageAttempt
        stageAttemptToTaskIndices -= stageAttempt
        stageAttemptToSpeculativeTaskIndices -= stageAttempt
        stageAttemptToExecutorPlacementHints -= stageAttempt
        val rp = stageAttemptToResourceProfile(stageAttempt)
        logInfo("stage completed rp is: " + rp)
        resourceProfileIdToStageAttempt(rp.id) -= stageAttempt
        execResourceProfIdToNumTasks(rp.id) -= numTasks
        stageAttemptToResourceProfile -= stageAttempt

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

    def pendingTasksPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty)
      attempts.map { attempt =>
        stageAttemptToNumTasks.map { case (stageAttempt, numTasks) =>
          numTasks - stageAttemptToTaskIndices.get(stageAttempt).map(_.size).getOrElse(0)
        }.sum
      }.sum
    }



    def pendingSpeculativeTasks(): Int = {
      stageAttemptToNumSpeculativeTasks.map { case (stageAttempt, numTasks) =>
        numTasks - stageAttemptToSpeculativeTaskIndices.get(stageAttempt).map(_.size).getOrElse(0)
      }.sum
    }

    def pendingSpeculativeTasksPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty)
      attempts.map { attempt =>
        stageAttemptToNumSpeculativeTasks.map { case (stageAttempt, numTasks) =>
          numTasks - stageAttemptToSpeculativeTaskIndices.get(stageAttempt).map(_.size).getOrElse(0)
        }.sum
      }.sum
    }



    def totalPendingTasks(): Int = {
      pendingTasks + pendingSpeculativeTasks
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
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty)
      attempts.map { attempt =>
        stageAttemptToNumRunningTask.getOrElseUpdate(attempt, 0)
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
      var localityAwareTasks = 0
      var localityAwareTasksPerResourceProfileId = new mutable.HashMap[Int, Int]
      val localityToCount = new mutable.HashMap[(String, ResourceProfile), Int]()
      val hostToRProfile = new mutable.HashMap[String, (Option[ResourceProfile], Int)]()
      stageAttemptToExecutorPlacementHints.values.foreach {
        case (numTasksPending, localities, rp) =>
          localityAwareTasks += numTasksPending
          val rpNumPending =
            localityAwareTasksPerResourceProfileId.getOrElse(rp.id, 0)
          localityAwareTasksPerResourceProfileId(rp.id) = rpNumPending + numTasksPending
          localities.foreach { case (hostname, count) =>
            val updatedCount = localityToCount.getOrElse((hostname, rp), 0) + count
            localityToCount((hostname, rp)) = updatedCount
          }
      }
      allocationManager.localityAwareTasks = localityAwareTasks
      allocationManager.numLocalityAwareTasksPerResourceProfileId =
        localityAwareTasksPerResourceProfileId
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

    // TODO - how to do metrics per stage????
    registerGauge("numberExecutorsToAdd", 0, 0)
    registerGauge("numberExecutorsPendingToRemove", executorMonitor.pendingRemovalCount, 0)
    registerGauge("numberAllExecutors", executorMonitor.executorCount, 0)
    // TODO - fix
    registerGauge("numberTargetExecutors", 0, 0)
    registerGauge("numberMaxNeededExecutors", 0, 0)
  }
}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue
}
