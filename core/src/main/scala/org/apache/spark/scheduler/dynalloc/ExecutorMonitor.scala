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

package org.apache.spark.scheduler.dynalloc

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceProfile.UNKNOWN_RESOURCE_PROFILE_ID
import org.apache.spark.scheduler._
import org.apache.spark.storage.{RDDBlockId, ShuffleDataBlockId}
import org.apache.spark.util.Clock

/**
 * A monitor for executor activity, used by ExecutorAllocationManager to detect idle executors.
 */
private[spark] class ExecutorMonitor(
    conf: SparkConf,
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    clock: Clock) extends SparkListener with CleanerListener with Logging {

  private val idleTimeoutNs = TimeUnit.SECONDS.toNanos(
    conf.get(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT))
  private val storageTimeoutNs = TimeUnit.SECONDS.toNanos(
    conf.get(DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT))
  private val shuffleTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
    conf.get(DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT))

  private val fetchFromShuffleSvcEnabled = conf.get(SHUFFLE_SERVICE_ENABLED) &&
    conf.get(SHUFFLE_SERVICE_FETCH_RDD_ENABLED)
  private val shuffleTrackingEnabled = !conf.get(SHUFFLE_SERVICE_ENABLED) &&
    conf.get(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED)

  private val executors = new ConcurrentHashMap[String, Tracker]()
  private val execResourceProfileCount = new ConcurrentHashMap[Int, Int]()

  // The following fields are an optimization to avoid having to scan all executors on every EAM
  // schedule interval to find out which ones are timed out. They keep track of when the next
  // executor timeout is expected to happen, and the current list of timed out executors. There's
  // also a flag that forces the EAM task to recompute the timed out executors, in case some event
  // arrives on the listener bus that may cause the current list of timed out executors to change.
  //
  // There's also per-executor state used for this purpose, so that recomputations can be triggered
  // only when really necessary.
  //
  // Note that this isn't meant to, and cannot, always make the right decision about which executors
  // are indeed timed out. For example, the EAM thread may detect a timed out executor while a new
  // "task start" event has just been posted to the listener bus and hasn't yet been delivered to
  // this listener. There are safeguards in other parts of the code that would prevent that executor
  // from being removed.
  private val nextTimeout = new AtomicLong(Long.MaxValue)
  private var timedOutExecs = Seq.empty[(String, Int)]

  // Active job tracking.
  //
  // The following state is used when an external shuffle service is not in use, and allows Spark
  // to scale down based on whether the shuffle data stored in executors is in use.
  //
  // The algorithm works as following: when jobs start, some state is kept that tracks which stages
  // are part of that job, and which shuffle ID is attached to those stages. As tasks finish, the
  // executor tracking code is updated to include the list of shuffles for which it's storing
  // shuffle data.
  //
  // If executors hold shuffle data that is related to an active job, then the executor is
  // considered to be in "shuffle busy" state; meaning that the executor is not allowed to be
  // removed. If the executor has shuffle data but it doesn't relate to any active job, then it
  // may be removed when idle, following the shuffle-specific timeout configuration.
  //
  // The following fields are not thread-safe and should be only used from the event thread.
  private val shuffleToActiveJobs = new mutable.HashMap[Int, mutable.ArrayBuffer[Int]]()
  private val stageToShuffleID = new mutable.HashMap[Int, Int]()
  private val jobToStageIDs = new mutable.HashMap[Int, Seq[Int]]()

  def reset(): Unit = {
    executors.clear()
    execResourceProfileCount.clear()
    nextTimeout.set(Long.MaxValue)
    timedOutExecs = Nil
  }

  /**
   * Returns the list of executors and their ResourceProfile id that are currently considered to
   * be timed out. Should only be called from the EAM thread.
   */
  def timedOutExecutors(): Seq[(String, Int)] = {
    val now = clock.nanoTime()
    if (now >= nextTimeout.get()) {
      // Temporarily set the next timeout at Long.MaxValue. This ensures that after
      // scanning all executors below, we know when the next timeout for non-timed out
      // executors is (whether that update came from the scan, or from a new event
      // arriving in a different thread).
      nextTimeout.set(Long.MaxValue)

      var newNextTimeout = Long.MaxValue
      timedOutExecs = executors.asScala
        .filter { case (_, exec) =>
          !exec.pendingRemoval && !exec.hasActiveShuffle && !exec.decommissioning}
        .filter { case (_, exec) =>
          val deadline = exec.timeoutAt
          if (deadline > now) {
            newNextTimeout = math.min(newNextTimeout, deadline)
            exec.timedOut = false
            false
          } else {
            exec.timedOut = true
            true
          }
        }
        .map { case (name, exec) => (name, exec.resourceProfileId)}
        .toSeq
      updateNextTimeout(newNextTimeout)
    }
    timedOutExecs
  }

  /**
   * Mark the given executors as pending to be removed. Should only be called in the EAM thread.
   * This covers both kills and decommissions.
   */
  def executorsKilled(ids: Seq[String]): Unit = {
    ids.foreach { id =>
      val tracker = executors.get(id)
      if (tracker != null) {
        tracker.pendingRemoval = true
      }
    }

    // Recompute timed out executors in the next EAM callback, since this call invalidates
    // the current list.
    nextTimeout.set(Long.MinValue)
  }

  private[spark] def executorsDecommissioned(ids: Seq[String]): Unit = {
    ids.foreach { id =>
      val tracker = executors.get(id)
      if (tracker != null) {
        tracker.decommissioning = true
      }
    }

    // Recompute timed out executors in the next EAM callback, since this call invalidates
    // the current list.
    nextTimeout.set(Long.MinValue)
  }

  def executorCount: Int = executors.size()

  def executorCountWithResourceProfile(id: Int): Int = {
    execResourceProfileCount.getOrDefault(id, 0)
  }

  // for testing
  def getResourceProfileId(executorId: String): Int = {
    val execTrackingInfo = executors.get(executorId)
    if (execTrackingInfo != null) {
      execTrackingInfo.resourceProfileId
    } else {
      UNKNOWN_RESOURCE_PROFILE_ID
    }
  }

  def pendingRemovalCount: Int = executors.asScala.count { case (_, exec) => exec.pendingRemoval }

  def pendingRemovalCountPerResourceProfileId(id: Int): Int = {
    executors.asScala.count { case (k, v) => v.resourceProfileId == id && v.pendingRemoval }
  }

  def decommissioningCount: Int = executors.asScala.count { case (_, exec) =>
    exec.decommissioning
  }

  def decommissioningPerResourceProfileId(id: Int): Int = {
    executors.asScala.count { case (k, v) =>
      v.resourceProfileId == id && v.decommissioning
    }
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    if (!shuffleTrackingEnabled) {
      return
    }

    val shuffleStages = event.stageInfos.flatMap { s =>
      s.shuffleDepId.toSeq.map { shuffleId =>
        s.stageId -> shuffleId
      }
    }

    var updateExecutors = false
    shuffleStages.foreach { case (stageId, shuffle) =>
      val jobIDs = shuffleToActiveJobs.get(shuffle) match {
        case Some(jobs) =>
          // If a shuffle is being re-used, we need to re-scan the executors and update their
          // tracker with the information that the shuffle data they're storing is in use.
          logDebug(s"Reusing shuffle $shuffle in job ${event.jobId}.")
          updateExecutors = true
          jobs

        case _ =>
          logDebug(s"Registered new shuffle $shuffle (from stage $stageId).")
          val jobs = new mutable.ArrayBuffer[Int]()
          shuffleToActiveJobs(shuffle) = jobs
          jobs
      }
      jobIDs += event.jobId
    }

    if (updateExecutors) {
      val activeShuffleIds = shuffleStages.map(_._2).toSeq
      var needTimeoutUpdate = false
      val activatedExecs = new ExecutorIdCollector()
      executors.asScala.foreach { case (id, exec) =>
        if (!exec.hasActiveShuffle) {
          exec.updateActiveShuffles(activeShuffleIds)
          if (exec.hasActiveShuffle) {
            needTimeoutUpdate = true
            activatedExecs.add(id)
          }
        }
      }

      if (activatedExecs.nonEmpty) {
        logDebug(s"Activated executors $activatedExecs due to shuffle data needed by new job" +
          s"${event.jobId}.")
      }

      if (needTimeoutUpdate) {
        nextTimeout.set(Long.MinValue)
      }
    }

    stageToShuffleID ++= shuffleStages
    jobToStageIDs(event.jobId) = shuffleStages.map(_._1).toSeq
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    if (!shuffleTrackingEnabled) {
      return
    }

    var updateExecutors = false
    val activeShuffles = new mutable.ArrayBuffer[Int]()
    shuffleToActiveJobs.foreach { case (shuffleId, jobs) =>
      jobs -= event.jobId
      if (jobs.nonEmpty) {
        activeShuffles += shuffleId
      } else {
        // If a shuffle went idle we need to update all executors to make sure they're correctly
        // tracking active shuffles.
        updateExecutors = true
      }
    }

    if (updateExecutors) {
      if (log.isDebugEnabled()) {
        if (activeShuffles.nonEmpty) {
          logDebug(
            s"Job ${event.jobId} ended, shuffles ${activeShuffles.mkString(",")} still active.")
        } else {
          logDebug(s"Job ${event.jobId} ended, no active shuffles remain.")
        }
      }

      val deactivatedExecs = new ExecutorIdCollector()
      executors.asScala.foreach { case (id, exec) =>
        if (exec.hasActiveShuffle) {
          exec.updateActiveShuffles(activeShuffles)
          if (!exec.hasActiveShuffle) {
            deactivatedExecs.add(id)
          }
        }
      }

      if (deactivatedExecs.nonEmpty) {
        logDebug(s"Executors $deactivatedExecs do not have active shuffle data after job " +
          s"${event.jobId} finished.")
      }
    }

    jobToStageIDs.remove(event.jobId).foreach { stages =>
      stages.foreach { id => stageToShuffleID -= id }
    }
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    val executorId = event.taskInfo.executorId
    // Guard against a late arriving task start event (SPARK-26927).
    if (client.isExecutorActive(executorId)) {
      val exec = ensureExecutorIsTracked(executorId, UNKNOWN_RESOURCE_PROFILE_ID)
      exec.updateRunningTasks(1)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val executorId = event.taskInfo.executorId
    val exec = executors.get(executorId)
    if (exec != null) {
      // If the task succeeded and the stage generates shuffle data, record that this executor
      // holds data for the shuffle. This code will track all executors that generate shuffle
      // for the stage, even if speculative tasks generate duplicate shuffle data and end up
      // being ignored by the map output tracker.
      //
      // This means that an executor may be marked as having shuffle data, and thus prevented
      // from being removed, even though the data may not be used.
      // TODO: Only track used files (SPARK-31974)
      if (shuffleTrackingEnabled && event.reason == Success) {
        stageToShuffleID.get(event.stageId).foreach { shuffleId =>
          exec.addShuffle(shuffleId)
        }
      }

      // Update the number of running tasks after checking for shuffle data, so that the shuffle
      // information is up-to-date in case the executor is going idle.
      exec.updateRunningTasks(-1)
    }
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val exec = ensureExecutorIsTracked(event.executorId, event.executorInfo.resourceProfileId)
    exec.updateRunningTasks(0)
    logInfo(s"New executor ${event.executorId} has registered (new total is ${executors.size()})")
  }

  private def decrementExecResourceProfileCount(rpId: Int): Unit = {
    val count = execResourceProfileCount.getOrDefault(rpId, 0)
    execResourceProfileCount.replace(rpId, count, count - 1)
    execResourceProfileCount.remove(rpId, 0)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    val removed = executors.remove(event.executorId)
    if (removed != null) {
      decrementExecResourceProfileCount(removed.resourceProfileId)
      if (!removed.pendingRemoval || !removed.decommissioning) {
        nextTimeout.set(Long.MinValue)
      }
    }
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    val exec = ensureExecutorIsTracked(event.blockUpdatedInfo.blockManagerId.executorId,
      UNKNOWN_RESOURCE_PROFILE_ID)

    // Check if it is a shuffle file, or RDD to pick the correct codepath for update
    if (!event.blockUpdatedInfo.blockId.isInstanceOf[RDDBlockId]) {
      if (event.blockUpdatedInfo.blockId.isInstanceOf[ShuffleDataBlockId] &&
        shuffleTrackingEnabled) {
        /**
         * The executor monitor keeps track of locations of cache and shuffle blocks and this can
         * be used to decide which executor(s) Spark should shutdown first. Since we move shuffle
         * blocks around now this wires it up so that it keeps track of it. We only do this for
         * data blocks as index and other blocks blocks do not necessarily mean the entire block
         * has been committed.
         */
        event.blockUpdatedInfo.blockId match {
          case ShuffleDataBlockId(shuffleId, _, _) => exec.addShuffle(shuffleId)
          case _ => // For now we only update on data blocks
        }
      }
      return
    }

    val storageLevel = event.blockUpdatedInfo.storageLevel
    val blockId = event.blockUpdatedInfo.blockId.asInstanceOf[RDDBlockId]

    // SPARK-27677. When a block can be fetched from the external shuffle service, the executor can
    // be removed without hurting the application too much, since the cached data is still
    // available. So don't count blocks that can be served by the external service.
    if (storageLevel.isValid && (!fetchFromShuffleSvcEnabled || !storageLevel.useDisk)) {
      val hadCachedBlocks = exec.cachedBlocks.nonEmpty
      val blocks = exec.cachedBlocks.getOrElseUpdate(blockId.rddId,
        new mutable.BitSet(blockId.splitIndex))
      blocks += blockId.splitIndex

      if (!hadCachedBlocks) {
        exec.updateTimeout()
      }
    } else {
      exec.cachedBlocks.get(blockId.rddId).foreach { blocks =>
        blocks -= blockId.splitIndex
        if (blocks.isEmpty) {
          exec.cachedBlocks -= blockId.rddId
          if (exec.cachedBlocks.isEmpty) {
            exec.updateTimeout()
          }
        }
      }
    }
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    executors.values().asScala.foreach { exec =>
      exec.cachedBlocks -= event.rddId
      if (exec.cachedBlocks.isEmpty) {
        exec.updateTimeout()
      }
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case ShuffleCleanedEvent(id) => cleanupShuffle(id)
    case _ =>
  }

  override def rddCleaned(rddId: Int): Unit = { }

  override def shuffleCleaned(shuffleId: Int): Unit = {
    // Only post the event if tracking is enabled
    if (shuffleTrackingEnabled) {
      // Because this is called in a completely separate thread, we post a custom event to the
      // listener bus so that the internal state is safely updated.
      listenerBus.post(ShuffleCleanedEvent(shuffleId))
    }
  }

  override def broadcastCleaned(broadcastId: Long): Unit = { }

  override def accumCleaned(accId: Long): Unit = { }

  override def checkpointCleaned(rddId: Long): Unit = { }

  // Visible for testing.
  private[dynalloc] def isExecutorIdle(id: String): Boolean = {
    Option(executors.get(id)).map(_.isIdle).getOrElse(throw new NoSuchElementException(id))
  }

  // Visible for testing
  private[dynalloc] def timedOutExecutors(when: Long): Seq[String] = {
    executors.asScala.flatMap { case (id, tracker) =>
      if (tracker.isIdle && tracker.timeoutAt <= when) Some(id) else None
    }.toSeq
  }

  // Visible for testing
  private[spark] def executorsPendingToRemove(): Set[String] = {
    executors.asScala.filter { case (_, exec) => exec.pendingRemoval }.keys.toSet
  }

  // Visible for testing
  private[spark] def executorsDecommissioning(): Set[String] = {
    executors.asScala.filter { case (_, exec) => exec.decommissioning }.keys.toSet
  }

  /**
   * This method should be used when updating executor state. It guards against a race condition in
   * which the `SparkListenerTaskStart` event is posted before the `SparkListenerBlockManagerAdded`
   * event, which is possible because these events are posted in different threads. (see SPARK-4951)
   */
  private def ensureExecutorIsTracked(id: String, resourceProfileId: Int): Tracker = {
    val numExecsWithRpId = execResourceProfileCount.computeIfAbsent(resourceProfileId, _ => 0)
    val execTracker = executors.computeIfAbsent(id, _ => {
        val newcount = numExecsWithRpId + 1
        execResourceProfileCount.put(resourceProfileId, newcount)
        logDebug(s"Executor added with ResourceProfile id: $resourceProfileId " +
          s"count is now $newcount")
        new Tracker(resourceProfileId)
      })
    // if we had added executor before without knowing the resource profile id, fix it up
    if (execTracker.resourceProfileId == UNKNOWN_RESOURCE_PROFILE_ID &&
        resourceProfileId != UNKNOWN_RESOURCE_PROFILE_ID) {
      logDebug(s"Executor: $id, resource profile id was unknown, setting " +
        s"it to $resourceProfileId")
      execTracker.resourceProfileId = resourceProfileId
      // fix up the counts for each resource profile id
      execResourceProfileCount.put(resourceProfileId, numExecsWithRpId + 1)
      decrementExecResourceProfileCount(UNKNOWN_RESOURCE_PROFILE_ID)
    }
    execTracker
  }

  private def updateNextTimeout(newValue: Long): Unit = {
    while (true) {
      val current = nextTimeout.get()
      if (newValue >= current || nextTimeout.compareAndSet(current, newValue)) {
        return
      }
    }
  }

  private def cleanupShuffle(id: Int): Unit = {
    logDebug(s"Cleaning up state related to shuffle $id.")
    shuffleToActiveJobs -= id
    executors.asScala.foreach { case (_, exec) =>
      exec.removeShuffle(id)
    }
  }

  private class Tracker(var resourceProfileId: Int) {
    @volatile var timeoutAt: Long = Long.MaxValue

    // Tracks whether this executor is thought to be timed out. It's used to detect when the list
    // of timed out executors needs to be updated due to the executor's state changing.
    @volatile var timedOut: Boolean = false

    var pendingRemoval: Boolean = false
    var decommissioning: Boolean = false
    var hasActiveShuffle: Boolean = false

    private var idleStart: Long = -1
    private var runningTasks: Int = 0

    // Maps RDD IDs to the partition IDs stored in the executor.
    // This should only be used in the event thread.
    val cachedBlocks = new mutable.HashMap[Int, mutable.BitSet]()

    // The set of shuffles for which shuffle data is held by the executor.
    // This should only be used in the event thread.
    private val shuffleIds = if (shuffleTrackingEnabled) new mutable.HashSet[Int]() else null

    def isIdle: Boolean = idleStart >= 0 && !hasActiveShuffle

    def updateRunningTasks(delta: Int): Unit = {
      runningTasks = math.max(0, runningTasks + delta)
      idleStart = if (runningTasks == 0) clock.nanoTime() else -1L
      updateTimeout()
    }

    def updateTimeout(): Unit = {
      val oldDeadline = timeoutAt
      val newDeadline = if (idleStart >= 0) {
        val timeout = if (cachedBlocks.nonEmpty || (shuffleIds != null && shuffleIds.nonEmpty)) {
          val _cacheTimeout = if (cachedBlocks.nonEmpty) storageTimeoutNs else Long.MaxValue
          val _shuffleTimeout = if (shuffleIds != null && shuffleIds.nonEmpty) {
            shuffleTimeoutNs
          } else {
            Long.MaxValue
          }
          math.min(_cacheTimeout, _shuffleTimeout)
        } else {
          idleTimeoutNs
        }
        val deadline = idleStart + timeout
        if (deadline >= 0) deadline else Long.MaxValue
      } else {
        Long.MaxValue
      }

      timeoutAt = newDeadline

      // If the executor was thought to be timed out, but the new deadline is later than the
      // old one, ask the EAM thread to update the list of timed out executors.
      if (newDeadline > oldDeadline && timedOut) {
        nextTimeout.set(Long.MinValue)
      } else {
        updateNextTimeout(newDeadline)
      }
    }

    def addShuffle(id: Int): Unit = {
      if (shuffleIds.add(id)) {
        hasActiveShuffle = true
      }
    }

    def removeShuffle(id: Int): Unit = {
      if (shuffleIds.remove(id) && shuffleIds.isEmpty) {
        hasActiveShuffle = false
        if (isIdle) {
          updateTimeout()
        }
      }
    }

    def updateActiveShuffles(ids: Iterable[Int]): Unit = {
      val hadActiveShuffle = hasActiveShuffle
      hasActiveShuffle = ids.exists(shuffleIds.contains)
      if (hadActiveShuffle && isIdle) {
        updateTimeout()
      }
    }
  }

  private case class ShuffleCleanedEvent(id: Int) extends SparkListenerEvent {
    override protected[spark] def logEvent: Boolean = false
  }

  /** Used to collect executor IDs for debug messages (and avoid too long messages). */
  private class ExecutorIdCollector {
    private val ids = if (log.isDebugEnabled) new mutable.ArrayBuffer[String]() else null
    private var excess = 0

    def add(id: String): Unit = if (log.isDebugEnabled) {
      if (ids.size < 10) {
        ids += id
      } else {
        excess += 1
      }
    }

    def nonEmpty: Boolean = ids != null && ids.nonEmpty

    override def toString(): String = {
      ids.mkString(",") + (if (excess > 0) s" (and $excess more)" else "")
    }
  }
}
