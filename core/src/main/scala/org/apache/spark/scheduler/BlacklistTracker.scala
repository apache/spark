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

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Clock
import org.apache.spark.util.SystemClock
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.Utils

/**
 * BlacklistTracker is designed to track problematic executors and nodes. It belongs to
 * SparkContext as an centralized and unified collection for all tasks with same SparkContext.
 * So that a new TaskSet could be benefit from previous experiences of other TaskSets.
 *
 * Once task finished, the callback method in TaskSetManager should update
 * executorIdToFailureStatus Map.
 */
private[spark] class BlacklistTracker(
    sparkConf: SparkConf,
    scheduler: TaskSchedulerImpl,
    clock: Clock = new SystemClock()) extends BlacklistCache with Logging {

  // maintain a ExecutorId --> FailureStatus HashMap
  private val executorIdToFailureStatus: mutable.HashMap[String, FailureStatus] = mutable.HashMap()
  // Apply Strategy pattern here to change different blacklist detection logic
  private val strategy = BlacklistStrategy(sparkConf)


  // A daemon thread to expire blacklist executor periodically
  private val expireBlacklistTimer = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "spark-scheduler-blacklist-expire-timer")

  private val recoverPeriod = sparkConf.getTimeAsSeconds(
    "spark.scheduler.blacklist.recoverPeriod", "60s")

  def start(): Unit = {
    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        Utils.logUncaughtExceptions(expireExecutorsInBlackList())
      }
    }
    expireBlacklistTimer.scheduleAtFixedRate(scheduleTask, 0L, recoverPeriod, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
    expireBlacklistTimer.shutdown()
    expireBlacklistTimer.awaitTermination(10, TimeUnit.SECONDS)
    logInfo(s"Executor Blacklist callcount =" +
      s" ${strategy.asInstanceOf[SingleTaskStrategy].executorBlacklistCallCount}")
    strategy match {
      case as: AdvancedSingleTaskStrategy =>
        logInfo(s"Node Blacklist callcount =" +
          s" ${as.nodeBlacklistCallCount}")
      case _ => // no op
    }
  }

  // The actual implementation is delegated to strategy
  private[scheduler] def expireExecutorsInBlackList(): Unit = synchronized {
    val updated = strategy.expireExecutorsInBlackList(executorIdToFailureStatus, clock)
    logInfo(s"Checked for expired blacklist: ${updated}")
    if (updated) {
      invalidateCache()
    }
  }

  // The actual implementation is delegated to strategy
  def executorBlacklist(
      stageId: Int,
      partition: Int): Set[String] = synchronized {
    // note that this is NOT only called from the dag scheduler event loop
    val atomTask = StageAndPartition(stageId, partition)
    if (!isBlacklistExecutorCacheValid) {
      reEvaluateExecutorBlacklistAndUpdateCache(atomTask, clock)
    } else {
      getExecutorBlacklistFromCache(atomTask).getOrElse {
        // We lazily invalidate the cache data, so that lack of an entry doesn't mean the
        // executor is definitely not in the blacklist.  TODO rework the cache logic to avoid this
        reEvaluateExecutorBlacklistAndUpdateCache(atomTask, clock)
      }
    }
  }

  // The actual implementation is delegated to strategy
  def nodeBlacklist(): Set[String] = synchronized {
    if (isBlacklistNodeCacheValid) {
      getNodeBlacklistFromCache
    } else {
      reEvaluateNodeBlacklist()
    }
  }

  private def reEvaluateNodeBlacklist(): Set[String] = synchronized {
    val prevBlacklistedNodes = getNodeBlacklistFromCache
    val nodes = strategy.getNodeBlacklist(executorIdToFailureStatus, clock)
    if (nodes != prevBlacklistedNodes) {
      logInfo(s"updating node blacklist to $nodes")
    }
    setBlacklistNodeCache(nodes)
    nodes
  }

  // The actual implementation is delegated to strategy
  def nodeBlacklistForStage(stageId: Int): Set[String] = synchronized {
    // TODO here and elsewhere -- we invalidate the cache way too often.  In general, we should
    // be able to do an in-place update of the caches.  (a) this is slow and (b) it makes
    // it really hard to track when the blacklist actually changes (would be *really* nice to
    // log a msg about node level blacklisting at least)
    if (isBlacklistNodeForStageCacheValid) {
      getNodeBlacklistForStageFromCache(stageId).getOrElse(
        reEvaluateNodeBlacklistForStageAndUpdateCache(stageId))
    } else {
      reEvaluateNodeBlacklistForStageAndUpdateCache(stageId)
    }
  }

  def taskSucceeded(
      stageId: Int,
      partition: Int,
      info: TaskInfo): Unit = synchronized {
    // when an executor successfully completes any task, we remove it from the blacklist
    // for *all* tasks (?? TODO ??)
    val exec = info.executorId
    val node = scheduler.getHostForExecutor(exec)
    removeFailedExecutorsForTaskId(exec, stageId, partition)
    // TODO executor level logic as well
    // TODO synchronization?
    if (getNodeBlacklistForStageFromCache(stageId).map(_.contains(node)).getOrElse(false)) {
      reEvaluateNodeBlacklistForStageAndUpdateCache(stageId)
    }
    if (getNodeBlacklistFromCache.contains(node)) {
      logInfo(s"reevaluating node blacklist for $node after partition $partition in Stage " +
        s"$stageId succeeded")
      reEvaluateNodeBlacklist()
    }
  }


  def taskFailed(
      stageId: Int,
      partition: Int,
      info: TaskInfo): Unit = synchronized {
    // If the task failed, update latest failure time and failedTaskIds
    val atomTask = StageAndPartition(stageId, partition)
    val executorId = info.executorId
    executorIdToFailureStatus.get(executorId) match {
      case Some(failureStatus) =>
        failureStatus.updatedTime = clock.getTimeMillis()
        val failedTimes = failureStatus.numFailuresPerTask.getOrElse(atomTask, 0) + 1
        failureStatus.numFailuresPerTask(atomTask) = failedTimes
      case None =>
        val failedTasks = mutable.HashMap(atomTask -> 1)
        val failureStatus = new FailureStatus(
          clock.getTimeMillis(), info.host, failedTasks)
        executorIdToFailureStatus(executorId) = failureStatus
    }
    reEvaluateNodeBlacklist()
    reEvaluateNodeBlacklistForStageAndUpdateCache(stageId)
    reEvaluateExecutorBlacklistAndUpdateCache(atomTask, clock)
  }

  /** remove the executorId from executorIdToFailureStatus */
  def removeFailedExecutors(executorId: String) : Unit = synchronized {
    executorIdToFailureStatus.remove(executorId)
  }

  /**
   * Remove the failure record related to given taskId from executorIdToFailureStatus. If the
   * number of records of given executorId becomes 0, remove the completed executorId.
   */
  def removeFailedExecutorsForTaskId(
      executorId: String,
      stageId: Int,
      partition: Int) : Unit = synchronized {
    val atomTask = StageAndPartition(stageId, partition)
    executorIdToFailureStatus.get(executorId).map{ fs =>
      fs.numFailuresPerTask.remove(atomTask).foreach { _ =>
        // this executor had previously failed for this specific task -- we need to update our
        // cache so we know its OK now
        logInfo(s"Executor $executorId is no longer blacklisted for partition $partition in " +
          s"Stage $stageId")
        reEvaluateExecutorBlacklistAndUpdateCache(atomTask, clock)
      }
      if (fs.numFailuresPerTask.isEmpty) {
        executorIdToFailureStatus.remove(executorId)
      }
    }
  }

  /**
   * Return true if this executor is blacklisted for the given task.  Note that this does *not*
   * need to return true if the executor is blacklisted for the entire stage, or blacklisted
   * altogether.  That is handled by other methods.  TODO reference those methods
   */
  def isExecutorBlacklisted(
      executorId: String,
      stageId: Int,
      partition: Int) : Boolean = {
    executorBlacklist(stageId, partition).contains(executorId)
  }

  // If the node is in blacklist, all executors allocated on that node will
  // also be put into  executor blacklist.
  private def executorsOnBlacklistedNode(atomTask: StageAndPartition): Set[String] = {
    nodeBlacklistForStage(atomTask.stageId).flatMap(scheduler.getExecutorsAliveOnHost(_)
      .getOrElse(Set.empty[String]))
  }

  private def reEvaluateExecutorBlacklistAndUpdateCache(
      atomTask: StageAndPartition,
      clock: Clock): Set[String] = {
    // This is so fine grained, its a little too verbose to do any logging here
    val executors = strategy.getExecutorBlacklist(executorIdToFailureStatus, atomTask, clock)
    updateBlacklistExecutorCache(atomTask, executors)
    executors
  }

  private def reEvaluateNodeBlacklistForStageAndUpdateCache(stageId: Int): Set[String] = {
    val prevBlacklist = getNodeBlacklistForStageFromCache(stageId)
    val nodes = strategy.getNodeBlacklistForStage(executorIdToFailureStatus, stageId, clock)
    if (nodes != prevBlacklist.getOrElse(Set.empty[String])) {
      logInfo(s"Blacklist for Stage $stageId updated to $nodes")
    }
    updateBlacklistNodeForStageCache(stageId, nodes)
    nodes
  }
}

/**
 * Hide cache details in this trait to make code clean and avoid operation mistake
 */
private[scheduler] trait BlacklistCache extends Logging {
  // local cache to minimize the the work when query blacklisted executor and node
  private val blacklistExecutorCache = mutable.HashMap.empty[StageAndPartition, Set[String]]
  private val blacklistNodeCache = mutable.Set.empty[String]
  private val blacklistNodeForStageCache = mutable.HashMap.empty[Int, Set[String]]

  // The flag to mark if cache is valid, it will be set to false when executorIdToFailureStatus be
  // updated and it will be set to true, when called executorBlacklist and nodeBlacklist.
  private var _isBlacklistExecutorCacheValid : Boolean = false
  private var _isBlacklistNodeCacheValid: Boolean = false
  private var _isBlacklistNodeForStageCacheValid: Boolean = false

  private val cacheLock = new Object()

  protected def isBlacklistExecutorCacheValid : Boolean = _isBlacklistExecutorCacheValid
  protected def isBlacklistNodeCacheValid: Boolean = _isBlacklistNodeCacheValid
  protected def isBlacklistNodeForStageCacheValid: Boolean = _isBlacklistNodeForStageCacheValid

  protected def updateBlacklistExecutorCache(
      atomTask: StageAndPartition,
      blacklistExecutor: Set[String]): Unit = cacheLock.synchronized {
    if (!_isBlacklistExecutorCacheValid) {
      blacklistExecutorCache.clear()
    }
    blacklistExecutorCache.update(atomTask, blacklistExecutor)
    _isBlacklistExecutorCacheValid = true
  }

  protected def setBlacklistNodeCache(
      blacklistNode: Traversable[String]): Unit = cacheLock.synchronized {
    blacklistNodeCache.clear()
    blacklistNodeCache ++= blacklistNode
    // TODO this flag shouldn't be necessary at all
    _isBlacklistNodeCacheValid = true
  }

  protected def updateBlacklistNodeForStageCache(
      stageId: Int,
      blacklistNode: Set[String]): Unit = cacheLock.synchronized {
    // TODO this needs to actually get called, and add unit test
    val wasBlacklisted = blacklistNodeForStageCache.getOrElse(stageId, Set.empty[String])
    if (wasBlacklisted != blacklistNode) {
      logInfo(s"Updating node blacklist for Stage ${stageId} to ${blacklistNode}")
    }
    if (!_isBlacklistNodeForStageCacheValid) {
      blacklistNodeForStageCache.clear()
    }
    blacklistNodeForStageCache.update(stageId, blacklistNode)
    _isBlacklistNodeForStageCacheValid = true
  }

  protected def invalidateCache(): Unit = cacheLock.synchronized {
    _isBlacklistExecutorCacheValid = false
    _isBlacklistNodeCacheValid = false
    _isBlacklistNodeForStageCacheValid = false
  }

  protected def getExecutorBlacklistFromCache(
      atomTask: StageAndPartition): Option[Set[String]] = {
    blacklistExecutorCache.get(atomTask)
  }

  protected def getNodeBlacklistFromCache: Set[String] = blacklistNodeCache.toSet

  protected def getNodeBlacklistForStageFromCache(stageId: Int): Option[Set[String]] =
    blacklistNodeForStageCache.get(stageId)
}

/**
 * A class to record details of failure.
 *
 * @param initialTime the time when failure status be created
 * @param host the node name which running executor on
 * @param numFailuresPerTask all tasks failed on the executor (key is StageAndPartition, value
 *        is the number of failures of this task)
 */
private[scheduler] final class FailureStatus(
    initialTime: Long,
    val host: String,
    val numFailuresPerTask: mutable.HashMap[StageAndPartition, Int]) {

  var updatedTime = initialTime
  def totalNumFailures : Int = numFailuresPerTask.values.sum
}

private[scheduler] case class StageAndPartition(val stageId: Int, val partition: Int)
