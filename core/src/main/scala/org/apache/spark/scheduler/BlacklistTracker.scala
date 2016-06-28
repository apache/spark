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

import scala.collection.Set
import scala.collection.mutable.{HashMap, HashSet}

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
    conf: SparkConf,
    clock: Clock = new SystemClock()) extends BlacklistCache with Logging {


  private val MAX_FAILURES_PER_EXEC =
    conf.getInt("spark.blacklist.maxFailedTasksPerExecutor", 2)
  private val MAX_FAILURES_PER_EXEC_STAGE =
    conf.getInt("spark.blacklist.maxFailedTasksPerExecutorStage", 2)
  private val MAX_FAILED_EXEC_PER_NODE =
    conf.getInt("spark.blacklist.maxFailedExecutorsPerNode", 2)
  private val MAX_FAILED_EXEC_PER_NODE_STAGE =
    conf.getInt("spark.blacklist.maxFailedExecutorsPerNodeStage", 2)
  private[scheduler] val EXECUTOR_RECOVERY_MILLIS = conf.getTimeAsMs(
    "spark.scheduler.blacklist.recoverPeriod", (60 * 60 * 1000).toString)

  // a count of failed tasks for each executor.  Only counts failures after tasksets complete
  // successfully
  private val executorIdToFailureCount: HashMap[String, Int] = HashMap()
  // failures for each executor by stage.  Only tracked while the stage is running.
  private val stageIdToExecToFailures: HashMap[Int, HashMap[String, FailureStatus]] = new HashMap()
  private val stageIdToBlacklistedNodes: HashMap[Int, HashSet[String]] = new HashMap()
  private val executorIdToBlacklistTime: HashMap[String, Long] = new HashMap()
  private val nodeIdToBlacklistTime: HashMap[String, Long] = new HashMap()

  // A daemon thread to expire blacklist executor periodically
  private val expireBlacklistTimer = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "spark-scheduler-blacklist-expire-timer")

  private val recoverPeriod = conf.getTimeAsSeconds(
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
//    logInfo(s"Executor Blacklist callcount =" +
//      s" ${strategy.asInstanceOf[SingleTaskStrategy].executorBlacklistCallCount}")
//    strategy match {
//      case as: AdvancedSingleTaskStrategy =>
//        logInfo(s"Node Blacklist callcount =" +
//          s" ${as.nodeBlacklistCallCount}")
//      case _ => // no op
//    }
  }

  // The actual implementation is delegated to strategy
  private[scheduler] def expireExecutorsInBlackList(): Unit = synchronized {
    // TODO do we need to re-introduce strategy here?
    val maxTime = clock.getTimeMillis() - EXECUTOR_RECOVERY_MILLIS
    val execsToClear = executorIdToBlacklistTime.filter(_._2 < maxTime).keys
    if (execsToClear.nonEmpty) {
      logInfo(s"Removing executors $execsToClear from blacklist during periodic recovery")
      execsToClear.foreach { exec => executorIdToBlacklistTime.remove(exec) }
    }
    val nodesToClear = nodeIdToBlacklistTime.filter(_._2 < maxTime).keys
    if (nodesToClear.nonEmpty) {
      logInfo(s"Removing nodes $nodesToClear from blacklist during periodic recovery")
      nodesToClear.foreach { node => nodeIdToBlacklistTime.remove(node) }
    }


  }

  def taskSetSucceeded(stageId: Int, scheduler: TaskSchedulerImpl): Unit = synchronized {
    // if any tasks failed, we count them towards the overall failure count for the executor at
    // this point.  Also clean out all data abotu the stage to avoid memory leak.
    stageIdToExecToFailures.remove(stageId).map { failuresForStage =>
      failuresForStage.foreach { case (exec, newFailures) =>
        val prevFailures = executorIdToFailureCount.getOrElse(exec, 0)
        val newTotal = prevFailures + newFailures.totalFailures
        logInfo(s"exec $exec now has $newTotal failures after adding" +
          s" ${newFailures.totalFailures} from stage $stageId")

        if (newTotal >= MAX_FAILURES_PER_EXEC) {
          logInfo(s"Blacklisting executor $exec because it had $newTotal" +
            s" task failures in successful task sets")
          val now = clock.getTimeMillis()
          executorIdToBlacklistTime.put(exec, now)
          executorIdToFailureCount.remove(exec)

          val node = scheduler.getHostForExecutor(exec)
          val execs = scheduler.getExecutorsAliveOnHost(node).getOrElse(Set())
          val blacklistedExecs = execs.filter(executorIdToBlacklistTime.contains(_))
          logInfo(s"On node $node, blacklisted $blacklistedExecs")
          if (blacklistedExecs.size >= MAX_FAILED_EXEC_PER_NODE) {
            logInfo(s"Blacklisting node $node because it has ${blacklistedExecs.size} executors " +
              s"blacklisted: ${blacklistedExecs}")
            nodeIdToBlacklistTime.put(node, now)
          }
        } else {
          executorIdToFailureCount.put(exec, newTotal)
        }
      }
    }
  }

  def taskSetFailed(stageId: Int): Unit = {
    // TODO
  }

  /**
   * Return true iff this executor is EITHER (a) completely blacklisted or (b) blacklisted
   * for the given stage.
   */
  def isExecutorBlacklistedForStage(stageId: Int, executorId: String): Boolean = synchronized {
    // TODO any point in caching anything here?  do we need to avoid the lock?
    // TODO should TaskSchedulerImpl just filter out completely blacklisted executors earlier?
    val stageExecFailures = stageIdToExecToFailures.getOrElse(stageId, new HashMap())
      .getOrElse(executorId, new FailureStatus)
    stageExecFailures.totalFailures >= MAX_FAILURES_PER_EXEC_STAGE ||
      executorIdToBlacklistTime.contains(executorId)
  }

  def executorBlacklist(): Set[String] = synchronized {
    executorIdToBlacklistTime.keySet
  }

  def nodeBlacklistForStage(stageId: Int): Set[String] = synchronized {
    stageIdToBlacklistedNodes.getOrElse(stageId, Set())
  }

  def nodeBlacklist(): Set[String] = synchronized {
    nodeIdToBlacklistTime.keySet
  }

  def taskSucceeded(
      stageId: Int,
      partition: Int,
      info: TaskInfo): Unit = synchronized {
    // no-op intentionally for now.  success to failure ratio is irrelevant, we just blacklist
    // based on failures.  Furthermore, one success does not override previous failures, since
    // the bad node / executor may not fail *every* time
  }

  def taskFailed(
      stageId: Int,
      partition: Int,
      info: TaskInfo): Unit = synchronized {
    val stageFailures = stageIdToExecToFailures.getOrElseUpdate(stageId, new HashMap())
    val failureStatus = stageFailures.getOrElseUpdate(info.executorId, new FailureStatus())
    failureStatus.totalFailures += 1
    failureStatus.failuresByPart += partition
    if (failureStatus.totalFailures >= MAX_FAILURES_PER_EXEC_STAGE) {
      // this executor has been pushed into the blacklist for this stage.  Lets check if it pushes
      // the whole node into the blacklist
      val blacklistedExecutors =
        stageFailures.filter{_._2.totalFailures >= MAX_FAILURES_PER_EXEC_STAGE}
      if (blacklistedExecutors.size >= MAX_FAILED_EXEC_PER_NODE_STAGE) {
        logInfo(s"Blacklisting ${info.host} for stage $stageId")
        stageIdToBlacklistedNodes.getOrElseUpdate(stageId, new HashSet()) += info.host
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
    // note that this is NOT only called from the dag scheduler event loop
    stageIdToExecToFailures.get(stageId) match {
      case Some(stageFailures) =>
        stageFailures.get(executorId) match {
          case Some(failures) =>
            failures.failuresByPart.contains(partition)
          case None =>
            false
        }
      case None =>
        // intentionally avoiding .getOrElse(..., new HashMap()) to avoid lots of object
        // creation on this very commonly called method
        false
    }

  }

  def removeExecutor(executorId: String): Unit = {
    executorIdToBlacklistTime -= executorId
    executorIdToFailureCount -= executorId
    stageIdToExecToFailures.values.foreach { execFailureOneStage =>
      execFailureOneStage -= executorId
    }
  }
}

/**
 * Hide cache details in this trait to make code clean and avoid operation mistake
 */
private[scheduler] trait BlacklistCache extends Logging {
  // local cache to minimize the the work when query blacklisted executor and node
  private val blacklistExecutorCache = HashMap.empty[StageAndPartition, Set[String]]
  private val blacklistNodeCache = Set.empty[String]
  private val blacklistNodeForStageCache = HashMap.empty[Int, Set[String]]

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
    throw new NotImplementedError()
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
 * Failures for one executor, within one stage
 */
private[scheduler] final class FailureStatus {
  val failuresByPart = HashSet[Int]()
  var totalFailures = 0
}

private[scheduler] case class StageAndPartition(val stageId: Int, val partition: Int)


private[spark] class NoopBlacklistTracker(
    sparkConf: SparkConf) extends BlacklistTracker(sparkConf) {

  // TODO don't extend, just have a common interface
  override def start: Unit = {}
  override def stop: Unit = {}

  override def taskSetSucceeded(stageId: Int, scheduler: TaskSchedulerImpl): Unit = {}

  override def taskSetFailed(stageId: Int): Unit = {}

  override def isExecutorBlacklistedForStage(stageId: Int, executorId: String): Boolean = {
    false
  }

  override def executorBlacklist(): Set[String] = {
    Set()
  }

  override def nodeBlacklist(): Set[String] = {
    Set()
  }

  override def isExecutorBlacklisted(
      executorId: String,
      stageId: Int,
      partition: Int) : Boolean = {
    false
  }

  override def taskSucceeded(
    stageId: Int,
    partition: Int,
    info: TaskInfo): Unit = {}

  override def taskFailed(
    stageId: Int,
    partition: Int,
    info: TaskInfo): Unit = {}
}
