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
 * Abstract interface for tracking all info related to executor and node blacklist.  Though we only
 * have one real implmentation currently, [[BlacklistTrackerImpl]], usage becomes much simpler
 * by adding [[NoopBlacklistTracker]] than by making all uses switch to Option[BlacklistTracker].
 * See [[BlacklistTrackerImpl]] for all meaningful documentation.
 */
private[spark] trait BlacklistTracker {
  def start(): Unit

  def stop(): Unit

  def taskSetSucceeded(stageId: Int, scheduler: TaskSchedulerImpl): Unit

  def taskSetFailed(stageId: Int): Unit

  def isExecutorBlacklistedForStage(stageId: Int, executorId: String): Boolean

  /**
   * expensive, but thread-safe, when you need the full blacklist
   */
  def nodeBlacklist(): Set[String]

  def isNodeBlacklisted(node: String): Boolean

  def isExecutorBlacklisted(executorId: String): Boolean

  def nodeBlacklistForStage(stageId: Int): Set[String]

  def isExecutorBlacklisted(
    executorId: String,
    stageId: Int,
    partition: Int) : Boolean

  def taskSucceeded(
    stageId: Int,
    partition: Int,
    info: TaskInfo): Unit

  def taskFailed(
    stageId: Int,
    partition: Int,
    info: TaskInfo): Unit

  def removeExecutor(executorId: String): Unit
}

/**
 * BlacklistTracker is designed to track problematic executors and nodes.  It supports blacklisting
 * specific (executor, task) pairs within a stage, blacklisting entire executors and nodes for a
 * stage, and blacklisting executors and nodes across an entire application (with a periodic
 * expiry).
 *
 * The tracker needs to deal with a variety of workloads, eg.: bad user code, which may lead to many
 * task failures, but that should not count against individual executors; many small stages, which
 * may prevent a bad executor for having many failures within one stage, but still many failures
 * over the entire application; "flaky" executors, that don't fail every task, but are still
 * faulty; etc.
 *
 * THREADING: All public methods in this class are thread-safe, simply by being synchronized on
 * this.  Most access will occur from within TaskSchedulerImpl, and will already have a lock on
 * the taskScheduler.  However, it will also be accessed by the YarnSchedulerBackend, to determine
 * the set of currently blacklisted nodes, and by an internal thread to periodically remove
 * nodes and executors from the blacklist.  This also means we can't expose any of the internal
 * datastructures at all (eg., no exposing someInternalMap.keySet).
 */
private[spark] class BlacklistTrackerImpl(
    conf: SparkConf,
    clock: Clock = new SystemClock()) extends BlacklistTracker with Logging {


  private val MAX_FAILURES_PER_EXEC =
    conf.getInt("spark.blacklist.maxFailedTasksPerExecutor", 2)
  private val MAX_FAILURES_PER_EXEC_STAGE =
    conf.getInt("spark.blacklist.maxFailedTasksPerExecutorStage", 2)
  private val MAX_FAILED_EXEC_PER_NODE =
    conf.getInt("spark.blacklist.maxFailedExecutorsPerNode", 2)
  private val MAX_FAILED_EXEC_PER_NODE_STAGE =
    conf.getInt("spark.blacklist.maxFailedExecutorsPerNodeStage", 2)
  private[scheduler] val EXECUTOR_RECOVERY_MILLIS = BlacklistTracker.getBlacklistExpiryTime(conf)

  // a count of failed tasks for each executor.  Only counts failures after tasksets complete
  // successfully
  private val executorIdToFailureCount: HashMap[String, Int] = HashMap()
  // failures for each executor by stage.  Only tracked while the stage is running.
  private[scheduler] val stageIdToExecToFailures: HashMap[Int, HashMap[String, FailureStatus]] =
    new HashMap()
  private[scheduler] val stageIdToBlacklistedNodes: HashMap[Int, HashSet[String]] = new HashMap()
  private val executorIdToBlacklistTime: HashMap[String, Long] = new HashMap()
  private val nodeIdToBlacklistTime: HashMap[String, Long] = new HashMap()

  // A daemon thread to expire blacklist executor periodically
  private val expireBlacklistTimer = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "spark-scheduler-blacklist-expire-timer")

  private val recoverPeriod = conf.getTimeAsSeconds(
    "spark.scheduler.blacklist.recoverPeriod", "60s")

  override def start(): Unit = {
    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        Utils.logUncaughtExceptions(expireExecutorsInBlackList())
      }
    }
    expireBlacklistTimer.scheduleAtFixedRate(scheduleTask, 0L, recoverPeriod, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    expireBlacklistTimer.shutdown()
    expireBlacklistTimer.awaitTermination(10, TimeUnit.SECONDS)
  }

  private[scheduler] def expireExecutorsInBlackList(): Unit = synchronized {
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

  override def taskSetSucceeded(stageId: Int, scheduler: TaskSchedulerImpl): Unit = synchronized {
    // if any tasks failed, we count them towards the overall failure count for the executor at
    // this point.  Also clean out all data about the stage to avoid increasing memory use.
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
    // when we blacklist a node within a stage, we don't directly promote that node to being
    // blacklisted for the app.  Instead, we use the mechanism above to decide whether or not to
    // blacklist any executors for the app, and when doing so we'll check whether or not to also
    // blacklist the node.
    stageIdToBlacklistedNodes.remove(stageId)
  }

  override def taskSetFailed(stageId: Int): Unit = synchronized {
    // just throw away all the info for the failures in this taskSet -- assume the executors were
    // fine, the failures were just b/c the taskSet itself was bad (eg., bad user code)
    stageIdToExecToFailures.remove(stageId)
    stageIdToBlacklistedNodes.remove(stageId)
  }

  /**
   * Return true iff this executor is EITHER (a) completely blacklisted or (b) blacklisted
   * for the given stage.
   */
  override def isExecutorBlacklistedForStage(
      stageId: Int,
      executorId: String): Boolean = synchronized {
    // TODO any point in caching anything here?  do we need to avoid the lock?
    // TODO should TaskSchedulerImpl just filter out completely blacklisted executors earlier?
    val stageExecFailures = stageIdToExecToFailures.getOrElse(stageId, new HashMap())
      .getOrElse(executorId, new FailureStatus)
    stageExecFailures.totalFailures >= MAX_FAILURES_PER_EXEC_STAGE ||
      executorIdToBlacklistTime.contains(executorId)
  }

  override def isExecutorBlacklisted(executorId: String): Boolean = synchronized {
    executorIdToBlacklistTime.contains(executorId)
  }

  override def nodeBlacklistForStage(stageId: Int): Set[String] = synchronized {
    stageIdToBlacklistedNodes.getOrElse(stageId, Set())
  }

  override def nodeBlacklist(): Set[String] = synchronized {
    // copy so that its safe to use from outside
    Set() ++ nodeIdToBlacklistTime.keySet
  }

  override def isNodeBlacklisted(node: String): Boolean = synchronized {
    nodeIdToBlacklistTime.contains(node)
  }

  override def taskSucceeded(
      stageId: Int,
      partition: Int,
      info: TaskInfo): Unit = synchronized {
    // no-op intentionally, included just for symmetry.  success to failure ratio is irrelevant, we
    // just blacklist based on failures.  Furthermore, one success does not override previous
    // failures, since the bad node / executor may not fail *every* time
  }

  override def taskFailed(
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
   * Return true if this executor is blacklisted for the given task.  This does *not*
   * need to return true if the executor is blacklisted for the entire stage, or blacklisted
   * altogether.
   *
   * This method is called by multiple threads, though always with a lock on TaskSchedulerImpl.
   */
  override def isExecutorBlacklisted(
      executorId: String,
      stageId: Int,
      partition: Int) : Boolean = synchronized {
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
        // creation, since this method gets called a *lot*
        false
    }
  }

  override def removeExecutor(executorId: String): Unit = synchronized {
    executorIdToBlacklistTime -= executorId
    executorIdToFailureCount -= executorId
    stageIdToExecToFailures.values.foreach { execFailureOneStage =>
      execFailureOneStage -= executorId
    }
  }
}


private[spark] object BlacklistTracker extends Logging {
  val LEGACY_TIMEOUT_CONF = "spark.scheduler.executorTaskBlacklistTime"
  val EXPIRY_TIMEOUT_CONF = "spark.scheduler.blacklist.recoverPeriod"
  val ENABLED_CONF = "spark.scheduler.blacklist.enabled"

  /**
   * Return true if the blacklist is enabled, based on the following order of preferences:
   * 1. Is it specifically enabled or disabled?
   * 2. Is it enabled via the legacy timeout conf?
   * 3. Use the default for the spark-master:
   *   - off for local mode
   *   - on for distributed modes (including local-cluster)
   */
  def isBlacklistEnabled(conf: SparkConf): Boolean = {
    val isEnabled = conf.get(ENABLED_CONF, null)
    if (isEnabled == null) {
      // if they've got a non-zero setting for the legacy conf, always enable the blacklist,
      // otherwise, use the default based on the cluster-mode (off for local-mode, on otherwise).
      val legacyTimeout = conf.getLong(LEGACY_TIMEOUT_CONF, 0L)
      if (legacyTimeout > 0) {
        // mostly this is necessary just for tests, since real users that want the blacklist will
        // get it anyway by default
        logWarning(s"Turning on blacklisting due to legacy configuration: $LEGACY_TIMEOUT_CONF > 0")
        true
      } else {
        // local-cluster is *not* considered local for these purposes, we still want the blacklist
        // enabled by default
        Utils.isLocalMaster(conf)
      }
    } else {
      // always take whatever value is explicitly set by the user
      isEnabled.toBoolean
    }
  }

  def getBlacklistExpiryTime(conf: SparkConf): Long = {
    conf.getTimeAsMs(BlacklistTracker.EXPIRY_TIMEOUT_CONF,
      conf.get(BlacklistTracker.LEGACY_TIMEOUT_CONF, (60 * 60 * 1000).toString))
  }
}

/**
 * Failures for one executor, within one stage
 */
private[scheduler] final class FailureStatus {
  val failuresByPart = HashSet[Int]()
  var totalFailures = 0

  override def toString(): String = {
    s"totalFailures = $totalFailures; partitionsFailed = $failuresByPart"
  }
}

private[scheduler] case class StageAndPartition(val stageId: Int, val partition: Int)


private[spark] object NoopBlacklistTracker extends BlacklistTracker {

  override def start: Unit = {}

  override def stop: Unit = {}

  override def taskSetSucceeded(stageId: Int, scheduler: TaskSchedulerImpl): Unit = {}

  override def taskSetFailed(stageId: Int): Unit = {}

  override def isExecutorBlacklistedForStage(stageId: Int, executorId: String): Boolean = {
    false
  }

  override def isExecutorBlacklisted(executorId: String): Boolean = {
    false
  }

  override def nodeBlacklist(): Set[String] = {
    Set()
  }

  override def isNodeBlacklisted(node: String): Boolean = {
    false
  }

  override def nodeBlacklistForStage(stageId: Int): Set[String] = {
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

  override def removeExecutor(executorId: String): Unit = {}
}
