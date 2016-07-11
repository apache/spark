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

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.util.Clock
import org.apache.spark.util.SystemClock
import org.apache.spark.util.Utils

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
 * THREADING: As with most helpers of TaskSchedulerImpl, this is not thread-safe.  Though it is
  * called by multiple threads, callers must already have a lock on the TaskSchedulerImpl.  The
  * one exception is [[nodeBlacklist()]], which can be called without holding a lock.
 */
private[scheduler] class BlacklistTracker (
    conf: SparkConf,
    clock: Clock = new SystemClock()) extends Logging {

  private val MAX_TASK_FAILURES_PER_NODE = conf.get(config.MAX_TASK_FAILURES_PER_NODE)
  private val MAX_FAILURES_PER_EXEC = conf.get(config.MAX_FAILURES_PER_EXEC)
  private val MAX_FAILURES_PER_EXEC_STAGE = conf.get(config.MAX_FAILURES_PER_EXEC_STAGE)
  private val MAX_FAILED_EXEC_PER_NODE = conf.get(config.MAX_FAILED_EXEC_PER_NODE)
  private val MAX_FAILED_EXEC_PER_NODE_STAGE = conf.get(config.MAX_FAILED_EXEC_PER_NODE_STAGE)
  val EXECUTOR_RECOVERY_MILLIS = BlacklistTracker.getBlacklistExpiryTime(conf)

  // a count of failed tasks for each executor.  Only counts failures after tasksets complete
  // successfully
  private val executorIdToFailureCount: HashMap[String, Int] = HashMap()
  // failures for each executor by stage.  Only tracked while the stage is running.
  val stageIdToExecToFailures: HashMap[Int, HashMap[String, FailureStatus]] =
    new HashMap()
  val stageIdToNodeBlacklistedTasks: HashMap[Int, HashMap[String, HashSet[Int]]] =
    new HashMap()
  val stageIdToBlacklistedNodes: HashMap[Int, HashSet[String]] = new HashMap()
  private val executorIdToBlacklistExpiryTime: HashMap[String, Long] = new HashMap()
  private val nodeIdToBlacklistExpiryTime: HashMap[String, Long] = new HashMap()
  private val _nodeBlacklist: AtomicReference[Set[String]] = new AtomicReference(Set())
  private var nextExpiryTime: Long = Long.MaxValue

  def start(): Unit = {}

  def stop(): Unit = {}

  def expireExecutorsInBlacklist(): Unit = {
    val now = clock.getTimeMillis()
    // quickly check if we've got anything to expire from blacklist -- if not, avoid doing any work
    if (now > nextExpiryTime) {
      val execsToClear = executorIdToBlacklistExpiryTime.filter(_._2 < now).keys
      if (execsToClear.nonEmpty) {
        logInfo(s"Removing executors $execsToClear from blacklist during periodic recovery")
        execsToClear.foreach { exec => executorIdToBlacklistExpiryTime.remove(exec) }
      }
      if (executorIdToBlacklistExpiryTime.nonEmpty) {
        nextExpiryTime = executorIdToBlacklistExpiryTime.map{_._2}.min
      } else {
        nextExpiryTime = Long.MaxValue
      }
      val nodesToClear = nodeIdToBlacklistExpiryTime.filter(_._2 < now).keys
      if (nodesToClear.nonEmpty) {
        logInfo(s"Removing nodes $nodesToClear from blacklist during periodic recovery")
        nodesToClear.foreach { node => nodeIdToBlacklistExpiryTime.remove(node) }
        // make a copy of the blacklisted nodes so nodeBlacklist() is threadsafe
        _nodeBlacklist.set(nodeIdToBlacklistExpiryTime.keySet.toSet)
      }
    }
 }

  def taskSetSucceeded(stageId: Int, scheduler: TaskSchedulerImpl): Unit = {
    // if any tasks failed, we count them towards the overall failure count for the executor at
    // this point.  Also clean out all data about the stage to avoid increasing memory use.
    stageIdToExecToFailures.remove(stageId).map { failuresForStage =>
      failuresForStage.foreach { case (exec, newFailures) =>
        val prevFailures = executorIdToFailureCount.getOrElse(exec, 0)
        val newTotal = prevFailures + newFailures.totalFailures

        if (newTotal >= MAX_FAILURES_PER_EXEC) {
          logInfo(s"Blacklisting executor $exec because it has $newTotal" +
            s" task failures in successful task sets")
          val now = clock.getTimeMillis()
          val expiryTime = now + EXECUTOR_RECOVERY_MILLIS
          executorIdToBlacklistExpiryTime.put(exec, expiryTime)
          executorIdToFailureCount.remove(exec)
          if (expiryTime < nextExpiryTime) {
            nextExpiryTime = expiryTime
          }

          val node = scheduler.getHostForExecutor(exec)
          val execs = scheduler.getExecutorsAliveOnHost(node).getOrElse(Set())
          val blacklistedExecs = execs.filter(executorIdToBlacklistExpiryTime.contains(_))
          if (blacklistedExecs.size >= MAX_FAILED_EXEC_PER_NODE) {
            logInfo(s"Blacklisting node $node because it has ${blacklistedExecs.size} executors " +
              s"blacklisted: ${blacklistedExecs}")
            nodeIdToBlacklistExpiryTime.put(node, expiryTime)
            // make a copy of the blacklisted nodes so nodeBlacklist() is threadsafe
            _nodeBlacklist.set(nodeIdToBlacklistExpiryTime.keySet.toSet)
          }
        } else {
          executorIdToFailureCount.put(exec, newTotal)
        }
      }
    }
    // when we blacklist a node within a stage, we don't directly promote that node to being
    // blacklisted for the app.  Instead, we use the mechanism above to decide whether or not to
    // blacklist any executors for the app, and when doing so we'll check whether or not to also
    // blacklist the node.  That is why we just remove this entry without doing any promotion to
    // the full app blacklist.
    stageIdToBlacklistedNodes.remove(stageId)
    stageIdToNodeBlacklistedTasks.remove(stageId)
  }

  def taskSetFailed(stageId: Int): Unit = {
    // just throw away all the info for the failures in this taskSet -- assume the executors were
    // fine, the failures were just b/c the taskSet itself was bad (eg., bad user code)
    stageIdToExecToFailures.remove(stageId)
    stageIdToBlacklistedNodes.remove(stageId)
    stageIdToNodeBlacklistedTasks.remove(stageId)
  }

  /**
   * Return true if this executor is blacklisted for the given stage.  Completely ignores whether
   * the executor is blacklisted overall (or anything to do with the node the executor is on).  That
   * is to keep this method as fast as possible in the inner-loop of the scheduler, where those
   * filters will already have been applied.
   */
  def isExecutorBlacklistedForStage(
      stageId: Int,
      executorId: String): Boolean = {
    stageIdToExecToFailures.get(stageId).flatMap(_.get(executorId))
      .map(_.totalFailures >= MAX_FAILURES_PER_EXEC_STAGE).getOrElse(false)
  }

  def isExecutorBlacklisted(executorId: String): Boolean = {
    executorIdToBlacklistExpiryTime.contains(executorId)
  }

  def isNodeBlacklistedForStage(node: String, stageId: Int): Boolean = {
    stageIdToBlacklistedNodes.get(stageId).map(_.contains(node)).getOrElse(false)
  }

  /**
   * Get the full set of nodes that are blacklisted.  Unlike other methods in this class, this *IS*
   * thread-safe -- no lock required on a taskScheduler.
   */
  def nodeBlacklist(): Set[String] = {
    _nodeBlacklist.get()
  }

  def isNodeBlacklisted(node: String): Boolean = {
    nodeIdToBlacklistExpiryTime.contains(node)
  }

  def taskSucceeded(
      stageId: Int,
      indexInTaskSet: Int,
      info: TaskInfo,
      scheduler: TaskSchedulerImpl): Unit = {
    // no-op intentionally, included just for symmetry.  success to failure ratio is irrelevant, we
    // just blacklist based on failures.  Furthermore, one success does not clear previous
    // failures, since the bad node / executor may not fail *every* time
  }

  def taskFailed(
      stageId: Int,
      indexInTaskSet: Int,
      info: TaskInfo,
      scheduler: TaskSchedulerImpl): Unit = {
    val stageFailures = stageIdToExecToFailures.getOrElseUpdate(stageId, new HashMap())
    val failureStatus = stageFailures.getOrElseUpdate(info.executorId, new FailureStatus())
    failureStatus.totalFailures += 1
    failureStatus.tasksWithFailures += indexInTaskSet

    // check if this task has also failed on other executors on the same host, and if so, blacklist
    // this task from the host
    val failuresOnHost = (for {
      exec <- scheduler.getExecutorsAliveOnHost(info.host).getOrElse(Set()).toSeq
      failures <- stageFailures.get(exec)
    } yield {
      if (failures.tasksWithFailures.contains(indexInTaskSet)) 1 else 0
    }).sum
    if (failuresOnHost >= MAX_TASK_FAILURES_PER_NODE) {
      stageIdToNodeBlacklistedTasks.getOrElseUpdate(stageId, new HashMap())
        .getOrElseUpdate(info.host, new HashSet()) += indexInTaskSet
    }


    if (failureStatus.totalFailures >= MAX_FAILURES_PER_EXEC_STAGE) {
      // This executor has been pushed into the blacklist for this stage.  Let's check if it pushes
      // the whole node into the blacklist
      val blacklistedExecutors =
        stageFailures.filter(_._2.totalFailures >= MAX_FAILURES_PER_EXEC_STAGE)
      if (blacklistedExecutors.size >= MAX_FAILED_EXEC_PER_NODE_STAGE) {
        logInfo(s"Blacklisting ${info.host} for stage $stageId")
        stageIdToBlacklistedNodes.getOrElseUpdate(stageId, new HashSet()) += info.host
      }
    }
  }

  /**
   * Return true if this executor is blacklisted for the given task.  This does *not*
   * need to return true if the executor is blacklisted for the entire stage, or blacklisted
   * altogether.  That is to keep this method as fast as possible in the inner-loop of the
   * scheduler, where those filters will have already been applied.
   */
  def isExecutorBlacklistedForTask(
      executorId: String,
      stageId: Int,
      indexInTaskSet: Int): Boolean = {
    stageIdToExecToFailures.get(stageId)
      .flatMap(_.get(executorId))
      .map(_.tasksWithFailures.contains(indexInTaskSet))
      .getOrElse(false)
  }

  def isNodeBlacklistedForTask(
      node: String,
      stageId: Int,
      indexInTaskSet: Int): Boolean = {
    val n = stageIdToNodeBlacklistedTasks.get(stageId)
      .flatMap(_.get(node))
      .map(_.contains(indexInTaskSet))
      .getOrElse(false)
    if (n) {
      logInfo(s"blacklisting $stageId, $indexInTaskSet on node $node")
    }
    n
  }

  def removeExecutor(executorId: String): Unit = {
    executorIdToBlacklistExpiryTime -= executorId
    executorIdToFailureCount -= executorId
    stageIdToExecToFailures.values.foreach { execFailureOneStage =>
      execFailureOneStage -= executorId
    }
  }
}


private[scheduler] object BlacklistTracker extends Logging {

  /**
   * Return true if the blacklist is enabled, based on the following order of preferences:
   * 1. Is it specifically enabled or disabled?
   * 2. Is it enabled via the legacy timeout conf?
   * 3. Use the default for the spark-master:
   *   - off for local mode
   *   - on for distributed modes (including local-cluster)
   */
  def isBlacklistEnabled(conf: SparkConf): Boolean = {
    conf.get(config.BLACKLIST_ENABLED) match {
      case Some(isEnabled) =>
        isEnabled
      case None =>
      // if they've got a non-zero setting for the legacy conf, always enable the blacklist,
      // otherwise, use the default based on the cluster-mode (off for local-mode, on otherwise).
        val legacyKey = config.BLACKLIST_LEGACY_TIMEOUT_CONF.key
      conf.getOption(legacyKey) match {
        case Some(legacyTimeout) =>
          if (legacyTimeout.toLong > 0) {
            // mostly this is necessary just for tests, since real users that want the blacklist
            // will get it anyway by default
            logWarning(s"Turning on blacklisting due to legacy configuration:" +
              s" $legacyKey > 0")
            true
          } else {
            logWarning(s"Turning off blacklisting due to legacy configuaration:" +
              s" $legacyKey == 0")
            false
          }
        case None =>
          // local-cluster is *not* considered local for these purposes, we still want the blacklist
          // enabled by default
          !Utils.isLocalMaster(conf)
      }
    }
  }

  def getBlacklistExpiryTime(conf: SparkConf): Long = {
    val timeoutConf = conf.get(config.BLACKLIST_EXPIRY_TIMEOUT_CONF)
    val legacyTimeoutConf = conf.get(config.BLACKLIST_LEGACY_TIMEOUT_CONF)
    (timeoutConf, legacyTimeoutConf) match {
      case (Some(x), _) => x
      case (None, Some(y)) => y
      case (None, None) =>
        Utils.timeStringAsMs("1h")
    }
  }
}

/** Failures for one executor, within one taskset */
private[scheduler] final class FailureStatus {
  /** index of the tasks in the taskset that have failed on this executor. */
  val tasksWithFailures = HashSet[Int]()
  var totalFailures = 0

  override def toString(): String = {
    s"totalFailures = $totalFailures; tasksFailed = $tasksWithFailures"
  }
}
