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
import org.apache.spark.util.{Clock, SystemClock, Utils}

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

  private val MAX_FAILURES_PER_EXEC = conf.get(config.MAX_FAILURES_PER_EXEC)
  private val MAX_FAILED_EXEC_PER_NODE = conf.get(config.MAX_FAILED_EXEC_PER_NODE)
  val EXECUTOR_RECOVERY_MILLIS = BlacklistTracker.getBlacklistExpiryTime(conf)

  // a count of failed tasks for each executor.  Only counts failures after tasksets complete
  // successfully
  private val executorIdToFailureCount: HashMap[String, Int] = new HashMap()
  private val executorIdToBlacklistStatus: HashMap[String, BlacklistedExecutor] = new HashMap()
  private val nodeIdToBlacklistExpiryTime: HashMap[String, Long] = new HashMap()
  private val _nodeBlacklist: AtomicReference[Set[String]] = new AtomicReference(Set())
  private var nextExpiryTime: Long = Long.MaxValue
  // for blacklisted executors, the node it is on.  We do *not* remove from this when executors are
  // removed from spark, so we can track when we get multiple successive blacklisted executors on
  // one node.
  val nodeToFailedExecs: HashMap[String, HashSet[String]] = new HashMap()

  def expireExecutorsInBlacklist(): Unit = {
    val now = clock.getTimeMillis()
    // quickly check if we've got anything to expire from blacklist -- if not, avoid doing any work
    if (now > nextExpiryTime) {
      val execsToClear = executorIdToBlacklistStatus.filter(_._2.expiryTime < now).keys
      if (execsToClear.nonEmpty) {
        logInfo(s"Removing executors $execsToClear from blacklist during periodic recovery")
        execsToClear.foreach { exec =>
          val status = executorIdToBlacklistStatus.remove(exec).get
          val failedExecsOnNode = nodeToFailedExecs(status.node)
          failedExecsOnNode.remove(exec)
          if (failedExecsOnNode.isEmpty) {
            nodeToFailedExecs.remove(status.node)
          }
        }
      }
      if (executorIdToBlacklistStatus.nonEmpty) {
        nextExpiryTime = executorIdToBlacklistStatus.map{_._2.expiryTime}.min
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

  def taskSetSucceeded(failuresByExec: HashMap[String, FailureStatus]): Unit = {
    // if any tasks failed, we count them towards the overall failure count for the executor at
    // this point.
    failuresByExec.foreach { case (exec, newFailures) =>
      val prevFailures = executorIdToFailureCount.getOrElse(exec, 0)
      val newTotal = prevFailures + newFailures.totalFailures
      val node = newFailures.node

      if (newTotal >= MAX_FAILURES_PER_EXEC) {
        logInfo(s"Blacklisting executor id: $exec because it has $newTotal" +
          s" task failures in successful task sets")
        val now = clock.getTimeMillis()
        val expiryTime = now + EXECUTOR_RECOVERY_MILLIS
        executorIdToBlacklistStatus.put(exec, BlacklistedExecutor(node, expiryTime))
        val blacklistedExecsOnNode = nodeToFailedExecs.getOrElseUpdate(node, HashSet[String]())
        blacklistedExecsOnNode += exec
        executorIdToFailureCount.remove(exec)
        if (expiryTime < nextExpiryTime) {
          nextExpiryTime = expiryTime
        }

        if (blacklistedExecsOnNode.size >= MAX_FAILED_EXEC_PER_NODE) {
          logInfo(s"Blacklisting node $node because it has ${blacklistedExecsOnNode.size} " +
            s"executors blacklisted: ${blacklistedExecsOnNode}")
          nodeIdToBlacklistExpiryTime.put(node, expiryTime)
          // make a copy of the blacklisted nodes so nodeBlacklist() is threadsafe
          _nodeBlacklist.set(nodeIdToBlacklistExpiryTime.keySet.toSet)
        }
      } else {
        executorIdToFailureCount.put(exec, newTotal)
      }
    }
  }

  def isExecutorBlacklisted(executorId: String): Boolean = {
    executorIdToBlacklistStatus.contains(executorId)
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

  def removeExecutor(executorId: String): Unit = {
    // we intentionally do not clean up executors that are already blacklisted, so that if another
    // executor on the same node gets blacklisted, we can blacklist the entire node.
    executorIdToFailureCount -= executorId
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
        conf.get(config.BLACKLIST_LEGACY_TIMEOUT_CONF) match {
          case Some(legacyTimeout) if legacyTimeout == 0 =>
            logWarning(s"Turning off blacklisting due to legacy configuaration:" +
              s" $legacyKey == 0")
            false
          case Some(legacyTimeout) =>
              // mostly this is necessary just for tests, since real users that want the blacklist
              // will get it anyway by default
              logWarning(s"Turning on blacklisting due to legacy configuration:" +
                s" $legacyKey > 0")
              true
          case None =>
            // local-cluster is *not* considered local for these purposes, we still want the
            // blacklist enabled by default
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
private[scheduler] final class FailureStatus(val node: String) {
  /** index of the tasks in the taskset that have failed on this executor. */
  val tasksWithFailures = HashSet[Int]()
  var totalFailures = 0

  override def toString(): String = {
    s"totalFailures = $totalFailures; tasksFailed = $tasksWithFailures"
  }
}

private final case class BlacklistedExecutor(node: String, expiryTime: Long)
